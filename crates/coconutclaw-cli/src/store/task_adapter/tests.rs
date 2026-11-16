use super::*;
mod driver;
mod roundtrip;
mod runtime_bridge;
use coconutclaw_sync::synthetic::membership::domain::Origin;
use coconutclaw_sync::synthetic::membership::{Member, Policy, SignedPolicy};
use iroh_docs::{Capability, NamespaceSecret};
use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const NOW: &str = "2026-11-16T00:00:00Z";

fn key() -> SecretKey {
    // Unique artificial fixture identities even across simultaneous test cases.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_le_bytes(),
    );
    bytes[16..24].copy_from_slice(
        &NEXT
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_le_bytes(),
    );
    SecretKey::from(bytes)
}

struct Network {
    root: tempfile::TempDir,
    origin: BoundOrigin,
    owner: SecretKey,
    core: SecretKey,
    runner: SecretKey,
    ka: SecretKey,
    kb: SecretKey,
    ns: NamespaceSecret,
    policy: SignedPolicy,
    a: ScopedPeer,
    b: ScopedPeer,
}

impl Network {
    async fn new() -> Self {
        Self::with_runner(None).await
    }

    async fn with_runner(runner_id: Option<EndpointId>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let (owner, core, runner, ka, kb) = (key(), key(), key(), key(), key());
        let ns = NamespaceSecret::from_bytes(&key().to_bytes());
        let origin = BoundOrigin::sign(
            Origin {
                owner: owner.public(),
                store: "ab".repeat(32),
                namespace: ns.id(),
                scope: "synthetic/chat-a".into(),
                core: core.public(),
                runners: BTreeSet::from([runner_id.unwrap_or_else(|| runner.public())]),
            },
            &owner,
        )
        .unwrap();
        let mut a = ScopedPeer::create(
            &root.path().join("a"),
            ka.clone(),
            owner.public(),
            Capability::Write(ns.clone()),
        )
        .await
        .unwrap();
        let mut b = ScopedPeer::create(
            &root.path().join("b"),
            kb.clone(),
            owner.public(),
            Capability::Write(ns.clone()),
        )
        .await
        .unwrap();
        let author = a.author().await.unwrap();
        let runner_author = b.author().await.unwrap();
        let policy = SignedPolicy::sign(
            Policy {
                generation: 1,
                expires_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
                namespace: ns.id(),
                scope: origin.origin.scope.clone(),
                members: vec![
                    Member {
                        endpoint: ka.public(),
                        authors: BTreeSet::from([author]),
                    },
                    Member {
                        endpoint: kb.public(),
                        authors: BTreeSet::from([runner_author]),
                    },
                ],
            },
            &owner,
        )
        .unwrap();
        a.install_policy(&policy).unwrap();
        b.install_policy(&policy).unwrap();
        Self {
            root,
            origin,
            owner,
            core,
            runner,
            ka,
            kb,
            ns,
            policy,
            a,
            b,
        }
    }

    fn config(&self, name: &str) -> RuntimeConfig {
        RuntimeConfig::test_config_at(self.root.path().join(name))
    }

    async fn close(self) {
        self.a.shutdown().await.unwrap();
        self.b.shutdown().await.unwrap();
    }
}

async fn receive(
    a: &ScopedPeer,
    b: &mut ScopedPeer,
    origin: &BoundOrigin,
    record: &Record,
) -> Result<bool> {
    b.receive_domain(origin, a.addr(), record.origin.parse()?, &record.key())
        .await
}

fn params(prompt: &str, session: &SessionKey, update: Option<String>) -> InsertTaskRunParams {
    InsertTaskRunParams {
        session_id: session.id(),
        channel: "local".into(),
        source_chat_id: None,
        source_user_id: None,
        update_id: update,
        prompt: prompt.into(),
        created_at: NOW.into(),
        progress_message_id: None,
        scheduled_task_id: None,
    }
}

fn enqueue(store: &Store, session: &SessionKey, index: u32) -> i64 {
    enqueue_text(store, session, index, "artificial delegated request")
}
pub(in crate::store::task_adapter) fn enqueue_text(
    store: &Store,
    session: &SessionKey,
    index: u32,
    prompt: &str,
) -> i64 {
    let parent = match store.get_task_run(1).unwrap() {
        Some(_) => 1,
        None => {
            let id = store
                .insert_task_run(params("artificial parent", session, None))
                .unwrap();
            store
                .finish_task_run(id, TaskRunStatus::Completed, NOW, None, Some("delegated"))
                .unwrap();
            id
        }
    };
    let request = TaskRequest {
        session: session.clone(),
        source: TaskSource::Local,
        input: crate::TurnInput {
            input_type: crate::InputType::Text,
            user_text: prompt.into(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "local".into(),
        },
        update_id: Some(format!("delegation:{parent}:{index}")),
        media: None,
        quoted: crate::QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        delivery: DeliveryTarget::Stdout,
        source_user_id: None,
        progress_message_id: None,
        delegated: true,
    };
    let tx = store.conn.unchecked_transaction().unwrap();
    let id = store
        .insert_task_run(params(
            &request.input.user_text,
            session,
            request.update_id.clone(),
        ))
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
            params![id, build_resume_payload(&request, &request.input).unwrap()],
        )
        .unwrap();
    tx.commit().unwrap();
    id
}

fn count(store: &Store) -> i64 {
    store
        .conn
        .query_row("SELECT count(*) FROM task_runs", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn task_adapter_real_store_transport_restart_and_duplicates_stay_held() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let mut n = Network::new().await;
        let session = SessionKey::local("delegated-fixture");
        let source_cfg = n.config("source");
        let target_cfg = n.config("target");
        let source = Store::open(&source_cfg).unwrap();
        let target = Store::open(&target_cfg).unwrap();
        let id = enqueue(&source, &session, 0);
        let export = TaskAdapter::open(&source, &n.origin, &session).unwrap();
        let record = export
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .unwrap();
        assert_eq!(
            source.get_task_run(id).unwrap().unwrap().status,
            "waiting_remote"
        );
        assert!(receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap());
        assert!(!receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap());
        let incoming = TaskAdapter::open(&target, &n.origin, &session).unwrap();
        let received = incoming.accept(&mut n.b, 1, n.runner.public()).unwrap();
        assert_eq!(
            incoming.accept(&mut n.b, 1, n.runner.public()).unwrap(),
            received
        );
        assert_ne!(received, id); // Source local row 2, domain assignment 1, recipient row 1.
        assert_eq!(count(&target), 1);
        assert_eq!(
            target.get_task_run(received).unwrap().unwrap().prompt,
            "artificial delegated request"
        );
        assert!(
            target
                .get_task_run(received)
                .unwrap()
                .unwrap()
                .session_id
                .contains(&n.origin.hash().unwrap())
        );
        target.mark_stale_task_runs_failed(NOW).unwrap();
        assert_eq!(
            target.get_task_run(received).unwrap().unwrap().status,
            "waiting_remote"
        );
        assert!(
            n.b.domain_history(&n.origin)
                .unwrap()
                .validate_replacement_activation()
                .is_err()
        );
        drop((source, target));
        n.b.shutdown().await.unwrap();
        // Reuse the endpoint identity only after awaited shutdown, with a fresh
        // client/holder. Persisted peer history starts quarantined.
        n.b = ScopedPeer::reopen(
            &n.root.path().join("b"),
            n.kb.clone(),
            n.origin.origin.owner,
            Capability::Read(n.ns.id()),
        )
        .await
        .unwrap();
        let source = Store::open(&source_cfg).unwrap();
        let target = Store::open(&target_cfg).unwrap();
        let incoming = TaskAdapter::open(&target, &n.origin, &session).unwrap();
        assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
        n.b.install_policy(&n.policy).unwrap();
        assert_eq!(
            incoming.accept(&mut n.b, 1, n.runner.public()).unwrap(),
            received
        );
        assert!(!receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap());
        let export = TaskAdapter::open(&source, &n.origin, &session).unwrap();
        let replay = export
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .unwrap();
        receive(&n.a, &mut n.b, &n.origin, &replay).await.unwrap();
        assert_eq!(
            incoming.accept(&mut n.b, 1, n.runner.public()).unwrap(),
            received
        );
        assert_eq!(count(&target), 1);
        assert_eq!(n.b.domain_history(&n.origin).unwrap().jobs.len(), 1);
        assert!(
            n.b.domain_history(&n.origin).unwrap().jobs[&1]
                .result
                .is_none()
        );
        let history = n.a.domain_history(&n.origin).unwrap();
        let original = SignedEvent::parse(&history.export()[0]).unwrap();
        let Action::Assign { request, .. } = original.event.action else {
            unreachable!()
        };
        let duplicate_source = SignedEvent::sign(
            history
                .next(
                    &n.origin,
                    n.core.public(),
                    Action::Assign {
                        job: 2,
                        attempt: 1,
                        runner: n.runner.public(),
                        request,
                    },
                )
                .unwrap(),
            &n.core,
        )
        .unwrap();
        let record =
            n.a.publish_domain(&n.origin, &duplicate_source.bytes().unwrap())
                .await
                .unwrap();
        receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap();
        assert!(incoming.accept(&mut n.b, 2, n.runner.public()).is_err());
        assert_eq!(count(&target), 1); // New event ID cannot duplicate the same source task.
        n.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_adapter_rejects_nonqueued_unknown_cancelled_and_malformed_store_requests() {
    let mut n = Network::new().await;
    let cfg = n.config("source");
    let store = Store::open(&cfg).unwrap();
    let session = SessionKey::local("delegated-fixture");
    let adapter = TaskAdapter::open(&store, &n.origin, &session).unwrap();
    for case in 0..10 {
        let id = enqueue(&store, &session, case);
        match case {
            0 => {
                store.update_task_run_started(id, NOW).unwrap();
                store.mark_stale_task_runs_failed(NOW).unwrap();
            }
            1 => store.mark_task_run_cancel_requested(id, NOW).unwrap(),
            2 => store
                .finish_task_run(id, TaskRunStatus::Cancelled, NOW, None, None)
                .unwrap(),
            3 => {
                store
                    .conn
                    .execute("DELETE FROM task_payloads WHERE task_run_id=?1", [id])
                    .unwrap();
            }
            4 => {
                store
                    .conn
                    .execute(
                        "UPDATE task_payloads SET request='{}' WHERE task_run_id=?1",
                        [id],
                    )
                    .unwrap();
            }
            5 => {
                store
                    .conn
                    .execute(
                        "UPDATE task_runs SET prompt='substituted' WHERE id=?1",
                        [id],
                    )
                    .unwrap();
            }
            6 => {
                store
                    .conn
                    .execute(
                        "UPDATE task_runs SET session_id='local:other-scope' WHERE id=?1",
                        [id],
                    )
                    .unwrap();
            }
            7 => {
                store
                    .conn
                    .execute(
                        "UPDATE task_payloads SET output='uncertain output' WHERE task_run_id=?1",
                        [id],
                    )
                    .unwrap();
            }
            8 => {
                store
                    .conn
                    .execute(
                        "UPDATE task_runs SET update_id='delegation:01:0' WHERE id=?1",
                        [id],
                    )
                    .unwrap();
            }
            _ => {
                let raw: String = store
                    .conn
                    .query_row(
                        "SELECT request FROM task_payloads WHERE task_run_id=?1",
                        [id],
                        |r| r.get(0),
                    )
                    .unwrap();
                let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
                value["attachment_path"] = serde_json::json!("/not-a-real-file");
                store
                    .conn
                    .execute(
                        "UPDATE task_payloads SET request=?2 WHERE task_run_id=?1",
                        params![id, value.to_string()],
                    )
                    .unwrap();
            }
        }
        let before = store.get_task_run(id).unwrap().unwrap().status;
        assert!(
            adapter
                .export(&mut n.a, id, &n.core, n.runner.public())
                .await
                .is_err(),
            "case {case}"
        );
        assert_eq!(store.get_task_run(id).unwrap().unwrap().status, before);
    }
    assert!(n.a.domain_history(&n.origin).unwrap().jobs.is_empty());
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM adapter_outgoing", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    n.close().await;
}

#[tokio::test]
async fn task_adapter_failed_publication_replays_exact_reservation_after_restart() {
    let mut n = Network::new().await;
    let cfg = n.config("source");
    let session = SessionKey::local("delegated-fixture");
    let source = Store::open(&cfg).unwrap();
    let id = enqueue(&source, &session, 0);
    let adapter = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let projection = n.root.path().join("a/projection.sqlite");
    let connection = Connection::open(&projection).unwrap();
    connection.execute_batch("CREATE TRIGGER abort_adapter_publication BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected publication failure'); END;").unwrap();
    assert!(
        adapter
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .is_err()
    );
    assert_eq!(
        source.get_task_run(id).unwrap().unwrap().status,
        "waiting_remote"
    );
    let original: Vec<u8> = source
        .conn
        .query_row(
            "SELECT event FROM adapter_outgoing WHERE run=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    let second = enqueue(&source, &session, 1);
    assert!(
        adapter
            .export(&mut n.a, second, &n.core, n.runner.public())
            .await
            .is_err()
    );
    assert_eq!(
        source.get_task_run(second).unwrap().unwrap().status,
        "queued"
    );
    drop(source);
    n.a.shutdown().await.unwrap();
    connection
        .execute_batch("DROP TRIGGER abort_adapter_publication")
        .unwrap();
    drop(connection);
    n.a = ScopedPeer::reopen(
        &n.root.path().join("a"),
        n.ka.clone(),
        n.origin.origin.owner,
        Capability::Write(n.ns.clone()),
    )
    .await
    .unwrap();
    n.a.install_policy(&n.policy).unwrap();
    let source = Store::open(&cfg).unwrap();
    source.mark_stale_task_runs_failed(NOW).unwrap();
    let adapter = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let record = adapter
        .export(&mut n.a, id, &n.core, n.runner.public())
        .await
        .unwrap();
    assert_eq!(
        n.a.domain_history(&n.origin).unwrap().export(),
        vec![original]
    );
    assert!(receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap());
    let recipient = Store::open(&n.config("target")).unwrap();
    let incoming = TaskAdapter::open(&recipient, &n.origin, &session).unwrap();
    incoming.accept(&mut n.b, 1, n.runner.public()).unwrap();
    assert_eq!(count(&recipient), 1);
    n.close().await;
}

#[tokio::test]
async fn task_adapter_inbox_transaction_conflict_and_cancellation_never_requeue() {
    let mut n = Network::new().await;
    let session = SessionKey::local("delegated-fixture");
    let source = Store::open(&n.config("source")).unwrap();
    let recipient = Store::open(&n.config("target")).unwrap();
    let id = enqueue(&source, &session, 0);
    let adapter = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let record = adapter
        .export(&mut n.a, id, &n.core, n.runner.public())
        .await
        .unwrap();
    receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap();
    let incoming = TaskAdapter::open(&recipient, &n.origin, &session).unwrap();
    recipient.conn.execute_batch("CREATE TRIGGER abort_adapter_inbox BEFORE INSERT ON adapter_incoming BEGIN SELECT RAISE(ABORT,'injected inbox failure'); END;").unwrap();
    assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
    assert_eq!(count(&recipient), 0);
    recipient
        .conn
        .execute_batch("DROP TRIGGER abort_adapter_inbox")
        .unwrap();
    assert!(incoming.accept(&mut n.b, 1, n.core.public()).is_err());
    let local = incoming.accept(&mut n.b, 1, n.runner.public()).unwrap();
    recipient
        .conn
        .execute(
            "UPDATE task_payloads SET delivered=1 WHERE task_run_id=?1",
            [local],
        )
        .unwrap();
    assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
    recipient
        .conn
        .execute(
            "UPDATE task_payloads SET delivered=0 WHERE task_run_id=?1",
            [local],
        )
        .unwrap();
    recipient
        .conn
        .execute(
            "UPDATE task_payloads SET request='conflicting local payload' WHERE task_run_id=?1",
            [local],
        )
        .unwrap();
    assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
    assert_eq!(count(&recipient), 1);
    recipient
        .mark_task_run_cancel_requested(local, NOW)
        .unwrap();
    source.mark_task_run_cancel_requested(id, NOW).unwrap();
    assert!(
        adapter
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .is_err()
    );
    assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
    recipient.mark_stale_task_runs_failed(NOW).unwrap();
    assert!(
        recipient
            .get_task_run(local)
            .unwrap()
            .unwrap()
            .error_summary
            .unwrap()
            .contains("outcome unknown")
    );
    assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
    assert_eq!(
        recipient.get_task_run(local).unwrap().unwrap().status,
        "failed"
    );
    assert_eq!(n.b.domain_history(&n.origin).unwrap().jobs.len(), 1);
    n.close().await;
}

#[tokio::test]
async fn task_adapter_hostile_payload_attempt_scope_conflicts_remain_unavailable_after_restart() {
    for case in ["payload", "attempt", "scope"] {
        let mut n = Network::new().await;
        let source = Store::open(&n.config("source")).unwrap();
        let recipient = Store::open(&n.config("target")).unwrap();
        let session = SessionKey::local("delegated-fixture");
        let id = enqueue(&source, &session, 0);
        let adapter = TaskAdapter::open(&source, &n.origin, &session).unwrap();
        let record = adapter
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .unwrap();
        receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap();
        let incoming = TaskAdapter::open(&recipient, &n.origin, &session).unwrap();
        incoming.accept(&mut n.b, 1, n.runner.public()).unwrap();
        let mut event = SignedEvent::parse(&n.a.domain_history(&n.origin).unwrap().export()[0])
            .unwrap()
            .event;
        match case {
            "payload" => {
                if let Action::Assign { request, .. } = &mut event.action {
                    *request = "changed payload".into();
                }
            }
            "attempt" => {
                if let Action::Assign { attempt, .. } = &mut event.action {
                    *attempt = 2;
                }
            }
            _ => event.origin = "foreign scope/origin".into(),
        }
        let bytes = SignedEvent::sign(event, &n.core).unwrap().bytes().unwrap();
        assert!(n.a.publish_domain(&n.origin, &bytes).await.is_err());
        let hostile =
            n.a.publish("CoconutClaw synthetic domain event v1", &bytes)
                .await
                .unwrap();
        assert!(receive(&n.a, &mut n.b, &n.origin, &hostile).await.is_err());
        assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
        assert_eq!(count(&recipient), 1);
        assert_eq!(
            recipient.get_task_run(1).unwrap().unwrap().status,
            "waiting_remote"
        );
        n.b.shutdown().await.unwrap();
        n.b = ScopedPeer::reopen(
            &n.root.path().join("b"),
            n.kb.clone(),
            n.origin.origin.owner,
            Capability::Read(n.ns.id()),
        )
        .await
        .unwrap();
        n.b.install_policy(&n.policy).unwrap();
        assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
        assert_eq!(count(&recipient), 1);
        n.close().await;
    }
}

#[tokio::test]
async fn task_adapter_lost_export_mapping_never_reassigns_an_existing_source_run() {
    let mut n = Network::new().await;
    let source = Store::open(&n.config("source")).unwrap();
    let session = SessionKey::local("delegated-fixture");
    let id = enqueue(&source, &session, 0);
    let adapter = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    adapter
        .export(&mut n.a, id, &n.core, n.runner.public())
        .await
        .unwrap();
    assert!(
        adapter
            .export(&mut n.a, id, &n.core, key().public())
            .await
            .is_err()
    );
    let payload: String = source
        .conn
        .query_row(
            "SELECT request FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    let mut changed = request_from_payload(&payload, false).unwrap();
    changed.input.user_text = "conflicting replacement request".into();
    source
        .conn
        .execute(
            "UPDATE task_runs SET prompt=?2 WHERE id=?1",
            params![id, changed.input.user_text],
        )
        .unwrap();
    source
        .conn
        .execute(
            "UPDATE task_payloads SET request=?2 WHERE task_run_id=?1",
            params![id, build_resume_payload(&changed, &changed.input).unwrap()],
        )
        .unwrap();
    assert!(
        adapter
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .is_err()
    );
    source
        .conn
        .execute(
            "UPDATE task_runs SET prompt='artificial delegated request' WHERE id=?1",
            [id],
        )
        .unwrap();
    source
        .conn
        .execute(
            "UPDATE task_payloads SET request=?2 WHERE task_run_id=?1",
            params![id, payload],
        )
        .unwrap();
    let before = n.a.domain_history(&n.origin).unwrap().export();
    // A rollback/lost adapter table must not reinterpret old work as a new job.
    source
        .conn
        .execute("DELETE FROM adapter_outgoing WHERE run=?1", [id])
        .unwrap();
    source
        .conn
        .execute("UPDATE task_runs SET status='queued' WHERE id=?1", [id])
        .unwrap();
    assert!(
        adapter
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .is_err()
    );
    assert_eq!(n.a.domain_history(&n.origin).unwrap().export(), before);
    assert_eq!(source.get_task_run(id).unwrap().unwrap().status, "queued");
    n.close().await;
}

#[tokio::test]
async fn task_adapter_malformed_assignments_binding_and_suspension_refuse() {
    let mut n = Network::new().await;
    let session = SessionKey::local("delegated-fixture");
    let recipient = Store::open(&n.config("target")).unwrap();
    let incoming = TaskAdapter::open(&recipient, &n.origin, &session).unwrap();
    assert!(TaskAdapter::open(&recipient, &n.origin, &SessionKey::local("other")).is_err());
    for request in [
        "not-json",
        "{}",
        "{\"run\":2,\"parent\":1,\"index\":0,\"payload\":\"{}\",\"approved\":true}",
    ] {
        let history = n.a.domain_history(&n.origin).unwrap();
        let mut event = history
            .next(
                &n.origin,
                n.core.public(),
                Action::Assign {
                    job: 0,
                    attempt: 1,
                    runner: n.runner.public(),
                    request: request.into(),
                },
            )
            .unwrap();
        if let Action::Assign { job, .. } = &mut event.action {
            *job = event.sequence;
        }
        let assignment = event.sequence;
        let signed = SignedEvent::sign(event, &n.core).unwrap();
        let record =
            n.a.publish_domain(&n.origin, &signed.bytes().unwrap())
                .await
                .unwrap();
        receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap();
        assert!(
            incoming
                .accept(&mut n.b, assignment, n.runner.public())
                .is_err()
        );
        assert_eq!(count(&recipient), 0);
    }
    // Owner fixture suspension still prevents adapter admission. This does not
    // exercise or enable operational replacement activation.
    let mut changed = n.origin.clone();
    changed.origin.scope = "synthetic/other".into();
    assert!(TaskAdapter::open(&recipient, &changed, &session).is_err());
    let history = n.a.domain_history(&n.origin).unwrap();
    let fence = SignedEvent::sign(
        history
            .next(
                &n.origin,
                n.owner.public(),
                Action::Fence {
                    previous_epoch: 1,
                    epoch: 2,
                    core: key().public(),
                    runners: BTreeSet::new(),
                },
            )
            .unwrap(),
        &n.owner,
    )
    .unwrap();
    let record =
        n.a.publish_domain(&n.origin, &fence.bytes().unwrap())
            .await
            .unwrap();
    receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap();
    assert!(incoming.accept(&mut n.b, 1, n.runner.public()).is_err());
    let source = Store::open(&n.config("source")).unwrap();
    let id = enqueue(&source, &session, 0);
    let export = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    assert!(
        export
            .export(&mut n.a, id, &n.core, n.runner.public())
            .await
            .is_err()
    );
    assert_eq!(source.get_task_run(id).unwrap().unwrap().status, "queued");
    assert!(
        n.b.domain_history(&n.origin)
            .unwrap()
            .validate_replacement_activation()
            .is_err()
    );
    n.close().await;
}
