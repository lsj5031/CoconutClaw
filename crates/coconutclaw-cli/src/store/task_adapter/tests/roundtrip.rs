use super::*;
use coconutclaw_sync::synthetic::membership::domain::Outcome;
use std::cell::Cell;

async fn assigned(n: &mut Network) -> (Store, Store, SessionKey, i64) {
    let source = Store::open(&n.config("source")).unwrap();
    let target = Store::open(&n.config("target")).unwrap();
    let session = SessionKey::local("delegated-fixture");
    let id = enqueue(&source, &session, 0);
    let adapter = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let record = adapter
        .export(&mut n.a, id, &n.core, n.runner.public())
        .await
        .unwrap();
    receive(&n.a, &mut n.b, &n.origin, &record).await.unwrap();
    TaskAdapter::open(&target, &n.origin, &session)
        .unwrap()
        .accept(&mut n.b, 1, n.runner.public())
        .unwrap();
    (source, target, session, id)
}

pub(super) fn output(store: &Store, id: i64) -> Option<String> {
    store
        .conn
        .query_row(
            "SELECT output FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
}

pub(super) fn unknown(store: &Store, id: i64) {
    let run = store.get_task_run(id).unwrap().unwrap();
    assert_eq!(run.status, "failed");
    assert!(run.error_summary.unwrap().contains("outcome unknown"));
}

async fn return_records(n: &mut Network, start: &Record, result: &Record) {
    receive(&n.b, &mut n.a, &n.origin, start).await.unwrap();
    receive(&n.b, &mut n.a, &n.origin, result).await.unwrap();
}

#[tokio::test]
async fn mock_roundtrip_persists_start_before_execution_and_exact_result_through_restart() {
    let mut n = Network::new().await;
    let (source, target, session, id) = assigned(&mut n).await;
    let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
    let calls = Cell::new(0);
    let (dispatch, start) = b.start_mock(&mut n.b, 1, &n.runner).await.unwrap();
    receive(&n.b, &mut n.a, &n.origin, &start).await.unwrap();
    assert_eq!(a.apply_start(&mut n.a, 1).unwrap(), id);
    assert_eq!(source.get_task_run(id).unwrap().unwrap().status, "running");
    let target_cfg = n.config("target");
    let result = b
        .complete_mock(&mut n.b, &n.runner, dispatch, |store, run| {
            calls.set(calls.get() + 1);
            let persisted = Store::open(&target_cfg).unwrap();
            assert!(persisted.get_task_run(run)?.unwrap().started_at.is_some());
            assert_eq!(store.get_task_run(run)?.unwrap().status, "running");
            assert_eq!(
                store.conn.query_row(
                    "SELECT claimed FROM adapter_execution WHERE assignment=1",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                1
            );
            Ok((
                Outcome::Completed,
                "persisted artificial mock result".into(),
            ))
        })
        .await
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(target.get_task_run(1).unwrap().unwrap().status, "completed");
    receive(&n.b, &mut n.a, &n.origin, &result).await.unwrap();
    assert_eq!(a.apply_result(&mut n.a, 1).unwrap(), id);
    assert_eq!(a.apply_result(&mut n.a, 1).unwrap(), id);
    assert_eq!(
        source
            .conn
            .query_row(
                "SELECT COUNT(*) FROM runtime_inbox WHERE kind='result' AND task_run_id=?1",
                [id],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1,
        "verified remote result must enter the runtime inbox exactly once"
    );
    a.apply_start(&mut n.a, 1).unwrap(); // Late duplicate start cannot reset completion.
    assert_eq!(
        source.get_task_run(id).unwrap().unwrap().status,
        "completed"
    );
    assert_eq!(
        output(&source, id).as_deref(),
        Some("persisted artificial mock result")
    );
    let before = n.a.domain_history(&n.origin).unwrap().export();
    drop((source, target));
    n.a.shutdown().await.unwrap();
    n.b.shutdown().await.unwrap();
    n.a = ScopedPeer::reopen(
        &n.root.path().join("a"),
        n.ka.clone(),
        n.origin.origin.owner,
        Capability::Write(n.ns.clone()),
    )
    .await
    .unwrap();
    n.b = ScopedPeer::reopen(
        &n.root.path().join("b"),
        n.kb.clone(),
        n.origin.origin.owner,
        Capability::Write(n.ns.clone()),
    )
    .await
    .unwrap();
    let source = Store::open(&n.config("source")).unwrap();
    let target = Store::open(&n.config("target")).unwrap();
    let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
    assert!(b.recover_result(&mut n.b, 1, &n.runner).await.is_err()); // Quarantine.
    n.a.install_policy(&n.policy).unwrap();
    n.b.install_policy(&n.policy).unwrap();
    let replay = b.recover_result(&mut n.b, 1, &n.runner).await.unwrap();
    receive(&n.b, &mut n.a, &n.origin, &replay).await.unwrap();
    a.apply_result(&mut n.a, 1).unwrap();
    assert_eq!(n.a.domain_history(&n.origin).unwrap().export(), before);
    assert_eq!(calls.get(), 1);
    assert!(b.start_mock(&mut n.b, 1, &n.runner).await.is_err());
    assert!(
        n.a.domain_history(&n.origin)
            .unwrap()
            .validate_replacement_activation()
            .is_err()
    );
    n.close().await;
}

#[tokio::test]
async fn mock_roundtrip_restart_after_start_and_failed_result_persistence_never_reexecutes() {
    for crash in ["after-start", "result-commit", "bad-output"] {
        let mut n = Network::new().await;
        let (source, target, session, id) = assigned(&mut n).await;
        let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
        let (dispatch, start) = b.start_mock(&mut n.b, 1, &n.runner).await.unwrap();
        let calls = Cell::new(0);
        if crash == "after-start" {
            drop(dispatch);
        } else {
            if crash == "result-commit" {
                target.conn.execute_batch("CREATE TRIGGER abort_mock_result BEFORE UPDATE OF result ON adapter_execution BEGIN SELECT RAISE(ABORT,'injected result commit failure'); END;").unwrap();
            }
            assert!(
                b.complete_mock(&mut n.b, &n.runner, dispatch, |_, _| {
                    calls.set(calls.get() + 1);
                    Ok((
                        Outcome::Completed,
                        if crash == "bad-output" {
                            "x".repeat(2049)
                        } else {
                            "lost mock result".into()
                        },
                    ))
                })
                .await
                .is_err()
            );
            if crash == "result-commit" {
                target
                    .conn
                    .execute_batch("DROP TRIGGER abort_mock_result")
                    .unwrap();
            }
        }
        let expected_calls = calls.get();
        drop(target);
        n.b.shutdown().await.unwrap();
        n.b = ScopedPeer::reopen(
            &n.root.path().join("b"),
            n.kb.clone(),
            n.origin.origin.owner,
            Capability::Write(n.ns.clone()),
        )
        .await
        .unwrap();
        n.b.install_policy(&n.policy).unwrap();
        let target = Store::open(&n.config("target")).unwrap();
        target.mark_stale_task_runs_failed(NOW).unwrap();
        let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
        assert!(b.start_mock(&mut n.b, 1, &n.runner).await.is_err());
        let result = b.recover_result(&mut n.b, 1, &n.runner).await.unwrap();
        return_records(&mut n, &start, &result).await;
        let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
        a.apply_result(&mut n.a, 1).unwrap();
        unknown(&source, id);
        unknown(&target, 1);
        assert_eq!(calls.get(), expected_calls);
        assert_eq!(
            n.a.domain_history(&n.origin).unwrap().jobs[&1]
                .result
                .as_ref()
                .unwrap()
                .0,
            Outcome::Unknown
        );
        n.close().await;
    }
}

#[tokio::test]
async fn mock_roundtrip_start_publication_failure_is_durable_unknown_not_dispatch_retry() {
    let mut n = Network::new().await;
    let (source, target, session, id) = assigned(&mut n).await;
    let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
    let sql = Connection::open(n.root.path().join("b/projection.sqlite")).unwrap();
    sql.execute_batch("CREATE TRIGGER abort_start BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected start publication failure'); END;").unwrap();
    assert!(b.start_mock(&mut n.b, 1, &n.runner).await.is_err());
    assert!(
        target
            .get_task_run(1)
            .unwrap()
            .unwrap()
            .started_at
            .is_some()
    );
    let bytes: Vec<u8> = target
        .conn
        .query_row(
            "SELECT start FROM adapter_execution WHERE assignment=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    sql.execute_batch("DROP TRIGGER abort_start").unwrap();
    let result = b.recover_result(&mut n.b, 1, &n.runner).await.unwrap();
    let start = n.b.publish_domain(&n.origin, &bytes).await.unwrap();
    return_records(&mut n, &start, &result).await;
    TaskAdapter::open(&source, &n.origin, &session)
        .unwrap()
        .apply_result(&mut n.a, 1)
        .unwrap();
    unknown(&source, id);
    assert!(
        TaskAdapter::open(&target, &n.origin, &session)
            .unwrap()
            .start_mock(&mut n.b, 1, &n.runner)
            .await
            .is_err()
    );
    n.close().await;
}

#[tokio::test]
async fn mock_roundtrip_result_publication_loss_and_source_commit_failure_resend_exactly() {
    let mut n = Network::new().await;
    let (source, target, session, id) = assigned(&mut n).await;
    let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
    let (dispatch, start) = b.start_mock(&mut n.b, 1, &n.runner).await.unwrap();
    receive(&n.b, &mut n.a, &n.origin, &start).await.unwrap();
    a.apply_start(&mut n.a, 1).unwrap();
    let sql = Connection::open(n.root.path().join("b/projection.sqlite")).unwrap();
    sql.execute_batch("CREATE TRIGGER abort_result BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected result publication failure'); END;").unwrap();
    let calls = Cell::new(0);
    assert!(
        b.complete_mock(&mut n.b, &n.runner, dispatch, |_, _| {
            calls.set(calls.get() + 1);
            Ok((
                Outcome::Completed,
                "durable result before failed publication".into(),
            ))
        })
        .await
        .is_err()
    );
    let bytes: Vec<u8> = target
        .conn
        .query_row(
            "SELECT result FROM adapter_execution WHERE assignment=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(target.get_task_run(1).unwrap().unwrap().status, "completed");
    sql.execute_batch("DROP TRIGGER abort_result").unwrap();
    let record = b.recover_result(&mut n.b, 1, &n.runner).await.unwrap();
    let result_event =
        n.b.domain_history(&n.origin)
            .unwrap()
            .export()
            .last()
            .unwrap()
            .clone();
    assert_eq!(result_event, bytes);
    // Lose the first return. Reopen the recipient Store and resend durable bytes.
    drop(target);
    let target = Store::open(&n.config("target")).unwrap();
    let replay = TaskAdapter::open(&target, &n.origin, &session)
        .unwrap()
        .recover_result(&mut n.b, 1, &n.runner)
        .await
        .unwrap();
    receive(&n.b, &mut n.a, &n.origin, &replay).await.unwrap();
    source.conn.execute_batch("CREATE TRIGGER abort_return BEFORE UPDATE OF result ON adapter_returns BEGIN SELECT RAISE(ABORT,'injected source result commit failure'); END;").unwrap();
    assert!(a.apply_result(&mut n.a, 1).is_err());
    assert_eq!(source.get_task_run(id).unwrap().unwrap().status, "running");
    assert!(output(&source, id).is_none());
    source
        .conn
        .execute_batch("DROP TRIGGER abort_return")
        .unwrap();
    drop(source);
    let source = Store::open(&n.config("source")).unwrap();
    let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    a.apply_result(&mut n.a, 1).unwrap();
    receive(&n.b, &mut n.a, &n.origin, &record).await.unwrap();
    a.apply_result(&mut n.a, 1).unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(
        output(&source, id).as_deref(),
        Some("durable result before failed publication")
    );
    n.close().await;
}

#[tokio::test]
async fn mock_roundtrip_cancellation_before_during_and_late_result_preserves_evidence() {
    for when in ["before", "during", "late"] {
        let mut n = Network::new().await;
        let (source, target, session, id) = assigned(&mut n).await;
        let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
        let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
        if when == "before" {
            let cancel = a.request_cancel(&mut n.a, 1, &n.core).await.unwrap();
            n.b.receive(n.a.addr(), cancel.origin.parse().unwrap(), &cancel.key())
                .await
                .unwrap();
            b.apply_cancellation(&mut n.b, 1).unwrap();
            b.apply_cancellation(&mut n.b, 1).unwrap();
        }
        let (dispatch, start) = b.start_mock(&mut n.b, 1, &n.runner).await.unwrap();
        receive(&n.b, &mut n.a, &n.origin, &start).await.unwrap();
        a.apply_start(&mut n.a, 1).unwrap();
        let calls = Cell::new(0);
        let result = b
            .complete_mock(&mut n.b, &n.runner, dispatch, |store, run| {
                calls.set(calls.get() + 1);
                if when == "during" {
                    store.mark_task_run_cancel_requested(run, NOW)?;
                }
                Ok((Outcome::Completed, "late mock completion retained".into()))
            })
            .await
            .unwrap();
        if when != "before" {
            let cancel = a.request_cancel(&mut n.a, 1, &n.core).await.unwrap();
            n.b.receive(n.a.addr(), cancel.origin.parse().unwrap(), &cancel.key())
                .await
                .unwrap();
            b.apply_cancellation(&mut n.b, 1).unwrap();
        }
        receive(&n.b, &mut n.a, &n.origin, &result).await.unwrap();
        a.apply_result(&mut n.a, 1).unwrap();
        assert_eq!(calls.get(), if when == "before" { 0 } else { 1 });
        if when == "before" {
            assert_eq!(
                source.get_task_run(id).unwrap().unwrap().status,
                "cancelled"
            );
        } else {
            unknown(&source, id);
            assert_eq!(
                output(&source, id).as_deref(),
                Some("late mock completion retained")
            );
            if when == "during" {
                unknown(&target, 1);
            }
        }
        let replay = b.recover_result(&mut n.b, 1, &n.runner).await.unwrap();
        receive(&n.b, &mut n.a, &n.origin, &replay).await.unwrap();
        a.apply_result(&mut n.a, 1).unwrap();
        if when != "before" {
            unknown(&source, id);
        }
        assert!(
            n.a.domain_history(&n.origin)
                .unwrap()
                .validate_replacement_activation()
                .is_err()
        );
        n.close().await;
    }
}

#[tokio::test]
async fn mock_roundtrip_wrong_result_identity_attempt_malformed_and_conflict_refuse() {
    for attack in [
        "runner",
        "assignment",
        "attempt",
        "origin",
        "malformed",
        "conflict",
    ] {
        let mut n = Network::new().await;
        let (source, target, session, id) = assigned(&mut n).await;
        let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
        let (dispatch, start) = b.start_mock(&mut n.b, 1, &n.runner).await.unwrap();
        receive(&n.b, &mut n.a, &n.origin, &start).await.unwrap();
        let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
        a.apply_start(&mut n.a, 1).unwrap();
        let result = b
            .complete_mock(&mut n.b, &n.runner, dispatch, |_, _| {
                Ok((Outcome::Completed, "mock result".into()))
            })
            .await
            .unwrap();
        let history = n.b.domain_history(&n.origin).unwrap();
        let mut event = SignedEvent::parse(history.export().last().unwrap())
            .unwrap()
            .event;
        let mut signer = n.runner.clone();
        match attack {
            "runner" => {
                signer = n.core.clone();
                event.actor = signer.public();
            }
            "assignment" => {
                event.action = Action::Result {
                    assignment: 999,
                    outcome: Outcome::Completed,
                    output: "wrong".into(),
                };
            }
            "attempt" => {
                event = SignedEvent::parse(&history.export()[0]).unwrap().event;
                if let Action::Assign { attempt, .. } = &mut event.action {
                    *attempt = 2;
                }
                signer = n.core.clone();
            }
            "origin" => event.origin = "foreign origin".into(),
            _ => {
                if let Action::Result { output, .. } = &mut event.action {
                    *output = "conflicting result".into();
                }
            }
        }
        let mut bytes = SignedEvent::sign(event, &signer).unwrap().bytes().unwrap();
        if attack == "malformed" {
            bytes = b"{\"approved\":true}".to_vec();
        }
        let hostile =
            n.b.publish("CoconutClaw synthetic domain event v1", &bytes)
                .await
                .unwrap();
        if attack == "conflict" {
            // A valid alternative is a conflict only once both signed values
            // have been disclosed, not because the receiver guessed one.
            receive(&n.b, &mut n.a, &n.origin, &result).await.unwrap();
        }
        assert!(receive(&n.b, &mut n.a, &n.origin, &hostile).await.is_err());
        assert!(a.apply_result(&mut n.a, 1).is_err());
        assert_eq!(source.get_task_run(id).unwrap().unwrap().status, "running");
        assert!(output(&source, id).is_none());
        // A subsequently delivered legitimate result cannot silently win a fork.
        assert!(receive(&n.b, &mut n.a, &n.origin, &result).await.is_err());
        assert!(a.apply_result(&mut n.a, 1).is_err());
        n.close().await;
    }
}

#[tokio::test]
async fn mock_roundtrip_forged_cancellation_refuses_before_start_without_partial_state() {
    for malformed in [false, true] {
        let mut n = Network::new().await;
        let (source, target, session, _) = assigned(&mut n).await;
        let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
        let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
        assert!(a.request_cancel(&mut n.a, 1, &n.runner).await.is_err());
        let command = a.request_cancel(&mut n.a, 1, &n.core).await.unwrap();
        let text = if malformed {
            "CoconutClaw mock cancel v1:{}".to_string()
        } else {
            command.text.replace("\"attempt\":1", "\"attempt\":2")
        };
        assert_ne!(text, command.text);
        let forged =
            n.a.publish(&text, b"artificial forged cancellation")
                .await
                .unwrap();
        n.b.receive(n.a.addr(), forged.origin.parse().unwrap(), &forged.key())
            .await
            .unwrap();
        assert!(b.apply_cancellation(&mut n.b, 1).is_err());
        assert!(b.start_mock(&mut n.b, 1, &n.runner).await.is_err());
        let run = target.get_task_run(1).unwrap().unwrap();
        assert_eq!(run.status, "waiting_remote");
        assert!(run.cancel_requested_at.is_none() && run.started_at.is_none());
        n.close().await;
    }
}

#[tokio::test]
async fn mock_roundtrip_source_restart_unknown_and_recovered_token_never_reexecute() {
    let mut n = Network::new().await;
    let (source, target, session, id) = assigned(&mut n).await;
    let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
    let (dispatch, start) = b.start_mock(&mut n.b, 1, &n.runner).await.unwrap();
    receive(&n.b, &mut n.a, &n.origin, &start).await.unwrap();
    a.apply_start(&mut n.a, 1).unwrap();
    source.mark_stale_task_runs_failed(NOW).unwrap();
    unknown(&source, id);
    let result = b.recover_result(&mut n.b, 1, &n.runner).await.unwrap();
    let calls = Cell::new(0);
    assert!(
        b.complete_mock(&mut n.b, &n.runner, dispatch, |_, _| {
            calls.set(calls.get() + 1);
            Ok((Outcome::Completed, "must not execute".into()))
        })
        .await
        .is_err()
    );
    assert_eq!(calls.get(), 0);
    receive(&n.b, &mut n.a, &n.origin, &result).await.unwrap();
    a.apply_result(&mut n.a, 1).unwrap();
    unknown(&source, id);
    assert_eq!(
        n.a.domain_history(&n.origin).unwrap().jobs[&1]
            .result
            .as_ref()
            .unwrap()
            .0,
        Outcome::Unknown
    );
    n.close().await;
}

#[tokio::test]
async fn mock_roundtrip_late_completed_result_cannot_erase_source_restart_unknown() {
    let mut n = Network::new().await;
    let (source, target, session, id) = assigned(&mut n).await;
    let b = TaskAdapter::open(&target, &n.origin, &session).unwrap();
    let (dispatch, start) = b.start_mock(&mut n.b, 1, &n.runner).await.unwrap();
    receive(&n.b, &mut n.a, &n.origin, &start).await.unwrap();
    TaskAdapter::open(&source, &n.origin, &session)
        .unwrap()
        .apply_start(&mut n.a, 1)
        .unwrap();
    drop(source);
    let source = Store::open(&n.config("source")).unwrap();
    source.mark_stale_task_runs_failed(NOW).unwrap();
    unknown(&source, id);
    let result = b
        .complete_mock(&mut n.b, &n.runner, dispatch, |_, _| {
            Ok((
                Outcome::Completed,
                "late persisted completion is evidence, not retry authority".into(),
            ))
        })
        .await
        .unwrap();
    receive(&n.b, &mut n.a, &n.origin, &result).await.unwrap();
    let a = TaskAdapter::open(&source, &n.origin, &session).unwrap();
    a.apply_result(&mut n.a, 1).unwrap();
    a.apply_result(&mut n.a, 1).unwrap();
    unknown(&source, id);
    assert_eq!(
        output(&source, id).as_deref(),
        Some("late persisted completion is evidence, not retry authority")
    );
    assert_eq!(
        n.a.domain_history(&n.origin).unwrap().jobs[&1]
            .result
            .as_ref()
            .unwrap()
            .0,
        Outcome::Completed
    );
    n.close().await;
}
