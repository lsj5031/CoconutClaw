use super::*;
use rusqlite::OptionalExtension;

fn command(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    id: i64,
    text: &str,
) -> String {
    let line = json!({"update_id":id,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":text}}).to_string();
    crate::loops::webhook::process_webhook_line(cfg, store, scheduler, &line).unwrap();
    store
        .conn
        .query_row(
            "SELECT coalesce(output,'') FROM telegram_ingress WHERE update_id=?1",
            [id.to_string()],
            |r| r.get(0),
        )
        .unwrap()
}

fn acknowledged_ack(store: &Store) -> i64 {
    let raw = json!({"update_id":1,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":"/tasks"}}).to_string();
    let id = store
        .admit_telegram("1", &raw, Some("telegram:321#7"), "now")
        .unwrap();
    store.conn.execute("UPDATE telegram_ingress SET state='done',output='TELEGRAM_REPLY: retained',route='telegram:321#7',ack_phase='unknown',delivery_state=?2 WHERE seq=?1",
        rusqlite::params![id,json!({"completed_ops":["telegram:text"],"text_plan":{"kind":"rich","markdown":"retained","phase":"sending","attempts":1}}).to_string()]).unwrap();
    id
}

#[test]
fn unknown_reconciliation_real_command_acknowledges_without_resend() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    let target = acknowledged_ack(&store);
    let reply = command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile ack {target}"),
    );
    assert!(reply.contains("Durable acknowledgment"), "{reply}");
    let token = reply
        .split("token=")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap();
    let apply = format!("/reconcile ack {target} token={token}");
    let reply = command(&cfg, &mut store, &scheduler, 3, &apply);
    assert!(reply.contains("Reconciled"), "{reply}");
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ack_phase FROM telegram_ingress WHERE seq=?1",
                [target],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "sent"
    );
    assert!(command(&cfg, &mut store, &scheduler, 4, &apply).contains("Already reconciled"));
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

fn token(reply: &str) -> String {
    reply
        .split("token=")
        .nth(1)
        .unwrap_or_else(|| panic!("{reply}"))
        .split_whitespace()
        .next()
        .unwrap()
        .into()
}

fn setup() -> (RuntimeConfig, Store, SessionScheduler, i64) {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    let target = acknowledged_ack(&store);
    (cfg, store, scheduler, target)
}

#[test]
fn unknown_reconciliation_unproven_effects_stay_unknown() {
    let (cfg, mut store, scheduler, target) = setup();
    for (index, state) in [
        json!({"completed_ops":[],"text_plan":{"kind":"rich","markdown":"retained","phase":"unknown","attempts":1}}),
        json!({"completed_ops":[],"text_plan":{"kind":"rich","markdown":"retained","phase":"ready","attempts":0}}),
        json!({"completed_ops":["telegram:text"],"text_plan":{"kind":"rich","markdown":"retained","phase":"unknown","attempts":1}}),
        json!({"completed_ops":["telegram:text","telegram:document:0"],"text_plan":{"kind":"rich","markdown":"retained","phase":"sending","attempts":1}}),
        json!({"completed_ops":["telegram:text"],"text_plan":{"kind":"rich","markdown":"retained","phase":"sending","attempts":4}}),
        json!({"completed_ops":["telegram:text"],"text_plan":{"kind":"rich","markdown":"different","phase":"sending","attempts":1}}),
        json!({"completed_ops":["telegram:text"],"text_plan":{"kind":"rich","markdown":"retained","phase":"unknown","attempts":1},"operations":{"telegram:document:0":{"phase":"unknown","attempts":1}}}),
        json!([]),
    ].iter().enumerate() {
        store.conn.execute("UPDATE telegram_ingress SET delivery_state=?2 WHERE seq=?1",rusqlite::params![target,state.to_string()]).unwrap();
        let reply = command(&cfg,&mut store,&scheduler,10+index as i64,&format!("/reconcile ack {target}"));
        assert!(reply.contains("Unknown retained"),"{reply}");
        assert!(!reply.contains("token="));
        assert_eq!(store.conn.query_row("SELECT ack_phase FROM telegram_ingress WHERE seq=?1",[target],|r|r.get::<_,String>(0)).unwrap(),"unknown");
    }
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_offers",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

#[test]
fn unknown_reconciliation_authority_topic_actor_and_target_refused() {
    let (cfg, mut store, scheduler, target) = setup();
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile ack {target}"),
    ));
    for (id, actor, topic) in [(3, 321, 8), (4, 322, 7)] {
        let line = json!({"update_id":id,"message":{"chat":{"id":321,"type":"private"},"from":{"id":actor},"message_thread_id":topic,"text":format!("/reconcile ack {target} {offer}")}}).to_string();
        crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &line).unwrap();
    }
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            5,
            &format!("/reconcile task {target} {offer}")
        )
        .contains("wrong authority or target")
    );
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            6,
            &format!("/reconcile ack {} {offer}", target + 1)
        )
        .contains("wrong authority or target")
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_audit",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(store.conn.query_row("SELECT count(*) FROM telegram_reconciliation_controls WHERE seq=(SELECT seq FROM telegram_ingress WHERE update_id='4')",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}

#[test]
fn unknown_reconciliation_stale_state_conflicts_and_immutable_audit() {
    let (cfg, mut store, scheduler, target) = setup();
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile ack {target}"),
    ));
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET progress_state='unknown' WHERE seq=?1",
            [target],
        )
        .unwrap();
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            3,
            &format!("/reconcile ack {target} {offer}")
        )
        .contains("stale or conflicting")
    );
    let newer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        4,
        &format!("/reconcile ack {target}"),
    ));
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            5,
            &format!("/reconcile ack {target} {newer}")
        )
        .contains("Reconciled")
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT progress_state FROM telegram_ingress WHERE seq=?1",
                [target],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "unknown"
    );
    assert!(
        store
            .conn
            .execute("DELETE FROM runtime_reconciliation_audit", [])
            .is_err()
    );
    assert!(
        store
            .conn
            .execute("UPDATE runtime_reconciliation_offers SET snapshot='{}'", [])
            .is_err()
    );
    let conflict =
        json!({"update_id":5,"message":{"chat":{"id":321,"type":"private"},"text":"/cancel"}})
            .to_string();
    assert!(
        crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &conflict)
            .is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_audit",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}

#[test]
fn unknown_reconciliation_atomic_failure_restart_and_exact_replay() {
    let (cfg, mut store, scheduler, target) = setup();
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile ack {target}"),
    ));
    store.conn.execute_batch("CREATE TRIGGER fixture_abort BEFORE INSERT ON runtime_reconciliation_audit BEGIN SELECT RAISE(ABORT,'fixture audit failure'); END;").unwrap();
    let apply = format!("/reconcile ack {target} {offer}");
    assert!(command(&cfg, &mut store, &scheduler, 3, &apply).contains("fixture audit failure"));
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ack_phase FROM telegram_ingress WHERE seq=?1",
                [target],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "unknown"
    );
    let raw: String = store
        .conn
        .query_row(
            "SELECT delivery_state FROM telegram_ingress WHERE seq=?1",
            [target],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&raw).unwrap()["text_plan"]["phase"],
        "sending"
    );
    store
        .conn
        .execute_batch("DROP TRIGGER fixture_abort")
        .unwrap();
    drop(store);
    let mut store = Store::open(&cfg).unwrap();
    store.recover_telegram_claims().unwrap();
    assert!(command(&cfg, &mut store, &scheduler, 4, &apply).contains("Reconciled"));
    assert!(command(&cfg, &mut store, &scheduler, 4, &apply).contains("Reconciled"));
    assert!(command(&cfg, &mut store, &scheduler, 5, &apply).contains("Already reconciled"));
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_audit",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}

#[test]
fn unknown_reconciliation_unknown_fence_bypass_is_control_only() {
    let (cfg, mut store, scheduler, target) = setup();
    let raw = json!({"update_id":2,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":"/cancel"}}).to_string();
    let fence = store
        .admit_telegram("2", &raw, Some("telegram:321#7"), "now")
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET state='unknown' WHERE seq=?1",
            [fence],
        )
        .unwrap();
    let reply = command(
        &cfg,
        &mut store,
        &scheduler,
        3,
        &format!("/reconcile ack {target}"),
    );
    assert!(reply.contains("Durable acknowledgment"), "{reply}");
    assert_eq!(
        command(&cfg, &mut store, &scheduler, 4, "ordinary work"),
        ""
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM telegram_ingress WHERE seq=?1",
                [fence],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "unknown"
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    // A persisted pending reconciliation survives restart and remains bounded
    // by active owners, not by an older genuinely Unknown control.
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET state='pending' WHERE update_id='3'",
            [],
        )
        .unwrap();
    assert_eq!(store.pending_telegram().unwrap().len(), 1);
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET state='applying' WHERE seq=?1",
            [fence],
        )
        .unwrap();
    assert!(store.pending_telegram().unwrap().is_empty());
}

fn acknowledged_task(store: &Store) -> (i64, TaskRequest) {
    let mut request = make_request(
        SessionKey::telegram("321#7"),
        "telegram",
        "immutable original",
    );
    request.source_user_id = Some("321".into());
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some("7".into()),
    };
    let tx = store.conn.unchecked_transaction().unwrap();
    let (run, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some("reconcile-task"),
            &request,
            "now",
        )
        .unwrap();
    store
        .record_inbox(
            &format!("result:{run}"),
            crate::store::inbox::EventKind::Result,
            &request.session.id(),
            r#"{"status":"ok","output":"retained"}"#,
            run,
            "now",
        )
        .unwrap();
    store.conn.execute("UPDATE task_runs SET status='completed',result_summary='immutable',finished_at='now' WHERE id=?1",[run]).unwrap();
    store.conn.execute("UPDATE task_payloads SET output='TELEGRAM_REPLY: retained',delivery_state=?2 WHERE task_run_id=?1",
        rusqlite::params![run,json!({"completed_ops":["telegram:text"],"text_plan":{"kind":"rich","markdown":"retained","phase":"sending","attempts":1}}).to_string()]).unwrap();
    tx.commit().unwrap();
    (run, request)
}

#[test]
fn unknown_reconciliation_task_cancel_approval_and_head_changes_refuse() {
    let (cfg, mut store, scheduler, _) = setup();
    let (run, request) = acknowledged_task(&store);
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile task {run}"),
    ));
    store
        .conn
        .execute(
            "UPDATE task_runs SET cancel_requested_at='later' WHERE id=?1",
            [run],
        )
        .unwrap();
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            3,
            &format!("/reconcile task {run} {offer}")
        )
        .contains("stale or conflicting")
    );
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        4,
        &format!("/reconcile task {run}"),
    ));
    let tx = store.conn.unchecked_transaction().unwrap();
    crate::approval::create_approval_request(
        &store,
        &request,
        &request.input,
        run,
        "decision",
        &cfg.timezone,
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            5,
            &format!("/reconcile task {run} {offer}")
        )
        .contains("not terminal")
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_audit",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

#[test]
fn unknown_reconciliation_real_wait_reducer_preserves_outcome_and_one_child() {
    let (cfg, mut store, scheduler, _) = setup();
    let (run, request) = acknowledged_task(&store);
    let tx = store.conn.unchecked_transaction().unwrap();
    let (waiter, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some("reconcile-waiter"),
            &request,
            "now",
        )
        .unwrap();
    store
        .wait_for_task(
            waiter,
            run,
            &build_resume_payload(&request, &request.input).unwrap(),
        )
        .unwrap();
    store.conn.execute("UPDATE task_payloads SET output='TELEGRAM_REPLY: waiting',delivered=1 WHERE task_run_id=?1",[waiter]).unwrap();
    tx.commit().unwrap();
    assert!(store.resume_task_events("before").unwrap().is_empty());
    let before: String = store
        .conn
        .query_row(
            "SELECT status||result_summary FROM task_runs WHERE id=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile task {run}"),
    ));
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            3,
            &format!("/reconcile task {run} {offer}")
        )
        .contains("Reconciled")
    );
    let children = store.resume_task_events("after").unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(store.task_identity(children[0].0).unwrap(), waiter);
    assert!(store.resume_task_events("replay").unwrap().is_empty());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT status||result_summary FROM task_runs WHERE id=?1",
                [run],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        before
    );
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            4,
            &format!("/reconcile task {waiter}")
        )
        .contains("stale physical run")
    );
}

#[test]
fn unknown_reconciliation_real_http_receipt_not_operator_assertion() {
    let (mut cfg, mut store, scheduler, target) = setup();
    let server = FakeTelegramServer::start();
    cfg.telegram_api_base = Some(server.base_url.clone());
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET delivery_state='[]' WHERE seq=?1",
            [target],
        )
        .unwrap();
    let client = reqwest::blocking::Client::new();
    assert!(
        crate::delivery::dispatch_task_output(
            &store,
            &cfg,
            Some(&client),
            &DeliveryTarget::Telegram {
                chat_id: "321".into(),
                thread_id: Some("7".into())
            },
            crate::delivery::TaskDispatch {
                task_id: crate::delivery::DeliveryKey::Telegram(target),
                output: "TELEGRAM_REPLY: retained",
                progress_message_id: None
            },
            "[]"
        )
        .unwrap()
    );
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile ack {target}"),
    ));
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            3,
            &format!("/reconcile ack {target} {offer}")
        )
        .contains("Reconciled")
    );
    crate::loops::webhook::deliver_telegram_acks(&cfg, &store).unwrap();
    crate::loops::webhook::deliver_telegram_acks(&cfg, &store).unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(requests.len(), 3); // Original text, inspect reply, apply reply.
    assert!(requests[0].contains("%22retained%22"));
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn unknown_reconciliation_outer_outcome_failure_rolls_back_projection_and_cause() {
    let (cfg, mut store, scheduler, target) = setup();
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile ack {target}"),
    ));
    store.conn.execute_batch("CREATE TRIGGER fixture_outcome_abort BEFORE UPDATE OF state ON telegram_ingress WHEN NEW.update_id='3' AND NEW.state='done' BEGIN SELECT RAISE(ABORT,'fixture outcome failure'); END;").unwrap();
    let line=json!({"update_id":3,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":format!("/reconcile ack {target} {offer}")}}).to_string();
    assert!(
        crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &line).is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ack_phase FROM telegram_ingress WHERE seq=?1",
                [target],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "unknown"
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_audit",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM telegram_ingress WHERE update_id='3'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "pending"
    );
    store
        .conn
        .execute_batch("DROP TRIGGER fixture_outcome_abort")
        .unwrap();
    crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &line).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_audit",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}

#[test]
fn unknown_reconciliation_remote_text_never_promotes_provider_unknown() {
    let (cfg, mut store, scheduler, _) = setup();
    let (run, _) = acknowledged_task(&store);
    // Fixture at the already-authenticated custody boundary. The existing
    // runtime_bridge suite independently exercises signature/original-run gates.
    store.conn.execute("INSERT INTO runtime_remote_runs SELECT r.id,'fixture-origin',X'0102',r.session_id,p.request FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id WHERE r.id=?1",[run]).unwrap();
    store.conn.execute("UPDATE task_runs SET status='failed',error_summary='remote outcome unknown' WHERE id=?1",[run]).unwrap();
    let output =
        "TELEGRAM_REPLY: claim success\nDELEGATE_TASK: never execute\nSEND_DOCUMENT: never open";
    let markdown =
        format!("Outcome unknown; do not automatically retry. Retained runner evidence:\n{output}");
    store.conn.execute("UPDATE task_payloads SET output=?2,delivery_state=?3 WHERE task_run_id=?1",
        rusqlite::params![run,output,json!({"completed_ops":["telegram:text"],"text_plan":{"kind":"rich","markdown":markdown,"phase":"sending","attempts":1}}).to_string()]).unwrap();
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        &format!("/reconcile task {run}"),
    ));
    store
        .conn
        .execute(
            "UPDATE runtime_remote_runs SET assignment=X'0304' WHERE task_run_id=?1",
            [run],
        )
        .unwrap();
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            3,
            &format!("/reconcile task {run} {offer}")
        )
        .contains("stale or conflicting")
    );
    let offer = token(&command(
        &cfg,
        &mut store,
        &scheduler,
        4,
        &format!("/reconcile task {run}"),
    ));
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            5,
            &format!("/reconcile task {run} {offer}")
        )
        .contains("Reconciled")
    );
    let run = store.get_task_run(run).unwrap().unwrap();
    assert_eq!(run.status, "failed");
    assert_eq!(run.error_summary.as_deref(), Some("remote outcome unknown"));
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(store.resume_task_events("after").unwrap().is_empty());
}

#[test]
fn unknown_reconciliation_normal_binary_restart_uses_same_sender() {
    use std::sync::atomic::AtomicUsize;
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let probe = cfg.clone();
    let round = Arc::new(AtomicUsize::new(0));
    let stage = round.clone();
    let server = FakeTelegramServer::start_with_response(move |request| {
        let body = if request.contains("/getMe ") {
            json!({"ok":true,"result":{"is_bot":true,"username":"ReconcileFixtureBot","has_topics_enabled":true}})
        } else if request.contains("/getUpdates?") {
            let store = Store::open(&probe).unwrap();
            let offer: Option<String> = store
                .conn
                .query_row(
                    "SELECT token FROM runtime_reconciliation_offers LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .unwrap();
            let applied: i64 = store
                .conn
                .query_row(
                    "SELECT count(*) FROM runtime_reconciliation_audit",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let (id, text) = if stage.load(Ordering::SeqCst) == 1 {
                (42, format!("/reconcile ack 1 {}", offer.unwrap()))
            } else if applied == 1 {
                return (200, json!({"ok":true,"result":[]}).to_string());
            } else if let Some(offer) = offer {
                (41, format!("/reconcile ack 1 {offer}"))
            } else {
                (40, "/reconcile ack 1".into())
            };
            json!({"ok":true,"result":[{"update_id":id,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":text}}]})
        } else {
            json!({"ok":true,"result":{"message_id":123}})
        };
        (200, body.to_string())
    });
    cfg.telegram_api_base = Some(server.base_url.clone());
    cfg.codex.bin = write_fake_provider_script(
        &cfg.instance_dir,
        "reconcile-provider",
        "#!/bin/sh\nbase=$(dirname \"$0\"); echo unexpected >> \"$base/reconcile-provider-calls\"; exit 1",
        "[IO.File]::AppendAllText((Join-Path $PSScriptRoot 'reconcile-provider-calls'),'unexpected'); exit 1",
    );
    let store = Store::open(&cfg).unwrap();
    let target = acknowledged_ack(&store);
    // Generate the receipt using the actual HTTP sender, not a supplied claim.
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET delivery_state='[]' WHERE seq=?1",
            [target],
        )
        .unwrap();
    assert!(
        crate::delivery::dispatch_task_output(
            &store,
            &cfg,
            Some(&reqwest::blocking::Client::new()),
            &DeliveryTarget::Telegram {
                chat_id: "321".into(),
                thread_id: Some("7".into())
            },
            crate::delivery::TaskDispatch {
                task_id: crate::delivery::DeliveryKey::Telegram(target),
                output: "TELEGRAM_REPLY: retained",
                progress_message_id: None
            },
            "[]"
        )
        .unwrap()
    );
    let fence=json!({"update_id":2,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":"/cancel"}}).to_string();
    let fence = store
        .admit_telegram("2", &fence, Some("telegram:321#7"), "now")
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET state='unknown' WHERE seq=?1",
            [fence],
        )
        .unwrap();
    let binary = crate::util::test_binary_path();
    let hash = blake3::hash(&fs::read(&binary).unwrap());
    struct Owned(std::process::Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
            }
            let _ = self.0.wait();
        }
    }
    let start = |round| {
        Owned(
            crate::util::scrubbed_command(&binary)
                .arg("--data-dir")
                .arg(&cfg.instance_dir)
                .arg("run")
                .env("HOME", &cfg.instance_dir)
                .env("FACTORY_AIRGAP_ENABLED", "1")
                .env("TELEGRAM_BOT_TOKEN", "123:token")
                .env("TELEGRAM_CHAT_ID", "321")
                .env(
                    "TELEGRAM_API_BASE",
                    server.base_url.trim_end_matches("/bot123:token"),
                )
                .env("POLL_INTERVAL_SECONDS", "1")
                .env("TELEGRAM_API_RETRY_ATTEMPTS", "0")
                .env("CODEX_BIN", &cfg.codex.bin)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::from(
                    fs::File::create(
                        cfg.instance_dir
                            .join(format!("reconcile-normal-{round}.log")),
                    )
                    .unwrap(),
                ))
                .spawn()
                .unwrap(),
        )
    };
    let wait = |id| {
        let until = Instant::now() + Duration::from_secs(90);
        loop {
            let done: bool=store.conn.query_row("SELECT EXISTS(SELECT 1 FROM telegram_ingress WHERE update_id=?1 AND ack_phase='sent')",[id],|r|r.get(0)).unwrap();
            if done {
                break;
            }
            assert!(
                Instant::now() < until,
                "normal command {id} timeout; fixture={}",
                cfg.instance_dir.display()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let first = start(0);
    wait("41");
    drop(first);
    round.store(1, Ordering::SeqCst);
    let second = start(1);
    wait("42");
    drop(second);
    assert_eq!(blake3::hash(&fs::read(&binary).unwrap()), hash);
    assert!(!cfg.instance_dir.join("reconcile-provider-calls").exists());
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_reconciliation_audit",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM telegram_ingress WHERE seq=?1",
                [fence],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "unknown"
    );
    let sent = server.full_requests.lock().unwrap();
    assert_eq!(
        sent.iter().filter(|r| r.contains("%22retained%22")).count(),
        1
    );
    assert_eq!(
        sent.iter()
            .filter(|r| r.contains("/sendRichMessage "))
            .count(),
        4
    );
    if let Some(base) = std::env::var_os("COCONUTCLAW_RECONCILIATION_ARTIFACTS") {
        let dst = PathBuf::from(base);
        fs::create_dir_all(&dst).unwrap();
        for entry in fs::read_dir(&cfg.instance_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), dst.join(entry.file_name())).unwrap();
            }
        }
        fs::write(dst.join("http-requests.json"), json!(*sent).to_string()).unwrap();
    }
}
