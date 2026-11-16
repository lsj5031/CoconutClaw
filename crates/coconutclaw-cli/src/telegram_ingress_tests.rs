use super::*;
use crate::loops::webhook::{
    drain_telegram_ingress, process_webhook_line, restore_inflight_update,
};

#[test]
fn ingress_concurrent_exact_webhook_replays_share_one_control_and_reply() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, store, scheduler) = fixture(&server);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers = (0..2)
        .map(|_| {
            let cfg = cfg.clone();
            let scheduler = scheduler.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut store = Store::open(&cfg).unwrap();
                barrier.wait();
                process_webhook_line(
                    &cfg,
                    &mut store,
                    &scheduler,
                    &update(1, 7, "/memory remember concurrent fixture"),
                )
                .unwrap()
                .output
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let replies = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(replies[0], replies[1]);
    assert_eq!(count(&store, "memory_history"), 1);
    assert_eq!(count(&store, "telegram_ingress"), 1);
    scheduler.retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    scheduler.shutdown().unwrap();
}

#[test]
fn ingress_persisted_ack_rechecks_topic_and_chat_authority_without_root_fallback() {
    let server = FakeTelegramServer::start();
    let (_root, mut cfg, mut store, scheduler) = fixture(&server);
    process_webhook_line(&cfg, &mut store, &scheduler, &update(1, 7, "/fresh")).unwrap();
    scheduler.shutdown().unwrap();
    cfg.telegram_topics_enabled = Some(false);
    let off = SessionScheduler::new(cfg.clone(), None);
    restore_inflight_update(&cfg, &mut store, &off, None).unwrap();
    off.retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 0);
    assert_eq!(state(&store, 1).1, "ready");
    off.shutdown().unwrap();
    cfg.telegram_topics_enabled = Some(true);
    cfg.telegram_chat_id = Some("999".into());
    cfg.telegram_chat_ids = vec!["999".into()];
    let revoked = SessionScheduler::new(cfg.clone(), None);
    revoked.retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 0);
    revoked.shutdown().unwrap();
    cfg.telegram_chat_id = Some("321".into());
    cfg.telegram_chat_ids = vec!["321".into()];
    let allowed = SessionScheduler::new(cfg.clone(), None);
    allowed.retry_deliveries().unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("chat_id=321") && requests[0].contains("message_thread_id=7"));
    drop(requests);
    allowed.shutdown().unwrap();
}

#[test]
fn ingress_legacy_effect_receipt_does_not_authorize_a_new_payload_or_ack() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    store
        .memory_command(
            "telegram:321#7",
            "/memory remember legacy fixture",
            Some("9"),
            "fixture",
        )
        .unwrap();
    let outcome =
        process_webhook_line(&cfg, &mut store, &scheduler, &update(9, 7, "/memory undo")).unwrap();
    assert!(outcome.output.is_none());
    assert_eq!(state(&store, 9), ("unknown".into(), "unknown".into()));
    assert_eq!(count(&store, "memory_history"), 1);
    scheduler.retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 0);
    scheduler.shutdown().unwrap();
}

fn fixture(
    server: &FakeTelegramServer,
) -> (tempfile::TempDir, RuntimeConfig, Store, SessionScheduler) {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = RuntimeConfig::test_config_at(root.path().into());
    cfg.codex.bin = "/nonexistent/never-invoke-a-real-provider".into();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    cfg.telegram_topics_enabled = Some(true);
    cfg.telegram_api_retry_attempts = 0;
    cfg.poll_interval_seconds = 1;
    let store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    (root, cfg, store, scheduler)
}

fn update(id: u64, topic: i64, text: &str) -> String {
    json!({"update_id":id,"message":{"chat":{"id":"321","type":"private"},
        "from":{"id":"321"},"message_thread_id":topic,"text":text}})
    .to_string()
}

#[cfg(unix)]
fn native_fixture(
    server: &FakeTelegramServer,
    body: &str,
) -> (
    tempfile::TempDir,
    RuntimeConfig,
    Store,
    SessionScheduler,
    PathBuf,
) {
    let (root, mut cfg, store, old) = fixture(server);
    old.shutdown().unwrap();
    let calls = root.path().join("provider-calls");
    let script = root.path().join("native-fixture.sh");
    fs::write(
        &script,
        format!(
            r#"printf 'call\n' >> '{}'
cat >/dev/null
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
{}
"#,
            calls.display(),
            body.replace("CALLS", &format!("'{}'", calls.display()))
        ),
    )
    .unwrap();
    cfg.codex.bin = format!("sh '{}'", script.display());
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    (root, cfg, store, scheduler, calls)
}

#[test]
#[cfg(unix)]
fn ingress_dispatch_reservation_and_raw_custody_have_one_sqlite_order() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler, calls) = native_fixture(
        &server,
        r#"printf 'TELEGRAM_REPLY: reserved fixture\n' > "$out""#,
    );
    let original_cursor = store.kv_get("last_update_id").unwrap();
    let reached = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    *scheduler.inner.reservation_probe.lock().unwrap() = Some((reached.clone(), resume.clone()));
    let worker = scheduler.clone();
    let admission = std::thread::spawn(move || {
        let mut request = make_request(
            SessionKey::telegram("321#7"),
            "telegram",
            "reserved fixture",
        );
        request.delivery = DeliveryTarget::Stdout;
        worker.enqueue(request).unwrap()
    });
    reached.wait();
    let raw = update(9, 7, "/fresh");
    let canonical = serde_json::from_str::<serde_json::Value>(&raw)
        .unwrap()
        .to_string();
    store.conn.busy_timeout(Duration::ZERO).unwrap();
    let attempted = store.admit_telegram("9", &canonical, Some("telegram:321#7"), "fixture");
    let blocked = attempted
        .err()
        .is_some_and(|e| e.to_string().contains("database is locked"));
    let custody = count(&store, "telegram_ingress");
    let cursor = store.kv_get("last_update_id").unwrap();
    resume.wait();
    let id = admission.join().unwrap();
    store.conn.busy_timeout(Duration::from_secs(5)).unwrap();
    process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
    wait_for_terminal_tasks(&cfg, &[id]);
    scheduler.shutdown().unwrap();
    assert!(
        blocked,
        "raw admission slipped between ingress snapshot and lane reservation"
    );
    assert_eq!(custody, 0);
    assert_eq!(cursor, original_cursor);
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
}

#[test]
#[cfg(unix)]
fn ingress_acknowledged_cancel_replay_cannot_cancel_a_new_active_run() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler, calls) = native_fixture(
        &server,
        r#"sleep 1
printf 'TELEGRAM_REPLY: completed fixture\n' > "$out""#,
    );
    process_webhook_line(&cfg, &mut store, &scheduler, &update(1, 7, "first fixture")).unwrap();
    wait(|| calls.exists());
    let raw = update(2, 7, "/cancel");
    let reply = process_webhook_line(&cfg, &mut store, &scheduler, &raw)
        .unwrap()
        .output;
    wait_for_terminal_tasks(&cfg, &[1]);
    assert_eq!(store.get_task_run(1).unwrap().unwrap().status, "cancelled");
    let mut next = make_request(
        SessionKey::telegram("321#7"),
        "telegram",
        "new active fixture",
    );
    next.delivery = DeliveryTarget::Stdout;
    let next = scheduler.enqueue(next).unwrap();
    wait(|| fs::read_to_string(&calls).unwrap().lines().count() == 2);
    assert_eq!(store.get_task_run(next).unwrap().unwrap().status, "running");
    assert_eq!(
        process_webhook_line(&cfg, &mut store, &scheduler, &raw)
            .unwrap()
            .output,
        reply
    );
    assert!(
        store
            .get_task_run(next)
            .unwrap()
            .unwrap()
            .cancel_requested_at
            .is_none()
    );
    wait_for_terminal_tasks(&cfg, &[next]);
    assert_eq!(
        store.get_task_run(next).unwrap().unwrap().status,
        "completed"
    );
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
    assert_eq!(
        process_webhook_line(&cfg, &mut store, &restarted, &raw)
            .unwrap()
            .output,
        reply
    );
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 2);
    restarted.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn ingress_normal_turn_admission_cut_recovers_one_provider_without_repeating_progress() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler, calls) =
        native_fixture(&server, r#"printf 'TELEGRAM_REPLY: once\n' > "$out""#);
    store.conn.execute_batch("CREATE TRIGGER completion_cut BEFORE UPDATE OF state ON telegram_ingress
        WHEN NEW.state='done' BEGIN SELECT RAISE(ABORT,'fixture after durable task before outcome'); END;").unwrap();
    let raw = update(1, 7, "synthetic provider turn");
    assert!(process_webhook_line(&cfg, &mut store, &scheduler, &raw).is_err());
    assert_eq!(count(&store, "task_runs"), 1);
    assert_eq!(
        state(&store, 1).0,
        "applying",
        "known admission can be reconstructed"
    );
    assert!(
        !calls.exists(),
        "raw admission still fences execution before its completion"
    );
    let progress = server
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.ends_with("/sendMessage"))
        .count();
    assert_eq!(progress, 1);
    store
        .conn
        .execute_batch("DROP TRIGGER completion_cut")
        .unwrap();
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
    wait_for_terminal_tasks(&cfg, &[1]);
    process_webhook_line(&cfg, &mut store, &restarted, &raw).unwrap();
    assert_eq!(fs::read_to_string(&calls).unwrap().lines().count(), 1);
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with("/sendMessage"))
            .count(),
        progress
    );
    assert_eq!(state(&store, 1).0, "done");
    restarted.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn ingress_cancel_effect_cut_does_not_cancel_a_later_run_or_hold_other_topics_slots() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler, calls) = native_fixture(
        &server,
        r#"if [ "$(wc -l < CALLS)" -eq 1 ]; then sleep 3; fi
printf 'TELEGRAM_REPLY: fixture done\n' > "$out""#,
    );
    process_webhook_line(
        &cfg,
        &mut store,
        &scheduler,
        &update(1, 7, "running fixture"),
    )
    .unwrap();
    wait(|| calls.exists());
    store.conn.execute_batch("CREATE TRIGGER cancel_cut BEFORE UPDATE OF state ON telegram_ingress
        WHEN NEW.update_id='2' AND NEW.state='done' BEGIN SELECT RAISE(ABORT,'fixture after cancel before outcome'); END;").unwrap();
    let cancel = json!({"update_id":2,"callback_query":{"from":{"id":"321"},"data":"cancel",
        "message":{"chat":{"id":"321","type":"private"},"message_thread_id":7}}})
    .to_string();
    assert!(process_webhook_line(&cfg, &mut store, &scheduler, &cancel).is_err());
    assert_eq!(state(&store, 2).0, "unknown");
    store.conn.execute_batch("DROP TRIGGER cancel_cut").unwrap();
    wait_for_terminal_tasks(&cfg, &[1]);
    assert_eq!(store.get_task_run(1).unwrap().unwrap().status, "cancelled");
    process_webhook_line(&cfg, &mut store, &scheduler, &cancel).unwrap();
    let mut blocked = make_request(SessionKey::telegram("321#7"), "telegram", "must wait");
    blocked.delivery = DeliveryTarget::Stdout;
    let blocked = scheduler.enqueue(blocked).unwrap();
    let mut independent = make_request(SessionKey::telegram("321#8"), "telegram", "independent");
    independent.delivery = DeliveryTarget::Stdout;
    let independent = scheduler.enqueue(independent).unwrap();
    wait_for_terminal_tasks(&cfg, &[independent]);
    assert_eq!(
        store.get_task_run(blocked).unwrap().unwrap().status,
        "queued"
    );
    assert_eq!(fs::read_to_string(&calls).unwrap().lines().count(), 2);
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
    process_webhook_line(&cfg, &mut store, &restarted, &cancel).unwrap();
    assert_eq!(
        store.get_task_run(blocked).unwrap().unwrap().status,
        "queued"
    );
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 2);
    restarted.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn ingress_approval_topic_authority_exact_reply_and_committed_effect_crash_are_preserved() {
    for interrupted in [false, true] {
        let server = FakeTelegramServer::start();
        let (_root, cfg, mut store, scheduler, calls) = native_fixture(
            &server,
            r#"if [ "$(wc -l < CALLS)" -eq 1 ]; then
 printf 'TELEGRAM_REPLY: fixture approval\nSEND_APPROVAL: allow fixture\n' > "$out"
else printf 'TELEGRAM_REPLY: continued once\n' > "$out"; fi"#,
        );
        let mut request = make_request(
            SessionKey::telegram("321#7"),
            "telegram",
            "approval fixture",
        );
        request.delivery = DeliveryTarget::Stdout;
        let first = scheduler.enqueue(request).unwrap();
        wait(|| store.get_task_run(first).unwrap().unwrap().status == "awaiting_approval");
        let approval: i64 = store
            .conn
            .query_row(
                "SELECT id FROM approvals WHERE task_run_id=?1",
                [first],
                |r| r.get(0),
            )
            .unwrap();
        let wrong = process_webhook_line(
            &cfg,
            &mut store,
            &scheduler,
            &update(5, 8, &format!("/approve {approval}")),
        )
        .unwrap();
        assert!(wrong.output.unwrap().contains("not authorized"));
        assert_eq!(
            store
                .get_pending_approval(approval)
                .unwrap()
                .unwrap()
                .status,
            "pending"
        );
        if interrupted {
            store.conn.execute_batch("CREATE TRIGGER approval_cut BEFORE UPDATE OF state ON telegram_ingress
            WHEN NEW.update_id='6' AND NEW.state='done' BEGIN SELECT RAISE(ABORT,'fixture after approval commit before outcome'); END;").unwrap();
        }
        let raw = update(6, 7, &format!("/approve {approval}"));
        let result = process_webhook_line(&cfg, &mut store, &scheduler, &raw);
        assert_eq!(
            store
                .get_pending_approval(approval)
                .unwrap()
                .unwrap()
                .status,
            "approved"
        );
        assert_eq!(
            count(&store, "task_runs"),
            2,
            "one approval continuation, no ack task"
        );
        if interrupted {
            assert!(result.is_err());
            assert_eq!(state(&store, 6).0, "unknown");
            store
                .conn
                .execute_batch("DROP TRIGGER approval_cut")
                .unwrap();
            scheduler.shutdown().unwrap();
            let restarted = SessionScheduler::new(cfg.clone(), None);
            restarted.recover().unwrap();
            restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
            process_webhook_line(&cfg, &mut store, &restarted, &raw).unwrap();
            assert_eq!(store.get_task_run(2).unwrap().unwrap().status, "queued");
            assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
            restarted.shutdown().unwrap();
        } else {
            let response = result.unwrap().output;
            assert_eq!(
                process_webhook_line(&cfg, &mut store, &scheduler, &raw)
                    .unwrap()
                    .output,
                response
            );
            wait_for_terminal_tasks(&cfg, &[2]);
            assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 2);
            scheduler.shutdown().unwrap();
        }
    }
}

fn count(store: &Store, table: &str) -> i64 {
    store
        .conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}
fn state(store: &Store, id: u64) -> (String, String) {
    store
        .conn
        .query_row(
            "SELECT state,ack_phase FROM telegram_ingress WHERE update_id=?1",
            [id.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}
fn wait(mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < end, "ingress condition did not settle");
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn admit(store: &Store, id: u64, topic: i64, text: &str) -> i64 {
    store
        .admit_telegram(
            &id.to_string(),
            &update(id, topic, text),
            Some(&format!("telegram:321#{topic}")),
            "fixture",
        )
        .unwrap()
}

#[test]
fn ingress_exact_replay_is_canonical_conflict_refuses_and_cursor_never_regresses() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    let raw = update(10, 7, "/memory remember synthetic fixture");
    let first = process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
    let pretty =
        serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(&raw).unwrap())
            .unwrap();
    assert_eq!(
        first.output,
        process_webhook_line(&cfg, &mut store, &scheduler, &pretty)
            .unwrap()
            .output
    );
    assert!(
        process_webhook_line(&cfg, &mut store, &scheduler, &update(10, 7, "/memory undo")).is_err()
    );
    assert!(process_webhook_line(&cfg, &mut store, &scheduler, &update(10, 8, "/fresh")).is_err());
    process_webhook_line(&cfg, &mut store, &scheduler, &update(20, 8, "/fresh")).unwrap();
    process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
    assert_eq!(count(&store, "memory_history"), 1);
    assert_eq!(count(&store, "telegram_ingress"), 2);
    assert_eq!(
        count(&store, "task_runs"),
        0,
        "acks are not fake model runs"
    );
    assert_eq!(
        store.kv_get("last_update_id").unwrap().as_deref(),
        Some("20")
    );
    scheduler.retry_deliveries().unwrap();
    let sends = server.requests.lock().unwrap().len();
    assert_eq!(sends, 2);
    process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
    scheduler.retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), sends);
    scheduler.shutdown().unwrap();
}

#[test]
fn ingress_sql_control_and_ack_rollback_together_then_recover_once() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    store.conn.execute_batch("CREATE TRIGGER crash_cut BEFORE UPDATE OF state ON telegram_ingress
        WHEN NEW.state='done' BEGIN SELECT RAISE(ABORT,'fixture crash after effect before outcome'); END;").unwrap();
    let raw = update(1, 7, "/memory remember atomic fixture");
    assert!(process_webhook_line(&cfg, &mut store, &scheduler, &raw).is_err());
    assert_eq!(count(&store, "memory_history"), 0);
    assert_eq!(count(&store, "turns"), 0);
    assert_eq!(state(&store, 1), ("pending".into(), "ready".into()));
    assert_eq!(
        store.kv_get("last_update_id").unwrap().as_deref(),
        Some("1"),
        "raw custody is durable"
    );
    store.conn.execute_batch("DROP TRIGGER crash_cut").unwrap();
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
    wait(|| state(&store, 1).1 == "sent");
    assert_eq!(count(&store, "memory_history"), 1);
    assert_eq!(count(&store, "task_runs"), 0);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    process_webhook_line(&cfg, &mut store, &restarted, &raw).unwrap();
    restarted.retry_deliveries().unwrap();
    assert_eq!(count(&store, "memory_history"), 1);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    restarted.shutdown().unwrap();
}

#[test]
fn ingress_admission_failure_does_not_advance_cursor_or_apply_control() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER no_custody BEFORE INSERT ON telegram_ingress
        BEGIN SELECT RAISE(ABORT,'fixture storage unavailable'); END;",
        )
        .unwrap();
    assert!(process_webhook_line(&cfg, &mut store, &scheduler, &update(1, 7, "/fresh")).is_err());
    assert_eq!(
        store.kv_get("last_update_id").unwrap().as_deref(),
        Some("0")
    );
    assert_eq!(count(&store, "turns"), 0);
    assert_eq!(count(&store, "telegram_ingress"), 0);
    scheduler.shutdown().unwrap();
}

#[test]
fn ingress_pending_order_is_per_topic_not_a_global_control_barrier() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    admit(&store, 1, 7, "/memory remember first");
    process_webhook_line(
        &cfg,
        &mut store,
        &scheduler,
        &update(2, 7, "/memory remember second"),
    )
    .unwrap();
    assert_eq!(count(&store, "memory_history"), 0);
    process_webhook_line(
        &cfg,
        &mut store,
        &scheduler,
        &update(3, 8, "/memory remember independent"),
    )
    .unwrap();
    assert_eq!(count(&store, "memory_history"), 1);
    drain_telegram_ingress(&cfg, &mut store, &scheduler).unwrap();
    drain_telegram_ingress(&cfg, &mut store, &scheduler).unwrap();
    let mut stmt = store
        .conn
        .prepare("SELECT content FROM memory_records WHERE scope='telegram:321#7' ORDER BY id")
        .unwrap();
    let ordered = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(ordered, ["first", "second"]);
    assert_eq!(state(&store, 2).0, "done");
    scheduler.shutdown().unwrap();
}

#[test]
fn ingress_interrupted_runtime_claim_is_unknown_and_blocks_only_its_origin() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    let seq = admit(&store, 1, 7, "/cancel");
    assert!(store.claim_telegram(seq).unwrap());
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
    assert_eq!(state(&store, 1).0, "unknown");
    process_webhook_line(&cfg, &mut store, &restarted, &update(1, 7, "/cancel")).unwrap();
    process_webhook_line(
        &cfg,
        &mut store,
        &restarted,
        &update(2, 7, "/memory remember must wait"),
    )
    .unwrap();
    process_webhook_line(
        &cfg,
        &mut store,
        &restarted,
        &update(3, 8, "/memory remember allowed"),
    )
    .unwrap();
    assert_eq!(state(&store, 2).0, "pending");
    assert_eq!(count(&store, "memory_history"), 1);
    assert_eq!(count(&store, "task_runs"), 0);
    restarted.shutdown().unwrap();
}

#[test]
fn ingress_ack_unknown_never_resends_after_restart_but_control_is_known() {
    let server = FakeTelegramServer::start_with_response(|_| {
        (
            500,
            r#"{"ok":false,"description":"fixture ambiguous ack"}"#.into(),
        )
    });
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    let raw = update(1, 7, "/memory remember known effect");
    process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
    scheduler.retry_deliveries().unwrap();
    assert_eq!(state(&store, 1), ("done".into(), "unknown".into()));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
    process_webhook_line(&cfg, &mut store, &restarted, &raw).unwrap();
    restarted.retry_deliveries().unwrap();
    assert_eq!(count(&store, "memory_history"), 1);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    restarted.shutdown().unwrap();
}

#[test]
fn ingress_ack_crash_cuts_before_and_after_receipt_persistence_never_resend() {
    for committed in [false, true] {
        let server = FakeTelegramServer::start();
        let (_root, cfg, mut store, scheduler) = fixture(&server);
        let raw = update(1, 7, "/fresh");
        process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
        if committed {
            store.conn.execute_batch("CREATE TRIGGER crash_ack BEFORE UPDATE OF ack_phase ON telegram_ingress
                WHEN NEW.ack_phase='sent' BEGIN SELECT RAISE(ABORT,'fixture after durable send receipt'); END;").unwrap();
        } else {
            store
                .conn
                .execute_batch(
                    "CREATE TRIGGER crash_ack BEFORE UPDATE OF delivery_state ON telegram_ingress
                WHEN json_array_length(json_extract(NEW.delivery_state,'$.completed_ops'))>0
                BEGIN SELECT RAISE(ABORT,'fixture after HTTP before durable receipt'); END;",
                )
                .unwrap();
        }
        let _ = scheduler.retry_deliveries();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        store.conn.execute_batch("DROP TRIGGER crash_ack").unwrap();
        scheduler.shutdown().unwrap();
        let restarted = SessionScheduler::new(cfg.clone(), None);
        restore_inflight_update(&cfg, &mut store, &restarted, None).unwrap();
        restarted.retry_deliveries().unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert_eq!(
            state(&store, 1).1,
            if committed { "sent" } else { "unknown" }
        );
        assert_eq!(count(&store, "turns"), 1);
        restarted.shutdown().unwrap();
    }
}

#[test]
fn ingress_authority_refuses_other_chat_sender_bot_and_disabled_topic_without_effects() {
    let server = FakeTelegramServer::start();
    let (_root, mut cfg, mut store, scheduler) = fixture(&server);
    cfg.telegram_bot_username = Some("FixtureBot".into());
    for (id, mut value) in [
        (
            1,
            json!({"message":{"chat":{"id":"999"},"text":"/memory remember unauthorized"}}),
        ),
        (
            2,
            json!({"message":{"chat":{"id":"321"},"from":{"id":"999"},"text":"/fresh"}}),
        ),
        (
            3,
            json!({"message":{"chat":{"id":"321"},"text":"/memory@OtherBot remember unauthorized"}}),
        ),
        (
            4,
            json!({"callback_query":{"from":{"id":"999"},"data":"cancel","message":{"chat":{"id":"321"}}}}),
        ),
        (
            5,
            json!({"message":{"chat":{"id":"321","type":"group"},"text":"/fresh"}}),
        ),
    ] {
        value["update_id"] = json!(id);
        assert!(
            process_webhook_line(&cfg, &mut store, &scheduler, &value.to_string())
                .unwrap()
                .output
                .is_none()
        );
    }
    cfg.telegram_topics_enabled = Some(false);
    process_webhook_line(&cfg, &mut store, &scheduler, &update(6, 7, "/fresh")).unwrap();
    assert_eq!(count(&store, "memory_history"), 0);
    assert_eq!(count(&store, "turns"), 0);
    assert_eq!(count(&store, "task_runs"), 0);
    scheduler.shutdown().unwrap();
}

#[test]
fn ingress_unknown_legacy_checkpoint_is_preserved_not_replayed_or_dropped() {
    let server = FakeTelegramServer::start();
    let (_root, cfg, mut store, scheduler) = fixture(&server);
    let raw = update(9, 7, "/memory undo");
    store.kv_set("inflight_update_json", &raw).unwrap();
    store.kv_set("inflight_update_id", "9").unwrap();
    restore_inflight_update(&cfg, &mut store, &scheduler, None).unwrap();
    assert_eq!(state(&store, 9), ("unknown".into(), "unknown".into()));
    assert!(store.kv_get("inflight_update_json").unwrap().is_none());
    assert_eq!(count(&store, "memory_history"), 0);
    assert_eq!(server.requests.lock().unwrap().len(), 0);
    store
        .kv_set("inflight_update_json", "{malformed fixture")
        .unwrap();
    assert!(restore_inflight_update(&cfg, &mut store, &scheduler, None).is_err());
    assert_eq!(
        store.kv_get("inflight_update_json").unwrap().as_deref(),
        Some("{malformed fixture")
    );
    scheduler.shutdown().unwrap();
}

#[test]
fn ingress_actual_poll_deduplicates_controls_and_acknowledges_durable_custody() {
    let batch=json!({"ok":true,"result":[
        serde_json::from_str::<serde_json::Value>(&update(1,7,"/memory remember poll fixture")).unwrap(),
        serde_json::from_str::<serde_json::Value>(&update(1,7,"/memory remember poll fixture")).unwrap(),
        serde_json::from_str::<serde_json::Value>(&update(2,8,"/fresh")).unwrap()
    ]}).to_string();
    let first = Arc::new(AtomicBool::new(true));
    let server = FakeTelegramServer::start_with_response(move |request| {
        if request.contains("/getUpdates") {
            (
                200,
                if first.swap(false, Ordering::SeqCst) {
                    batch.clone()
                } else {
                    r#"{"ok":true,"result":[]}"#.into()
                },
            )
        } else {
            (200, r#"{"ok":true,"result":{"message_id":77}}"#.into())
        }
    });
    let (_root, cfg, store, scheduler) = fixture(&server);
    let stop = Arc::new(AtomicBool::new(false));
    let poll_stop = stop.clone();
    let poll_cfg = cfg.clone();
    let polling = scheduler.clone();
    let worker = std::thread::spawn(move || {
        let mut store = Store::open(&poll_cfg).unwrap();
        crate::loops::run_poll_loop(
            &poll_cfg,
            &mut store,
            &polling,
            &crate::telegram::build_telegram_client(&poll_cfg).unwrap(),
            &poll_stop,
        )
    });
    wait(|| count(&store, "telegram_ingress") == 2 && state(&store, 2).1 == "sent");
    wait(|| {
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.contains("offset=3"))
    });
    stop.store(true, Ordering::SeqCst);
    worker.join().unwrap().unwrap();
    assert_eq!(count(&store, "memory_history"), 1);
    assert_eq!(count(&store, "task_runs"), 0);
    assert!(store.kv_get("inflight_update_json").unwrap().is_none());
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with("/sendRichMessage"))
            .count(),
        2
    );
    scheduler.shutdown().unwrap();
}
