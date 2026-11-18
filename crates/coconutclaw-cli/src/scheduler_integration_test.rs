use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use coconutclaw_config::RuntimeConfig;
use serde_json::json;

use super::{DeliveryTarget, SessionKey, SessionScheduler, TaskRequest, TaskSource};
use crate::approval::{
    build_resume_payload, cleanup_attachment_from_resume_payload, request_from_resume_payload,
};
use crate::store::Store;
use crate::{InputType, QuotedMessage, TurnInput};

fn outbox_fixture(cfg: &RuntimeConfig, name: &str) -> PathBuf {
    let root = crate::delivery::media::outbox(cfg);
    fs::create_dir_all(&root).unwrap();
    root.join(name)
}

#[path = "effect_boundary_tests.rs"]
mod effect_boundaries;

#[path = "phase0_tests.rs"]
mod phase0;

#[path = "topics_rich_tests.rs"]
mod topics_rich;

#[path = "scheduling/tests.rs"]
mod scheduling;

#[path = "telegram_ingress_tests.rs"]
mod ingress;

#[path = "reconciliation_tests.rs"]
mod reconciliation;

#[path = "context_notification_tests.rs"]
mod context_notifications;

fn graph_admit(store: &Store, session: &str, key: &str) -> (i64, TaskRequest) {
    let mut request = make_stdout_request(SessionKey::from_id(session.into()), key);
    request.update_id = Some(key.into());
    let tx = store.conn.unchecked_transaction().unwrap();
    let (id, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some(key),
            &request,
            "now",
        )
        .unwrap();
    tx.commit().unwrap();
    (id, request)
}

#[test]
fn waiting_graph_control_ack_releases_snapshot_before_parallel_admission() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let probe = cfg.clone();
    let wrote = Arc::new(AtomicBool::new(false));
    let flag = wrote.clone();
    let server = FakeTelegramServer::start_with_response(move |_| {
        if !flag.swap(true, Ordering::SeqCst) {
            let store = Store::open(&probe).unwrap();
            graph_admit(&store, "local:independent", "admitted-during-ack");
        }
        (200, r#"{"ok":true,"result":{"message_id":123}}"#.into())
    });
    cfg.telegram_api_base = Some(server.base_url.clone());
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap(); // Explicitly drive the sole sender below.
    crate::loops::webhook::process_webhook_line(
        &cfg,
        &mut store,
        &scheduler,
        r#"{"update_id":40,"message":{"from":{"id":321},"chat":{"id":321,"type":"private"},"text":"/tasks"}}"#,
    )
    .unwrap();
    crate::loops::webhook::deliver_telegram_acks(&cfg, &store).unwrap();
    assert!(wrote.load(Ordering::SeqCst));
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ack_phase FROM telegram_ingress WHERE update_id='40'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "sent"
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    crate::loops::webhook::deliver_telegram_acks(&cfg, &store).unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

fn graph_wait(store: &Store, run: i64, dependency: i64, request: &TaskRequest) {
    let tx = store.conn.unchecked_transaction().unwrap();
    store
        .wait_for_task(
            run,
            dependency,
            &build_resume_payload(request, &request.input).unwrap(),
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE task_payloads SET output='TELEGRAM_REPLY: waiting' WHERE task_run_id=?1",
            [run],
        )
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn waiting_graph_attachment_rejection_and_cancelled_queue_cleanup() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, _) = graph_admit(&store, "local:owned", "source");
    let (waiter, mut request) = graph_admit(&store, "local:owned", "waiter");
    let path = cfg.tmp_dir.join("owned-wait.txt");
    fs::write(&path, "owned fixture").unwrap();
    request.input.attachment_path = Some(path.clone());
    request.input.attachment_owned = true;
    store
        .conn
        .execute(
            "UPDATE task_payloads SET request=?2 WHERE task_run_id=?1",
            rusqlite::params![
                waiter,
                build_resume_payload(&request, &request.input).unwrap()
            ],
        )
        .unwrap();
    graph_wait(&store, waiter, source, &request);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.retry_deliveries().unwrap();
    assert!(path.exists(), "waiting still owns continuation input");
    assert!(
        scheduler
            .cancel_task_for_session(waiter, Some("local:owned"))
            .unwrap()
    );
    assert!(!path.exists());
    let (question, qr) = graph_admit(&store, "local:owned", "question");
    let tx = store.conn.unchecked_transaction().unwrap();
    let approval = crate::approval::create_approval_request(
        &store,
        &qr,
        &qr.input,
        question,
        "decision",
        &cfg.timezone,
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(
        scheduler
            .resolve_approval(approval, "reject", "local:owned")
            .unwrap()
            .unwrap()
            .contains("rejected")
    );
    scheduler.inner.recovering.store(true, Ordering::SeqCst);
    let mut queued = make_stdout_request(SessionKey::local("owned"), "queued");
    queued.delegated = true;
    let id = scheduler.enqueue(queued).unwrap();
    assert!(scheduler.cancel_task(id).unwrap());
    assert!(scheduler.inner.lanes.lock().unwrap().is_empty());
    scheduler.shutdown().unwrap();
}

#[test]
fn waiting_graph_acknowledged_send_phase_is_not_an_unresolved_effect() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, sr) = graph_admit(&store, "local:ack", "source");
    let (waiter, wr) = graph_admit(&store, "local:ack", "waiter");
    graph_wait(&store, waiter, source, &wr);
    graph_complete(&store, source, &sr);
    // Exact shape retained by the real sender after acknowledging its text op.
    store.conn.execute("UPDATE task_payloads SET delivered=0,delivery_state=?2 WHERE task_run_id=?1",
        rusqlite::params![source,r#"{"completed_ops":["telegram:text"],"operations":{},"text_plan":{"attempts":1,"kind":"rich","markdown":"fixture complete","phase":"sending"}}"#]).unwrap();
    assert!(store.resume_task_events("inflight").unwrap().is_empty());
    store
        .conn
        .execute(
            "UPDATE task_payloads SET delivered=1 WHERE task_run_id=?1",
            [source],
        )
        .unwrap();
    let child = store.resume_task_events("acknowledged").unwrap()[0].0;
    assert!(store.task_progress_context(child).is_ok());
    assert!(store.resume_task_events("replay").unwrap().is_empty());
    let (blocked, request) = graph_admit(&store, "local:ack", "still-unknown");
    graph_wait(&store, blocked, source, &request);
    store.conn.execute("UPDATE task_payloads SET delivery_state=json_set(delivery_state,'$.text_plan.phase','unknown') WHERE task_run_id=?1",[source]).unwrap();
    assert!(
        store
            .resume_task_events("unknown-is-not-ack")
            .unwrap()
            .is_empty()
    );
    assert!(store.task_progress_context(child).is_err());
}

fn graph_complete(store: &Store, run: i64, request: &TaskRequest) {
    let tx = store.conn.unchecked_transaction().unwrap();
    store
        .record_inbox(
            &format!("result:{run}"),
            crate::store::inbox::EventKind::Result,
            &request.session.id(),
            r#"{"status":"ok","output":"fixture complete"}"#,
            run,
            "now",
        )
        .unwrap();
    store
        .finish_task_run(
            run,
            crate::store::TaskRunStatus::Completed,
            "now",
            None,
            Some("fixture complete"),
        )
        .unwrap();
    store.conn.execute("UPDATE task_payloads SET output='TELEGRAM_REPLY: fixture complete' WHERE task_run_id=?1",[run]).unwrap();
    tx.commit().unwrap();
}

#[test]
fn waiting_graph_legacy_backfill_and_trigger_repair_preserve_custody() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, sr) = graph_admit(&store, "local:migration", "source");
    let (waiter, wr) = graph_admit(&store, "local:migration", "waiter");
    graph_complete(&store, source, &sr);
    store.conn.execute_batch("DROP TRIGGER runtime_task_root; DROP TABLE runtime_waits;
        DROP TABLE runtime_attempts; DROP TABLE runtime_tasks; DELETE FROM kv WHERE key='runtime_graph_v1'").unwrap();
    drop(store);
    let store = Store::open(&cfg).unwrap();
    assert_eq!(store.task_identity(source).unwrap(), source);
    assert_eq!(store.task_identity(waiter).unwrap(), waiter);
    assert_eq!(
        store.get_task_run(source).unwrap().unwrap().status,
        "completed"
    );
    graph_wait(&store, waiter, source, &wr);
    let child = store.resume_task_events("now").unwrap()[0].0;
    store
        .conn
        .execute_batch("DROP TRIGGER runtime_task_root")
        .unwrap();
    let repaired = Store::open(&cfg).unwrap();
    assert_eq!(repaired.task_identity(child).unwrap(), waiter);
    let (extra, _) = graph_admit(&repaired, "local:migration", "after-repair");
    assert_eq!(repaired.task_identity(extra).unwrap(), extra);
    assert_eq!(
        repaired
            .conn
            .query_row("SELECT COUNT(*) FROM runtime_tasks", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        3
    );
}

#[test]
fn waiting_graph_frozen_evidence_and_stale_callback_do_not_overwrite_head() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, sr) = graph_admit(&store, "local:bound", "source");
    let (waiter, wr) = graph_admit(&store, "local:bound", "waiter");
    graph_wait(&store, waiter, source, &wr);
    graph_complete(&store, source, &sr);
    let child = store.resume_task_events("now").unwrap()[0].0;
    store
        .conn
        .execute(
            "UPDATE task_payloads SET output='substituted' WHERE task_run_id=?1",
            [source],
        )
        .unwrap();
    let data = store.task_progress_context(child).unwrap();
    assert!(data.contains("fixture complete"));
    assert!(!data.contains("substituted"));
    let scheduler = SessionScheduler::new(cfg, None);
    assert!(
        scheduler
            .persist_result(
                &store,
                waiter,
                &wr,
                &wr.input,
                "TELEGRAM_REPLY: stale",
                crate::TurnStatus::Ok
            )
            .is_err()
    );
    assert!(store.is_current_attempt(child).unwrap());
    store
        .conn
        .execute(
            "UPDATE task_runs SET error_summary='outcome unknown' WHERE id=?1",
            [source],
        )
        .unwrap();
    assert!(store.task_progress_context(child).is_err());
    scheduler.shutdown().unwrap();
}

#[test]
fn waiting_graph_scopes_cycles_stale_owner_and_consumed_events_refuse() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (a, ar) = graph_admit(&store, "local:scope", "a");
    let (b, br) = graph_admit(&store, "local:scope", "b");
    let (foreign, _) = graph_admit(&store, "local:other", "foreign");
    for dependency in [b, foreign] {
        let tx = store.conn.unchecked_transaction().unwrap();
        assert!(
            store
                .wait_for_task(
                    b,
                    dependency,
                    &build_resume_payload(&br, &br.input).unwrap()
                )
                .is_err()
        );
        drop(tx);
    }
    graph_wait(&store, a, b, &ar);
    let tx = store.conn.unchecked_transaction().unwrap();
    assert!(
        store
            .wait_for_task(b, a, &build_resume_payload(&br, &br.input).unwrap())
            .is_err()
    );
    drop(tx);
    graph_complete(&store, b, &br);
    let ready = store.resume_task_events("later").unwrap();
    assert_eq!(ready.len(), 1);
    let child = ready[0].0;
    assert_eq!(store.task_identity(child).unwrap(), a);
    assert!(!store.is_current_attempt(a).unwrap());
    assert!(store.resume_task_events("again").unwrap().is_empty());
    let tx = store.conn.unchecked_transaction().unwrap();
    assert!(
        store
            .wait_for_task(a, b, &build_resume_payload(&ar, &ar.input).unwrap())
            .is_err()
    );
    assert!(
        store
            .wait_for_task(
                child,
                b,
                &build_resume_payload(&ready[0].1, &ready[0].1.input).unwrap()
            )
            .is_err()
    );
    drop(tx);
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        4
    );
}

#[test]
fn waiting_graph_custody_rollback_conflicts_and_unknown_do_not_resume() {
    for cut in ["rollback", "unknown", "actor", "payload", "event", "cancel"] {
        let cfg = scheduler_test_config("NEVER-EXECUTE".into());
        let store = Store::open(&cfg).unwrap();
        let (source, sr) = graph_admit(&store, "local:graph", "source");
        let (waiter, wr) = graph_admit(&store, "local:graph", "waiter");
        graph_wait(&store, waiter, source, &wr);
        graph_complete(&store, source, &sr);
        match cut {
            "rollback" => {
                store.conn.execute_batch("CREATE TRIGGER resume_cut BEFORE UPDATE OF current_run_id ON runtime_tasks BEGIN SELECT RAISE(ABORT,'resume cut'); END").unwrap();
                assert!(store.resume_task_events("now").is_err());
                store.conn.execute_batch("DROP TRIGGER resume_cut").unwrap();
            }
            "unknown" => {
                store
                    .conn
                    .execute(
                        "UPDATE task_runs SET error_summary='outcome unknown' WHERE id=?1",
                        [source],
                    )
                    .unwrap();
            }
            "actor" => {
                store
                    .conn
                    .execute(
                        "UPDATE task_runs SET source_user_id='foreign' WHERE id=?1",
                        [source],
                    )
                    .unwrap();
            }
            "payload" => {
                store
                    .conn
                    .execute(
                        "UPDATE task_payloads SET request='{}' WHERE task_run_id=?1",
                        [waiter],
                    )
                    .unwrap();
            }
            "event" => {
                let tx = store.conn.unchecked_transaction().unwrap();
                store
                    .record_inbox(
                        &format!("dependency:{waiter}"),
                        crate::store::inbox::EventKind::Request,
                        "local:foreign",
                        "{}",
                        waiter,
                        "now",
                    )
                    .unwrap();
                tx.commit().unwrap();
                assert!(store.resume_task_events("now").is_err());
            }
            _ => {
                let scheduler = SessionScheduler::new(cfg.clone(), None);
                assert!(
                    scheduler
                        .cancel_task_for_session(waiter, Some("local:graph"))
                        .unwrap()
                );
                scheduler.shutdown().unwrap();
            }
        }
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        if cut == "rollback" {
            let reopened = Store::open(&cfg).unwrap();
            assert_eq!(reopened.resume_task_events("restart").unwrap().len(), 1);
            assert!(reopened.resume_task_events("replay").unwrap().is_empty());
        } else if cut != "event" {
            assert!(store.resume_task_events("retry").unwrap().is_empty());
        }
    }
}

#[test]
fn waiting_graph_bounded_thirty_three_waits_preserve_last_until_next_drain() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, sr) = graph_admit(&store, "local:batch", "source");
    for i in 0..33 {
        let (run, request) = graph_admit(&store, "local:batch", &format!("wait-{i}"));
        graph_wait(&store, run, source, &request);
    }
    graph_complete(&store, source, &sr);
    assert_eq!(store.resume_task_events("first").unwrap().len(), 32);
    assert!(store.is_current_attempt(34).unwrap());
    assert_eq!(
        store.get_task_run(34).unwrap().unwrap().status,
        "waiting_event"
    );
    assert_eq!(store.resume_task_events("second").unwrap().len(), 1);
    assert!(store.resume_task_events("third").unwrap().is_empty());
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM runtime_tasks", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        34
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM runtime_attempts", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        67
    );
}

#[test]
fn waiting_graph_approval_cancel_race_has_at_most_one_bound_continuation() {
    for _ in 0..6 {
        let cfg = scheduler_test_config("NEVER-EXECUTE".into());
        let store = Store::open(&cfg).unwrap();
        let (run, request) = graph_admit(&store, "local:race", "question");
        let tx = store.conn.unchecked_transaction().unwrap();
        let approval = crate::approval::create_approval_request(
            &store,
            &request,
            &request.input,
            run,
            "decision",
            &cfg.timezone,
        )
        .unwrap();
        tx.commit().unwrap();
        let a = SessionScheduler::new(cfg.clone(), None);
        let b = SessionScheduler::new(cfg, None);
        a.inner.recovering.store(true, Ordering::SeqCst);
        b.inner.recovering.store(true, Ordering::SeqCst);
        let gate = Arc::new(std::sync::Barrier::new(2));
        let g = gate.clone();
        let aa = a.clone();
        let worker = std::thread::spawn(move || {
            g.wait();
            aa.resolve_approval(approval, "approve", "local:race")
                .unwrap();
        });
        gate.wait();
        b.cancel_task_for_session(run, Some("local:race")).unwrap();
        worker.join().unwrap();
        let state = store
            .get_pending_approval(approval)
            .unwrap()
            .unwrap()
            .status;
        let count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM runtime_attempts WHERE task_id=?1",
                [run],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, if state == "approved" { 2 } else { 1 });
        assert!(matches!(state.as_str(), "approved" | "expired"));
        a.shutdown().unwrap();
        b.shutdown().unwrap();
    }
}

#[test]
fn waiting_graph_delegated_same_conversation_uses_five_slots_and_cleans_up() {
    let cfgroot = tempfile::tempdir().unwrap();
    let mut cfg = RuntimeConfig::test_config_at(cfgroot.path().to_path_buf());
    cfg.codex.bin = write_fake_provider_script(
        &cfg.instance_dir,
        "graph-capacity",
        "#!/bin/sh\nbase=$(dirname \"$0\"); out=''; while [ \"$#\" -gt 0 ]; do if [ \"$1\" = '--output-last-message' ]; then shift; out=\"$1\"; fi; shift; done; touch \"$base/started-$$\"; while [ ! -f \"$base/release\" ]; do sleep 0.02; done; printf 'TELEGRAM_REPLY: capacity complete' > \"$out\"",
        "$base=$PSScriptRoot; $a=$args; $i=[Array]::IndexOf($a,'--output-last-message'); [IO.File]::WriteAllText((Join-Path $base \"started-$PID\"),'started'); while(-not(Test-Path (Join-Path $base 'release'))){Start-Sleep -Milliseconds 20}; [IO.File]::WriteAllText($a[$i+1],'TELEGRAM_REPLY: capacity complete')",
    );
    Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    struct Release(PathBuf, SessionScheduler);
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = fs::write(&self.0, "release");
            let _ = self.1.shutdown();
        }
    }
    let _release = Release(cfg.instance_dir.join("release"), scheduler.clone());
    let mut ids = Vec::new();
    for i in 0..6 {
        let mut req =
            make_stdout_request(SessionKey::local("same-owner"), &format!("independent-{i}"));
        req.delegated = true;
        ids.push(scheduler.enqueue(req).unwrap());
    }
    let started = || {
        fs::read_dir(&cfg.instance_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("started-"))
            .count()
    };
    let until = Instant::now() + Duration::from_secs(10);
    while started() < 5 {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(started(), 5);
    assert_eq!(scheduler.inner.task_state.lock().unwrap().len(), 5);
    assert_eq!(
        scheduler
            .inner
            .lanes
            .lock()
            .unwrap()
            .values()
            .map(|l| l.queue.len())
            .sum::<usize>(),
        1
    );
    fs::write(cfg.instance_dir.join("release"), "release").unwrap();
    wait_for_terminal_tasks(&cfg, &ids);
    scheduler.shutdown().unwrap();
    assert_eq!(started(), 6);
    assert!(scheduler.inner.task_state.lock().unwrap().is_empty());
}

#[test]
fn waiting_graph_normal_binary_wait_event_attempt_outbox_and_restart() {
    use std::sync::atomic::AtomicUsize;
    let stage = Arc::new(AtomicUsize::new(0));
    let step = stage.clone();
    let polls = Arc::new(AtomicUsize::new(0));
    let fetched = polls.clone();
    let server = FakeTelegramServer::start_with_response(move |request| {
        let body = if request.contains("/getMe ") {
            json!({"ok":true,"result":{"is_bot":true,"username":"GraphFixtureBot","has_topics_enabled":true}})
        } else if request.contains("/getUpdates?") {
            fetched.fetch_add(1, Ordering::SeqCst);
            let phase = step.load(Ordering::SeqCst);
            let result = if phase < 3 {
                json!([{"update_id":40+phase,"message":{"from":{"id":321},"chat":{"id":321,"type":"private"},"message_thread_id":7,"text":match phase{0=>"waiter fixture",1=>"independent fixture",_=>"/approve 1"}}}])
            } else {
                json!([])
            };
            json!({"ok":true,"result":result})
        } else {
            json!({"ok":true,"result":{"message_id":123}})
        };
        (200, body.to_string())
    });
    let root = tempfile::tempdir().unwrap();
    let mut cfg = RuntimeConfig::test_config_at(root.path().to_path_buf());
    struct Evidence(PathBuf);
    impl Drop for Evidence {
        fn drop(&mut self) {
            if let Some(target) = std::env::var_os("COCONUTCLAW_WAITING_GRAPH_ARTIFACTS") {
                let target = PathBuf::from(target).join("fixture-diagnostics");
                let _ = fs::create_dir_all(&target);
                if let Ok(files) = fs::read_dir(&self.0) {
                    for entry in files.flatten() {
                        let name = entry.file_name();
                        if entry.path().is_file()
                            && (name.to_string_lossy().starts_with("graph-") || name == "state.db")
                        {
                            let _ = fs::copy(entry.path(), target.join(name));
                        }
                    }
                }
            }
        }
    }
    let _evidence = Evidence(cfg.instance_dir.clone());
    cfg.codex.bin = write_fake_provider_script(
        &cfg.instance_dir,
        "graph-provider",
        "#!/bin/sh\nbase=$(dirname \"$0\"); out=''; while [ \"$#\" -gt 0 ]; do if [ \"$1\" = '--output-last-message' ]; then shift; out=\"$1\"; fi; shift; done; input=$(cat); printf '%s' \"$input\" > \"$base/graph-last-context\"; echo run >> \"$base/graph-calls\"; case \"$input\" in *'independent fixture'*) printf 'TELEGRAM_REPLY: graph-independent' > \"$out\" ;; *'waiter fixture'*) if [ ! -f \"$base/waited\" ]; then touch \"$base/waited\"; printf 'WAIT_FOR_TASK: 1' > \"$out\"; else printf 'TELEGRAM_REPLY: graph-final' > \"$out\"; fi ;; *) printf 'TELEGRAM_REPLY: graph-independent' > \"$out\" ;; esac",
        "$a=$args; $i=[Array]::IndexOf($a,'--output-last-message'); $prompt=[Console]::In.ReadToEnd(); [IO.File]::WriteAllText((Join-Path $PSScriptRoot 'graph-last-context'),$prompt); [IO.File]::AppendAllText((Join-Path $PSScriptRoot 'graph-calls'),\"run`n\"); $flag=Join-Path $PSScriptRoot 'waited'; if($prompt.Contains('independent fixture')){$text='TELEGRAM_REPLY: graph-independent'}elseif($prompt.Contains('waiter fixture')){if(-not(Test-Path $flag)){[IO.File]::WriteAllText($flag,'waited');$text='WAIT_FOR_TASK: 1'}else{$text='TELEGRAM_REPLY: graph-final'}}else{$text='TELEGRAM_REPLY: graph-independent'}; [IO.File]::WriteAllText($a[$i+1],$text)",
    );
    let store = Store::open(&cfg).unwrap();
    let mut dependency = make_request(
        SessionKey::telegram("321#7"),
        "telegram",
        "dependency fixture",
    );
    dependency.source_user_id = Some("321".into());
    dependency.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some("7".into()),
    };
    let tx = store.conn.unchecked_transaction().unwrap();
    let (run, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some("graph-dependency"),
            &dependency,
            "now",
        )
        .unwrap();
    assert_eq!(run, 1);
    crate::approval::create_approval_request(
        &store,
        &dependency,
        &dependency.input,
        run,
        "genuine decision",
        &cfg.timezone,
    )
    .unwrap();
    tx.commit().unwrap();
    let binary = crate::util::test_binary_path();
    let before = blake3::hash(&fs::read(&binary).unwrap()).to_string();
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
        let log = cfg.instance_dir.join(format!("graph-normal-{round}.log"));
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
                .stderr(std::process::Stdio::from(fs::File::create(log).unwrap()))
                .spawn()
                .unwrap(),
        )
    };
    let wait = |id, status: &str| {
        let until = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(r) = store.get_task_run(id).unwrap() {
                assert_ne!(r.status, "failed", "{:?}", r.error_summary);
                let delivered: bool = store
                    .conn
                    .query_row(
                        "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                        [id],
                        |r| r.get(0),
                    )
                    .unwrap();
                if r.status == status && delivered {
                    break;
                }
            }
            assert!(
                Instant::now() < until,
                "graph run{id} did not reach {status}: {:?}; all runs: {:?}",
                store.get_task_run(id).unwrap(),
                (1..=5)
                    .map(|i| store.get_task_run(i).unwrap())
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let first = start(0);
    wait(2, "waiting_event");
    stage.store(1, Ordering::SeqCst);
    wait(3, "completed");
    assert_eq!(
        fs::read_to_string(cfg.instance_dir.join("graph-calls"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    drop(first);
    stage.store(2, Ordering::SeqCst);
    let second = start(1);
    wait(5, "completed");
    assert_eq!(store.task_identity(5).unwrap(), 2);
    assert_eq!(store.task_identity(4).unwrap(), 1);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ordinal FROM runtime_attempts WHERE run_id=5",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    drop(second);
    stage.store(3, Ordering::SeqCst);
    let before_polls = polls.load(Ordering::SeqCst);
    let third = start(2);
    let until = Instant::now() + Duration::from_secs(10);
    while polls.load(Ordering::SeqCst) == before_polls {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(third);
    let context = fs::read_to_string(cfg.instance_dir.join("graph-last-context")).unwrap();
    let progress = context
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v["dependency_run_id"] == 4)
        .expect("native resumption receives structured triggering outcome");
    assert_eq!(progress["task_id"], 2);
    assert_eq!(progress["attempt"], 2);
    assert!(
        progress["previous_outcome"]
            .as_str()
            .unwrap()
            .contains("WAIT_FOR_TASK: 1")
    );
    assert!(
        progress["dependency_output"]
            .as_str()
            .unwrap()
            .contains("graph-independent")
    );
    assert_eq!(
        fs::read_to_string(cfg.instance_dir.join("graph-calls"))
            .unwrap()
            .lines()
            .count(),
        4
    );
    assert_eq!(
        server
            .full_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.contains("graph-final"))
            .count(),
        1
    );
    assert_eq!(
        before,
        blake3::hash(&fs::read(&binary).unwrap()).to_string()
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM runtime_tasks", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        3
    );
    if let Some(path) = std::env::var_os("COCONUTCLAW_WAITING_GRAPH_ARTIFACTS") {
        let path = PathBuf::from(path);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("normal-graph-acceptance.json"),serde_json::to_string_pretty(&json!({"binary":binary,"blake3_unchanged":before,"starts":3,"provider_invocations":4,"logical_tasks":3,"runs":5,"waiting_run":2,"resumed_run":5,"final_sends":1,"independent_run":3})).unwrap()).unwrap();
        for round in 0..3 {
            fs::copy(
                cfg.instance_dir.join(format!("graph-normal-{round}.log")),
                path.join(format!("normal-{round}.log")),
            )
            .unwrap();
        }
    }
}

pub(crate) fn write_fake_provider_script(
    dir: &std::path::Path,
    stem: &str,
    unix_body: &str,
    windows_body: &str,
) -> String {
    let (path, command) = if cfg!(windows) {
        let path = dir.join(format!("{stem}.ps1"));
        // Windows PowerShell 5.1 rejects the provider's bare `-` stdin marker
        // when a script is launched with -File; PowerShell 7 passes it through.
        let command = format!(
            "pwsh -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
            path.display()
        );
        (path, command)
    } else {
        let path = dir.join(format!("{stem}.sh"));
        let command = path.display().to_string();
        (path, command)
    };

    fs::write(
        &path,
        if cfg!(windows) {
            windows_body
        } else {
            unix_body
        },
    )
    .expect("write provider script");

    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod provider script");

    command
}

fn scheduler_test_config(provider_bin: String) -> RuntimeConfig {
    let mut cfg = RuntimeConfig::test_config();
    cfg.codex.bin = provider_bin;
    Store::open(&cfg).expect("bootstrap scheduler fixture");
    cfg
}

fn make_request(session: SessionKey, channel: &str, prompt: &str) -> TaskRequest {
    let source = match channel {
        "telegram" => TaskSource::Telegram,
        _ => TaskSource::Local,
    };
    TaskRequest {
        session,
        source,
        input: TurnInput {
            input_type: InputType::Text,
            user_text: prompt.to_string(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: channel.to_string(),
        },
        update_id: None,
        media: None,
        quoted: QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        delivery: DeliveryTarget::Stdout,
        source_user_id: None,
        progress_message_id: None,
        delegated: false,
    }
}

fn make_stdout_request(session: SessionKey, prompt: &str) -> TaskRequest {
    make_request(session, "local", prompt)
}

#[test]
fn waiting_graph_approval_continuation_has_one_logical_owner() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let request = make_stdout_request(SessionKey::local("graph"), "approved work");
    let tx = store.conn.unchecked_transaction().unwrap();
    let (run, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some("graph-request"),
            &request,
            "now",
        )
        .unwrap();
    let approval = crate::approval::create_approval_request(
        &store,
        &request,
        &request.input,
        run,
        "decision",
        &cfg.timezone,
    )
    .unwrap();
    tx.commit().unwrap();
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler
        .inner
        .recovering
        .store(true, std::sync::atomic::Ordering::SeqCst);
    scheduler
        .resolve_approval(approval, "approve", &request.session.id())
        .unwrap();
    let child: i64 = store
        .conn
        .query_row(
            "SELECT task_run_id FROM runtime_inbox WHERE event_key=?1",
            [format!("approval:{approval}")],
            |r| r.get(0),
        )
        .unwrap();
    let linked: (i64, i64, i64) = store
        .conn
        .query_row(
            "SELECT task_id,ordinal,parent_run_id FROM runtime_attempts WHERE run_id=?1",
            [child],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(linked, (run, 2, run));
    assert_ne!(run, child);
    scheduler.shutdown().unwrap();
}

fn wait_for_terminal_tasks(cfg: &RuntimeConfig, task_ids: &[i64]) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let store = crate::store::Store::open(cfg).expect("open store");
        let all_done = task_ids.iter().all(|task_id| {
            store
                .get_task_run(*task_id)
                .expect("task lookup")
                .and_then(|task| match task.status.as_str() {
                    "completed" | "failed" | "cancelled" => Some(task),
                    _ => None,
                })
                .is_some()
        });
        if all_done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "tasks did not reach terminal state"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub(crate) struct FakeTelegramServer {
    pub(crate) base_url: String,
    pub(crate) requests: Arc<Mutex<Vec<String>>>,
    pub(crate) full_requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl FakeTelegramServer {
    pub(crate) fn start() -> Self {
        Self::start_with_response(|_| {
            (
                200,
                r#"{"ok":true,"result":{"message_id":123,"message_thread_id":0}}"#.into(),
            )
        })
    }

    pub(crate) fn start_with_response(
        mut respond: impl FnMut(&str) -> (u16, String) + Send + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake telegram server");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking listener");
        let addr = listener.local_addr().expect("listener addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let requests_clone = Arc::clone(&requests);
        let full_requests = Arc::new(Mutex::new(Vec::new()));
        let full_requests_clone = full_requests.clone();
        let stop_clone = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !stop_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                        let mut buf = [0_u8; 8192];
                        let mut data = Vec::new();
                        let deadline = Instant::now() + Duration::from_secs(10);
                        loop {
                            if Instant::now() >= deadline {
                                break;
                            }
                            let read = match stream.read(&mut buf) {
                                Ok(0) => break,
                                Ok(read) => read,
                                // A short read timeout means the body is still
                                // arriving, not that the request ended.
                                Err(err)
                                    if matches!(
                                        err.kind(),
                                        std::io::ErrorKind::WouldBlock
                                            | std::io::ErrorKind::TimedOut
                                    ) =>
                                {
                                    continue;
                                }
                                Err(_) => break,
                            };
                            data.extend_from_slice(&buf[..read]);
                            let request = String::from_utf8_lossy(&data);
                            if let Some((header, body)) = request.split_once("\r\n\r\n") {
                                let low = header.to_ascii_lowercase();
                                let len = low.lines().find_map(|line| {
                                    line.strip_prefix("content-length:")
                                        .and_then(|s| s.trim().parse::<usize>().ok())
                                });
                                let complete = match len {
                                    Some(len) => body.len() >= len,
                                    None if low.contains("transfer-encoding: chunked") => {
                                        body.ends_with("\r\n0\r\n\r\n") || body == "0\r\n\r\n"
                                    }
                                    None => true,
                                };
                                if complete {
                                    break;
                                }
                            }
                        }
                        let request = String::from_utf8_lossy(&data).to_string();
                        full_requests_clone.lock().unwrap().push(request.clone());
                        let path = request
                            .lines()
                            .next()
                            .and_then(|line| line.split_whitespace().nth(1))
                            .unwrap_or("/")
                            .to_string();
                        requests_clone.lock().expect("requests lock").push(path);
                        let (status, body) = respond(&request);
                        let response = format!(
                            "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            base_url: format!("http://{addr}/bot123:token"),
            requests,
            full_requests,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for FakeTelegramServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(
            self.base_url
                .trim_start_matches("http://")
                .split('/')
                .next()
                .unwrap_or_default(),
        );
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[test]
fn request_from_resume_payload_restores_owned_attachment() {
    let payload = r#"{
        "session_id": "telegram:321",
        "channel": "telegram",
        "input_type": "document",
        "user_text": "review this",
        "asr_text": "",
        "attachment_type": "application/pdf",
        "attachment_path": "/tmp/approval.pdf",
        "attachment_owned": true,
        "dispatch": {"kind": "telegram", "chat_id": "321"},
        "quoted": {}
    }"#;

    let request = request_from_resume_payload(payload).expect("resume request");

    assert!(request.input.attachment_owned);
    assert_eq!(
        request
            .input
            .attachment_path
            .as_deref()
            .map(|path| path.to_string_lossy().to_string()),
        Some("/tmp/approval.pdf".to_string())
    );
}

#[test]
fn build_resume_payload_keeps_attachment_ownership() {
    let request = TaskRequest {
        session: SessionKey::telegram("321"),
        source: TaskSource::Telegram,
        input: TurnInput {
            input_type: InputType::Document,
            user_text: "ship it".to_string(),
            attachment_type: Some("application/pdf".to_string()),
            attachment_path: Some(PathBuf::from("/tmp/input.pdf")),
            attachment_owned: true,
            supplemental_context: None,
            channel: "telegram".to_string(),
        },
        update_id: Some("evt-1".to_string()),
        media: None,
        quoted: QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        delivery: DeliveryTarget::Telegram {
            chat_id: "321".into(),
            thread_id: None,
        },
        source_user_id: Some("U123".to_string()),
        progress_message_id: None,
        delegated: false,
    };

    let payload = build_resume_payload(&request, &request.input).expect("resume payload");

    assert!(payload.contains(r#""attachment_owned":true"#));
}

#[test]
fn different_sessions_can_run_in_parallel() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let markers_dir = tmp_dir.path().join("parallel-markers");
    fs::create_dir_all(&markers_dir).expect("create markers dir");
    let start_one = markers_dir.join("start-one");
    let start_two = markers_dir.join("start-two");
    let saw_one = markers_dir.join("saw-one");
    let saw_two = markers_dir.join("saw-two");
    let provider_bin = write_fake_provider_script(
        tmp_dir.path(),
        "parallel-provider",
        &format!(
            r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
if [[ "${{!i}}" == "--output-last-message" ]]; then
    j=$((i + 1))
    OUT_FILE="${{!j}}"
fi
done
if [[ "${{@: -1}}" == "-" ]]; then
    CONTEXT="$(cat)"
else
    CONTEXT="${{@: -1}}"
fi
if [[ "$CONTEXT" == *"parallel-one"* ]]; then
touch "{start_one}"
for _ in $(seq 1 40); do
    if [[ -f "{start_two}" ]]; then
        touch "{saw_one}"
        break
    fi
    sleep 0.1
done
printf 'TELEGRAM_REPLY: one\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"parallel-two"* ]]; then
touch "{start_two}"
for _ in $(seq 1 40); do
    if [[ -f "{start_one}" ]]; then
        touch "{saw_two}"
        break
    fi
    sleep 0.1
done
printf 'TELEGRAM_REPLY: two\n' > "$OUT_FILE"
else
printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
            start_one = start_one.display(),
            start_two = start_two.display(),
            saw_one = saw_one.display(),
            saw_two = saw_two.display()
        ),
        &format!(
            r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
if ($args[$i] -eq "--output-last-message") {{
    $outFile = $args[$i + 1]
    $i++
}}
}}
if ($args[$args.Count - 1] -eq "-") {{
    $context = [Console]::In.ReadToEnd()
}} else {{
    $context = $args[$args.Count - 1]
}}
if ($context -match "parallel-one") {{
New-Item -ItemType File -Path "{start_one}" -Force | Out-Null
for ($i = 0; $i -lt 40; $i++) {{
    if (Test-Path "{start_two}") {{
        New-Item -ItemType File -Path "{saw_one}" -Force | Out-Null
        break
    }}
    Start-Sleep -Milliseconds 100
}}
"TELEGRAM_REPLY: one" | Set-Content -Path $outFile
}} elseif ($context -match "parallel-two") {{
New-Item -ItemType File -Path "{start_two}" -Force | Out-Null
for ($i = 0; $i -lt 40; $i++) {{
    if (Test-Path "{start_one}") {{
        New-Item -ItemType File -Path "{saw_two}" -Force | Out-Null
        break
    }}
    Start-Sleep -Milliseconds 100
}}
"TELEGRAM_REPLY: two" | Set-Content -Path $outFile
}} else {{
"TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
            start_one = start_one.display(),
            start_two = start_two.display(),
            saw_one = saw_one.display(),
            saw_two = saw_two.display()
        ),
    );
    let cfg = scheduler_test_config(provider_bin);
    let scheduler = SessionScheduler::new(cfg.clone(), None);

    let task_one = scheduler
        .enqueue(make_stdout_request(
            SessionKey::local("parallel-1"),
            "parallel-one",
        ))
        .expect("enqueue one");
    let task_two = scheduler
        .enqueue(make_stdout_request(
            SessionKey::local("parallel-2"),
            "parallel-two",
        ))
        .expect("enqueue two");
    wait_for_terminal_tasks(&cfg, &[task_one, task_two]);

    assert!(
        saw_one.exists() && saw_two.exists(),
        "expected both sessions to observe the other start marker, markers={:?}",
        [start_one, start_two, saw_one, saw_two]
    );
}

#[test]
fn same_session_stays_fifo() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let events_path = tmp_dir.path().join("fifo-events.log");
    let provider_bin = write_fake_provider_script(
        tmp_dir.path(),
        "fifo-provider",
        &format!(
            r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
if [[ "${{!i}}" == "--output-last-message" ]]; then
    j=$((i + 1))
    OUT_FILE="${{!j}}"
fi
done
if [[ "${{@: -1}}" == "-" ]]; then
    CONTEXT="$(cat)"
else
    CONTEXT="${{@: -1}}"
fi
if [[ "$CONTEXT" == *"fifo-second"* ]]; then
printf 'start:second\n' >> "{events}"
printf 'done:second\n' >> "{events}"
printf 'TELEGRAM_REPLY: second\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"fifo-first"* ]]; then
printf 'start:first\n' >> "{events}"
sleep 2
printf 'done:first\n' >> "{events}"
printf 'TELEGRAM_REPLY: first\n' > "$OUT_FILE"
else
printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
            events = events_path.display()
        ),
        &format!(
            r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
if ($args[$i] -eq "--output-last-message") {{
    $outFile = $args[$i + 1]
    $i++
}}
}}
if ($args[$args.Count - 1] -eq "-") {{
    $context = [Console]::In.ReadToEnd()
}} else {{
    $context = $args[$args.Count - 1]
}}
if ($context -match "fifo-second") {{
"start:second" | Add-Content -Path "{events}"
"done:second" | Add-Content -Path "{events}"
"TELEGRAM_REPLY: second" | Set-Content -Path $outFile
}} elseif ($context -match "fifo-first") {{
"start:first" | Add-Content -Path "{events}"
Start-Sleep -Seconds 2
"done:first" | Add-Content -Path "{events}"
"TELEGRAM_REPLY: first" | Set-Content -Path $outFile
}} else {{
"TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
            events = events_path.display()
        ),
    );
    let cfg = scheduler_test_config(provider_bin);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let session = SessionKey::local("shared-session");

    let first = scheduler
        .enqueue(make_stdout_request(session.clone(), "fifo-first"))
        .expect("enqueue first");
    let second = scheduler
        .enqueue(make_stdout_request(session, "fifo-second"))
        .expect("enqueue second");
    wait_for_terminal_tasks(&cfg, &[first, second]);

    let events = fs::read_to_string(&events_path).expect("read events");
    let lines: Vec<&str> = events.lines().collect();
    assert_eq!(
        lines,
        vec!["start:first", "done:first", "start:second", "done:second"],
        "same-session work should remain FIFO"
    );
}

#[test]
fn lane_is_released_when_store_reopen_fails_after_task_run() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let db_parent = tmp_dir.path().join("volatile-db");
    fs::create_dir_all(&db_parent).expect("create db dir");
    let broken_marker = tmp_dir.path().join("store-broken");
    let provider_bin = write_fake_provider_script(
        tmp_dir.path(),
        "store-reopen-failure-provider",
        &format!(
            r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
if [[ "${{!i}}" == "--output-last-message" ]]; then
    j=$((i + 1))
    OUT_FILE="${{!j}}"
fi
done
if [[ "${{@: -1}}" == "-" ]]; then
    CONTEXT="$(cat)"
else
    CONTEXT="${{@: -1}}"
fi
if [[ "$CONTEXT" == *"break-store"* ]]; then
rm -rf "{db_parent}"
touch "{db_parent}"
touch "{broken_marker}"
printf 'TELEGRAM_REPLY: first\n' > "$OUT_FILE"
else
printf 'TELEGRAM_REPLY: second\n' > "$OUT_FILE"
fi
"#,
            db_parent = db_parent.display(),
            broken_marker = broken_marker.display()
        ),
        &format!(
            r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
if ($args[$i] -eq "--output-last-message") {{
    $outFile = $args[$i + 1]
    $i++
}}
}}
if ($args[$args.Count - 1] -eq "-") {{
    $context = [Console]::In.ReadToEnd()
}} else {{
    $context = $args[$args.Count - 1]
}}
if ($context -match "break-store") {{
if (Test-Path "{db_parent}") {{
    Remove-Item -LiteralPath "{db_parent}" -Recurse -Force
}}
New-Item -ItemType File -Path "{db_parent}" -Force | Out-Null
New-Item -ItemType File -Path "{broken_marker}" -Force | Out-Null
"TELEGRAM_REPLY: first" | Set-Content -Path $outFile
}} else {{
"TELEGRAM_REPLY: second" | Set-Content -Path $outFile
}}
"#,
            db_parent = db_parent.display(),
            broken_marker = broken_marker.display()
        ),
    );
    let mut cfg = scheduler_test_config(provider_bin);
    cfg.sqlite_db_path = db_parent.join("state.sqlite");
    Store::open(&cfg).expect("bootstrap relocated scheduler fixture");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let session = SessionKey::local("reopen-failure");
    let session_id = session.id();

    let first = scheduler
        .enqueue(make_stdout_request(session.clone(), "break-store"))
        .expect("enqueue first");

    let deadline = Instant::now() + Duration::from_secs(10);
    while !broken_marker.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for store break marker"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    while {
        let task_active = scheduler
            .inner
            .task_state
            .lock()
            .expect("task_state")
            .contains_key(&first);
        let lane_active = scheduler
            .inner
            .lanes
            .lock()
            .expect("lanes")
            .contains_key(&session_id);
        task_active || lane_active
    } {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for failed store reopen cleanup"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    assert!(
        !scheduler
            .inner
            .task_state
            .lock()
            .expect("task_state")
            .contains_key(&first),
        "failed reopen should clear task state"
    );
    assert!(
        !scheduler
            .inner
            .lanes
            .lock()
            .expect("lanes")
            .contains_key(&session_id),
        "failed reopen should release the session lane"
    );
}

#[test]
fn telegram_chats_can_run_in_parallel() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let markers_dir = tmp_dir.path().join("telegram-parallel-markers");
    fs::create_dir_all(&markers_dir).expect("create markers dir");
    let start_one = markers_dir.join("start-321");
    let start_two = markers_dir.join("start-999");
    let saw_one = markers_dir.join("saw-321");
    let saw_two = markers_dir.join("saw-999");
    let provider_bin = write_fake_provider_script(
        tmp_dir.path(),
        "telegram-parallel-provider",
        &format!(
            r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
if [[ "${{!i}}" == "--output-last-message" ]]; then
    j=$((i + 1))
    OUT_FILE="${{!j}}"
fi
done
if [[ "${{@: -1}}" == "-" ]]; then
    CONTEXT="$(cat)"
else
    CONTEXT="${{@: -1}}"
fi
if [[ "$CONTEXT" == *"telegram-parallel-one"* ]]; then
touch "{start_one}"
for _ in $(seq 1 40); do
    if [[ -f "{start_two}" ]]; then
        touch "{saw_one}"
        break
    fi
    sleep 0.1
done
printf 'TELEGRAM_REPLY: one\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"telegram-parallel-two"* ]]; then
touch "{start_two}"
for _ in $(seq 1 40); do
    if [[ -f "{start_one}" ]]; then
        touch "{saw_two}"
        break
    fi
    sleep 0.1
done
printf 'TELEGRAM_REPLY: two\n' > "$OUT_FILE"
else
printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
            start_one = start_one.display(),
            start_two = start_two.display(),
            saw_one = saw_one.display(),
            saw_two = saw_two.display()
        ),
        &format!(
            r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
if ($args[$i] -eq "--output-last-message") {{
    $outFile = $args[$i + 1]
    $i++
}}
}}
if ($args[$args.Count - 1] -eq "-") {{
    $context = [Console]::In.ReadToEnd()
}} else {{
    $context = $args[$args.Count - 1]
}}
if ($context -match "telegram-parallel-one") {{
New-Item -ItemType File -Path "{start_one}" -Force | Out-Null
for ($i = 0; $i -lt 40; $i++) {{
    if (Test-Path "{start_two}") {{
        New-Item -ItemType File -Path "{saw_one}" -Force | Out-Null
        break
    }}
    Start-Sleep -Milliseconds 100
}}
"TELEGRAM_REPLY: one" | Set-Content -Path $outFile
}} elseif ($context -match "telegram-parallel-two") {{
New-Item -ItemType File -Path "{start_two}" -Force | Out-Null
for ($i = 0; $i -lt 40; $i++) {{
    if (Test-Path "{start_one}") {{
        New-Item -ItemType File -Path "{saw_two}" -Force | Out-Null
        break
    }}
    Start-Sleep -Milliseconds 100
}}
"TELEGRAM_REPLY: two" | Set-Content -Path $outFile
}} else {{
"TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
            start_one = start_one.display(),
            start_two = start_two.display(),
            saw_one = saw_one.display(),
            saw_two = saw_two.display()
        ),
    );
    let cfg = scheduler_test_config(provider_bin);
    let scheduler = SessionScheduler::new(cfg.clone(), None);

    let task_one = scheduler
        .enqueue(make_request(
            SessionKey::telegram("321"),
            "telegram",
            "telegram-parallel-one",
        ))
        .expect("enqueue one");
    let task_two = scheduler
        .enqueue(make_request(
            SessionKey::telegram("999"),
            "telegram",
            "telegram-parallel-two",
        ))
        .expect("enqueue two");
    wait_for_terminal_tasks(&cfg, &[task_one, task_two]);

    assert!(
        saw_one.exists() && saw_two.exists(),
        "expected both telegram chats to observe the other start marker, markers={:?}",
        [start_one, start_two, saw_one, saw_two]
    );
}

#[test]
fn telegram_same_chat_stays_fifo() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let events_path = tmp_dir.path().join("telegram-fifo-events.log");
    let provider_bin = write_fake_provider_script(
        tmp_dir.path(),
        "telegram-fifo-provider",
        &format!(
            r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
if [[ "${{!i}}" == "--output-last-message" ]]; then
    j=$((i + 1))
    OUT_FILE="${{!j}}"
fi
done
if [[ "${{@: -1}}" == "-" ]]; then
    CONTEXT="$(cat)"
else
    CONTEXT="${{@: -1}}"
fi
if [[ "$CONTEXT" == *"telegram-fifo-second"* ]]; then
printf 'start:second\n' >> "{events}"
printf 'done:second\n' >> "{events}"
printf 'TELEGRAM_REPLY: second\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"telegram-fifo-first"* ]]; then
printf 'start:first\n' >> "{events}"
sleep 2
printf 'done:first\n' >> "{events}"
printf 'TELEGRAM_REPLY: first\n' > "$OUT_FILE"
else
printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
            events = events_path.display()
        ),
        &format!(
            r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
if ($args[$i] -eq "--output-last-message") {{
    $outFile = $args[$i + 1]
    $i++
}}
}}
if ($args[$args.Count - 1] -eq "-") {{
    $context = [Console]::In.ReadToEnd()
}} else {{
    $context = $args[$args.Count - 1]
}}
if ($context -match "telegram-fifo-second") {{
"start:second" | Add-Content -Path "{events}"
"done:second" | Add-Content -Path "{events}"
"TELEGRAM_REPLY: second" | Set-Content -Path $outFile
}} elseif ($context -match "telegram-fifo-first") {{
"start:first" | Add-Content -Path "{events}"
Start-Sleep -Seconds 2
"done:first" | Add-Content -Path "{events}"
"TELEGRAM_REPLY: first" | Set-Content -Path $outFile
}} else {{
"TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
            events = events_path.display()
        ),
    );
    let cfg = scheduler_test_config(provider_bin);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let session = SessionKey::telegram("321");

    let first = scheduler
        .enqueue(make_request(
            session.clone(),
            "telegram",
            "telegram-fifo-first",
        ))
        .expect("enqueue first");
    let second = scheduler
        .enqueue(make_request(session, "telegram", "telegram-fifo-second"))
        .expect("enqueue second");
    wait_for_terminal_tasks(&cfg, &[first, second]);

    let events = fs::read_to_string(&events_path).expect("read events");
    let lines: Vec<&str> = events.lines().collect();
    assert_eq!(
        lines,
        vec!["start:first", "done:first", "start:second", "done:second"],
        "same telegram chat should remain FIFO"
    );
}

#[test]
fn cleanup_attachment_from_resume_payload_removes_owned_file() {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let attachment_path = tmp_dir.path().join("approval.txt");
    fs::write(&attachment_path, "pending").expect("write attachment");
    let payload = json!({
        "attachment_owned": true,
        "attachment_path": attachment_path.display().to_string(),
    })
    .to_string();

    cleanup_attachment_from_resume_payload(&payload).expect("cleanup payload");

    assert!(!attachment_path.exists());
}

#[test]
fn render_active_tasks_for_session_hides_other_sessions() {
    let cfg = RuntimeConfig::test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:321".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: Some("evt-1".to_string()),
            prompt: "session one".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert task one");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:999".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("999".to_string()),
            source_user_id: None,
            update_id: Some("evt-2".to_string()),
            prompt: "session two".to_string(),
            created_at: "2026-04-22T10:00:01+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert task two");

    let scheduler = SessionScheduler::new(cfg, None);
    let output = scheduler
        .render_active_tasks_for_session("telegram:321")
        .expect("render tasks");

    assert!(output.contains("session one"));
    assert!(!output.contains("session two"));
    assert!(!output.contains("telegram:999"));
}
