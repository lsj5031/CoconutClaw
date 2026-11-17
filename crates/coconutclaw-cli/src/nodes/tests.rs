use super::*;
use crate::{
    delivery::{DeliveryTarget, TaskSource},
    scheduler::TaskRequest,
    session::SessionKey,
};

fn fixture(role: Role, seed: u8) -> (tempfile::TempDir, RuntimeConfig) {
    let root = tempfile::tempdir().unwrap();
    #[cfg(windows)]
    {
        let account = format!(
            "{}\\{}",
            std::env::var("USERDOMAIN").unwrap(),
            std::env::var("USERNAME").unwrap()
        );
        assert!(
            std::process::Command::new("icacls.exe")
                .arg(root.path())
                .args([
                    "/inheritance:r",
                    "/grant:r",
                    &format!("{account}:(OI)(CI)F"),
                    "*S-1-5-18:(OI)(CI)F",
                    "*S-1-5-32-544:(OI)(CI)F"
                ])
                .output()
                .unwrap()
                .status
                .success()
        );
    }
    let cfg = RuntimeConfig::test_config_at(root.path().to_path_buf());
    for dir in [&cfg.runtime_dir, &cfg.tmp_dir, &cfg.instance_dir] {
        fs::create_dir_all(dir).unwrap();
    }
    fs::create_dir_all(directory(&cfg)).unwrap();
    let key = SecretKey::from([seed; 32]);
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let card = Card {
        version: 1,
        role,
        id: key.public(),
        address: socket.local_addr().unwrap(),
        revision: REVISION.into(),
    };
    create_private(&directory(&cfg).join("device-key"), &[seed; 32]).unwrap();
    create_private(
        &directory(&cfg).join("local.json"),
        &serde_json::to_vec(&Local {
            card,
            workspace: (role == Role::Runner).then(|| cfg.instance_dir.clone()),
        })
        .unwrap(),
    )
    .unwrap();
    (root, cfg)
}
fn pair(a: &RuntimeConfig, b: &RuntimeConfig) {
    let store = Store::open(a).unwrap();
    initialize(&store).unwrap();
    let card = local(b).unwrap().card;
    store
        .conn
        .execute(
            "INSERT INTO paired_nodes(id,card) VALUES(?1,?2)",
            params![card.id.to_string(), serde_json::to_string(&card).unwrap()],
        )
        .unwrap();
}
#[cfg(windows)]
#[test]
fn windows_rechecks_allowlist_custody_before_peer_dispatch() {
    let (_core, core) = fixture(Role::Coordinator, 81);
    let (_node, node) = fixture(Role::Runner, 82);
    pair(&node, &core);
    let peer = local(&core).unwrap().card.id;
    let runner = Arc::new(Mutex::new(Runner::open(node.clone()).unwrap()));
    let handler = transport::Handler::new(runner);
    assert!(matches!(
        handler.dispatch(peer, Request::Ping).unwrap(),
        Reply::Pong
    ));
    assert!(
        std::process::Command::new("icacls.exe")
            .arg(&node.sqlite_db_path)
            .args(["/grant", "*S-1-1-0:R"])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(handler.dispatch(peer, Request::Ping).is_err());
    assert!(Runner::open(node.clone()).is_err());
    assert!(
        std::process::Command::new("icacls.exe")
            .arg(&node.sqlite_db_path)
            .args(["/remove:g", "*S-1-1-0"])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(matches!(
        handler.dispatch(peer, Request::Ping).unwrap(),
        Reply::Pong
    ));
}

fn request() -> TaskRequest {
    TaskRequest {
        session: SessionKey::from_id("telegram:321#7".into()),
        source: TaskSource::Telegram,
        source_user_id: Some("321".into()),
        input: crate::TurnInput {
            input_type: crate::InputType::Text,
            user_text: "ordinary text task".into(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "telegram".into(),
        },
        update_id: Some("ordinary-1".into()),
        media: None,
        quoted: crate::QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        delivery: DeliveryTarget::Telegram {
            chat_id: "321".into(),
            thread_id: Some("7".into()),
        },
        progress_message_id: None,
        delegated: false,
    }
}
fn admit(store: &Store, request: &TaskRequest) -> i64 {
    let tx = store.conn.unchecked_transaction().unwrap();
    let (id, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            request.update_id.as_deref(),
            request,
            "fixture",
        )
        .unwrap();
    tx.commit().unwrap();
    id
}

#[test]
fn durable_claim_replay_restart_and_signed_cancellation_do_not_reinvoke() {
    let (_core_root, core) = fixture(Role::Coordinator, 31);
    let (_runner_root, cfg) = fixture(Role::Runner, 32);
    pair(&cfg, &core);
    let core_key = key(&core).unwrap();
    let mut runner = Runner::open(cfg.clone()).unwrap();
    let b = bundle(&core_key, runner.key.public(), "work").unwrap();
    assert!(runner.admit(core_key.public(), &b).unwrap().is_some());
    assert!(runner.admit(core_key.public(), &b).unwrap().is_none());
    let mut forged = Cancellation::sign(&b, &core_key).unwrap();
    forged.assignment = "0".repeat(64);
    assert!(runner.cancel(&b, &forged).is_err());
    runner
        .cancel(&b, &Cancellation::sign(&b, &core_key).unwrap())
        .unwrap();
    runner
        .finish(&b, Outcome::Completed, "late evidence")
        .unwrap();
    let Reply::Evidence { start, result } = runner.evidence(&b).unwrap() else {
        panic!()
    };
    let bytes = result.unwrap();
    let h = History::replay(
        &b.origin,
        core_key.public(),
        b.origin.origin.namespace,
        [b.assignment.clone(), start, bytes.clone()],
    )
    .unwrap();
    assert_eq!(h.jobs[&1].result.as_ref().unwrap().0, Outcome::Unknown);
    drop(runner);
    let mut runner = Runner::open(cfg.clone()).unwrap();
    assert!(runner.admit(core_key.public(), &b).unwrap().is_none());
    let Reply::Evidence { result, .. } = runner.evidence(&b).unwrap() else {
        panic!()
    };
    assert_eq!(result.unwrap(), bytes);
    let b2 = bundle(&core_key, runner.key.public(), "interrupted").unwrap();
    assert!(runner.admit(core_key.public(), &b2).unwrap().is_some());
    drop(runner);
    let mut runner = Runner::open(cfg).unwrap();
    assert!(runner.admit(core_key.public(), &b2).unwrap().is_none());
    let Reply::Evidence { start, result } = runner.evidence(&b2).unwrap() else {
        panic!()
    };
    let h = History::replay(
        &b2.origin,
        core_key.public(),
        b2.origin.origin.namespace,
        [b2.assignment.clone(), start, result.unwrap()],
    )
    .unwrap();
    assert_eq!(h.jobs[&1].result.as_ref().unwrap().0, Outcome::Unknown);
}

#[test]
fn admission_rejects_foreign_tasks_and_route_changes_without_claims() {
    let (_a, core) = fixture(Role::Coordinator, 41);
    let (_b, cfg) = fixture(Role::Runner, 42);
    let (_c, foreign) = fixture(Role::Coordinator, 43);
    pair(&cfg, &core);
    let mut runner = Runner::open(cfg).unwrap();
    let foreign_key = key(&foreign).unwrap();
    let b = bundle(&foreign_key, runner.key.public(), "foreign work").unwrap();
    assert!(runner.admit(foreign_key.public(), &b).is_err());
    assert!(runner.admit(key(&core).unwrap().public(), &b).is_err());
    let count: i64 = runner
        .store
        .conn
        .query_row("SELECT count(*) FROM node_claims", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    let mut frame = transport::encode(&Request::Begin(b)).unwrap();
    frame.push(b' ');
    assert!(transport::decode::<Request>(&frame).is_err());
}

#[test]
fn node_selection_is_frozen_per_ordinary_admission_and_session_scoped() {
    let (_a, core) = fixture(Role::Coordinator, 51);
    let (_b, runner) = fixture(Role::Runner, 52);
    pair(&core, &runner);
    let store = Store::open(&core).unwrap();
    let r = request();
    let id = local(&runner).unwrap().card.id.to_string();
    selection(&store, &r.session.id(), Some(&id)).unwrap();
    let run = admit(&store, &r);
    selection(&store, &r.session.id(), None).unwrap();
    let saved: String = store
        .conn
        .query_row("SELECT node FROM node_routes WHERE run=?1", [run], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(saved, id);
    let mut second = r.clone();
    second.update_id = Some("ordinary-2".into());
    let next = admit(&store, &second);
    let count: i64 = store
        .conn
        .query_row(
            "SELECT count(*) FROM node_routes WHERE run=?1",
            [next],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        status(&store, Some("telegram:other"))
            .unwrap()
            .contains("Next text task: local")
    );
    assert!(
        status(&store, Some(&r.session.id()))
            .unwrap()
            .contains(&format!("Run #{run}"))
    );
}

#[test]
fn nodes_buttons_use_authenticated_session_tokens_and_do_not_reroute_old_tasks() {
    let (_a, core) = fixture(Role::Coordinator, 71);
    let (_b, runner) = fixture(Role::Runner, 72);
    pair(&core, &runner);
    let store = Store::open(&core).unwrap();
    let request = request();
    let scheduler = crate::scheduler::SessionScheduler::new(core.clone(), None);
    let page =
        crate::telegram_ui::handle(&core, &store, &scheduler, &request.session, None, "/nodes")
            .unwrap()
            .unwrap();
    let page = crate::telegram_ui::decode(&page).unwrap();
    assert!(page.text.contains("Last contact: never"));
    let callback = page.keyboard["inline_keyboard"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|row| row.as_array().unwrap())
        .find(|button| button["text"].as_str().unwrap().starts_with("Use remote"))
        .unwrap()["callback_data"]
        .as_str()
        .unwrap()
        .strip_prefix("ui:")
        .unwrap()
        .to_owned();
    let other = SessionKey::from_id("telegram:321#8".into());
    let rejected = crate::telegram_ui::handle(
        &core,
        &store,
        &scheduler,
        &other,
        None,
        &format!("/button {callback}"),
    )
    .unwrap()
    .unwrap();
    assert!(
        crate::telegram_ui::decode(&rejected)
            .unwrap()
            .text
            .contains("invalid")
    );
    let before = admit(&store, &request);
    crate::telegram_ui::handle(
        &core,
        &store,
        &scheduler,
        &request.session,
        None,
        &format!("/button {callback}"),
    )
    .unwrap()
    .unwrap();
    let mut next = request.clone();
    next.update_id = Some("new-button-task".into());
    let after = admit(&store, &next);
    let routes: Vec<i64> = store
        .conn
        .prepare("SELECT run FROM node_routes ORDER BY run")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(routes, vec![after]);
    assert_ne!(before, after);
    scheduler.shutdown().unwrap();
}

#[test]
fn source_restart_and_late_signed_result_preserve_unknown_without_second_admission() {
    let (_a, core) = fixture(Role::Coordinator, 81);
    let (_b, cfg) = fixture(Role::Runner, 82);
    pair(&core, &cfg);
    pair(&cfg, &core);
    let store = Store::open(&core).unwrap();
    let request = request();
    let run = admit(&store, &request);
    let b = bundle(
        &key(&core).unwrap(),
        key(&cfg).unwrap().public(),
        &request.input.user_text,
    )
    .unwrap();
    {
        let tx = store.conn.unchecked_transaction().unwrap();
        store
            .conn
            .execute(
                "INSERT INTO node_assignments VALUES(?1,?2)",
                params![run, serde_json::to_string(&b).unwrap()],
            )
            .unwrap();
        store
            .reserve_runtime_run(run, &b.origin.hash().unwrap(), &b.assignment)
            .unwrap();
        tx.commit().unwrap();
    }
    let mut runner = Runner::open(cfg.clone()).unwrap();
    assert!(
        runner
            .admit(key(&core).unwrap().public(), &b)
            .unwrap()
            .is_some()
    );
    recover(&store, "restart").unwrap();
    runner
        .finish(&b, Outcome::Completed, "late durable output")
        .unwrap();
    assert!(
        accept(
            &store,
            run,
            &b,
            runner.evidence(&b).unwrap(),
            &core.timezone
        )
        .unwrap()
    );
    let task = store.get_task_run(run).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    assert!(task.error_summary.unwrap().contains("outcome unknown"));
    assert!(
        runner
            .admit(key(&core).unwrap().public(), &b)
            .unwrap()
            .is_none()
    );
    let before: String = store
        .conn
        .query_row(
            "SELECT hex(result) FROM runtime_remote_results WHERE task_run_id=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    drop(runner);
    let runner = Runner::open(cfg).unwrap();
    assert!(
        accept(
            &store,
            run,
            &b,
            runner.evidence(&b).unwrap(),
            &core.timezone
        )
        .unwrap()
    );
    let after: String = store
        .conn
        .query_row(
            "SELECT hex(result) FROM runtime_remote_results WHERE task_run_id=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(before, after);
}

#[cfg(unix)]
#[test]
fn ordinary_production_adapter_over_native_iroh_has_one_invocation_per_assignment() {
    use std::os::unix::fs::PermissionsExt;
    let (_a, mut core) = fixture(Role::Coordinator, 61);
    let (_b, mut cfg) = fixture(Role::Runner, 62);
    pair(&core, &cfg);
    pair(&cfg, &core);
    let script = cfg.instance_dir.join("fake-codex");
    fs::write(&script,"#!/bin/sh\nprintf 'called\\n' >> invocations\nout=''\nwhile [ \"$#\" -gt 0 ]; do if [ \"$1\" = '--output-last-message' ]; then shift; out=\"$1\"; fi; shift; done\nprintf 'SEND_DOCUMENT: /not-a-core-file\\nordinary answer' > \"$out\"\n").unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
    cfg.codex.bin = script.to_string_lossy().into();
    let rt = transport::runtime().unwrap();
    let state = Arc::new(Mutex::new(Runner::open(cfg.clone()).unwrap()));
    let router = rt.block_on(async {
        iroh::protocol::Router::builder(
            transport::endpoint(key(&cfg).unwrap(), local(&cfg).unwrap().card.address)
                .await
                .unwrap(),
        )
        .accept(ALPN, transport::Handler::new(state.clone()))
        .spawn()
    });
    let store = Store::open(&core).unwrap();
    let r = request();
    selection(
        &store,
        &r.session.id(),
        Some(&local(&cfg).unwrap().card.id.to_string()),
    )
    .unwrap();
    let run = admit(&store, &r);
    store.update_task_run_started(run, "fixture").unwrap();
    assert!(execute(&core, &store, run, &r, Arc::new(AtomicBool::new(false))).unwrap());
    assert_eq!(
        store.get_task_run(run).unwrap().unwrap().status,
        "completed"
    );
    let output: String = store
        .conn
        .query_row(
            "SELECT output FROM task_payloads WHERE task_run_id=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    assert!(output.contains("SEND_DOCUMENT: /not-a-core-file"));
    assert!(store.runtime_result_is_literal(run).unwrap());
    // A second ordinary task enters through the real scheduler, with only
    // Telegram HTTP replaced by the existing local mock server.
    let telegram = crate::scheduler::tests::FakeTelegramServer::start();
    core.telegram_api_base = Some(telegram.base_url.clone());
    let scheduler = crate::scheduler::SessionScheduler::new(core.clone(), None);
    let mut ordinary = r.clone();
    ordinary.update_id = Some("ordinary-scheduler-2".into());
    let scheduled = scheduler.enqueue(ordinary).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store.get_task_run(scheduled).unwrap().unwrap().status != "completed" {
        assert!(
            std::time::Instant::now() < deadline,
            "ordinary scheduler failed to complete"
        );
        std::thread::yield_now();
    }
    scheduler.retry_deliveries().unwrap();
    assert!(
        telegram
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.contains("/sendRichMessage") || request.contains("/sendMessage"))
    );
    assert!(
        telegram
            .full_requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.contains("SEND_DOCUMENT"))
    );
    assert!(
        store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [scheduled],
                |row| row.get::<_, bool>(0)
            )
            .unwrap()
    );
    scheduler.shutdown().unwrap();
    assert!(store.runtime_result_is_literal(scheduled).unwrap());
    assert!(
        !telegram
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.contains("/sendDocument"))
    );
    let json: String = store
        .conn
        .query_row(
            "SELECT bundle FROM node_assignments WHERE run=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    let b: Bundle = serde_json::from_str(&json).unwrap();
    rt.block_on(async {
        let endpoint = transport::coordinator(&core).await.unwrap();
        let card = local(&cfg).unwrap().card;
        transport::call(&endpoint, &card, Request::Begin(b.clone()))
            .await
            .unwrap();
        let evidence = transport::call(&endpoint, &card, Request::Status(b.clone()))
            .await
            .unwrap();
        assert!(accept(&store, run, &b, evidence, &core.timezone).unwrap());
        router.shutdown().await.unwrap();
    });
    assert_eq!(
        fs::read_to_string(cfg.instance_dir.join("invocations")).unwrap(),
        "called\ncalled\n"
    );
    assert_eq!(
        state
            .lock()
            .unwrap()
            .store
            .conn
            .query_row("SELECT count(*) FROM node_claims", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn offline_node_preserves_assignment_unknown_and_never_falls_back_locally() {
    let (_a, core) = fixture(Role::Coordinator, 91);
    let (_b, cfg) = fixture(Role::Runner, 92);
    pair(&core, &cfg);
    let store = Store::open(&core).unwrap();
    let request = request();
    selection(
        &store,
        &request.session.id(),
        Some(&local(&cfg).unwrap().card.id.to_string()),
    )
    .unwrap();
    let run = admit(&store, &request);
    store.update_task_run_started(run, "fixture").unwrap();
    assert!(
        execute(
            &core,
            &store,
            run,
            &request,
            Arc::new(AtomicBool::new(false))
        )
        .unwrap()
    );
    let task = store.get_task_run(run).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    assert!(task.error_summary.unwrap().contains("outcome unknown"));
    assert!(
        status(&store, Some(&request.session.id()))
            .unwrap()
            .contains("unavailable")
    );
    let assignments: i64 = store
        .conn
        .query_row(
            "SELECT count(*) FROM node_assignments WHERE run=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(assignments, 1);
    assert!(!cfg.instance_dir.join("invocations").exists());
}

#[cfg(unix)]
#[test]
fn cancellation_over_production_handler_stops_owned_fake_provider_once() {
    use std::os::unix::fs::PermissionsExt;
    let (_a, core) = fixture(Role::Coordinator, 101);
    let (_b, mut cfg) = fixture(Role::Runner, 102);
    pair(&core, &cfg);
    pair(&cfg, &core);
    let script = cfg.instance_dir.join("fake-codex");
    fs::write(&script,"#!/bin/sh\nprintf 'called\\n' >> invocations\nmkfifo provider-gate\nprintf ready > started\nread answer < provider-gate\n").unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
    cfg.codex.bin = script.to_string_lossy().into();
    let rt = transport::runtime().unwrap();
    let state = Arc::new(Mutex::new(Runner::open(cfg.clone()).unwrap()));
    let b = bundle(
        &key(&core).unwrap(),
        key(&cfg).unwrap().public(),
        "owned cancellation task",
    )
    .unwrap();
    rt.block_on(async {
        let router = iroh::protocol::Router::builder(
            transport::endpoint(key(&cfg).unwrap(), local(&cfg).unwrap().card.address)
                .await
                .unwrap(),
        )
        .accept(ALPN, transport::Handler::new(state.clone()))
        .spawn();
        let endpoint = transport::coordinator(&core).await.unwrap();
        let card = local(&cfg).unwrap().card;
        transport::call(&endpoint, &card, Request::Begin(b.clone()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !cfg.instance_dir.join("started").exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fake provider did not reach owned gate");
        transport::call(
            &endpoint,
            &card,
            Request::Cancel(
                b.clone(),
                Cancellation::sign(&b, &key(&core).unwrap()).unwrap(),
            ),
        )
        .await
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let reply = transport::call(&endpoint, &card, Request::Status(b.clone()))
                    .await
                    .unwrap();
                if let Reply::Evidence {
                    start,
                    result: Some(result),
                } = reply
                {
                    break (start, result);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned provider cancellation did not settle");
        let history = History::replay(
            &b.origin,
            b.origin.origin.owner,
            b.origin.origin.namespace,
            [b.assignment.clone(), result.0, result.1],
        )
        .unwrap();
        assert_eq!(
            history.jobs[&1].result.as_ref().unwrap().0,
            Outcome::Cancelled
        );
        // Exact Begin after cancellation is evidence replay, never another FIFO
        // creation or another provider invocation.
        transport::call(&endpoint, &card, Request::Begin(b.clone()))
            .await
            .unwrap();
        router.shutdown().await.unwrap();
    });
    assert_eq!(
        fs::read_to_string(cfg.instance_dir.join("invocations")).unwrap(),
        "called\n"
    );
    drop(state);
    let mut runner = Runner::open(cfg).unwrap();
    assert!(
        runner
            .admit(key(&core).unwrap().public(), &b)
            .unwrap()
            .is_none()
    );
}
