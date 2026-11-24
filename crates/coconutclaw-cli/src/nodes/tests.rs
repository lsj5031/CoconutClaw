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
    let mut cfg = RuntimeConfig::test_config_at(root.path().to_path_buf());
    cfg.codex.bin = "/nonexistent/never-invoke-a-real-provider".into();
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
    let tmp = &node.tmp_dir;
    fs::remove_dir(tmp).unwrap();
    assert!(handler.dispatch(peer, Request::Ping).is_err());
    let target = node.data_dir.join("private-output-fixture");
    fs::create_dir(&target).unwrap();
    assert!(
        std::process::Command::new("cmd.exe")
            .args(["/d", "/c", "mklink", "/J"])
            .arg(tmp)
            .arg(&target)
            .status()
            .unwrap()
            .success()
    );
    assert!(handler.dispatch(peer, Request::Ping).is_err());
    fs::remove_dir(tmp).unwrap();
    fs::create_dir(tmp).unwrap();
    assert!(
        std::process::Command::new("icacls.exe")
            .arg(tmp)
            .args(["/grant", "*S-1-1-0:R"])
            .status()
            .unwrap()
            .success()
    );
    assert!(handler.dispatch(peer, Request::Ping).is_err());
    assert!(
        std::process::Command::new("icacls.exe")
            .arg(tmp)
            .args(["/remove:g", "*S-1-1-0"])
            .status()
            .unwrap()
            .success()
    );
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

fn ingress_select_remote(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &crate::scheduler::SessionScheduler,
) {
    let raw = serde_json::json!({"update_id":30001,"message":{
        "chat":{"id":321,"type":"private"},"from":{"id":321},
        "message_thread_id":7,"text":"/nodes"}})
    .to_string();
    let outcome = crate::loops::webhook::process_webhook_line(cfg, store, scheduler, &raw).unwrap();
    let view = crate::telegram_ui::decode(outcome.output.as_deref().unwrap()).unwrap();
    let token = view.keyboard["inline_keyboard"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|row| row.as_array().unwrap())
        .find(|button| button["text"].as_str().unwrap().starts_with("Use Runner"))
        .unwrap()["callback_data"]
        .as_str()
        .unwrap()
        .to_string();
    // Telegram callback messages can omit the topic field. Only the stored
    // authenticated control may restore it, never a chat-wide fallback.
    let callback = |id, actor, topic: Option<i64>| {
        let mut value = serde_json::json!({"update_id":id,"callback_query":{
            "id":format!("synthetic-{id}"),"from":{"id":actor},"data":token,
            "message":{"message_id":123,"chat":{"id":321,"type":"private"},
                "from":{"id":999,"is_bot":true}}}});
        if let Some(topic) = topic {
            value["callback_query"]["message"]["message_thread_id"] = topic.into();
        }
        value.to_string()
    };
    for raw in [callback(30002, 322, None), callback(30003, 321, Some(8))] {
        crate::loops::webhook::process_webhook_line(cfg, store, scheduler, &raw).unwrap();
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM node_selection", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    let raw = callback(30004, 321, None);
    let mut topics_off = cfg.clone();
    topics_off.telegram_topics_enabled = Some(false);
    crate::loops::webhook::process_webhook_line(
        &topics_off,
        store,
        scheduler,
        &callback(30006, 321, None),
    )
    .unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM node_selection", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    crate::loops::webhook::process_webhook_line(cfg, store, scheduler, &raw).unwrap();
    let selected: String = store
        .conn
        .query_row("SELECT session FROM node_selection", [], |r| r.get(0))
        .unwrap();
    assert_eq!(selected, "telegram:321#7");
    // A replay uses the same canonical custody and does not reapply selection.
    crate::loops::webhook::process_webhook_line(cfg, store, scheduler, &raw).unwrap();
}

#[test]
fn ingress_selection_callback_restores_only_its_authorized_topic() {
    let (_a, mut core) = fixture(Role::Coordinator, 111);
    let (_b, runner) = fixture(Role::Runner, 112);
    pair(&core, &runner);
    let telegram = crate::scheduler::tests::FakeTelegramServer::start();
    core.telegram_api_base = Some(telegram.base_url.clone());
    core.telegram_topics_enabled = Some(true);
    let mut store = Store::open(&core).unwrap();
    let scheduler = crate::scheduler::SessionScheduler::new(core.clone(), None);
    ingress_select_remote(&core, &mut store, &scheduler);
    // Exact topic matching remains mandatory for ordinary admissions.
    let r = request();
    let run = admit(&store, &r);
    assert_eq!(
        store
            .conn
            .query_row("SELECT node FROM node_routes WHERE run=?1", [run], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
        local(&runner).unwrap().card.id.to_string()
    );
    let mut other = r;
    other.session = SessionKey::telegram("321#8");
    other.update_id = Some("other-topic".into());
    let run = admit(&store, &other);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM node_routes WHERE run=?1",
                [run],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    scheduler.shutdown().unwrap();
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
    let active = runner.admit(core_key.public(), &b2).unwrap().unwrap();
    runner.begin_shutdown();
    assert!(active.load(Ordering::SeqCst));
    let later = bundle(&core_key, runner.key.public(), "arrived during shutdown").unwrap();
    assert_eq!(
        runner
            .admit(core_key.public(), &later)
            .unwrap_err()
            .to_string(),
        "runner stopping"
    );
    assert!(runner.evidence(&later).is_err());
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
        status(&store, Some("telegram:other"), "UTC")
            .unwrap()
            .contains("New text tasks go to: this computer (local)")
    );
    assert!(
        status(&store, Some(&r.session.id()), "UTC")
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
    assert!(page.text.contains("Not checked yet"));
    assert!(page.text.contains("Last reached: not recorded"));
    let callback = page.keyboard["inline_keyboard"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|row| row.as_array().unwrap())
        .find(|button| button["text"].as_str().unwrap().starts_with("Use Runner"))
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
fn node_reports_name_verified_roles_without_claiming_live_health() {
    let (_a, mut core) = fixture(Role::Coordinator, 91);
    let (_b, runner) = fixture(Role::Runner, 92);
    core.timezone = "Asia/Shanghai".into();
    pair(&core, &runner);
    let store = Store::open(&core).unwrap();
    let id = local(&runner).unwrap().card.id.to_string();
    let name = display_name(&store, &id).unwrap();
    assert!(name.starts_with("Runner"));
    assert!(name.ends_with(&id[id.len() - 8..]));
    assert!(!name.contains(&id));
    let unenrolled = SecretKey::from([93; 32]).public().to_string();
    let fallback = display_name(&store, &unenrolled).unwrap();
    assert!(fallback.starts_with("Node"));
    assert!(fallback.ends_with(&unenrolled[unenrolled.len() - 8..]));
    selection(&store, "telegram:321#7", Some(&id)).unwrap();
    store.conn.execute(
        "UPDATE paired_nodes SET last_contact='2026-11-17T01:02:03Z',last_error='reachable at observation' WHERE id=?1",
        [&id],
    ).unwrap();
    let scheduler = crate::scheduler::SessionScheduler::new(core.clone(), None);
    let page = crate::telegram_ui::handle(
        &core,
        &store,
        &scheduler,
        &request().session,
        None,
        "/nodes",
    )
    .unwrap()
    .unwrap();
    let page = crate::telegram_ui::decode(&page).unwrap();
    assert_eq!(page.text.matches("Snapshot at").count(), 1);
    assert!(!page.text.contains("Observed at"));
    assert!(
        page.text
            .contains("17 Nov 2026 09:02 +08:00 (Asia/Shanghai)")
    );
    assert!(page.text.contains("Reachable when last checked"));
    assert!(page.text.contains(&format!("New text tasks go to: {name}")));
    for forbidden in [
        &id,
        "health unavailable",
        "heartbeat",
        "contact is an observation",
        "Next text task",
    ] {
        assert!(!page.text.contains(forbidden), "{forbidden}: {}", page.text);
    }
    store
        .conn
        .execute(
            "UPDATE paired_nodes SET last_error='unavailable at observation' WHERE id=?1",
            [&id],
        )
        .unwrap();
    let failed = status(&store, Some("telegram:321#7"), &core.timezone).unwrap();
    assert!(failed.contains("Not reachable at the last check"));
    assert!(failed.contains("check time not recorded"));
    assert!(failed.contains("Last reached: 17 Nov 2026 09:02"));
    assert!(failed.contains(&format!("New text tasks go to: {name}")));
    assert!(failed.contains("do not switch to this computer"));
    store
        .conn
        .execute("UPDATE paired_nodes SET last_error=NULL", [])
        .unwrap();
    let exchange = status(&store, None, "UTC").unwrap();
    assert!(exchange.contains("Reached during the last recorded exchange"));
    assert!(!exchange.contains("Not checked yet"));
    if let Some(dir) = std::env::var_os("COCONUTCLAW_UI_ARTIFACTS") {
        fs::create_dir_all(&dir).unwrap();
        fs::write(PathBuf::from(&dir).join("nodes-rendered.txt"), format!("PAIRED / LAST REACHED\n{}\n\nLATER FAILED CHECK\n{failed}\n\nCONTACT WITHOUT PROBE\n{exchange}", page.text)).unwrap();
    }
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
    let node = key(&cfg).unwrap().public().to_string();
    store
        .conn
        .execute("INSERT INTO node_routes VALUES(?1,?2)", params![run, node])
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE paired_nodes SET last_contact='2026-11-20T01:51:00Z',
         last_error='reachable at observation' WHERE id=?1",
            [&node],
        )
        .unwrap();
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
    let unverified = desktop::test_export(&core.sqlite_db_path, &request.session);
    assert_eq!(unverified["nodes"][0]["observation"], "reachable");
    assert_eq!(unverified["jobs"][0]["assigned_node"], node);
    assert_eq!(unverified["jobs"][0]["status"], "unknown");
    assert_eq!(unverified["jobs"][0]["signed_evidence"], "none");
    assert_eq!(unverified["jobs"][0]["delivery"], "pending");
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
    let verified = desktop::test_export(&core.sqlite_db_path, &request.session);
    assert_eq!(verified["jobs"][0]["assigned_node"], node);
    assert_eq!(verified["jobs"][0]["status"], "unknown");
    assert_eq!(verified["jobs"][0]["signed_evidence"], "verified");
    assert!(!verified.to_string().contains("late durable output"));
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
    let local_script = core.instance_dir.join("fake-local-codex");
    fs::write(
        &local_script,
        "#!/bin/sh\nprintf 'called\\n' >> local-invocations\nexit 1\n",
    )
    .unwrap();
    fs::set_permissions(&local_script, fs::Permissions::from_mode(0o700)).unwrap();
    core.codex.bin = local_script.to_string_lossy().into();
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
    let mut store = Store::open(&core).unwrap();
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
    core.telegram_topics_enabled = Some(true);
    let scheduler = crate::scheduler::SessionScheduler::new(core.clone(), None);
    selection(&store, &r.session.id(), None).unwrap();
    ingress_select_remote(&core, &mut store, &scheduler);
    let raw = serde_json::json!({"update_id":30005,"message":{
        "chat":{"id":321,"type":"private"},"from":{"id":321},
        "message_thread_id":7,"text":"ordinary next text from same topic"}})
    .to_string();
    crate::loops::webhook::process_webhook_line(&core, &mut store, &scheduler, &raw).unwrap();
    let scheduled: i64 = store
        .conn
        .query_row(
            "SELECT id FROM task_runs WHERE update_id='30005'",
            [],
            |r| r.get(0),
        )
        .unwrap();
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
    assert!(!core.instance_dir.join("local-invocations").exists());
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
fn scoped_memory_never_displaces_task_text_at_signed_bounds() {
    let core = SecretKey::from([103; 32]);
    let runner = SecretKey::from([104; 32]).public();
    // Exactly the text limit, then an input with room in text bytes but not in
    // the escaped signed event. Both valid plain assignments must be unchanged.
    let (mut low, mut high) = (1usize, 2048usize);
    let mut near_limit = None;
    while low < high {
        let mid = (low + high).div_ceil(2);
        let text = "\"".repeat(mid);
        match bundle(&core, runner, &text) {
            Ok(b) => {
                // Keep these exact bytes: the random origin affects the
                // signature, and its JSON array does not have fixed length.
                near_limit = Some((text, b));
                low = mid;
            }
            Err(_) => high = mid - 1,
        }
    }
    let (quoted, quoted_bundle) = near_limit.unwrap();
    assert!(quoted.len() + 100 < 2048);
    let full = "x".repeat(2048);
    let full_bundle = bundle(&core, runner, &full).unwrap();
    for (text, mut b) in [(full, full_bundle), (quoted, quoted_bundle)] {
        let plain = b.assignment.clone();
        b.include_memory(&core, &text, vec!["optional fact".into()])
            .unwrap();
        assert_eq!(b.assignment, plain);
        assert_eq!(b.validate(core.public(), runner).unwrap(), text);
    }
    // An oversized JSON-escaped fact must not prevent a later fact from fitting.
    let mut b = bundle(&core, runner, "task").unwrap();
    b.include_memory(&core, "task", vec!["\"".repeat(1100), "fits".into()])
        .unwrap();
    let prompt = b.validate(core.public(), runner).unwrap();
    assert!(prompt.ends_with("\n[\"fits\"]\n\nUser request:\ntask"));
}

#[cfg(unix)]
#[path = "memory_tests.rs"]
mod memory;

#[test]
fn offline_node_output_write_failure_does_not_publish_terminal_status() {
    let (_core_dir, core) = fixture(Role::Coordinator, 93);
    let (_runner_dir, runner) = fixture(Role::Runner, 94);
    pair(&core, &runner);
    let store = Store::open(&core).unwrap();
    let request = request();
    selection(
        &store,
        &request.session.id(),
        Some(&local(&runner).unwrap().card.id.to_string()),
    )
    .unwrap();
    let run = admit(&store, &request);
    store.update_task_run_started(run, "fixture").unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TEMP TRIGGER reject_output BEFORE UPDATE OF output ON task_payloads
             BEGIN SELECT RAISE(ABORT, 'fixture output write failed'); END",
        )
        .unwrap();
    let error = execute(
        &core,
        &store,
        run,
        &request,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap_err();
    assert!(error.to_string().contains("fixture output write failed"));
    assert_eq!(
        store.get_task_run(run).unwrap().unwrap().status,
        "waiting_remote"
    );
}

#[test]
fn offline_node_preserves_assignment_unknown_and_never_falls_back_locally() {
    let (_a, mut core) = fixture(Role::Coordinator, 91);
    let (_b, cfg) = fixture(Role::Runner, 92);
    pair(&core, &cfg);
    let telegram = crate::scheduler::tests::FakeTelegramServer::start();
    core.telegram_api_base = Some(telegram.base_url.clone());
    core.telegram_topics_enabled = Some(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let script = core.instance_dir.join("fake-local-codex");
        fs::write(
            &script,
            "#!/bin/sh\nprintf 'called\\n' >> local-invocations\nexit 1\n",
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
        core.codex.bin = script.to_string_lossy().into();
    }
    let mut store = Store::open(&core).unwrap();
    let request = request();
    let scheduler = crate::scheduler::SessionScheduler::new(core.clone(), None);
    ingress_select_remote(&core, &mut store, &scheduler);
    let raw = serde_json::json!({"update_id":30005,"message":{
        "chat":{"id":321,"type":"private"},"from":{"id":321},
        "message_thread_id":7,"text":"synthetic offline task"}})
    .to_string();
    crate::loops::webhook::process_webhook_line(&core, &mut store, &scheduler, &raw).unwrap();
    let run: i64 = store
        .conn
        .query_row(
            "SELECT id FROM task_runs WHERE update_id='30005'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while store.get_task_run(run).unwrap().unwrap().status != "failed" {
        assert!(
            std::time::Instant::now() < deadline,
            "offline route did not settle"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let task = store.get_task_run(run).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    assert!(task.error_summary.unwrap().contains("outcome unknown"));
    assert!(
        status(&store, Some(&request.session.id()), "UTC")
            .unwrap()
            .contains("did not confirm an outcome")
    );
    let report = status(&store, Some(&request.session.id()), "UTC").unwrap();
    assert!(report.contains("outcome unconfirmed"));
    assert!(!report.contains(" / failed"));
    let assignments: i64 = store
        .conn
        .query_row(
            "SELECT count(*) FROM node_assignments WHERE run=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(assignments, 1);
    let output: String = store
        .conn
        .query_row(
            "SELECT output FROM task_payloads WHERE task_run_id=?1",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    assert!(output.contains("outcome unconfirmed (Unknown)"));
    assert!(output.contains("Nothing was retried or moved to this computer"));
    assert!(output.contains("nodes reconcile"));
    assert!(!core.instance_dir.join("local-invocations").exists());
    assert!(!cfg.instance_dir.join("invocations").exists());
    scheduler.shutdown().unwrap();
}

#[test]
fn legacy_cutover_reconcile_retains_original_start_and_late_result_after_seal() {
    let (_core_root, core) = fixture(Role::Coordinator, 111);
    let (_node_root, node) = fixture(Role::Runner, 112);
    pair(&core, &node);
    pair(&node, &core);
    let store = Store::open(&core).unwrap();
    let source = request();
    let run = admit(&store, &source);
    let other_source = TaskRequest {
        update_id: Some("ordinary-2".into()),
        ..source.clone()
    };
    let other = admit(&store, &other_source);
    let core_key = key(&core).unwrap();
    let node_key = key(&node).unwrap();
    let assignment = bundle(&core_key, node_key.public(), &source.input.user_text).unwrap();
    let tx = store.conn.unchecked_transaction().unwrap();
    tx.execute(
        "INSERT INTO node_assignments VALUES(?1,?2)",
        params![run, serde_json::to_string(&assignment).unwrap()],
    )
    .unwrap();
    store
        .reserve_runtime_run(
            run,
            &assignment.origin.hash().unwrap(),
            &assignment.assignment,
        )
        .unwrap();
    let other_assignment = bundle(&core_key, node_key.public(), &source.input.user_text).unwrap();
    store
        .reserve_runtime_run(
            other,
            &other_assignment.origin.hash().unwrap(),
            &other_assignment.assignment,
        )
        .unwrap();
    tx.commit().unwrap();

    let mut runner = Runner::open(node.clone()).unwrap();
    let ingress = store
        .admit_telegram("54321", "{}", Some(&source.session.id()), "fixture")
        .unwrap();
    runner
        .admit(core_key.public(), &other_assignment)
        .unwrap()
        .unwrap();
    runner
        .finish(&other_assignment, Outcome::Completed, "before freeze")
        .unwrap();
    assert!(
        accept(
            &store,
            other,
            &other_assignment,
            runner.evidence(&other_assignment).unwrap(),
            "UTC"
        )
        .unwrap()
    );
    let tx = store.conn.unchecked_transaction().unwrap();
    assert!(store.claim_telegram(ingress).unwrap());
    store
        .finish_telegram(
            ingress,
            &crate::ProcessOutcome {
                should_ack: true,
                update_id: Some("54321".into()),
                chat_id: None,
                output: None,
                cleanup_path: None,
                progress_message_id: None,
            },
        )
        .unwrap();
    tx.commit().unwrap();

    runner
        .admit(core_key.public(), &assignment)
        .unwrap()
        .unwrap();
    let Reply::Evidence {
        start,
        result: None,
    } = runner.evidence(&assignment).unwrap()
    else {
        panic!("missing original start");
    };
    let state = Arc::new(Mutex::new(runner));
    let runtime = transport::runtime().unwrap();
    let router = runtime.block_on(async {
        iroh::protocol::Router::builder(
            transport::endpoint(node_key.clone(), local(&node).unwrap().card.address)
                .await
                .unwrap(),
        )
        .accept(ALPN, transport::Handler::new(state.clone()))
        .spawn()
    });
    store
        .conn
        .execute("INSERT INTO legacy_admission_freeze VALUES(1)", [])
        .unwrap();
    Store::seal_legacy_cutover(&core).unwrap();
    let action = Args {
        action: Command::Reconcile { run },
    };
    assert!(
        super::run(&core, &action)
            .unwrap_err()
            .to_string()
            .contains("runner still active")
    );
    let saved = |store: &Store| {
        store
            .conn
            .query_row(
                "SELECT start,result FROM node_signed_receipts WHERE task_run_id=?1",
                [run],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<Vec<u8>>>(1)?)),
            )
            .unwrap()
    };
    assert_eq!(saved(&store), (start.clone(), None));
    drop(store);

    let mut runner = state.lock().unwrap();
    runner
        .store
        .conn
        .execute("INSERT INTO legacy_admission_freeze VALUES(1)", [])
        .unwrap();
    let foreign = bundle(&core_key, node_key.public(), "foreign attempt").unwrap();
    assert!(
        runner
            .admit(core_key.public(), &foreign)
            .unwrap_err()
            .to_string()
            .contains("frozen")
    );
    assert!(
        runner
            .admit(core_key.public(), &assignment)
            .unwrap()
            .is_none()
    );
    runner
        .finish(&assignment, Outcome::Completed, "late signed output")
        .unwrap();
    let Reply::Evidence {
        result: Some(result),
        ..
    } = runner.evidence(&assignment).unwrap()
    else {
        panic!("missing original result");
    };
    drop(runner);

    let reopened = Store::connect(&core).unwrap();
    let foreign_receipt = || Reply::Evidence {
        start: start.clone(),
        result: Some(result.clone()),
    };
    assert!(accept(&reopened, other, &assignment, foreign_receipt(), "UTC").is_err());
    let mut corrupt = result.clone();
    corrupt[0] ^= 1;
    assert!(
        accept(
            &reopened,
            run,
            &assignment,
            Reply::Evidence {
                start: start.clone(),
                result: Some(corrupt),
            },
            "UTC"
        )
        .is_err()
    );
    assert_eq!(saved(&reopened), (start.clone(), None));
    super::run(&core, &action).unwrap();
    super::run(&core, &action).unwrap();
    let history = History::replay(
        &assignment.origin,
        core_key.public(),
        assignment.origin.origin.namespace,
        [assignment.assignment.clone(), start.clone()],
    )
    .unwrap();
    let conflicting = SignedEvent::sign(
        history
            .next(
                &assignment.origin,
                node_key.public(),
                Action::Result {
                    assignment: 1,
                    outcome: Outcome::Completed,
                    output: "conflicting output".into(),
                },
            )
            .unwrap(),
        &node_key,
    )
    .unwrap()
    .bytes()
    .unwrap();
    assert!(
        accept(
            &reopened,
            run,
            &assignment,
            Reply::Evidence {
                start: start.clone(),
                result: Some(conflicting),
            },
            "UTC"
        )
        .unwrap_err()
        .to_string()
        .contains("custody conflict")
    );
    assert!(
        !accept(
            &reopened,
            run,
            &assignment,
            Reply::Evidence {
                start: start.clone(),
                result: None,
            },
            "UTC"
        )
        .unwrap()
    );
    assert_eq!(saved(&reopened), (start, Some(result)));
    assert_eq!(
        reopened.get_task_run(run).unwrap().unwrap().status,
        "waiting_remote"
    );
    let counts: (i64, i64, i64) = reopened
        .conn
        .query_row(
            "SELECT (SELECT count(*) FROM node_signed_receipts),
                (SELECT count(*) FROM runtime_remote_results WHERE task_run_id=?1 AND state='pending'),
                (SELECT count(*) FROM runtime_inbox WHERE kind='result' AND task_run_id=?2)",
            [other, run],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(counts, (2, 1, 0));
    assert_eq!(
        reopened.get_task_run(other).unwrap().unwrap().status,
        "waiting_remote"
    );
    runtime.block_on(router.shutdown()).unwrap();
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
