use super::*;
use crate::loops::webhook::parse_webhook_action;
use crate::types::WebhookAction;

fn form(request: &str) -> std::collections::HashMap<String, String> {
    let body = request.split_once("\r\n\r\n").unwrap().1;
    reqwest::Url::parse(&format!("http://mock/?{body}"))
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

fn save_output(cfg: &RuntimeConfig, output: &str, topic: Option<&str>) -> (Store, i64) {
    let store = Store::open(cfg).unwrap();
    let mut request = make_request(SessionKey::telegram("321"), "telegram", "saved");
    request.session.thread_id = topic.map(str::to_string);
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: topic.map(str::to_string),
    };
    let id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: request.session.id(),
            channel: "telegram".into(),
            source_chat_id: Some("321".into()),
            source_user_id: None,
            update_id: None,
            prompt: "saved".into(),
            created_at: "now".into(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO task_payloads(task_run_id,request,output) VALUES(?1,?2,?3)",
            rusqlite::params![
                id,
                build_resume_payload(&request, &request.input).unwrap(),
                output
            ],
        )
        .unwrap();
    (store, id)
}

fn state(store: &Store, id: i64) -> serde_json::Value {
    let raw: String = store
        .conn
        .query_row(
            "SELECT delivery_state FROM task_payloads WHERE task_run_id=?1",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

#[test]
fn ingress_metadata_separates_root_topics_and_rejects_unexpected_routes() {
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_topics_enabled = Some(true);
    for (topic, expected) in [
        (None, "telegram:321"),
        (Some(7), "telegram:321#7"),
        (Some(8), "telegram:321#8"),
    ] {
        let mut message = json!({"chat":{"id":321,"type":"private"},"text":"hello"});
        if let Some(topic) = topic {
            message["message_thread_id"] = topic.into();
        }
        let action = parse_webhook_action(&cfg, &json!({"message":message}).to_string()).unwrap();
        let WebhookAction::Turn(turn) = action else {
            panic!("valid private route ignored");
        };
        assert_eq!(turn.session.id(), expected);
    }
    for message in [
        json!({"chat":{"id":999,"type":"private"},"message_thread_id":7}),
        json!({"chat":{"id":321,"type":"group"},"message_thread_id":7}),
        json!({"chat":{"id":321},"message_thread_id":0}),
        json!({"chat":{"id":321},"message_thread_id":"7"}),
        json!({"chat":{"id":321},"message_thread_id":-1}),
        json!({"chat":{"id":321},"direct_messages_topic_id":7}),
        json!({"chat":{"id":321},"direct_messages_topic":{"topic_id":7}}),
    ] {
        assert!(matches!(
            parse_webhook_action(&cfg, &json!({"message":message}).to_string()).unwrap(),
            WebhookAction::Ignore { .. }
        ));
    }
    cfg.telegram_topics_enabled = Some(false);
    assert!(matches!(
        parse_webhook_action(
            &cfg,
            r#"{"message":{"chat":{"id":321},"message_thread_id":7,"text":"hello"}}"#
        )
        .unwrap(),
        WebhookAction::Ignore { .. }
    ));
    assert!(matches!(
        parse_webhook_action(&cfg, r#"{"message":{"chat":{"id":321},"text":"hello"}}"#).unwrap(),
        WebhookAction::Turn(_)
    ));
    cfg.telegram_topics_enabled = Some(true);
    let action = parse_webhook_action(&cfg, r#"{"callback_query":{"data":"cancel","message":{"chat":{"id":321},"message_thread_id":8}}}"#).unwrap();
    assert!(
        matches!(action, WebhookAction::Cancel { session, .. } if session.id()=="telegram:321#8")
    );
}

#[test]
fn memory_history_boundaries_and_resume_payloads_remain_topic_local() {
    let cfg = RuntimeConfig::test_config();
    let store = Store::open(&cfg).unwrap();
    for scope in ["telegram:321", "telegram:321#7", "telegram:321#8"] {
        store
            .memory_command(
                scope,
                &format!("/memory remember unique-{scope}"),
                None,
                "now",
            )
            .unwrap();
        let revision = store.memory_revision(scope).unwrap();
        assert!(revision > 0);
        for other in ["telegram:321", "telegram:321#7", "telegram:321#8"] {
            let memory = store.curated_memory(other).unwrap();
            assert_eq!(memory.contains(&format!("unique-{scope}")), other == scope);
        }
    }
    let mut request = make_request(SessionKey::telegram("321#7"), "telegram", "hello");
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some("7".into()),
    };
    let raw = build_resume_payload(&request, &request.input).unwrap();
    let saved: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(saved["dispatch"]["chat_id"], "321");
    assert_eq!(saved["dispatch"]["message_thread_id"], "7");
    let restored = request_from_resume_payload(&raw).unwrap();
    assert_eq!(restored.session.id(), "telegram:321#7");
    assert_eq!(restored.delivery, request.delivery);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let mut mutable_store = Store::open(&cfg).unwrap();
    crate::loops::webhook::process_webhook_line(
        &cfg,
        &mut mutable_store,
        &scheduler,
        r#"{"update_id":90001,"message":{"chat":{"id":321},"message_thread_id":7,"text":"/fresh"}}"#,
    )
    .unwrap();
    assert!(
        store
            .latest_boundary_id("telegram:321#7", "telegram")
            .unwrap()
            > 0
    );
    assert_eq!(
        store
            .latest_boundary_id("telegram:321#8", "telegram")
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .latest_boundary_id("telegram:321", "telegram")
            .unwrap(),
        0
    );
    assert!(
        store
            .curated_memory("telegram:321#7")
            .unwrap()
            .contains("unique-telegram:321#7")
    );
}

#[test]
fn rich_success_preserves_gfm_math_unicode_and_topic_without_parse_mode() {
    let server = FakeTelegramServer::start();
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let markdown = "# Résumé 🥥\n\n| A | B |\n|---|---|\n| 1 | 2 |\n\n- item\n\n<details><summary>More</summary>\n\n$$E=mc^2$$\n\n</details>\n\n```rust\nlet x = 1;\n```";
    let (store, id) = save_output(&cfg, &format!("TELEGRAM_REPLY: {markdown}"), Some("7"));
    SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/sendRichMessage "));
    let params = form(&requests[0]);
    assert_eq!(params["chat_id"], "321");
    assert_eq!(params["message_thread_id"], "7");
    assert!(!params.contains_key("parse_mode"));
    let rich: serde_json::Value = serde_json::from_str(&params["rich_message"]).unwrap();
    assert_eq!(rich, json!({"markdown":markdown}));
    assert_eq!(state(&store, id)["completed_ops"], json!(["telegram:text"]));
}

#[test]
fn definitive_rich_format_rejection_persists_plain_fallback_but_unknown_never_duplicates() {
    for status in [400, 500] {
        let server = FakeTelegramServer::start_with_response(move |request| {
            if request.contains("/sendRichMessage ") {
                (
                    status,
                    format!(
                        r#"{{"ok":false,"error_code":{status},"description":"rich format rejected"}}"#
                    ),
                )
            } else {
                (200, r#"{"ok":true,"result":{"message_id":1}}"#.into())
            }
        });
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let (store, id) = save_output(&cfg, "TELEGRAM_REPLY: # answer $x^2$", Some("9"));
        SessionScheduler::new(cfg.clone(), None)
            .retry_deliveries()
            .unwrap();
        SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
        let requests = server.full_requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.contains("/sendRichMessage "))
                .count(),
            1
        );
        if status == 400 {
            assert_eq!(requests.len(), 2);
            let fallback = form(&requests[1]);
            assert_eq!(fallback["text"], "# answer $x^2$");
            assert_eq!(fallback["message_thread_id"], "9");
            assert!(!fallback.contains_key("parse_mode"));
            assert_eq!(state(&store, id)["text_plan"]["kind"], "chunks");
        } else {
            assert_eq!(requests.len(), 1);
            assert_eq!(state(&store, id)["text_plan"]["phase"], "unknown");
        }
    }
}

#[test]
fn rich_rate_limit_waits_and_crash_send_window_does_not_resend() {
    let server = FakeTelegramServer::start_with_response(|_| {
        (
            429,
            r#"{"ok":false,"error_code":429,"parameters":{"retry_after":3600}}"#.into(),
        )
    });
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let (store, id) = save_output(&cfg, "TELEGRAM_REPLY: answer", Some("9"));
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.retry_deliveries().unwrap();
    scheduler.retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    let mut saved = state(&store, id);
    assert!(saved["text_plan"]["retry_at"].as_u64().unwrap() > 0);
    saved["text_plan"]["phase"] = "sending".into();
    saved["text_plan"]["retry_at"] = 0.into();
    store
        .conn
        .execute(
            "UPDATE task_payloads SET delivery_state=?2 WHERE task_run_id=?1",
            rusqlite::params![id, saved.to_string()],
        )
        .unwrap();
    SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[test]
fn timeout_after_acceptance_holds_rich_and_acknowledged_text_is_not_replayed_for_media() {
    let server = FakeTelegramServer::start_with_response(|_| {
        std::thread::sleep(Duration::from_millis(1200));
        (200, r#"{"ok":true,"result":{"message_id":1}}"#.into())
    });
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    cfg.telegram_api_timeout_secs = 1;
    let (store, id) = save_output(&cfg, "TELEGRAM_REPLY: unknown accepted answer", Some("7"));
    SessionScheduler::new(cfg.clone(), None)
        .retry_deliveries()
        .unwrap();
    assert_eq!(state(&store, id)["text_plan"]["phase"], "unknown");
    SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);

    let mut media_calls = 0;
    let server = FakeTelegramServer::start_with_response(move |request| {
        if request.contains("/sendDocument ") {
            media_calls += 1;
            if media_calls == 1 {
                return (
                    400,
                    r#"{"ok":false,"error_code":400,"description":"document rejected"}"#.into(),
                );
            }
        }
        (200, r#"{"ok":true,"result":{"message_id":1}}"#.into())
    });
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let file = cfg.tmp_dir.join("artifact.txt");
    fs::write(&file, "explicit marker artifact").unwrap();
    let (store, id) = save_output(
        &cfg,
        &format!("TELEGRAM_REPLY: answer\nSEND_DOCUMENT: {}", file.display()),
        Some("7"),
    );
    SessionScheduler::new(cfg.clone(), None)
        .retry_deliveries()
        .unwrap();
    assert!(
        state(&store, id)["completed_ops"]
            .as_array()
            .unwrap()
            .contains(&json!("telegram:text"))
    );
    SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("/sendRichMessage "))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("/sendDocument "))
            .count(),
        2
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.contains("/sendDocument "))
            .all(|request| request.contains("name=\"message_thread_id\"\r\n\r\n7"))
    );
}

#[test]
fn rich_character_limit_is_checked_at_its_unicode_boundary() {
    for length in [32768, 32769] {
        let server = FakeTelegramServer::start();
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let text = "🥥".repeat(length);
        let (_, _) = save_output(&cfg, &format!("TELEGRAM_REPLY: {text}"), Some("7"));
        SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
        let requests = server.full_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        if length == 32768 {
            assert!(requests[0].contains("/sendRichMessage "));
            let rich: serde_json::Value =
                serde_json::from_str(&form(&requests[0])["rich_message"]).unwrap();
            assert_eq!(rich["markdown"], text);
        } else {
            assert!(requests[0].contains("/sendDocument "));
            assert!(requests[0].contains(&text));
        }
    }
}

#[test]
fn long_unicode_code_table_math_is_preserved_in_routed_document() {
    let server = FakeTelegramServer::start();
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let markdown = format!(
        "```text\n{}\n```\n|A|B|\n|-|-|\n|1|2|\n$$x^2$$",
        "🥥".repeat(32768)
    );
    let (store, id) = save_output(&cfg, &format!("TELEGRAM_REPLY: {markdown}"), Some("11"));
    SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/sendDocument "));
    assert!(requests[0].contains(&markdown));
    assert!(requests[0].contains("name=\"chat_id\"\r\n\r\n321"));
    assert!(requests[0].contains("name=\"message_thread_id\"\r\n\r\n11"));
    assert_eq!(state(&store, id)["text_plan"]["kind"], "document");
}

#[test]
fn rich_embedded_actions_and_files_are_literal_regular_text() {
    let server = FakeTelegramServer::start();
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let malicious = "<tg-button type=\"callback_data\" data=\"approve\">Go</tg-button>\n![](file:///private/secret)";
    let (_, _) = save_output(&cfg, &format!("TELEGRAM_REPLY: {malicious}"), Some("7"));
    SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/sendMessage "));
    let params = form(&requests[0]);
    assert_eq!(params["text"], malicious);
    assert!(!params.contains_key("parse_mode"));
    assert!(!params.contains_key("reply_markup"));
}

#[test]
fn topic_approval_and_delegated_results_resume_only_in_origin_topic() {
    for delegated in [false, true] {
        let server = FakeTelegramServer::start();
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let script = cfg.tmp_dir.join("resume-provider.py");
        fs::write(&script, format!(r#"import sys,pathlib
args=sys.argv[1:]
out=args[args.index("--output-last-message")+1]
prompt=sys.stdin.read()
root=pathlib.Path(__file__).parent
with (root/"calls").open("a") as file: file.write("call\n")
done="Administrator approval granted" in prompt or "delegated child sentinel" in prompt
pathlib.Path(out).write_text("TELEGRAM_REPLY: child result" if done else "TELEGRAM_REPLY: first\n{}")
"#, if delegated { "DELEGATE_TASK: delegated child sentinel" } else { "SEND_APPROVAL: may I continue?" })).unwrap();
        cfg.codex.bin = format!("{} {}", python_bin(), script.display());
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        let mut request =
            make_request(SessionKey::telegram("321#7"), "telegram", "origin sentinel");
        request.delivery = DeliveryTarget::Telegram {
            chat_id: "321".into(),
            thread_id: Some("7".into()),
        };
        let id = scheduler.enqueue(request).unwrap();
        let store = Store::open(&cfg).unwrap();
        if !delegated {
            wait_until(|| store.get_task_run(id).unwrap().unwrap().status == "awaiting_approval");
            let approval: i64 = store
                .conn
                .query_row("SELECT id FROM approvals WHERE task_run_id=?1", [id], |r| {
                    r.get(0)
                })
                .unwrap();
            let denied = scheduler
                .resolve_approval(approval, "approve", "telegram:321#8")
                .unwrap()
                .unwrap();
            assert!(denied.contains("not authorized"));
            assert_eq!(
                fs::read_to_string(cfg.tmp_dir.join("calls"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            scheduler
                .resolve_approval(approval, "approve", "telegram:321#7")
                .unwrap();
        }
        wait_until(|| {
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM task_payloads WHERE delivered=1",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap()
                == 2
        });
        scheduler.shutdown().unwrap();
        assert_eq!(
            fs::read_to_string(cfg.tmp_dir.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        let child_scope: String = store
            .conn
            .query_row(
                "SELECT session_id FROM task_runs ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(child_scope, "telegram:321#7");
        for request in server.full_requests.lock().unwrap().iter() {
            let params = form(request);
            assert_eq!(params["chat_id"], "321");
            assert_eq!(params["message_thread_id"], "7");
        }
    }
}

fn python_bin() -> &'static str {
    if cfg!(windows) {
        "python"
    } else {
        "/usr/bin/python3"
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for mock provider"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(unix)]
#[test]
fn five_actual_providers_overlap_sixth_waits_topic_fifo_cancel_and_restart_are_isolated() {
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("gated-provider.py");
    fs::write(
        &script,
        r#"import os,sys,re,time,pathlib
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
out=args[args.index("--output-last-message")+1]
text=sys.stdin.read()
number=re.findall(r"gate-(\d+)",text)[-1]
(root/("started-"+number)).write_text(str(os.getpid()))
while not (root/("release-"+number)).exists(): time.sleep(.025)
pathlib.Path(out).write_text("TELEGRAM_REPLY: done "+number)
"#,
    )
    .unwrap();
    cfg.codex.bin = format!("/usr/bin/python3 {}", script.display());
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let ids: Vec<_> = (1..=6)
        .map(|n| {
            scheduler
                .enqueue(make_stdout_request(
                    SessionKey::telegram(&format!("321#{n}")),
                    &format!("gate-{n}"),
                ))
                .unwrap()
        })
        .collect();
    let seventh = scheduler
        .enqueue(make_stdout_request(SessionKey::telegram("321#1"), "gate-7"))
        .unwrap();
    wait_until(|| (1..=5).all(|n| cfg.tmp_dir.join(format!("started-{n}")).exists()));
    let alive = |n| {
        fs::read_to_string(cfg.tmp_dir.join(format!("started-{n}")))
            .ok()
            .is_some_and(|pid| {
                let pid: i32 = pid.parse().unwrap();
                assert!(pid > 1);
                unsafe { libc::kill(pid, 0) == 0 }
            })
    };
    assert!(
        (1..=5).all(alive),
        "five distinct actual provider processes must overlap"
    );
    assert!(!cfg.tmp_dir.join("started-6").exists());
    assert!(!cfg.tmp_dir.join("started-7").exists());
    let store = Store::open(&cfg).unwrap();
    assert_eq!(
        store.get_task_run(ids[5]).unwrap().unwrap().status,
        "queued"
    );
    assert!(
        !scheduler
            .cancel_task_for_session(ids[0], Some("telegram:321#2"))
            .unwrap()
    );
    assert!((1..=5).all(alive));
    assert!(
        scheduler
            .cancel_task_for_session(ids[1], Some("telegram:321#2"))
            .unwrap()
    );
    wait_until(|| cfg.tmp_dir.join("started-6").exists());
    assert!([1, 3, 4, 5, 6].into_iter().all(alive));
    assert!(!alive(2));
    assert!(!cfg.tmp_dir.join("started-7").exists());
    assert!(
        scheduler
            .cancel_task_for_session(ids[0], Some("telegram:321#1"))
            .unwrap()
    );
    wait_until(|| cfg.tmp_dir.join("started-7").exists());
    assert!([3, 4, 5, 6, 7].into_iter().all(alive));
    assert!(!alive(1));
    let eighth = scheduler
        .enqueue(make_stdout_request(SessionKey::telegram("321#8"), "gate-8"))
        .unwrap();
    scheduler.shutdown().unwrap();
    assert!(!(1..=7).any(alive));
    assert!(!cfg.tmp_dir.join("started-8").exists());
    assert_eq!(
        store.get_task_run(eighth).unwrap().unwrap().status,
        "queued"
    );
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    wait_until(|| cfg.tmp_dir.join("started-8").exists());
    assert!(alive(8));
    fs::write(cfg.tmp_dir.join("release-8"), "").unwrap();
    wait_for_terminal_tasks(&cfg, &[eighth, seventh]);
    restarted.shutdown().unwrap();
    assert!(!(1..=8).any(alive));
}

#[test]
fn recovery_admits_five_and_approval_error_and_delegation_release_slots() {
    for mode in ["approval", "error", "delegation"] {
        let mut cfg = RuntimeConfig::test_config();
        cfg.provider_max_retries = 0;
        let script = cfg.tmp_dir.join("release-provider.py");
        fs::write(&script, format!(r#"import sys,pathlib,re,time,os
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
out=args[args.index("--output-last-message")+1]
n=re.findall(r"gate-(\d+)",sys.stdin.read())[-1]
(root/("started-"+n)).write_text(str(os.getpid()))
while not (root/("release-"+n)).exists(): time.sleep(.025)
if n=="1":
 if "{mode}"=="error": sys.exit(1)
 pathlib.Path(out).write_text("SEND_APPROVAL: Continue?" if "{mode}"=="approval" else "DELEGATE_TASK: gate-8")
else: pathlib.Path(out).write_text("TELEGRAM_REPLY: done")
"#)).unwrap();
        cfg.codex.bin = format!("{} {}", python_bin(), script.display());
        let store = Store::open(&cfg).unwrap();
        let mut ids = Vec::new();
        for n in 1..=7 {
            let request = make_stdout_request(
                SessionKey::telegram(&format!("321#{n}")),
                &format!("gate-{n}"),
            );
            let id = store
                .insert_task_run(crate::store::InsertTaskRunParams {
                    session_id: request.session.id(),
                    channel: "local".into(),
                    source_chat_id: None,
                    source_user_id: None,
                    update_id: None,
                    prompt: request.input.user_text.clone(),
                    created_at: "now".into(),
                    progress_message_id: None,
                    scheduled_task_id: None,
                })
                .unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
                    rusqlite::params![id, build_resume_payload(&request, &request.input).unwrap()],
                )
                .unwrap();
            ids.push(id);
        }
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        scheduler.recover().unwrap();
        wait_until(|| (1..=5).all(|n| cfg.tmp_dir.join(format!("started-{n}")).exists()));
        assert!(!cfg.tmp_dir.join("started-6").exists());
        assert!(!cfg.tmp_dir.join("started-7").exists());
        fs::write(cfg.tmp_dir.join("release-1"), "").unwrap();
        wait_until(|| cfg.tmp_dir.join("started-6").exists());
        assert!(!cfg.tmp_dir.join("started-7").exists());
        assert!(!cfg.tmp_dir.join("started-8").exists());
        let status = store.get_task_run(ids[0]).unwrap().unwrap().status;
        assert_eq!(
            status,
            if mode == "approval" {
                "awaiting_approval"
            } else if mode == "error" {
                "failed"
            } else {
                "completed"
            }
        );
        scheduler.cancel_task(ids[1]).unwrap();
        wait_until(|| cfg.tmp_dir.join("started-7").exists());
        assert!(!cfg.tmp_dir.join("started-8").exists());
        if mode == "delegation" {
            scheduler.cancel_task(ids[2]).unwrap();
            wait_until(|| cfg.tmp_dir.join("started-8").exists());
        }
        scheduler.shutdown().unwrap();
    }
}

#[test]
fn rich_5xx_never_falls_back_even_with_a_misleading_error_body() {
    let server = FakeTelegramServer::start_with_response(|_| {
        (
            503,
            r#"{"ok":false,"error_code":400,"description":"rich format rejected"}"#.into(),
        )
    });
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let (store, id) = save_output(&cfg, "TELEGRAM_REPLY: answer", Some("7"));
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.retry_deliveries().unwrap();
    scheduler.retry_deliveries().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert_eq!(state(&store, id)["text_plan"]["phase"], "unknown");
}

#[test]
fn regular_format_fallback_is_persisted_and_ambiguous_edits_never_send_new_messages() {
    for ambiguous in [false, true] {
        let server = FakeTelegramServer::start_with_response(move |request| {
            if ambiguous {
                return (500, "{}".into());
            }
            if request.contains("parse_mode=") {
                return (
                    400,
                    r#"{"ok":false,"error_code":400,"description":"can't parse entities"}"#.into(),
                );
            }
            (200, r#"{"ok":true,"result":{"message_id":1}}"#.into())
        });
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let (store, id) = save_output(&cfg, "TELEGRAM_REPLY: old saved plan", Some("7"));
        let raw: String = store
            .conn
            .query_row(
                "SELECT request FROM task_payloads WHERE task_run_id=?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        let mut request: serde_json::Value = serde_json::from_str(&raw).unwrap();
        request["progress_message_id"] = "123".into();
        store.conn.execute("UPDATE task_payloads SET request=?2,delivery_state=?3 WHERE task_run_id=?1",
            rusqlite::params![id, request.to_string(), json!({"completed_ops":[],"text_plan":{"kind":"chunks","chunks":["<b>old</b>"],"parse_mode":"HTML","fallback_plain":true}}).to_string()]).unwrap();
        SessionScheduler::new(cfg.clone(), None)
            .retry_deliveries()
            .unwrap();
        if !ambiguous {
            assert_eq!(
                state(&store, id)["operations"]["telegram:text:chunk:0"]["plain"],
                true
            );
        }
        SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
        let requests = server.full_requests.lock().unwrap();
        assert_eq!(requests.len(), if ambiguous { 1 } else { 2 });
        assert!(
            requests
                .iter()
                .all(|request| request.contains("/editMessageText "))
        );
        if !ambiguous {
            let fallback = form(&requests[1]);
            assert!(!fallback.contains_key("parse_mode"));
            assert_eq!(fallback["chat_id"], "321");
            assert_eq!(fallback["message_thread_id"], "7");
        }
    }
}

#[test]
fn all_media_and_regular_progress_and_chunks_preserve_topic_route() {
    let server = FakeTelegramServer::start();
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    crate::telegram::send_progress_message(&cfg, "telegram:321#7").unwrap();
    let client = crate::telegram::build_telegram_client(&cfg).unwrap();
    crate::telegram::send_or_edit_text(&client, &cfg, "telegram:321#7", &"x".repeat(5000), None)
        .unwrap();
    let file = cfg.tmp_dir.join("artifact.txt");
    fs::write(&file, "explicit marker artifact").unwrap();
    let output = format!(
        "TELEGRAM_REPLY: final\nSEND_PHOTO: {}\nSEND_DOCUMENT: {}\nSEND_VIDEO: {}",
        file.display(),
        file.display(),
        file.display()
    );
    let (_, _) = save_output(&cfg, &output, Some("7"));
    SessionScheduler::new(cfg, None).retry_deliveries().unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(requests.len(), 7);
    for request in requests.iter() {
        if request.contains("multipart/form-data") {
            assert!(request.contains("name=\"chat_id\"\r\n\r\n321"));
            assert!(request.contains("name=\"message_thread_id\"\r\n\r\n7"));
        } else {
            let params = form(request);
            assert_eq!(params["chat_id"], "321");
            assert_eq!(params["message_thread_id"], "7");
        }
    }
    for method in [
        "/sendPhoto ",
        "/sendDocument ",
        "/sendVideo ",
        "/sendRichMessage ",
    ] {
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains(method))
                .count(),
            1
        );
    }
}

#[test]
fn capability_detection_and_saved_route_validation_do_not_change_external_settings() {
    for enabled in [false, true] {
        let server = FakeTelegramServer::start_with_response(move |_| {
            (200,
            json!({"ok":true,"result":{"is_bot":true,"username":"ActualBot","has_topics_enabled":enabled,"allows_users_to_create_topics":!enabled}}).to_string())
        });
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let client = crate::telegram::build_telegram_client(&cfg).unwrap();
        assert_eq!(
            crate::telegram::discover_bot_username(&client, &mut cfg).unwrap(),
            "ActualBot"
        );
        assert_eq!(cfg.telegram_topics_enabled, Some(enabled));
        assert_eq!(
            server.requests.lock().unwrap().as_slice(),
            ["/bot123:token/getMe"]
        );
    }
    for raw in [
        r#"{"kind":"telegram","chat_id":"321","message_thread_id":false}"#,
        r#"{"kind":"telegram","chat_id":"321","message_thread_id":0}"#,
        r#"{"kind":"telegram","chat_id":"321#7"}"#,
    ] {
        assert!(crate::delivery::parse_delivery_target(Some(raw)).is_none());
    }
    let mut request = make_request(SessionKey::telegram("321#7"), "telegram", "saved");
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some("8".into()),
    };
    assert!(
        request_from_resume_payload(&build_resume_payload(&request, &request.input).unwrap())
            .is_err()
    );
}
