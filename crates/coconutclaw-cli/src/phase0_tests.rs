use super::*;

#[test]
fn telegram_command_identity_comes_from_get_me() {
    for (result, expected) in [
        (
            r#"{"is_bot":true,"username":"ActualBotName"}"#,
            Some("ActualBotName"),
        ),
        (r#"{"is_bot":false,"username":"OtherBot"}"#, None),
        (r#"{"is_bot":true}"#, None),
        (r#"{"is_bot":true,"username":"Bot@Other"}"#, None),
    ] {
        let body = format!(r#"{{"ok":true,"result":{result}}}"#);
        let server = FakeTelegramServer::start_with_response(move |_| (200, body.clone()));
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let client = crate::telegram::build_telegram_client(&cfg).unwrap();
        let discovered = crate::telegram::discover_bot_username(&client, &mut cfg);
        assert_eq!(discovered.ok().as_deref(), expected);
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].ends_with("/getMe"));
    }
}

#[test]
#[cfg(unix)]
fn real_fresh_command_resets_native_session_across_raw_and_canonical_history_aliases() {
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("history.sh");
    let args = cfg.tmp_dir.join("history-args");
    let prompt = cfg.tmp_dir.join("history-prompt");
    fs::write(
        &script,
        format!(
            r#"printf '%s\n' "$@" > '{}'
cat > '{}'
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: retained answer\n' > "$out"
printf '%s\n' '{{"type":"thread.started","thread_id":"canonical-native"}}'
"#,
            args.display(),
            prompt.display()
        ),
    )
    .unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    let mut store = Store::open(&cfg).unwrap();
    store
        .kv_set("codex_session:321:0", "legacy-native")
        .unwrap();
    store.conn.execute("INSERT INTO turns(ts,chat_id,input_type,user_text,provider_raw,status,channel) VALUES('now','321','text','legacy-private-history','','ok','telegram')",[]).unwrap();
    for (attempt, fresh) in [false, false, true].into_iter().enumerate() {
        if fresh {
            let scheduler = SessionScheduler::new(cfg.clone(), None);
            crate::loops::webhook::process_webhook_line(
                &cfg,
                &mut store,
                &scheduler,
                r#"{"update_id":99,"message":{"from":{"id":321},"chat":{"id":321},"text":"/fresh"}}"#,
            )
            .unwrap();
        }
        let input = crate::turn::resolve_turn_input(
            &cfg,
            &store,
            None,
            Some("next".into()),
            None,
            "telegram",
        )
        .unwrap();
        crate::turn::process_turn(
            &cfg,
            &mut store,
            input,
            &TaskSource::Telegram,
            Some("321".into()),
            None,
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
            None,
        )
        .unwrap();
        let recorded_args = fs::read_to_string(&args).unwrap();
        let context = fs::read_to_string(&prompt).unwrap();
        if fresh {
            assert!(!recorded_args.lines().any(|line| line == "resume"));
            assert!(!context.contains("legacy-private-history"));
        } else {
            if attempt == 0 {
                assert!(!recorded_args.lines().any(|line| line == "resume"));
            } else {
                assert!(recorded_args.contains("resume\ncanonical-native\n"));
            }
            assert!(context.contains("legacy-private-history"));
        }
    }
    assert_eq!(
        store.kv_get("codex_session:321:0").unwrap().as_deref(),
        Some("legacy-native")
    );
}

#[test]
fn telegram_retry_budget_applies_to_form_polling_and_multipart_requests() {
    for budget in [0, 2] {
        let server = FakeTelegramServer::start_with_response(|_| (500, "{}".into()));
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        cfg.telegram_api_retry_attempts = budget;
        let client = crate::telegram::build_telegram_client(&cfg).unwrap();
        assert!(crate::telegram::register_bot_commands(&client, &cfg).is_err());
        assert_eq!(server.requests.lock().unwrap().len(), budget as usize + 1);
    }
    for multipart in [false, true] {
        let server = FakeTelegramServer::start_with_response(|_| (500, "{}".into()));
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        cfg.telegram_api_retry_attempts = 1;
        let client = crate::telegram::build_telegram_client(&cfg).unwrap();
        let file = outbox_fixture(&cfg, "upload.txt");
        fs::write(&file, "artifact").unwrap();
        let failed = if multipart {
            crate::telegram::telegram_send_media_file(
                &client,
                &cfg,
                "321",
                "sendDocument",
                "document",
                &file,
            )
            .is_err()
        } else {
            crate::telegram::fetch_poll_updates(&client, &cfg, None).is_err()
        };
        assert!(failed);
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}

#[test]
fn partial_text_chunks_and_document_fallback_survive_restart_without_resending_successes() {
    let observations = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen = observations.clone();
    let mut later_chunk_calls = 0;
    let server = FakeTelegramServer::start_with_response(move |request| {
        seen.lock().unwrap().push(request.into());
        if request.contains("/sendDocument ") {
            return (
                400,
                r#"{"ok":false,"description":"document rejected"}"#.into(),
            );
        }
        if request.contains(&"Z".repeat(100)) {
            later_chunk_calls += 1;
            if later_chunk_calls == 1 {
                return (
                    400,
                    r#"{"ok":false,"description":"later chunk rejected"}"#.into(),
                );
            }
        }
        (200, r#"{"ok":true,"result":{"message_id":1}}"#.into())
    });
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    cfg.telegram_api_retry_attempts = 0;
    let store = Store::open(&cfg).unwrap();
    let mut request = make_request(SessionKey::telegram("321"), "telegram", "saved reply");
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: None,
    };
    let id = save_request(&store, &request, None);
    let output = format!("TELEGRAM_REPLY: {}{}", "A".repeat(4096), "Z".repeat(100));
    store
        .conn
        .execute(
            // Existing persisted document plans must survive the new default
            // representation, including partial fallback acknowledgments.
            "UPDATE task_payloads SET output=?2,delivery_state=?3 WHERE task_run_id=?1",
            rusqlite::params![id, output, json!({"completed_ops":[],"text_plan":{"kind":"document","markdown":format!("{}{}", "A".repeat(4096), "Z".repeat(100))}}).to_string()],
        )
        .unwrap();
    SessionScheduler::new(cfg.clone(), None)
        .retry_deliveries()
        .unwrap();
    let (state, done): (String, bool) = store
        .conn
        .query_row(
            "SELECT delivery_state,delivered FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let state: serde_json::Value = serde_json::from_str(&state).unwrap();
    assert_eq!(state["text_plan"]["kind"], "chunks");
    assert!(
        state["completed_ops"]
            .as_array()
            .unwrap()
            .contains(&json!("telegram:text:chunk:0"))
    );
    assert!(!done);
    cfg.telegram_parse_mode = coconutclaw_config::TelegramParseMode::Html;
    let restarted = SessionScheduler::new(cfg, None);
    restarted.retry_deliveries().unwrap();
    restarted.retry_deliveries().unwrap();
    assert!(
        store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [id],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    let requests = observations.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.contains("/sendDocument "))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.contains("/sendMessage ") && r.contains(&"A".repeat(100)))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.contains("/sendMessage ") && r.contains(&"Z".repeat(100)))
            .count(),
        2
    );
    assert!(
        requests
            .iter()
            .filter(|r| r.contains("/sendMessage "))
            .all(|r| !r.contains("parse_mode"))
    );
}

#[test]
#[cfg(unix)]
fn provider_progress_is_persisted_without_a_telegram_placeholder() {
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("progress.sh");
    fs::write(&script, r#"cat >/dev/null
printf '%s\n' '{"type":"item.started","item":{"type":"command_execution","command":"bounded-provider-progress"}}'
sleep 2
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: complete\n' > "$out"
"#).unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let request = make_stdout_request(SessionKey::local("progress"), "progress");
    let id = scheduler.enqueue(request.clone()).unwrap();
    let store = Store::open(&cfg).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let progress = store
            .get_task_run(id)
            .unwrap()
            .unwrap()
            .last_progress
            .unwrap_or_default();
        if progress.contains("bounded-provider-progress") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no persisted provider progress without placeholder"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        scheduler
            .render_active_tasks_for_session(&request.session.id())
            .unwrap()
            .contains("bounded-provider-progress")
    );
    store
        .update_task_run_progress(id, &"λ".repeat(2048))
        .unwrap();
    assert_eq!(
        store
            .get_task_run(id)
            .unwrap()
            .unwrap()
            .last_progress
            .unwrap()
            .chars()
            .count(),
        512
    );
    wait_for_terminal_tasks(&cfg, &[id]);
}

#[test]
#[cfg(unix)]
fn recovery_runs_older_queued_request_before_new_delegation() {
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("fifo.sh");
    let trace = cfg.tmp_dir.join("trace");
    fs::write(
        &script,
        format!(
            r#"cat | grep '^USER_TEXT:' >> '{}'
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: done\n' > "$out"
"#,
            trace.display()
        ),
    )
    .unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    let store = Store::open(&cfg).unwrap();
    let parent = make_stdout_request(SessionKey::local("fifo"), "parent");
    let parent_id = save_request(&store, &parent, None);
    store.update_task_run_started(parent_id, "now").unwrap();
    store.conn.execute("INSERT INTO turns(ts,chat_id,input_type,provider_raw,telegram_reply,status,task_run_id,channel)
        VALUES('now','local:fifo','text','TELEGRAM_REPLY: saved\nDELEGATE_TASK: later-delegation','saved','ok',?1,'local')", [parent_id]).unwrap();
    let older = make_stdout_request(SessionKey::local("fifo"), "older-queued");
    let older_id = save_request(&store, &older, None);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    // Recovery selects the oldest eligible lane first. With multiple free
    // slots, independent delegation and conversation providers may physically
    // start in either OS order. Reserve four slots in this admission fixture
    // so the real remaining slot must go to the older request.
    for slot in 0..4 {
        scheduler.inner.lanes.lock().unwrap().insert(
            format!("capacity-fixture:{slot}"),
            crate::scheduler::SessionLane {
                active: Some(i64::MAX - slot),
                ..Default::default()
            },
        );
    }
    scheduler.recover().unwrap();
    let child: i64 = store
        .conn
        .query_row(
            "SELECT id FROM task_runs WHERE update_id=?1",
            [format!("delegation:{parent_id}:0")],
            |r| r.get(0),
        )
        .unwrap();
    wait_for_terminal_tasks(&cfg, &[older_id, child]);
    scheduler
        .inner
        .lanes
        .lock()
        .unwrap()
        .retain(|key, _| !key.starts_with("capacity-fixture:"));
    scheduler.shutdown().unwrap();
    assert_eq!(
        fs::read_to_string(trace)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["USER_TEXT: older-queued", "USER_TEXT: later-delegation"]
    );
}

#[test]
fn cancelled_saved_turn_suppresses_media_in_recovery_and_legacy_replay() {
    let server = FakeTelegramServer::start();
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let store = Store::open(&cfg).unwrap();
    let mut request = make_request(SessionKey::telegram("321"), "telegram", "cancel");
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: None,
    };
    let id = save_request(&store, &request, Some("cancel-replay".into()));
    store.update_task_run_started(id, "now").unwrap();
    store.conn.execute("INSERT INTO turns(ts,chat_id,input_type,provider_raw,telegram_reply,status,update_id,task_run_id,channel)
        VALUES('now','local:cancel-replay','text','TELEGRAM_REPLY: suppressed\nSEND_DOCUMENT: /must-not-send\nMEMORY_APPEND: must-not-apply','Cancelled.','cancelled','cancel-replay',?1,'local')", [id]).unwrap();
    let legacy = store
        .rendered_output_for_update_id("cancel-replay")
        .unwrap()
        .unwrap();
    assert!(!legacy.contains("SEND_DOCUMENT"), "{legacy}");
    assert!(
        legacy.contains("Cancelled."),
        "retain the saved reply, not new wording"
    );
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.recover().unwrap();
    scheduler.retry_deliveries().unwrap();
    let output: String = store
        .conn
        .query_row(
            "SELECT output FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!output.contains("SEND_DOCUMENT"));
    assert!(!output.contains("MEMORY_APPEND"));
    assert_eq!(
        crate::markers::parse_markers(&output)
            .telegram_reply
            .as_deref(),
        Some("Cancelled."),
        "scheduler recovery must retain the exact nonblank saved reply too"
    );
    assert_eq!(store.get_task_run(id).unwrap().unwrap().status, "cancelled");
    assert_eq!(
        super::topics_rich::state(&store, id)["text_plan"]["markdown"],
        "Cancelled."
    );
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|path| !path.contains("sendDocument"))
    );
    scheduler.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn polling_invalid_commands_acknowledge_and_allow_other_chat_work() {
    let cases = [
        ("/approve", "Use /approve <approval id>"),
        ("/reject", "Use /reject <approval id>"),
        ("/cancel typo", "Use /cancel [run ID]"),
        ("/approve typo", "Use /approve <approval id>"),
        ("/reject typo", "Use /reject <approval id>"),
        ("/approve 1 extra", "Use /approve <approval id>"),
        ("/reject 1 extra", "Use /reject <approval id>"),
        ("/cancel 1 extra", "Use /cancel [run ID]"),
        ("/approve 0", "Use /approve <approval id>"),
        ("/reject -1", "Use /reject <approval id>"),
        ("/cancel 0", "Use /cancel [run ID]"),
        ("/approve 9223372036854775808", "Use /approve <approval id>"),
        ("/reject 9223372036854775808", "Use /reject <approval id>"),
        ("/cancel 9223372036854775808", "Use /cancel [run ID]"),
        ("/approve 999999", "Approval request not found."),
        ("/reject 999999", "Approval request not found."),
        (
            "/cancel 999999",
            "Run #999999 is not active for this conversation/topic.",
        ),
    ];
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_chat_ids = vec!["321".into(), "654".into()];
    let script = cfg.tmp_dir.join("command-provider.sh");
    fs::write(
        &script,
        r#"cat > "$0.input"
if grep -q '^USER_TEXT: provider-fails' "$0.input"; then exit 1; fi
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: after-command-result\n' > "$out"
"#,
    )
    .unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    let mut store = Store::open(&cfg).unwrap();
    store.conn.execute_batch("INSERT INTO task_runs(session_id,channel,prompt,status,created_at)
        VALUES('telegram:321','telegram','awaiting approval','awaiting_approval','now');
        INSERT INTO approvals(task_run_id,session_id,channel,prompt_text,status,resume_payload,created_at)
        VALUES(1,'telegram:321','telegram','approve','pending','{}','now');").unwrap();
    let mut updates: Vec<_> = cases
        .iter()
        .enumerate()
        .map(|(i, (text, _))| json!({"update_id":i+1,"message":{"from":{"id":321},"chat":{"id":321},"text":text}}))
        .collect();
    for text in ["provider-fails", "after-command"] {
        updates
            .push(json!({"update_id":updates.len()+1,"message":{"from":{"id":654},"chat":{"id":654},"text":text}}));
    }
    let count = updates.len();
    let shutdown = Arc::new(AtomicBool::new(false));
    let replies = Arc::new(Mutex::new(Vec::new()));
    let recorded = replies.clone();
    let stop = shutdown.clone();
    let observed = replies.clone();
    let expected_replies = cases.len();
    let observer = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while observed.lock().unwrap().len() < expected_replies && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        stop.store(true, Ordering::SeqCst);
        observed.lock().unwrap().len() == expected_replies
    });
    let mut polls = 0;
    let server = FakeTelegramServer::start_with_response(move |request| {
        if request.starts_with("GET ") {
            polls += 1;
            if polls > 1 {
                return (200, json!({"ok":true,"result":[]}).to_string());
            }
            return (200, json!({"ok":true,"result":updates}).to_string());
        }
        let body = request.split_once("\r\n\r\n").unwrap().1;
        let url = reqwest::Url::parse(&format!("http://localhost/?{body}")).unwrap();
        let fields: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        if fields.get("chat_id").map(String::as_str) == Some("321") {
            let text = fields
                .get("rich_message")
                .map(|rich| {
                    serde_json::from_str::<serde_json::Value>(rich).unwrap()["markdown"]
                        .as_str()
                        .unwrap()
                        .to_string()
                })
                .or_else(|| fields.get("text").cloned())
                .unwrap();
            recorded.lock().unwrap().push(text);
        }
        (200, r#"{"ok":true,"result":{"message_id":123}}"#.into())
    });
    cfg.telegram_api_base = Some(server.base_url.clone());
    let client = crate::telegram::build_telegram_client(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    crate::loops::poll::run_poll_loop(&cfg, &mut store, &scheduler, &client, &shutdown).unwrap();
    assert!(
        observer.join().unwrap(),
        "durable command replies did not dispatch"
    );
    assert_eq!(
        store.kv_get("last_update_id").unwrap(),
        Some(count.to_string())
    );
    assert_eq!(store.kv_get("inflight_update_id").unwrap(), None);
    let replies = replies.lock().unwrap();
    assert_eq!(replies.len(), cases.len());
    for (reply, (_, expected)) in replies.iter().zip(cases) {
        assert!(
            reply.starts_with(expected),
            "{reply:?}, expected {expected:?}"
        );
    }
    wait_for_terminal_tasks(&cfg, &[2, 3]);
    scheduler.shutdown().unwrap();
    assert_eq!(
        store.get_task_run(1).unwrap().unwrap().status,
        "awaiting_approval"
    );
    assert_eq!(
        store.get_pending_approval(1).unwrap().unwrap().status,
        "pending"
    );
    assert_eq!(store.get_task_run(2).unwrap().unwrap().status, "failed");
    let task = store.get_task_run(3).unwrap().unwrap();
    assert_eq!(
        (
            task.session_id.as_str(),
            task.prompt.as_str(),
            task.status.as_str()
        ),
        ("telegram:654", "after-command", "completed")
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
}

#[test]
#[cfg(unix)]
fn polling_command_storage_failure_retries_before_other_chat_acceptance() {
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_chat_ids = vec!["321".into(), "654".into()];
    let script = cfg.tmp_dir.join("storage-retry-provider.sh");
    fs::write(
        &script,
        r#"cat >/dev/null
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: after-command-result\n' > "$out"
"#,
    )
    .unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    let mut store = Store::open(&cfg).unwrap();
    store.conn.execute_batch("INSERT INTO task_runs(session_id,channel,prompt,status,created_at)
        VALUES('telegram:321','telegram','awaiting approval','awaiting_approval','now');
        INSERT INTO approvals(task_run_id,session_id,channel,prompt_text,status,resume_payload,created_at)
        VALUES(1,'telegram:321','telegram','approve','pending','{}','now');
        CREATE TRIGGER transient_failure BEFORE UPDATE ON approvals
        BEGIN SELECT RAISE(ABORT,'synthetic transient storage failure'); END;").unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let stop = shutdown.clone();
    let probe_cfg = cfg.clone();
    let offsets = Arc::new(Mutex::new(Vec::new()));
    let recorded = offsets.clone();
    let mut polls = 0;
    let server = FakeTelegramServer::start_with_response(move |request| {
        if request.starts_with("GET ") {
            recorded
                .lock()
                .unwrap()
                .push(request.lines().next().unwrap().to_string());
            polls += 1;
            if polls == 2 {
                let probe = Store::open(&probe_cfg).unwrap();
                assert_eq!(
                    probe.kv_get("last_update_id").unwrap().as_deref(),
                    Some("100")
                );
                assert_eq!(probe.kv_get("inflight_update_id").unwrap(), None);
                assert_eq!(
                    probe
                        .conn
                        .query_row(
                            "SELECT state FROM telegram_ingress WHERE update_id='100'",
                            [],
                            |r| r.get::<_, String>(0)
                        )
                        .unwrap(),
                    "pending",
                    "approval rollback is known, so retry is safe"
                );
                assert_eq!(
                    probe.get_pending_approval(1).unwrap().unwrap().status,
                    "pending"
                );
                assert!(probe.get_task_run(2).unwrap().is_none());
                probe
                    .conn
                    .execute_batch("DROP TRIGGER transient_failure")
                    .unwrap();
            }
            if polls == 3 {
                stop.store(true, Ordering::SeqCst);
                return (200, json!({"ok":true,"result":[]}).to_string());
            }
            return (
                200,
                json!({"ok":true,"result":[
                    {"update_id":100,"message":{"from":{"id":321},"chat":{"id":321},"text":"/reject 1"}},
                    {"update_id":101,"message":{"from":{"id":654},"chat":{"id":654},"text":"after-command"}}
                ]})
                .to_string(),
            );
        }
        (200, r#"{"ok":true,"result":{"message_id":123}}"#.into())
    });
    cfg.telegram_api_base = Some(server.base_url.clone());
    let client = crate::telegram::build_telegram_client(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    crate::loops::poll::run_poll_loop(&cfg, &mut store, &scheduler, &client, &shutdown).unwrap();
    let offsets = offsets.lock().unwrap();
    assert_eq!(offsets.len(), 3);
    let has_offset = |line: &str, id| {
        line.contains(&format!("offset={id}&")) || line.contains(&format!("offset={id} "))
    };
    assert!(
        has_offset(&offsets[0], 1) && has_offset(&offsets[1], 101),
        "{offsets:?}"
    );
    assert!(has_offset(&offsets[2], 102), "{offsets:?}");
    assert_eq!(
        store.kv_get("last_update_id").unwrap().as_deref(),
        Some("101")
    );
    assert_eq!(store.kv_get("inflight_update_id").unwrap(), None);
    assert_eq!(
        store.get_pending_approval(1).unwrap().unwrap().status,
        "rejected"
    );
    assert_eq!(store.get_task_run(1).unwrap().unwrap().status, "cancelled");
    wait_for_terminal_tasks(&cfg, &[2]);
    scheduler.shutdown().unwrap();
    let task = store.get_task_run(2).unwrap().unwrap();
    assert_eq!(
        (
            task.session_id.as_str(),
            task.prompt.as_str(),
            task.status.as_str()
        ),
        ("telegram:654", "after-command", "completed")
    );
    assert!(store.get_task_run(3).unwrap().is_none());
}

#[test]
fn polling_processing_failure_preserves_raw_custody_before_acknowledging_offset() {
    use std::io::{BufRead, BufReader, Write};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let mut cfg = RuntimeConfig::test_config();
    cfg.poll_interval_seconds = 1;
    let mut store = Store::open(&cfg).unwrap();
    store.conn.execute("INSERT INTO approvals(task_run_id,session_id,channel,prompt_text,status,resume_payload,created_at)
        VALUES(1,'telegram:321','telegram','approve','pending','{}','now')", []).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    cfg.telegram_api_base = Some(format!("http://{}", listener.local_addr().unwrap()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let stop = shutdown.clone();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for index in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            requests.push(line.clone());
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            let body = if index == 0 {
                r#"{"ok":true,"result":[{"update_id":100,"message":{"from":{"id":321},"message_id":1,"chat":{"id":321},"text":"/approve 1"}}]}"#
            } else {
                stop.store(true, Ordering::SeqCst);
                r#"{"ok":true,"result":[]}"#
            };
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        }
        requests
    });
    let client = crate::telegram::build_telegram_client(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    crate::loops::poll::run_poll_loop(&cfg, &mut store, &scheduler, &client, &shutdown).unwrap();
    let requests = server.join().unwrap();
    assert!(requests[0].contains("offset=1&") || requests[0].contains("offset=1 "));
    assert!(requests[1].contains("offset=101&") || requests[1].contains("offset=101 "));
    assert_eq!(
        store.kv_get("last_update_id").unwrap().as_deref(),
        Some("100")
    );
    assert_eq!(store.kv_get("inflight_update_id").unwrap().as_deref(), None);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM telegram_ingress WHERE update_id='100'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "pending"
    );
    assert_eq!(
        store.get_pending_approval(1).unwrap().unwrap().status,
        "pending"
    );
    scheduler.shutdown().unwrap();
}

#[test]
fn reconciliation_preserves_historical_audio_columns_and_inert_schedules() {
    let cfg = RuntimeConfig::test_config();
    let mut store = Store::open(&cfg).unwrap();
    store.conn.execute("INSERT INTO turns(ts,chat_id,input_type,user_text,asr_text,provider_raw,telegram_reply,voice_reply,status)
        VALUES('old','321','voice','old user input','historical transcript','TELEGRAM_REPLY: saved reply',NULL,'historical voice reply','ok')", []).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO scheduled_tasks(ts,source,prompt,schedule_time,pending_output)
        VALUES('old','legacy','do not execute','00:00','preserved legacy result')",
            [],
        )
        .unwrap();
    crate::recovery::pending::reconcile_pending_turn_side_effects(&cfg, &mut store).unwrap();
    let old: (String, String) = store
        .conn
        .query_row("SELECT asr_text,voice_reply FROM turns LIMIT 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(
        old,
        (
            "historical transcript".into(),
            "historical voice reply".into()
        )
    );
    drop(store);
    let store = Store::open(&cfg).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT pending_output FROM scheduled_tasks LIMIT 1",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "preserved legacy result"
    );
}

#[test]
fn queued_attachment_identity_survives_restart_without_approval_authorization() {
    let mut request = make_request(SessionKey::telegram("321"), "telegram", "inspect file");
    request.update_id = Some("12345".into());
    request.media = Some(super::super::TaskMedia::Telegram(
        crate::IncomingMedia::Document {
            file_id: "mock-document-id".into(),
            file_name: Some("report.pdf".into()),
        },
    ));
    let raw = build_resume_payload(&request, &request.input).unwrap();
    let restored = crate::approval::request_from_payload(&raw, false).unwrap();
    assert_eq!(restored.update_id.as_deref(), Some("12345"));
    assert!(
        matches!(restored.media, Some(super::super::TaskMedia::Telegram(crate::IncomingMedia::Document { file_id, .. })) if file_id == "mock-document-id")
    );
    assert!(restored.input.supplemental_context.is_none());
}

#[test]
fn owned_input_attachment_is_kept_until_delivery_is_acknowledged() {
    let mut cfg = RuntimeConfig::test_config();
    // A definite API rejection is safely retryable on every platform. An
    // unreachable local port can time out instead of refusing on Windows and is
    // then (correctly) held as an unknown delivery outcome with no retry.
    let rejecting = FakeTelegramServer::start_with_response(|_| {
        (
            400,
            r#"{"ok":false,"error_code":400,"description":"fixture rejection"}"#.into(),
        )
    });
    cfg.telegram_api_base = Some(
        rejecting
            .base_url
            .trim_end_matches("/bot123:token")
            .to_string(),
    );
    cfg.telegram_api_timeout_secs = 1;
    cfg.telegram_api_retry_attempts = 0;
    let store = Store::open(&cfg).unwrap();
    let path = outbox_fixture(&cfg, "owned-document.txt");
    fs::write(&path, "retained artifact").unwrap();
    let mut request = make_request(SessionKey::telegram("321"), "telegram", "return attachment");
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: None,
    };
    request.input.attachment_owned = true;
    request.input.attachment_path = Some(path.clone());
    let id = save_request(&store, &request, None);
    store
        .conn
        .execute(
            "UPDATE task_payloads SET output=?2 WHERE task_run_id=?1",
            rusqlite::params![id, format!("SEND_DOCUMENT: {}", path.display())],
        )
        .unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.retry_deliveries().unwrap();
    assert!(path.exists());
    assert!(
        !store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [id],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    // Model a restart, not two live delivery owners competing for one lease.
    scheduler.shutdown().unwrap();
    let server = FakeTelegramServer::start();
    cfg.telegram_api_base = Some(
        server
            .base_url
            .trim_end_matches("/bot123:token")
            .to_string(),
    );
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.retry_deliveries().unwrap();
    assert!(!path.exists());
    assert!(
        store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [id],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
}

#[test]
fn codex_json_fallback_and_retired_markers_do_not_trigger_removed_features() {
    let text = crate::markers::extract_assistant_text_from_json_stream(
        r#"{"type":"item.completed","item":{"type":"agent_message","text":"retained Codex answer"}}"#).unwrap();
    assert_eq!(text, "retained Codex answer");
    let parsed = crate::markers::parse_markers(
        "TELEGRAM_REPLY: hello\nVOICE_REPLY: ignored\nSCHEDULE_PROMPT: once 10:00|ignored",
    );
    assert_eq!(parsed.telegram_reply.as_deref(), Some("hello"));
    assert_eq!(
        parsed.to_effects(),
        vec![crate::markers::Effect::TelegramReply("hello".into())]
    );
}

fn save_request(store: &Store, request: &TaskRequest, update: Option<String>) -> i64 {
    let id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: request.session.id(),
            channel: request.source.channel_name().into(),
            source_chat_id: None,
            source_user_id: None,
            update_id: update,
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
            rusqlite::params![id, build_resume_payload(request, &request.input).unwrap()],
        )
        .unwrap();
    id
}

#[test]
fn approval_survives_restart_is_chat_scoped_and_resumes_only_once() {
    let temp = tempfile::tempdir().unwrap();
    let provider = write_fake_provider_script(
        temp.path(),
        "approval-mock",
        r#"#!/bin/sh
cat >/dev/null
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: approved work complete\n' > "$out"
"#,
        "Write-Output 'TELEGRAM_REPLY: approved work complete'",
    );
    let cfg = scheduler_test_config(provider);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let request = make_stdout_request(SessionKey::telegram("321"), "approved work");
    let store = Store::open(&cfg).unwrap();
    let id = save_request(&store, &request, None);
    assert!(
        scheduler
            .persist_result(
                &store,
                id,
                &request,
                &request.input,
                "TELEGRAM_REPLY: waiting\nSEND_APPROVAL: May I do this?",
                crate::TurnStatus::Ok
            )
            .unwrap()
    );
    let approval: i64 = store
        .conn
        .query_row("SELECT id FROM approvals", [], |r| r.get(0))
        .unwrap();
    drop(scheduler);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.recover().unwrap();
    assert_eq!(
        store.get_task_run(id).unwrap().unwrap().status,
        "awaiting_approval"
    );
    assert!(
        scheduler
            .resolve_approval(approval, "approve", "telegram:999")
            .unwrap()
            .unwrap()
            .contains("not authorized")
    );
    assert_eq!(
        store
            .get_pending_approval(approval)
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    scheduler
        .resolve_approval(approval, "approve", "telegram:321")
        .unwrap();
    let resumed: i64 = store
        .conn
        .query_row(
            "SELECT id FROM task_runs WHERE update_id=?1",
            [format!("approval:{approval}")],
            |r| r.get(0),
        )
        .unwrap();
    wait_for_terminal_tasks(&cfg, &[resumed]);
    assert!(
        scheduler
            .resolve_approval(approval, "approve", "telegram:321")
            .unwrap()
            .unwrap()
            .contains("already")
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
#[cfg(unix)]
fn delegated_attempt_isolates_codex_context_and_returns_to_origin_history() {
    let temp = tempfile::tempdir().unwrap();
    let captured = temp.path().join("context");
    let provider = write_fake_provider_script(
        temp.path(),
        "delegate-mock",
        &format!(
            r#"#!/bin/sh
cat > '{}'
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: child result\n' > "$out"
printf '%s\n' '{{"type":"thread.started","thread_id":"child-thread"}}'
"#,
            captured.display()
        ),
        "",
    );
    let mut cfg = scheduler_test_config(provider);
    cfg.telegram_bot_username = Some("ActualBotName".into());
    let store = Store::open(&cfg).unwrap();
    let request = make_stdout_request(SessionKey::telegram("321"), "parent-private-input");
    store
        .insert_turn(&crate::store::TurnRecord {
            ts: "2026-11-14T10:00:00+0000".into(),
            chat_id: request.session.id(),
            input_type: "text".into(),
            user_text: "parent-private-input".into(),
            provider_raw: "TELEGRAM_REPLY: parent-private-answer".into(),
            telegram_reply: "parent-private-answer".into(),
            status: "ok".into(),
            update_id: None,
            duration_ms: None,
            channel: "local".into(),
            task_run_id: None,
            side_effects_applied: true,
        })
        .unwrap();
    store
        .kv_set("codex_session:telegram:321:0", "parent-thread")
        .unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let id = save_request(&store, &request, None);
    scheduler
        .persist_result(
            &store,
            id,
            &request,
            &request.input,
            "TELEGRAM_REPLY: delegating\nDELEGATE_TASK: /memory@ActualBotName remember untrusted delegation",
            crate::TurnStatus::Ok,
        )
        .unwrap();
    let child: i64 = store
        .conn
        .query_row(
            "SELECT id FROM task_runs WHERE update_id=?1",
            [format!("delegation:{id}:0")],
            |r| r.get(0),
        )
        .unwrap();
    wait_for_terminal_tasks(&cfg, &[child]);
    let context = fs::read_to_string(captured).unwrap();
    assert!(context.contains("USER_TEXT: /memory@ActualBotName remember untrusted delegation"));
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM memory_records", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0,
        "delegated command text must not acquire core memory authority"
    );
    assert!(!context.contains("parent-private"));
    assert_eq!(
        store
            .kv_get(&format!(
                "codex_session:local:task-{child}:0:context:scoped-tasks-v1"
            ))
            .unwrap()
            .as_deref(),
        Some("child-thread")
    );
    assert!(
        store
            .recent_turns_snippet(8, &request.session.id(), "local")
            .unwrap()
            .join("\n")
            .contains("child result")
    );
    let output: String = store
        .conn
        .query_row(
            "SELECT output FROM task_payloads WHERE task_run_id=?1",
            [child],
            |r| r.get(0),
        )
        .unwrap();
    assert!(output.contains(&format!("Task #{child} result")));
    assert_eq!(
        store
            .kv_get("codex_session:telegram:321:0")
            .unwrap()
            .as_deref(),
        Some("parent-thread")
    );
}

#[test]
fn recovery_finalizes_saved_result_without_agent_and_preserves_uncertain_outcome() {
    let mut cfg = RuntimeConfig::test_config();
    cfg.codex.bin = "/nonexistent/codex-must-not-run".into();
    let store = Store::open(&cfg).unwrap();
    let request = make_stdout_request(SessionKey::local("recovery"), "never rerun");
    let saved = save_request(&store, &request, Some("saved".into()));
    let unknown = save_request(&store, &request, Some("unknown".into()));
    for id in [saved, unknown] {
        store.update_task_run_started(id, "now").unwrap();
    }
    store
        .insert_turn(&crate::store::TurnRecord {
            ts: "now".into(),
            chat_id: request.session.id(),
            input_type: "text".into(),
            user_text: "never rerun".into(),
            provider_raw: "TELEGRAM_REPLY: saved result".into(),
            telegram_reply: "saved result".into(),
            status: "ok".into(),
            update_id: None,
            duration_ms: None,
            channel: "local".into(),
            task_run_id: Some(saved),
            side_effects_applied: true,
        })
        .unwrap();
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.recover().unwrap();
    scheduler.recover().unwrap();
    assert_eq!(
        store.get_task_run(saved).unwrap().unwrap().status,
        "completed"
    );
    let unknown = store.get_task_run(unknown).unwrap().unwrap();
    assert_eq!(unknown.status, "failed");
    assert!(unknown.error_summary.unwrap().contains("outcome unknown"));
    // Recovery materializes synchronously but network/stdout dispatch now has
    // its own owner. Require automatic eventual delivery, not a manual drain.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !store
        .conn
        .query_row(
            "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
            [saved],
            |r| r.get::<_, bool>(0),
        )
        .unwrap()
    {
        assert!(
            Instant::now() < deadline,
            "recovered outbox did not dispatch"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [saved],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
}

#[test]
fn delivery_retry_skips_acknowledged_operations_after_restart() {
    let server = FakeTelegramServer::start();
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_api_base = Some(
        server
            .base_url
            .trim_end_matches("/bot123:token")
            .to_string(),
    );
    cfg.codex.bin = "/nonexistent/codex-must-not-run".into();
    let store = Store::open(&cfg).unwrap();
    let mut request = make_request(SessionKey::telegram("321"), "telegram", "saved");
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: None,
    };
    let file = outbox_fixture(&cfg, "result.txt");
    fs::write(&file, "artifact").unwrap();
    let id = save_request(&store, &request, None);
    store
        .conn
        .execute(
            "UPDATE task_payloads SET output=?2,delivery_state=?3 WHERE task_run_id=?1",
            rusqlite::params![
                id,
                format!(
                    "TELEGRAM_REPLY: already delivered\nSEND_DOCUMENT: {}",
                    file.display()
                ),
                "[\"telegram:text\"]"
            ],
        )
        .unwrap();
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.retry_deliveries().unwrap();
    scheduler.retry_deliveries().unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("sendDocument"));
}

#[test]
fn rejected_approval_never_resumes() {
    let mut cfg = RuntimeConfig::test_config();
    cfg.codex.bin = "/nonexistent/codex-must-not-run".into();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let store = Store::open(&cfg).unwrap();
    let request = make_stdout_request(SessionKey::telegram("321"), "do not execute");
    let id = save_request(&store, &request, None);
    scheduler
        .persist_result(
            &store,
            id,
            &request,
            &request.input,
            "TELEGRAM_REPLY: waiting\nSEND_APPROVAL: allow?",
            crate::TurnStatus::Ok,
        )
        .unwrap();
    let approval: i64 = store
        .conn
        .query_row("SELECT id FROM approvals", [], |r| r.get(0))
        .unwrap();
    scheduler
        .resolve_approval(approval, "reject", "telegram:321")
        .unwrap();
    assert_eq!(store.get_task_run(id).unwrap().unwrap().status, "cancelled");
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}
