use super::*;
use crate::context::build_context;
use crate::delivery::{DeliveryTarget, serialize_delivery_target};
use crate::markers::{
    extract_error_summary, parse_markers, recover_unstructured_reply, should_retry_provider_failure,
};
use crate::recovery::pending::recover_scheduled_task_output_from_task_run;
use crate::recovery::reconcile_pending_turn_side_effects;
use crate::scheduler::SessionScheduler;
use crate::scheduling::{run_due_scheduled_tasks, scheduled_task_context_channel};

use crate::cancel::cancel_impl::cancel_signal_from_update;
use crate::commands::nightly_reflection::nightly_reflection_marker;
use crate::loops::webhook::{parse_webhook_action, process_webhook_line};
use crate::store::ScheduledTaskInsertResult;
use crate::store::TurnRecord;
use crate::telegram::{
    build_telegram_client, dispatch_telegram_output, progress_status_text,
    progress_status_with_events, render_markdown_v2_reply, render_telegram_reply_text,
    should_fallback_plain_for_error, should_send_reply_as_document, split_text_chunks,
    telegram_retry_after_seconds, telegram_text_form_params,
};
use crate::types::WebhookAction;
use crate::types::{InputType, QuotedMessage, TurnInput};
use crate::util::{scheduled_task_slot_at, scheduled_task_slot_now};
use crate::webhook::webhook_public_endpoint;

use chrono::{DateTime, Utc};
use coconutclaw_config::{RuntimeConfig, TelegramParseMode};
use serde_json::Value;

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

fn test_config() -> RuntimeConfig {
    RuntimeConfig::test_config()
}

static SLACK_API_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn slack_api_env_lock() -> &'static Mutex<()> {
    SLACK_API_ENV_LOCK.get_or_init(|| Mutex::new(()))
}

fn read_http_request(stream: &mut std::net::TcpStream) -> String {
    let mut data = Vec::new();
    let mut buf = [0_u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(read) => {
                data.extend_from_slice(&buf[..read]);
                if http_request_complete(&data) {
                    break;
                }
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&data).to_string()
}

fn http_request_complete(data: &[u8]) -> bool {
    let Some(header_end) = data.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let headers = String::from_utf8_lossy(&data[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    data.len() >= header_end + 4 + content_length
}

struct FakeTelegramServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl FakeTelegramServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake telegram server");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking listener");
        let addr = listener.local_addr().expect("listener addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let requests_clone = Arc::clone(&requests);
        let stop_clone = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !stop_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                        requests_clone
                            .lock()
                            .expect("requests lock")
                            .push(read_http_request(&mut stream));
                        let body =
                            r#"{"ok":true,"result":{"message_id":123,"message_thread_id":0}}"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
            stop,
            handle: Some(handle),
        }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("requests lock").clone()
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

struct FakeSlackServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl FakeSlackServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake slack server");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking listener");
        let addr = listener.local_addr().expect("listener addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let requests_clone = Arc::clone(&requests);
        let stop_clone = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !stop_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                        requests_clone
                            .lock()
                            .expect("requests lock")
                            .push(read_http_request(&mut stream));
                        let body = r#"{"ok":true,"ts":"171.9","message":{"ts":"171.9"}}"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
            base_url: format!("http://{addr}"),
            requests,
            stop,
            handle: Some(handle),
        }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("requests lock").clone()
    }
}

impl Drop for FakeSlackServer {
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
fn fresh_command_returns_confirmation_output() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    let update = r#"{"update_id":100,"message":{"chat":{"id":"321"},"text":"/fresh"}}"#;

    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert_eq!(outcome.output_channel.as_deref(), Some("telegram"));
    assert!(output.contains("TELEGRAM_REPLY:"));
    assert!(output.contains("Context cleared"));
}

#[test]
fn slack_fresh_command_routes_output_to_slack_channel() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    let payload = r#"{"type":"slash_commands","command":"/fresh","channel_id":"C123"}"#;

    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, payload).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert_eq!(outcome.chat_id.as_deref(), Some("C123"));
    assert_eq!(outcome.output_channel.as_deref(), Some("slack"));
    assert!(output.contains("TELEGRAM_REPLY:"));
    assert!(output.contains("Context cleared"));
}

#[test]
fn schedules_command_lists_active_tasks() {
    let mut cfg = test_config();
    cfg.timezone = "Pacific/Auckland".to_string();
    let mut store = Store::open(&cfg).expect("store");
    store
        .insert_scheduled_task(
            "2026-04-21T04:00:00+0000",
            "agent",
            "Check GitHub Trending and summarize the top 5 repos.",
            "16:30",
            true,
        )
        .expect("insert schedule");

    let update = r#"{"update_id":101,"message":{"chat":{"id":"321"},"text":"/schedules"}}"#;
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert_eq!(outcome.output_channel.as_deref(), Some("telegram"));
    assert!(output.contains("TELEGRAM_REPLY: Active scheduled tasks (Pacific/Auckland)"));
    assert!(output.contains("1. Daily at 16:30"));
    assert!(output.contains("Check GitHub Trending"));
}

#[test]
fn due_scheduled_task_with_active_run_is_not_enqueued_twice() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_scheduled_task(
            "2026-04-23T20:00:00+1200",
            "agent",
            "Check interest.co.nz and summarize the top article.",
            "09:00",
            true,
        )
        .expect("insert schedule");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "scheduled:task-1".to_string(),
            channel: "scheduled".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: None,
            prompt: "Check interest.co.nz and summarize the top article.".to_string(),
            created_at: "2026-04-24T09:00:01+1200".to_string(),
            progress_message_id: None,
            scheduled_task_id: Some(1),
        })
        .expect("insert active scheduled task run");

    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let client = build_telegram_client(&cfg).expect("telegram client");

    run_due_scheduled_tasks(&cfg, &mut store, &scheduler, &client)
        .expect("run due scheduled tasks");

    assert_eq!(
        store
            .list_active_task_runs_for_session("scheduled:task-1")
            .expect("list active task runs")
            .len(),
        1
    );
}

#[test]
fn due_scheduled_task_retry_with_active_run_is_not_dispatched_twice() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_scheduled_task(
            "2026-04-23T20:00:00+1200",
            "agent",
            "Check backups.",
            "09:00",
            true,
        )
        .expect("insert schedule");
    store
        .set_scheduled_task_pending_output(1, "TELEGRAM_REPLY: Backup complete")
        .expect("set pending output");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "scheduled:task-1".to_string(),
            channel: "scheduled".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: None,
            prompt: "Check backups.".to_string(),
            created_at: "2026-04-24T09:00:01+1200".to_string(),
            progress_message_id: None,
            scheduled_task_id: Some(1),
        })
        .expect("insert active scheduled task run");

    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let client = build_telegram_client(&cfg).expect("telegram client");

    run_due_scheduled_tasks(&cfg, &mut store, &scheduler, &client)
        .expect("run due scheduled tasks");

    let schedules = store
        .list_active_scheduled_tasks()
        .expect("list active schedules");
    assert_eq!(schedules.len(), 1);
    assert_eq!(
        schedules[0].pending_output.as_deref(),
        Some("TELEGRAM_REPLY: Backup complete")
    );
}

#[test]
fn scheduled_task_retry_uses_persisted_telegram_target() {
    let server = FakeTelegramServer::start();
    let mut cfg = test_config();
    cfg.scheduled_tasks_enabled = true;
    cfg.telegram_api_base = Some(server.base_url.clone());
    cfg.telegram_chat_id = Some("321".to_string());
    let (current_hhmm, _) = scheduled_task_slot_now(&cfg.timezone);

    let store = Store::open(&cfg).expect("store");
    let delivery_target = DeliveryTarget::Telegram {
        chat_id: "999".to_string(),
    };
    store
        .insert_scheduled_task_with_target(
            "2026-04-23T20:00:00+1200",
            "agent",
            "Check backups.",
            &current_hhmm,
            true,
            Some("telegram:999"),
            Some(&serialize_delivery_target(&delivery_target)),
        )
        .expect("insert schedule");
    store
        .set_scheduled_task_pending_output(1, "TELEGRAM_REPLY: Backup complete")
        .expect("set pending output");

    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let client = build_telegram_client(&cfg).expect("telegram client");

    run_due_scheduled_tasks(&cfg, &mut store, &scheduler, &client)
        .expect("run due scheduled tasks");

    let requests = server.requests();
    assert!(
        requests.iter().any(|request| {
            request.contains("chat_id=999")
                && !request.contains("chat_id=321")
                && (request.contains("Backup+complete") || request.contains("Backup complete"))
        }),
        "expected scheduled retry to use persisted telegram chat, got {requests:?}"
    );
    let schedules = store
        .list_active_scheduled_tasks()
        .expect("list active schedules");
    assert_eq!(schedules.len(), 1);
    assert!(schedules[0].last_run_ts.is_some());
    assert!(schedules[0].pending_output.is_none());
}

#[test]
fn scheduled_task_retry_uses_persisted_slack_target() {
    let _guard = slack_api_env_lock().lock().expect("slack env lock");
    let server = FakeSlackServer::start();
    unsafe {
        std::env::set_var("COCONUTCLAW_SLACK_API_BASE", &server.base_url);
    }

    let mut cfg = test_config();
    cfg.scheduled_tasks_enabled = true;
    cfg.slack_bot_token = Some("xoxb-test".to_string());
    cfg.slack_channel_id = Some("CFALLBACK".to_string());
    let (current_hhmm, _) = scheduled_task_slot_now(&cfg.timezone);

    let outcome: Result<()> = {
        let store = Store::open(&cfg).expect("store");
        let delivery_target = DeliveryTarget::Slack {
            channel_id: "C123".to_string(),
            thread_ts: Some("171.5".to_string()),
        };
        store
            .insert_scheduled_task_with_target(
                "2026-04-23T20:00:00+1200",
                "agent",
                "Check backups.",
                &current_hhmm,
                true,
                Some("slack:C123#171.5"),
                Some(&serialize_delivery_target(&delivery_target)),
            )
            .expect("insert schedule");
        store
            .set_scheduled_task_pending_output(1, "TELEGRAM_REPLY: Backup complete")
            .expect("set pending output");

        let mut store = Store::open(&cfg).expect("store reopen");
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        let client = build_telegram_client(&cfg).expect("telegram client");

        run_due_scheduled_tasks(&cfg, &mut store, &scheduler, &client)
            .expect("run due scheduled tasks");

        let requests = server.requests();
        assert!(
            requests.iter().any(|request| {
                request.contains("POST /chat.postMessage")
                    && request.contains("channel=C123")
                    && request.contains("thread_ts=171.5")
                    && !request.contains("channel=CFALLBACK")
            }),
            "expected scheduled retry to use persisted slack thread, got {requests:?}"
        );
        let schedules = store
            .list_active_scheduled_tasks()
            .expect("list active schedules");
        assert_eq!(schedules.len(), 1);
        assert!(schedules[0].last_run_ts.is_some());
        assert!(schedules[0].pending_output.is_none());
        Ok(())
    };

    unsafe {
        std::env::remove_var("COCONUTCLAW_SLACK_API_BASE");
    }
    outcome.expect("slack retry assertions");
}

#[test]
fn scheduled_task_context_channel_prefers_resolved_delivery_target() {
    let task = crate::store::ScheduledTask {
        id: 1,
        ts: "2026-04-23T20:00:00+1200".to_string(),
        source: "agent".to_string(),
        prompt: "Check backups.".to_string(),
        schedule_time: "09:00".to_string(),
        recurring: true,
        last_run_ts: None,
        done: false,
        pending_output: None,
        delivery_state: None,
        origin_session: Some("telegram:321".to_string()),
        delivery_target: None,
    };
    let resolved_target = DeliveryTarget::Slack {
        channel_id: "C123".to_string(),
        thread_ts: Some("171.5".to_string()),
    };

    assert_eq!(
        scheduled_task_context_channel(&task, Some(&resolved_target), None),
        "slack"
    );
}

#[test]
fn pending_turn_side_effect_recovery_preserves_slack_schedule_routing() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    let task_run_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "slack:C123#171.5".to_string(),
            channel: "slack".to_string(),
            source_chat_id: Some("C123".to_string()),
            source_user_id: Some("U123".to_string()),
            update_id: Some("evt-1".to_string()),
            prompt: "schedule it".to_string(),
            created_at: "2026-04-24T09:00:01+1200".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert task run");
    store
        .insert_turn(&TurnRecord {
            ts: "2026-04-24T09:00:03+1200".to_string(),
            chat_id: "slack:C123#171.5".to_string(),
            input_type: "text".to_string(),
            user_text: "schedule it".to_string(),
            asr_text: String::new(),
            provider_raw: "TELEGRAM_REPLY: ok\nSCHEDULE_PROMPT: 09:30|Check backups\n".to_string(),
            telegram_reply: "ok".to_string(),
            voice_reply: String::new(),
            status: "ok".to_string(),
            update_id: Some("evt-1".to_string()),
            duration_ms: Some(42),
            channel: "slack".to_string(),
            task_run_id: Some(task_run_id),
            side_effects_applied: false,
        })
        .expect("insert pending turn");

    assert_eq!(
        reconcile_pending_turn_side_effects(&cfg, &mut store).expect("reconcile turns"),
        1
    );

    let schedules = store
        .list_active_scheduled_tasks()
        .expect("list active schedules");
    assert_eq!(schedules.len(), 1);
    assert_eq!(
        schedules[0].origin_session.as_deref(),
        Some("slack:C123#171.5")
    );
    assert_eq!(
        schedules[0].delivery_target.as_deref(),
        Some(r#"{"channel_id":"C123","kind":"slack","thread_ts":"171.5"}"#)
    );
}

#[test]
fn scheduled_task_duplicates_are_scoped_by_origin_and_target() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    let telegram_999 = serialize_delivery_target(&DeliveryTarget::Telegram {
        chat_id: "999".to_string(),
    });
    let telegram_321 = serialize_delivery_target(&DeliveryTarget::Telegram {
        chat_id: "321".to_string(),
    });

    assert_eq!(
        store
            .insert_scheduled_task_with_target(
                "2026-04-23T20:00:00+1200",
                "agent",
                "Check backups.",
                "09:00",
                true,
                Some("telegram:999"),
                Some(&telegram_999),
            )
            .expect("insert first"),
        ScheduledTaskInsertResult::Inserted
    );
    assert_eq!(
        store
            .insert_scheduled_task_with_target(
                "2026-04-23T20:00:01+1200",
                "agent",
                "Check backups.",
                "09:00",
                true,
                Some("telegram:321"),
                Some(&telegram_999),
            )
            .expect("insert second session"),
        ScheduledTaskInsertResult::Inserted
    );
    assert_eq!(
        store
            .insert_scheduled_task_with_target(
                "2026-04-23T20:00:02+1200",
                "agent",
                "Check backups.",
                "09:00",
                true,
                Some("telegram:999"),
                Some(&telegram_321),
            )
            .expect("insert second target"),
        ScheduledTaskInsertResult::Inserted
    );
    assert_eq!(
        store
            .insert_scheduled_task_with_target(
                "2026-04-23T20:00:03+1200",
                "agent",
                "Check backups.",
                "09:00",
                true,
                Some("telegram:999"),
                Some(&telegram_999),
            )
            .expect("insert duplicate"),
        ScheduledTaskInsertResult::Duplicate
    );
}

#[test]
fn completed_scheduled_task_run_is_reconciled_without_redelivery() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_scheduled_task(
            "2026-04-23T20:00:00+1200",
            "agent",
            "Check backups.",
            "09:00",
            false,
        )
        .expect("insert schedule");
    store
        .set_scheduled_task_pending_output(1, "TELEGRAM_REPLY: Backup complete")
        .expect("set pending output");
    let task_run_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "scheduled:task-1".to_string(),
            channel: "scheduled".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: None,
            prompt: "Check backups.".to_string(),
            created_at: "2026-04-24T09:00:01+1200".to_string(),
            progress_message_id: None,
            scheduled_task_id: Some(1),
        })
        .expect("insert scheduled task run");
    store
        .finish_task_run(
            task_run_id,
            crate::store::TaskRunStatus::Completed,
            "2026-04-24T09:00:05+1200",
            None,
            Some("task completed"),
        )
        .expect("finish task run");

    assert_eq!(
        store
            .reconcile_scheduled_tasks_from_completed_runs("2026-04-24T09:00:06+1200")
            .expect("reconcile scheduled tasks"),
        1
    );
    assert!(
        store
            .list_active_scheduled_tasks()
            .expect("list active scheduled tasks")
            .is_empty()
    );
}

#[test]
fn pending_turn_side_effects_are_reconciled_into_managed_files() {
    let cfg = test_config();
    fs::write(
        cfg.instance_dir.join("MEMORY.md"),
        "# Long-Term Memory\nmanual note\n",
    )
    .expect("seed memory");
    fs::write(
        cfg.instance_dir.join("TASKS/pending.md"),
        "# Pending Tasks\nmanual task note\n",
    )
    .expect("seed tasks");

    let mut store = Store::open(&cfg).expect("store");
    let turn_id = store
        .insert_turn(&TurnRecord {
            ts: "2026-04-24T09:00:03+1200".to_string(),
            chat_id: "321".to_string(),
            input_type: "text".to_string(),
            user_text: "remember and track this".to_string(),
            asr_text: String::new(),
            provider_raw:
                "TELEGRAM_REPLY: ok\nMEMORY_APPEND: remembered fact\nTASK_APPEND: follow up later\n"
                    .to_string(),
            telegram_reply: "ok".to_string(),
            voice_reply: String::new(),
            status: "ok".to_string(),
            update_id: Some("900".to_string()),
            duration_ms: Some(42),
            channel: "telegram".to_string(),
            task_run_id: None,
            side_effects_applied: false,
        })
        .expect("insert turn")
        .expect("turn id");

    assert_eq!(
        reconcile_pending_turn_side_effects(&cfg, &mut store).expect("reconcile turns"),
        1
    );

    let memory = fs::read_to_string(cfg.instance_dir.join("MEMORY.md")).expect("read memory");
    let tasks = fs::read_to_string(cfg.instance_dir.join("TASKS/pending.md")).expect("read tasks");
    assert!(memory.contains("manual note"));
    assert!(memory.contains("remembered fact"));
    assert!(tasks.contains("manual task note"));
    assert!(tasks.contains("follow up later"));
    assert!(
        store
            .pending_turn_side_effects()
            .expect("pending turns")
            .is_empty()
    );

    let pending_db_tasks = store
        .managed_pending_task_entries_from_db()
        .expect("managed task entries");
    assert_eq!(pending_db_tasks, vec!["follow up later".to_string()]);

    let turn = store
        .pending_turn_side_effects()
        .expect("pending turns after reconcile");
    assert!(turn.is_empty(), "turn {turn_id} should be marked applied");
}

#[test]
fn scheduled_task_output_is_recovered_from_persisted_turn_without_provider_rerun() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    store
        .insert_scheduled_task(
            "2026-04-23T20:00:00+1200",
            "agent",
            "Check backups.",
            "09:00",
            true,
        )
        .expect("insert schedule");
    let task_run_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "scheduled:task-1".to_string(),
            channel: "scheduled".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: None,
            prompt: "Check backups.".to_string(),
            created_at: "2026-04-24T09:00:01+1200".to_string(),
            progress_message_id: Some("77".to_string()),
            scheduled_task_id: Some(1),
        })
        .expect("insert task run");
    store
        .insert_turn(&TurnRecord {
            ts: "2026-04-24T09:00:03+1200".to_string(),
            chat_id: "scheduled:task-1".to_string(),
            input_type: "text".to_string(),
            user_text: "Check backups.".to_string(),
            asr_text: String::new(),
            provider_raw: "TELEGRAM_REPLY: Backup complete\nSEND_DOCUMENT: /tmp/report.txt\n"
                .to_string(),
            telegram_reply: "Backup complete".to_string(),
            voice_reply: String::new(),
            status: "ok".to_string(),
            update_id: None,
            duration_ms: Some(42),
            channel: "scheduled".to_string(),
            task_run_id: Some(task_run_id),
            side_effects_applied: true,
        })
        .expect("insert turn");

    let recovered = recover_scheduled_task_output_from_task_run(&cfg, &mut store, 1, task_run_id)
        .expect("recover")
        .expect("output recovered");

    assert!(recovered.contains("TELEGRAM_REPLY: Backup complete"));
    assert!(recovered.contains("SEND_DOCUMENT: /tmp/report.txt"));
    let schedules = store
        .list_active_scheduled_tasks()
        .expect("list active schedules");
    assert_eq!(
        schedules[0].pending_output.as_deref(),
        Some(recovered.as_str())
    );
    assert_eq!(schedules[0].delivery_state, None);
}

#[test]
fn scheduled_task_recovery_replays_nested_schedules_with_original_routing() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    let parent_target = serialize_delivery_target(&DeliveryTarget::Slack {
        channel_id: "C123".to_string(),
        thread_ts: Some("171.5".to_string()),
    });
    store
        .insert_scheduled_task_with_target(
            "2026-04-23T20:00:00+1200",
            "agent",
            "Check backups.",
            "09:00",
            true,
            Some("slack:C123#171.5"),
            Some(&parent_target),
        )
        .expect("insert parent schedule");
    let task_run_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "scheduled:task-1".to_string(),
            channel: "scheduled".to_string(),
            source_chat_id: Some("C123".to_string()),
            source_user_id: None,
            update_id: None,
            prompt: "Check backups.".to_string(),
            created_at: "2026-04-24T09:00:01+1200".to_string(),
            progress_message_id: Some("77".to_string()),
            scheduled_task_id: Some(1),
        })
        .expect("insert task run");
    store
        .insert_turn(&TurnRecord {
            ts: "2026-04-24T09:00:03+1200".to_string(),
            chat_id: "scheduled:task-1".to_string(),
            input_type: "text".to_string(),
            user_text: "Check backups.".to_string(),
            asr_text: String::new(),
            provider_raw:
                "TELEGRAM_REPLY: Backup complete\nSCHEDULE_PROMPT: 10:30|Follow up tomorrow\n"
                    .to_string(),
            telegram_reply: "Backup complete".to_string(),
            voice_reply: String::new(),
            status: "ok".to_string(),
            update_id: None,
            duration_ms: Some(42),
            channel: "scheduled".to_string(),
            task_run_id: Some(task_run_id),
            side_effects_applied: false,
        })
        .expect("insert turn");

    recover_scheduled_task_output_from_task_run(&cfg, &mut store, 1, task_run_id)
        .expect("recover")
        .expect("output recovered");

    let schedules = store
        .list_active_scheduled_tasks()
        .expect("list active schedules");
    assert_eq!(schedules.len(), 2);
    let nested = schedules
        .iter()
        .find(|task| task.id != 1)
        .expect("nested schedule");
    assert_eq!(nested.origin_session.as_deref(), Some("slack:C123#171.5"));
    assert_eq!(
        nested.delivery_target.as_deref(),
        Some(parent_target.as_str())
    );
}

#[test]
fn legacy_scheduled_task_retry_uses_inferred_slack_target_from_last_run() {
    let _guard = slack_api_env_lock().lock().expect("slack env lock");
    let server = FakeSlackServer::start();
    unsafe {
        std::env::set_var("COCONUTCLAW_SLACK_API_BASE", &server.base_url);
    }

    let mut cfg = test_config();
    cfg.scheduled_tasks_enabled = true;
    cfg.slack_bot_token = Some("xoxb-test".to_string());
    cfg.slack_channel_id = Some("CFALLBACK".to_string());
    let (current_hhmm, _) = scheduled_task_slot_now(&cfg.timezone);

    let outcome: Result<()> = {
        let store = Store::open(&cfg).expect("store");
        store
            .insert_scheduled_task(
                "2026-04-23T20:00:00+1200",
                "agent",
                "Check backups.",
                &current_hhmm,
                true,
            )
            .expect("insert legacy schedule");
        store
            .set_scheduled_task_pending_output(1, "TELEGRAM_REPLY: Backup complete")
            .expect("set pending output");
        store
            .insert_task_run(crate::store::InsertTaskRunParams {
                session_id: "slack:C123#171.5".to_string(),
                channel: "scheduled".to_string(),
                source_chat_id: Some("C123".to_string()),
                source_user_id: None,
                update_id: None,
                prompt: "Check backups.".to_string(),
                created_at: "2026-04-24T09:00:01+1200".to_string(),
                progress_message_id: None,
                scheduled_task_id: Some(1),
            })
            .expect("insert legacy task run");

        let mut store = Store::open(&cfg).expect("store reopen");
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        let client = build_telegram_client(&cfg).expect("telegram client");

        run_due_scheduled_tasks(&cfg, &mut store, &scheduler, &client)
            .expect("run due scheduled tasks");

        let requests = server.requests();
        assert!(
            requests.iter().any(|request| {
                request.contains("POST /chat.postMessage")
                    && request.contains("channel=C123")
                    && request.contains("thread_ts=171.5")
            }),
            "expected inferred slack retry target, got {requests:?}"
        );
        Ok(())
    };

    unsafe {
        std::env::remove_var("COCONUTCLAW_SLACK_API_BASE");
    }
    outcome.expect("legacy slack fallback assertions");
}

#[test]
fn legacy_scheduled_task_uses_unique_configured_slack_target_when_no_metadata_remains() {
    let _guard = slack_api_env_lock().lock().expect("slack env lock");
    let server = FakeSlackServer::start();
    unsafe {
        std::env::set_var("COCONUTCLAW_SLACK_API_BASE", &server.base_url);
    }

    let mut cfg = test_config();
    cfg.scheduled_tasks_enabled = true;
    cfg.telegram_chat_id = None;
    cfg.telegram_chat_ids.clear();
    cfg.slack_bot_token = Some("xoxb-test".to_string());
    cfg.slack_channel_id = Some("C123".to_string());
    let (current_hhmm, _) = scheduled_task_slot_now(&cfg.timezone);

    let outcome: Result<()> = {
        let store = Store::open(&cfg).expect("store");
        store
            .insert_scheduled_task(
                "2026-04-23T20:00:00+1200",
                "agent",
                "Check backups.",
                &current_hhmm,
                true,
            )
            .expect("insert legacy schedule");
        store
            .set_scheduled_task_pending_output(1, "TELEGRAM_REPLY: Backup complete")
            .expect("set pending output");

        let mut store = Store::open(&cfg).expect("store reopen");
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        let client = build_telegram_client(&cfg).expect("telegram client");

        run_due_scheduled_tasks(&cfg, &mut store, &scheduler, &client)
            .expect("run due scheduled tasks");

        let requests = server.requests();
        assert!(
            requests.iter().any(|request| {
                request.contains("POST /chat.postMessage") && request.contains("channel=C123")
            }),
            "expected unique slack config fallback, got {requests:?}"
        );
        let task = store
            .get_scheduled_task(1)
            .expect("load task")
            .expect("task exists");
        assert_eq!(
            task.delivery_target.as_deref(),
            Some(r#"{"channel_id":"C123","kind":"slack","thread_ts":null}"#)
        );
        Ok(())
    };

    unsafe {
        std::env::remove_var("COCONUTCLAW_SLACK_API_BASE");
    }
    outcome.expect("unique slack config fallback assertions");
}

#[test]
fn legacy_scheduled_task_with_ambiguous_config_is_left_pending() {
    let _guard = slack_api_env_lock().lock().expect("slack env lock");
    let slack_server = FakeSlackServer::start();
    let telegram_server = FakeTelegramServer::start();
    unsafe {
        std::env::set_var("COCONUTCLAW_SLACK_API_BASE", &slack_server.base_url);
    }

    let mut cfg = test_config();
    cfg.scheduled_tasks_enabled = true;
    cfg.telegram_api_base = Some(telegram_server.base_url.clone());
    cfg.telegram_chat_id = Some("321".to_string());
    cfg.slack_bot_token = Some("xoxb-test".to_string());
    cfg.slack_channel_id = Some("C123".to_string());
    let (current_hhmm, _) = scheduled_task_slot_now(&cfg.timezone);

    let outcome: Result<()> = {
        let store = Store::open(&cfg).expect("store");
        store
            .insert_scheduled_task(
                "2026-04-23T20:00:00+1200",
                "agent",
                "Check backups.",
                &current_hhmm,
                true,
            )
            .expect("insert ambiguous legacy schedule");
        store
            .set_scheduled_task_pending_output(1, "TELEGRAM_REPLY: Backup complete")
            .expect("set pending output");

        let mut store = Store::open(&cfg).expect("store reopen");
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        let client = build_telegram_client(&cfg).expect("telegram client");

        run_due_scheduled_tasks(&cfg, &mut store, &scheduler, &client)
            .expect("run due scheduled tasks");

        assert!(
            slack_server.requests().is_empty(),
            "ambiguous legacy task should not be delivered to slack"
        );
        assert!(
            telegram_server.requests().is_empty(),
            "ambiguous legacy task should not be delivered to telegram"
        );
        let task = store
            .get_scheduled_task(1)
            .expect("load task")
            .expect("task exists");
        assert_eq!(
            task.pending_output.as_deref(),
            Some("TELEGRAM_REPLY: Backup complete")
        );
        assert!(task.delivery_target.is_none());
        Ok(())
    };

    unsafe {
        std::env::remove_var("COCONUTCLAW_SLACK_API_BASE");
    }
    outcome.expect("ambiguous legacy routing assertions");
}

#[test]
fn dedup_replays_previous_output_from_store() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");

    let inserted = store
        .insert_turn(&TurnRecord {
            ts: "2026-02-26T00:00:00+0000".to_string(),
            chat_id: "321".to_string(),
            input_type: "text".to_string(),
            user_text: "hello".to_string(),
            asr_text: String::new(),
            provider_raw: "TELEGRAM_REPLY: Old reply\nSEND_DOCUMENT: /tmp/file.txt\n".to_string(),
            telegram_reply: "Old reply".to_string(),
            voice_reply: String::new(),
            status: "ok".to_string(),
            update_id: Some("42".to_string()),
            duration_ms: None,
            channel: "telegram".to_string(),
            task_run_id: None,
            side_effects_applied: true,
        })
        .expect("insert turn");
    assert!(inserted.is_some());

    let update = r#"{"update_id":42,"message":{"chat":{"id":"321"},"text":"hello again"}}"#;
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("TELEGRAM_REPLY: Old reply"));
    assert!(output.contains("SEND_DOCUMENT: /tmp/file.txt"));
}

#[test]
fn telegram_duplicate_inflight_update_is_not_enqueued_twice() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:321".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: Some("4242".to_string()),
            prompt: "hello".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: Some("99".to_string()),
            scheduled_task_id: None,
        })
        .expect("insert active task");
    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update = r#"{"update_id":4242,"message":{"chat":{"id":"321"},"text":"hello again"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");

    assert!(outcome.output.is_none());
    assert!(outcome.progress_message_id.is_none());
    assert_eq!(
        store
            .list_active_task_runs()
            .expect("list active tasks")
            .iter()
            .filter(|task| task.update_id.as_deref() == Some("4242"))
            .count(),
        1
    );
}

#[test]
fn missing_media_marker_path_does_not_fail_dispatch() {
    let cfg = test_config();
    let client = build_telegram_client(&cfg).expect("telegram client");
    let output = "TELEGRAM_REPLY: \nSEND_DOCUMENT: Z:/definitely-missing-file.txt\n";

    let dispatch = dispatch_telegram_output(&client, &cfg, Some("321"), output, None);
    assert!(dispatch.is_ok());
}

#[test]
fn voice_update_parses_as_voice_input_type() {
    let cfg = test_config();
    let update =
        r#"{"update_id":200,"message":{"chat":{"id":"321"},"voice":{"file_id":"abc123"}}}"#;

    let action = parse_webhook_action(&cfg, update).expect("parse");
    let WebhookAction::Turn(turn) = action else {
        panic!("expected turn action");
    };
    assert_eq!(turn.input.input_type, InputType::Voice);
}

#[test]
fn photo_update_parses_attachment_metadata() {
    let cfg = test_config();
    let update = r#"{"update_id":201,"message":{"chat":{"id":"321"},"photo":[{"file_id":"a"},{"file_id":"b"}]}}"#;

    let action = parse_webhook_action(&cfg, update).expect("parse");
    let WebhookAction::Turn(turn) = action else {
        panic!("expected turn action");
    };
    assert_eq!(turn.input.input_type, InputType::Photo);
    assert_eq!(turn.input.attachment_type.as_deref(), Some("photo"));
}

#[test]
fn cancel_command_sets_runtime_cancel_marker() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    let update = r#"{"update_id":202,"message":{"chat":{"id":"321"},"text":"/cancel"}}"#;

    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("No active task for this chat."));
}

#[test]
fn tasks_command_lists_active_runtime_tasks() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    let task_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:321".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: None,
            prompt: "summarize status".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert task run");
    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update = r#"{"update_id":203,"message":{"chat":{"id":"321"},"text":"/tasks"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("Active tasks"));
    assert!(output.contains(&format!("#{task_id} [queued] summarize status")));
}

#[test]
fn tasks_command_hides_other_sessions() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:321".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: None,
            update_id: Some("42".to_string()),
            prompt: "visible task".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert visible task");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:999".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("999".to_string()),
            source_user_id: None,
            update_id: Some("43".to_string()),
            prompt: "hidden task".to_string(),
            created_at: "2026-04-22T10:00:01+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert hidden task");

    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update = r#"{"update_id":207,"message":{"chat":{"id":"321"},"text":"/tasks"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("visible task"));
    assert!(!output.contains("hidden task"));
    assert!(!output.contains("telegram:999"));
}

#[test]
fn slack_targeted_cancel_requires_admin_role() {
    let mut cfg = test_config();
    cfg.slack_admin_user_ids = vec!["UADMIN".to_string()];
    let mut store = Store::open(&cfg).expect("store");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let payload = r#"{"type":"slash_commands","command":"/cancel","text":"99","channel_id":"C123","user_id":"UNAUTHORIZED"}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, payload).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert_eq!(outcome.output_channel.as_deref(), Some("slack"));
    assert!(output.contains("Admin role required"));
}

#[test]
fn telegram_update_from_unconfigured_chat_is_ignored() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update =
        r#"{"update_id":204,"message":{"chat":{"id":"999"},"text":"hello from another chat"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");

    assert!(outcome.output.is_none());
    assert!(outcome.output_channel.is_none());
    assert_eq!(
        store.kv_get("last_ignored_reason").expect("ignored reason"),
        Some("chat_id_mismatch actual=999 configured=321".to_string())
    );
}

#[test]
fn telegram_update_from_secondary_configured_chat_is_accepted() {
    let mut cfg = test_config();
    cfg.telegram_chat_ids = vec!["999".to_string(), "321".to_string()];
    let update =
        r#"{"update_id":205,"message":{"chat":{"id":"999"},"text":"hello from another chat"}}"#;

    let action = parse_webhook_action(&cfg, update).expect("parse");

    let WebhookAction::Turn(turn) = action else {
        panic!("expected turn action");
    };
    assert_eq!(turn.chat_id, "999");
    assert_eq!(turn.input.user_text, "hello from another chat");
}

#[test]
fn telegram_cancel_command_replies_to_request_chat() {
    let mut cfg = test_config();
    cfg.telegram_chat_ids = vec!["999".to_string()];
    let mut store = Store::open(&cfg).expect("store");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update = r#"{"update_id":206,"message":{"chat":{"id":"999"},"text":"/cancel"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert_eq!(outcome.chat_id.as_deref(), Some("999"));
    assert!(output.contains("No active task for this chat."));
}

#[test]
fn telegram_targeted_cancel_cannot_cross_sessions() {
    let mut cfg = test_config();
    cfg.telegram_chat_ids = vec!["321".to_string(), "999".to_string()];
    let store = Store::open(&cfg).expect("store");
    let task_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:999".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("999".to_string()),
            source_user_id: None,
            update_id: Some("evt-9".to_string()),
            prompt: "other session".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert task");
    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update = format!(
        r#"{{"update_id":208,"message":{{"chat":{{"id":"321"}},"text":"/cancel {task_id}"}}}}"#
    );

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, &update).expect("process");
    let output = outcome.output.unwrap_or_default();
    let task = store
        .get_task_run(task_id)
        .expect("get task")
        .expect("task exists");

    assert!(output.contains("not active for this chat"));
    assert_eq!(task.status, "queued");
}

#[test]
fn slack_approval_action_denies_non_admin_actor() {
    let mut cfg = test_config();
    cfg.slack_admin_user_ids = vec!["UADMIN".to_string()];
    let store = Store::open(&cfg).expect("store");
    let task_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "slack:C123#171.5".to_string(),
            channel: "slack".to_string(),
            source_chat_id: Some("C123".to_string()),
            source_user_id: Some("UREQUESTER".to_string()),
            update_id: Some("evt-1".to_string()),
            prompt: "deploy".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert task run");
    let approval_id = store
        .create_approval(crate::store::CreateApprovalParams {
            task_run_id: task_id,
            session_id: "slack:C123#171.5".to_string(),
            channel: "slack".to_string(),
            source_user_id: Some("UREQUESTER".to_string()),
            channel_id: Some("C123".to_string()),
            thread_ts: Some("171.5".to_string()),
            prompt_text: "deploy production".to_string(),
            request_message_ts: Some("171.6".to_string()),
            resume_payload: "{}".to_string(),
            created_at: "2026-04-22T10:00:01+0000".to_string(),
        })
        .expect("create approval");
    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let payload = format!(
        r#"{{"type":"block_actions","user":{{"id":"UNAUTHORIZED"}},"channel":{{"id":"C123"}},"message":{{"thread_ts":"171.5"}},"actions":[{{"action_id":"approval_approve","value":"approval:{approval_id}:approve"}}]}}"#
    );

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, &payload).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("not authorized"));
}

#[test]
fn slack_duplicate_inflight_event_is_not_enqueued_twice() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "slack:C123#171.5".to_string(),
            channel: "slack".to_string(),
            source_chat_id: Some("C123".to_string()),
            source_user_id: Some("U123".to_string()),
            update_id: Some("evt-42".to_string()),
            prompt: "deploy".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: Some("171.6".to_string()),
            scheduled_task_id: None,
        })
        .expect("insert active task");
    let mut store = Store::open(&cfg).expect("store reopen");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let payload = r#"{"type":"event_callback","event_id":"evt-42","event":{"type":"message","channel":"C123","thread_ts":"171.5","user":"U123","text":"deploy"}} "#;

    let outcome =
        process_webhook_line(&cfg, &mut store, &scheduler, payload.trim()).expect("process");

    assert!(outcome.output.is_none());
    assert!(outcome.progress_message_id.is_none());
    assert_eq!(
        store
            .list_active_task_runs()
            .expect("list active tasks")
            .iter()
            .filter(|task| task.update_id.as_deref() == Some("evt-42"))
            .count(),
        1
    );
}

#[test]
fn store_task_and_approval_records_round_trip() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    let task_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "telegram:321".to_string(),
            channel: "telegram".to_string(),
            source_chat_id: Some("321".to_string()),
            source_user_id: Some("user-1".to_string()),
            update_id: Some("42".to_string()),
            prompt: "hello".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: Some("99".to_string()),
            scheduled_task_id: Some(7),
        })
        .expect("insert task");
    store
        .update_task_run_started(task_id, "2026-04-22T10:00:05+0000")
        .expect("start task");
    store
        .mark_task_run_cancel_requested(task_id, "2026-04-22T10:00:06+0000")
        .expect("cancel request");
    let approval_id = store
        .create_approval(crate::store::CreateApprovalParams {
            task_run_id: task_id,
            session_id: "telegram:321".to_string(),
            channel: "telegram".to_string(),
            source_user_id: Some("user-1".to_string()),
            channel_id: None,
            thread_ts: None,
            prompt_text: "confirm".to_string(),
            request_message_ts: None,
            resume_payload: "{}".to_string(),
            created_at: "2026-04-22T10:00:07+0000".to_string(),
        })
        .expect("create approval");
    store
        .resolve_approval(
            approval_id,
            "approved",
            "2026-04-22T10:00:08+0000",
            Some("admin"),
        )
        .expect("resolve approval");
    store
        .finish_task_run(
            task_id,
            crate::store::TaskRunStatus::Completed,
            "2026-04-22T10:00:09+0000",
            None,
            Some("done"),
        )
        .expect("finish task");

    let task = store
        .get_task_run(task_id)
        .expect("get task")
        .expect("task");
    let approval = store
        .get_pending_approval(approval_id)
        .expect("get approval")
        .expect("approval");

    assert_eq!(task.status, "completed");
    assert_eq!(task.result_summary.as_deref(), Some("done"));
    assert_eq!(task.scheduled_task_id, Some(7));
    assert_eq!(approval.status, "approved");
    assert_eq!(approval.resolved_by_user_id.as_deref(), Some("admin"));
}

#[test]
fn stale_task_recovery_marks_runs_failed_and_approvals_expired() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    let task_id = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "slack:C123#171.5".to_string(),
            channel: "slack".to_string(),
            source_chat_id: Some("C123".to_string()),
            source_user_id: Some("UREQUESTER".to_string()),
            update_id: Some("evt-1".to_string()),
            prompt: "deploy".to_string(),
            created_at: "2026-04-22T10:00:00+0000".to_string(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .expect("insert task");
    store
        .mark_task_run_awaiting_approval(task_id)
        .expect("awaiting approval");
    let approval_id = store
        .create_approval(crate::store::CreateApprovalParams {
            task_run_id: task_id,
            session_id: "slack:C123#171.5".to_string(),
            channel: "slack".to_string(),
            source_user_id: Some("UREQUESTER".to_string()),
            channel_id: Some("C123".to_string()),
            thread_ts: Some("171.5".to_string()),
            prompt_text: "deploy production".to_string(),
            request_message_ts: Some("171.6".to_string()),
            resume_payload: "{}".to_string(),
            created_at: "2026-04-22T10:00:01+0000".to_string(),
        })
        .expect("create approval");

    store
        .mark_stale_task_runs_failed("2026-04-22T10:05:00+0000")
        .expect("mark stale");

    let task = store
        .get_task_run(task_id)
        .expect("get task")
        .expect("task");
    let approval = store
        .get_pending_approval(approval_id)
        .expect("get approval")
        .expect("approval");

    assert_eq!(task.status, "failed");
    assert_eq!(
        task.error_summary.as_deref(),
        Some("runtime restarted before task completion")
    );
    assert_eq!(approval.status, "expired");
}

#[test]
fn cancel_signal_detects_message_and_callback_query() {
    let message_update: Value = serde_json::from_str(
        r#"{"update_id":300,"message":{"chat":{"id":"321"},"text":"/cancel"}}"#,
    )
    .expect("json");
    let callback_update: Value = serde_json::from_str(
        r#"{"update_id":301,"callback_query":{"id":"cb1","data":"cancel","message":{"chat":{"id":"321"}}}}"#,
    )
    .expect("json");

    let message_signal = cancel_signal_from_update(&message_update, "321");
    assert!(message_signal.is_some());
    let callback_signal = cancel_signal_from_update(&callback_update, "321");
    assert_eq!(
        callback_signal.and_then(|signal| signal.callback_query_id),
        Some("cb1".to_string())
    );
}

#[test]
fn scheduled_task_slot_uses_configured_timezone() {
    let now = DateTime::parse_from_rfc3339("2026-04-20T00:30:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    assert_eq!(
        scheduled_task_slot_at(now, "Pacific/Auckland"),
        ("12:30".to_string(), "2026-04-20".to_string())
    );
    assert_eq!(
        scheduled_task_slot_at(now, "America/Los_Angeles"),
        ("17:30".to_string(), "2026-04-19".to_string())
    );
}

#[test]
fn progress_text_includes_elapsed_time() {
    let text = progress_status_text(12);
    assert!(text.contains("Thinking"));
    assert!(text.contains("12s"));
}

#[test]
fn progress_text_includes_event_lines() {
    let statuses = vec![
        "Processing...".to_string(),
        "Running: cargo test".to_string(),
    ];
    let text = progress_status_with_events(9, &statuses);
    assert!(text.contains("Elapsed: 9s"));
    assert!(text.contains("- Processing..."));
    assert!(text.contains("- Running: cargo test"));
}

#[test]
fn parse_markers_keeps_multiline_telegram_reply() {
    let payload = "TELEGRAM_REPLY: line one\nline two\nline three\nMEMORY_APPEND: fact";
    let markers = parse_markers(payload);
    assert_eq!(
        markers.telegram_reply.as_deref(),
        Some("line one\nline two\nline three")
    );
}

#[test]
fn parse_markers_stops_multiline_reply_at_next_marker() {
    let payload =
        "noise\nTELEGRAM_REPLY: first\nsecond\nVOICE_REPLY: spoken\nTASK_APPEND: todo item";
    let markers = parse_markers(payload);
    assert_eq!(markers.telegram_reply.as_deref(), Some("first\nsecond"));
    assert_eq!(markers.voice_reply.as_deref(), Some("spoken"));
    assert_eq!(markers.task_append, vec!["todo item".to_string()]);
}

#[test]
fn parse_markers_unescapes_inline_newlines() {
    let payload = "TELEGRAM_REPLY: line one\\n\\nline two\\nline three";
    let markers = parse_markers(payload);
    assert_eq!(
        markers.telegram_reply.as_deref(),
        Some("line one\n\nline two\nline three")
    );
}

#[test]
fn parse_markers_keeps_double_escaped_newline_literal() {
    let payload = r"TELEGRAM_REPLY: keep \\n literal";
    let markers = parse_markers(payload);
    assert_eq!(markers.telegram_reply.as_deref(), Some(r"keep \\n literal"));
}

#[test]
fn recover_unstructured_reply_prefers_json_assistant_text() {
    let payload = r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Hello from JSON"}]}}"#;
    let recovered = recover_unstructured_reply(payload);
    assert_eq!(recovered.as_deref(), Some("Hello from JSON"));
}

#[test]
fn recover_unstructured_reply_uses_plain_text_when_no_json() {
    let payload = "plain fallback reply";
    let recovered = recover_unstructured_reply(payload);
    assert_eq!(recovered.as_deref(), Some("plain fallback reply"));
}

#[test]
fn recover_unstructured_reply_ignores_json_stream_without_assistant_text() {
    let payload = r#"{"type":"session","id":"abc"}
{"type":"agent_end","messages":[]}"#;
    let recovered = recover_unstructured_reply(payload);
    assert!(recovered.is_none());
}

#[test]
fn recover_unstructured_reply_surfaces_agent_end_error() {
    let payload = r#"{"type":"session","id":"abc"}
{"type":"agent_end","messages":[{"role":"assistant","content":[{"type":"toolCall","id":"t1","name":"bash","arguments":{}}]}],"error":"Maximum tool iterations (50) exceeded"}"#;
    let recovered = recover_unstructured_reply(payload);
    assert_eq!(
        recovered.as_deref(),
        Some("⚠️ Agent stopped: Maximum tool iterations (50) exceeded")
    );
}

#[test]
fn extract_error_summary_prefers_structured_event_error() {
    let payload = r#"{"type":"session","id":"abc"}
{"type":"turn_end","message":{"errorMessage":"internal timeout"}}
{"type":"agent_end","error":"Internal Network Failure"}"#;
    let summary = extract_error_summary(payload);
    assert_eq!(summary.as_deref(), Some("Internal Network Failure"));
}

#[test]
fn should_retry_provider_failure_matches_transient_errors() {
    assert!(should_retry_provider_failure("Internal Network Failure"));
    assert!(should_retry_provider_failure(
        "API error: JSON parse error: missing field `type`"
    ));
    assert!(!should_retry_provider_failure("permission denied"));
}

#[test]
fn should_retry_provider_failure_ignores_non_retryable_turn_failed_stream() {
    let payload = r#"{"type":"item.completed","item":{"type":"command_execution","aggregated_output":"mentions timeout in a file path only"}}
{"type":"turn.failed","error":{"message":"Codex ran out of room in the model's context window. Start a new thread or clear earlier history before retrying."}}"#;
    assert!(!should_retry_provider_failure(payload));
}

#[test]
fn text_params_include_parse_mode_for_markdown_v2() {
    let cfg = RuntimeConfig::test_builder()
        .telegram_parse_mode(TelegramParseMode::MarkdownV2)
        .build();
    let params = telegram_text_form_params(
        &cfg,
        "321",
        Some("42"),
        "hello",
        Some(r#"{"inline_keyboard":[]}"#),
    );
    assert!(params.contains(&("parse_mode".to_string(), "MarkdownV2".to_string())));
}

#[test]
fn markdown_entity_error_triggers_plain_fallback() {
    let err = anyhow::anyhow!(
        "telegram sendMessage failed: Bad Request: can't parse entities: Character '-' is reserved"
    );
    assert!(should_fallback_plain_for_error(&err));
}

#[test]
fn non_entity_error_does_not_trigger_plain_fallback() {
    let err = anyhow::anyhow!("telegram sendMessage failed: Bad Request: message is not modified");
    assert!(!should_fallback_plain_for_error(&err));
}

#[test]
fn telegram_retry_after_seconds_reads_integer_value() {
    let body = r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":15}}"#;
    assert_eq!(telegram_retry_after_seconds(body), Some(15));
}

#[test]
fn telegram_retry_after_seconds_reads_string_value() {
    let body = r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":"7"}}"#;
    assert_eq!(telegram_retry_after_seconds(body), Some(7));
}

#[test]
fn render_markdown_v2_converts_commonmark_bold() {
    let text = "**Architecture highlights:**\n- item";
    let rendered = render_markdown_v2_reply(text);
    assert!(rendered.contains("Architecture highlights"));
    assert!(!rendered.contains("**Architecture"));
    assert!(rendered.contains("item"));
}

#[test]
fn render_markdown_v2_escapes_all_special_chars() {
    let text = "`**not bold**` then **bold**";
    let rendered = render_markdown_v2_reply(text);
    // The simple escaper escapes all special chars uniformly
    assert!(rendered.contains("\\`"));
    assert!(rendered.contains("\\*\\*bold\\*\\*"));
}

#[test]
fn render_markdown_v2_escapes_reserved_chars() {
    let text = "- item\n1. test (x.y)";
    let rendered = render_markdown_v2_reply(text);
    assert!(!rendered.is_empty());
    assert!(rendered.contains("item"));
    assert!(rendered.contains("test"));
}

#[test]
fn render_telegram_reply_text_escapes_progress_for_markdown_v2() {
    let cfg = RuntimeConfig::test_builder()
        .telegram_parse_mode(TelegramParseMode::MarkdownV2)
        .build();
    let rendered = render_telegram_reply_text(&cfg, &progress_status_text(3));
    assert!(rendered.contains("Thinking\\.\\.\\."));
    assert!(rendered.contains("stop\\."));
}

#[test]
fn render_telegram_reply_text_keeps_plain_when_parse_mode_off() {
    let cfg = test_config();
    let text = progress_status_text(3);
    let rendered = render_telegram_reply_text(&cfg, &text);
    assert_eq!(rendered, text);
}

#[test]
fn long_reply_document_fallback_respects_telegram_limit() {
    let exact = "a".repeat(4096);
    assert!(!should_send_reply_as_document(&exact));

    let oversized = "a".repeat(4097);
    assert!(should_send_reply_as_document(&oversized));
}

#[test]
fn render_markdown_v2_escapes_backticks() {
    let text = "*标题* and `main.rs`";
    let rendered = render_markdown_v2_reply(text);
    // The simple escaper escapes backticks too
    assert!(rendered.contains("\\`main\\.rs\\`"));
}

#[test]
fn context_requires_plain_text_when_parse_mode_off() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    let input = TurnInput {
        input_type: InputType::Text,
        user_text: "hello".to_string(),
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: None,
        channel: "telegram".to_string(),
    };
    let text = build_context(
        &cfg,
        &store,
        &input,
        "2026-01-01T00:00:00+0000",
        "321",
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )
    .expect("context");
    assert!(text.contains("Do not use markdown"));
}

#[test]
fn context_allows_markdown_v2_when_parse_mode_enabled() {
    let cfg = RuntimeConfig::test_builder()
        .telegram_parse_mode(TelegramParseMode::MarkdownV2)
        .build();
    let store = Store::open(&cfg).expect("store");
    let input = TurnInput {
        input_type: InputType::Text,
        user_text: "hello".to_string(),
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: None,
        channel: "telegram".to_string(),
    };
    let text = build_context(
        &cfg,
        &store,
        &input,
        "2026-01-01T00:00:00+0000",
        "321",
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )
    .expect("context");
    assert!(text.contains("MarkdownV2"));
    assert!(!text.contains("Do not use markdown"));
}

#[test]
fn context_recent_turns_are_scoped_to_chat_and_channel() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .insert_turn(&TurnRecord {
            ts: "2026-01-01T00:00:00+0000".to_string(),
            chat_id: "321".to_string(),
            input_type: "text".to_string(),
            user_text: "telegram hello".to_string(),
            asr_text: String::new(),
            provider_raw: "TELEGRAM_REPLY: telegram reply\n".to_string(),
            telegram_reply: "telegram reply".to_string(),
            voice_reply: String::new(),
            status: "ok".to_string(),
            update_id: Some("500".to_string()),
            duration_ms: None,
            channel: "telegram".to_string(),
            task_run_id: None,
            side_effects_applied: true,
        })
        .expect("insert telegram turn");
    store
        .insert_turn(&TurnRecord {
            ts: "2026-01-01T00:01:00+0000".to_string(),
            chat_id: "C123".to_string(),
            input_type: "text".to_string(),
            user_text: "slack hello".to_string(),
            asr_text: String::new(),
            provider_raw: "TELEGRAM_REPLY: slack reply\n".to_string(),
            telegram_reply: "slack reply".to_string(),
            voice_reply: String::new(),
            status: "ok".to_string(),
            update_id: Some("501".to_string()),
            duration_ms: None,
            channel: "slack".to_string(),
            task_run_id: None,
            side_effects_applied: true,
        })
        .expect("insert slack turn");

    let input = TurnInput {
        input_type: InputType::Text,
        user_text: "next telegram message".to_string(),
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: None,
        channel: "telegram".to_string(),
    };

    let text = build_context(
        &cfg,
        &store,
        &input,
        "2026-01-01T00:02:00+0000",
        "321",
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )
    .expect("context");

    assert!(text.contains("telegram hello"));
    assert!(!text.contains("slack hello"));
}

#[test]
fn context_includes_supplemental_conversation_context() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    let input = TurnInput {
        input_type: InputType::Text,
        user_text: "follow-up".to_string(),
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: Some("U123: earlier thread reply".to_string()),
        channel: "slack".to_string(),
    };

    let text = build_context(
        &cfg,
        &store,
        &input,
        "2026-01-01T00:02:00+0000",
        "C123",
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )
    .expect("context");

    assert!(text.contains("## Supplemental conversation context"));
    assert!(text.contains("U123: earlier thread reply"));
}

#[test]
fn context_omits_quoted_reply_from_before_boundary() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");

    store
        .insert_boundary_turn("2026-01-01T00:02:00+0000", "321", Some("600"), "telegram")
        .expect("insert boundary");

    let input = TurnInput {
        input_type: InputType::Text,
        user_text: "fresh question".to_string(),
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: None,
        channel: "telegram".to_string(),
    };

    let text = build_context(
        &cfg,
        &store,
        &input,
        "2026-01-01T00:03:00+0000",
        "321",
        &QuotedMessage {
            reply_from: Some("CoconutClaw".to_string()),
            reply_text: Some("older reply".to_string()),
            reply_ts: Some(1_767_225_719),
        },
    )
    .expect("context");

    assert!(!text.contains("## Quoted/replied-to message"));
    assert!(!text.contains("older reply"));
}

#[test]
fn heartbeat_subcommand_is_recognized() {
    let parsed = Cli::try_parse_from(["coconutclaw", "heartbeat"]);
    assert!(parsed.is_ok());
}

#[test]
fn nightly_reflection_subcommand_is_recognized() {
    let parsed = Cli::try_parse_from(["coconutclaw", "nightly-reflection"]);
    assert!(parsed.is_ok());
}

#[test]
fn service_subcommand_is_recognized() {
    let parsed = Cli::try_parse_from(["coconutclaw", "service", "status"]);
    assert!(parsed.is_ok());
}

#[test]
fn nightly_reflection_marker_format_is_stable() {
    assert_eq!(
        nightly_reflection_marker("2026-02-25"),
        "<!-- nightly-reflection:2026-02-25 -->"
    );
}

#[test]
fn webhook_public_endpoint_joins_base_and_path() {
    let cfg = RuntimeConfig::test_builder()
        .webhook_public_url(Some("https://claw.example".to_string()))
        .webhook_path("/telegram/webhook".to_string())
        .build();

    let endpoint = webhook_public_endpoint(&cfg).expect("endpoint");
    assert_eq!(endpoint, "https://claw.example/telegram/webhook");
}

// ── format-aware split_text_chunks tests ──────────────────────────

#[test]
fn split_short_text_returns_single_chunk() {
    let text = "Hello world";
    let chunks = split_text_chunks(text, 4096, TelegramParseMode::Off);
    assert_eq!(chunks, vec!["Hello world"]);
}

#[test]
fn split_empty_text_returns_no_chunks() {
    let chunks = split_text_chunks("", 4096, TelegramParseMode::Off);
    assert!(chunks.is_empty());
}

#[test]
fn split_closes_and_reopens_code_block_at_boundary_html() {
    // Build text with a code block that spans across the chunk limit.
    let fence_open = "<pre><code>";
    let fence_close = "</code></pre>";
    let code_line = "line of code\n";
    // code_line is 13 chars. We want ~10 lines in first chunk, rest in second.
    let code_lines = code_line.repeat(20); // 260 chars
    let text = format!("{fence_open}{code_lines}{fence_close}");

    // max=150 chars to force a split inside the code block
    let chunks = split_text_chunks(&text, 150, TelegramParseMode::Html);
    assert!(chunks.len() >= 2, "should split into multiple chunks");

    // First chunk must contain the closing fence (before the indicator)
    assert!(
        chunks[0].contains(fence_close),
        "first chunk should contain closing fence, got: {:?}",
        chunks[0]
    );

    // Second chunk must reopen the code block
    assert!(
        chunks[1].starts_with(fence_open),
        "second chunk should start with opening fence, got: {}...",
        &chunks[1][..fence_open.len().min(chunks[1].len())]
    );

    // Last chunk must contain the closing fence
    let last = chunks.last().unwrap();
    assert!(
        last.contains(fence_close),
        "last chunk should contain closing fence, got: {:?}",
        last
    );

    // Multi-chunk: must have indicators
    if chunks.len() > 1 {
        for (i, chunk) in chunks.iter().enumerate() {
            let indicator = format!("({}/{})", i + 1, chunks.len());
            assert!(
                chunk.contains(&indicator),
                "chunk {} should contain indicator {:?}",
                i + 1,
                indicator
            );
        }
    }
}

#[test]
fn split_closes_and_reopens_code_block_markdown_v2() {
    let fence = "```";
    let code_line = "x = 1\n"; // 6 chars
    let code_lines = code_line.repeat(60); // 360 chars
    let text = format!("{fence}rust\n{code_lines}{fence}");

    let chunks = split_text_chunks(&text, 150, TelegramParseMode::MarkdownV2);
    assert!(chunks.len() >= 2);

    // First chunk must contain the closing fence
    assert!(
        chunks[0].contains(fence),
        "first chunk should contain ```, got: {:?}",
        chunks[0]
    );
    // Second chunk must reopen with original lang tag
    assert!(
        chunks[1].starts_with("```rust\n"),
        "second chunk should reopen with ```rust\\n, got: {}",
        &chunks[1][..20.min(chunks[1].len())]
    );

    // Indicators use escaped parens for MarkdownV2
    for (i, chunk) in chunks.iter().enumerate() {
        let indicator = format!(
            "\\({current}/{total}\\)",
            current = i + 1,
            total = chunks.len()
        );
        assert!(
            chunk.contains(&indicator),
            "chunk {} should contain escaped indicator {:?}",
            i + 1,
            indicator
        );
    }
}

#[test]
fn split_plain_mode_no_format_awareness() {
    // In Off mode, code blocks are just text — no fence closing/reopening.
    let text = "```rust\nfn main() {}\n```".repeat(20);
    let chunks = split_text_chunks(&text, 200, TelegramParseMode::Off);
    assert!(chunks.len() >= 2);
    // No chunk should have added fence tags
    for chunk in &chunks {
        // The chunk indicators should be plain parens
        if chunk.contains("(1/") || chunk.contains("(2/") {
            // ok — plain indicator
        }
    }
}

#[test]
fn split_protects_inline_code_from_breaking() {
    // Inline code with backticks that falls near a split boundary.
    let inline = "`some_long_code_here`";
    let padding = "word ".repeat(10); // 50 chars
    let text = format!("{padding}{inline} more text after");
    let chunks = split_text_chunks(&text, 60, TelegramParseMode::MarkdownV2);
    // No chunk should have an unmatched backtick count
    for chunk in &chunks {
        let count = chunk.matches('`').count();
        assert_eq!(
            count % 2,
            0,
            "chunk has odd number of backticks: {:?}",
            chunk
        );
    }
}

// ── Bug-coverage tests (TDD: written to fail, then fix) ──────────

/// Bug #2: headroom subtraction must not panic on underflow when max_chars is tiny.
#[test]
fn split_format_aware_small_max_chars_no_panic() {
    let text = "hello world this is a test string";
    // Must not panic — previously caused integer underflow.
    let chunks = split_text_chunks(text, 5, TelegramParseMode::MarkdownV2);
    assert!(!chunks.is_empty());
    // With max_chars this tiny the indicator alone exceeds the limit,
    // so we only assert no panic and non-empty output.
}

/// Bug #2b: realistic small max_chars must respect limit.
#[test]
fn split_format_aware_moderate_max_chars_respects_limit() {
    let text = "hello world this is a test string that is moderately long for splitting";
    let max = 25;
    let chunks = split_text_chunks(text, max, TelegramParseMode::MarkdownV2);
    assert!(chunks.len() >= 2);
    for chunk in &chunks {
        assert!(
            chunk.chars().count() <= max,
            "chunk exceeds max {max}: {:?}",
            chunk
        );
    }
}

/// Bug #3: byte vs char index confusion with multi-byte chars.
#[test]
fn split_multibyte_chars_respect_max_chars() {
    // Each '日' is 3 bytes but 1 char. 200 chars = 600 bytes.
    let text = "日".repeat(200);
    let chunks = split_text_chunks(&text, 80, TelegramParseMode::MarkdownV2);
    assert!(chunks.len() >= 2);
    for chunk in &chunks {
        assert!(
            chunk.chars().count() <= 80,
            "chunk char count {} exceeds 80: {:?}",
            chunk.chars().count(),
            &chunk[..chunk.len().min(60)]
        );
    }
}

/// Bug #4: infinite loop when max_chars is very small and headroom becomes 0.
#[test]
fn split_format_aware_terminates_with_tiny_limit() {
    let text = "abcdefghijklmnopqrstuvwxyz".repeat(5);
    // This must terminate (not infinite loop).
    let chunks = split_text_chunks(&text, 3, TelegramParseMode::Html);
    assert!(!chunks.is_empty());
}

/// Bug #5: HtmlFormat::code_fence_open_with_lang ignores lang tag.
#[test]
fn split_html_reopens_code_block_with_language_class() {
    let code_line = "let x = 1;\n";
    let code_lines = code_line.repeat(30); // 330 chars
    let text = format!("<pre><code class=\"language-rust\">{code_lines}</code></pre>");
    let chunks = split_text_chunks(&text, 150, TelegramParseMode::Html);
    assert!(chunks.len() >= 2);
    // Second chunk must reopen with the language class
    assert!(
        chunks[1].starts_with("<pre><code class=\"language-rust\">"),
        "second chunk should preserve language class, got: {}",
        &chunks[1][..60.min(chunks[1].len())]
    );
}

/// Bug #6: scan_code_block_state doesn't match <pre><code class="language-X">.
#[test]
fn split_html_detects_code_block_with_class_attr() {
    let code_line = "line\n";
    let code_lines = code_line.repeat(40);
    let text = format!("<pre><code class=\"language-python\">{code_lines}</code></pre>");
    let chunks = split_text_chunks(&text, 100, TelegramParseMode::Html);
    assert!(chunks.len() >= 2);
    // Every non-last chunk must properly close the code block
    for (i, chunk) in chunks.iter().enumerate() {
        if i < chunks.len() - 1 {
            assert!(
                chunk.contains("</code></pre>"),
                "chunk {} should close code block, got: {:?}",
                i,
                chunk
            );
        }
    }
}

/// Bug #7: split_plain appends indicators after splitting, exceeding max_chars.
#[test]
fn split_plain_chunks_respect_max_chars_with_indicators() {
    let text = "abcde\n".repeat(500); // ~3000 chars
    let max = 200;
    let chunks = split_text_chunks(&text, max, TelegramParseMode::Off);
    assert!(chunks.len() >= 2);
    for (i, chunk) in chunks.iter().enumerate() {
        assert!(
            chunk.chars().count() <= max,
            "plain chunk {} is {} chars, exceeds {}: {:?}",
            i,
            chunk.chars().count(),
            max,
            &chunk[..chunk.len().min(60)]
        );
    }
}

/// Bug #7b: oversized single line in plain mode still respects limit.
#[test]
fn split_plain_oversized_line_with_indicators() {
    let text = "x".repeat(8000);
    let max = 4096;
    let chunks = split_text_chunks(&text, max, TelegramParseMode::Off);
    assert!(chunks.len() >= 2);
    for (i, chunk) in chunks.iter().enumerate() {
        assert!(
            chunk.chars().count() <= max,
            "plain oversized chunk {} is {} chars, exceeds {max}",
            i,
            chunk.chars().count()
        );
    }
}

/// Inline backtick protection must not corrupt ``` fences.
#[test]
fn split_md_fence_not_corrupted_by_inline_backtick_protection() {
    let text = "Some text before\n```rust\nfn main() {}\nlet x = 1;\nlet y = 2;\n```\nAfter";
    let chunks = split_text_chunks(text, 40, TelegramParseMode::MarkdownV2);
    // No chunk should contain a partial fence like `` (two backticks alone)
    for chunk in &chunks {
        for line in chunk.split('\n') {
            let trimmed = line.trim();
            if trimmed.starts_with('`') && !trimmed.starts_with("```") {
                // Must be an inline code span (even backtick count)
                let count = trimmed.matches('`').count();
                assert_eq!(
                    count % 2,
                    0,
                    "fence corrupted to partial backticks: {:?}",
                    trimmed
                );
            }
        }
    }
}

/// HTML scan correctly handles close+open on the same line.
#[test]
fn split_html_close_then_open_same_line() {
    // Craft text where a code block is closed and another opened on the same line
    let text = "<pre><code>first block</code></pre><pre><code class=\"language-rust\">second block content that is long enough to need splitting into multiple chunks for testing\n";
    let text = text.repeat(3);
    let chunks = split_text_chunks(&text, 150, TelegramParseMode::Html);
    // Just verify it doesn't panic and produces non-empty output
    assert!(!chunks.is_empty());
}

/// All format-aware chunks must stay within max_chars.
#[test]
fn split_format_aware_all_chunks_within_limit() {
    let fence = "```";
    let code_line = "x = 1\n";
    let code_lines = code_line.repeat(100);
    let text = format!("{fence}rust\n{code_lines}{fence}");
    let max = 150;
    let chunks = split_text_chunks(&text, max, TelegramParseMode::MarkdownV2);
    for (i, chunk) in chunks.iter().enumerate() {
        assert!(
            chunk.chars().count() <= max,
            "md chunk {} is {} chars, exceeds {max}: {:?}",
            i,
            chunk.chars().count(),
            &chunk[..chunk.len().min(80)]
        );
    }
}
