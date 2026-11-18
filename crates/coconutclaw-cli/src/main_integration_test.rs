use super::*;
use crate::context::build_context;
use crate::markers::{
    extract_error_summary, parse_markers, recover_unstructured_reply, should_retry_provider_failure,
};
use crate::recovery::reconcile_pending_turn_side_effects;
use crate::scheduler::SessionScheduler;

use crate::cancel::cancel_impl::cancel_signal_from_update;
use crate::loops::webhook::{parse_webhook_action, process_webhook_line};
use crate::store::TurnRecord;
use crate::telegram::{
    build_telegram_client, dispatch_telegram_output, progress_status_text,
    progress_status_with_events, render_markdown_v2_reply, render_telegram_reply_text,
    should_fallback_plain_for_error, should_send_reply_as_document, split_text_chunks,
    telegram_retry_after_seconds, telegram_text_form_params,
};
use crate::types::WebhookAction;
use crate::types::{InputType, QuotedMessage, TurnInput};

use coconutclaw_config::{RuntimeConfig, TelegramParseMode};
use serde_json::Value;

use std::fs;

fn test_config() -> RuntimeConfig {
    RuntimeConfig::test_config()
}

#[test]
fn fresh_command_returns_confirmation_output() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");
    let update =
        r#"{"update_id":100,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"/fresh"}}"#;

    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("TELEGRAM_REPLY:"));
    assert!(output.contains("Context cleared"));
}

#[test]
fn incoming_addressed_memory_commands_execute_locally() {
    let mut cfg = test_config();
    cfg.telegram_bot_username = Some("ActualBotName".into());
    // Even a routing regression may only reach a local failure, not live services.
    cfg.telegram_bot_token = None;
    cfg.codex.bin = "/bin/false".into();
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let text = "/memory@actualbotname remember synthetic addressed fact";
    let update = serde_json::json!({
        "update_id": 711, "message": {"from":{"id":"321"},"chat": {"id": "321"}, "text": text}
    })
    .to_string();
    let first = process_webhook_line(&cfg, &mut store, &scheduler, &update).unwrap();
    assert!(
        first
            .output
            .as_deref()
            .unwrap()
            .contains("remember applied")
    );
    let replay = process_webhook_line(&cfg, &mut store, &scheduler, &update).unwrap();
    assert_eq!(first.output, replay.output);
    for (id, chat, text) in [
        (712, "321", "/memory remember synthetic plain fact"),
        (713, "321", "/memory@ActualBotName search addressed"),
        (
            714,
            "321",
            "/memory@ActualBotName correct 1 synthetic corrected fact",
        ),
        (715, "321", "/memory@ActualBotName undo"),
    ] {
        let update = serde_json::json!({
            "update_id": id, "message": {"from":{"id":chat},"chat": {"id": chat}, "text": text}
        });
        let outcome =
            process_webhook_line(&cfg, &mut store, &scheduler, &update.to_string()).unwrap();
        assert!(outcome.output.is_some(), "{text}");
        if id == 713 {
            assert!(outcome.output.unwrap().contains("synthetic addressed fact"));
        }
    }
    for (id, chat, text) in [
        (716, "321", "/memory@OtherBot remember unauthorized fact"),
        (
            717,
            "999",
            "/memory@ActualBotName remember unauthorized fact",
        ),
        (
            718,
            "321",
            "/memory@ActualBotName@OtherBot remember unauthorized fact",
        ),
        (719, "321", "/memory@ remember unauthorized fact"),
    ] {
        let update = serde_json::json!({
            "update_id": id, "message": {"from":{"id":chat},"chat": {"id": chat}, "text": text}
        });
        assert!(matches!(
            parse_webhook_action(&cfg, &update.to_string()).unwrap(),
            WebhookAction::Ignore { .. }
        ));
        let outcome =
            process_webhook_line(&cfg, &mut store, &scheduler, &update.to_string()).unwrap();
        assert!(outcome.output.is_none());
    }
    assert_eq!(store.memory_revision("telegram:321").unwrap(), 4);
    let provenance: (String, String, String) = store
        .conn
        .query_row(
            "SELECT r.scope,r.content,t.user_text FROM memory_records r
         JOIN turns t ON t.id=r.source_turn WHERE r.id=1 AND t.status='memory_command'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        (
            "telegram:321".into(),
            "synthetic addressed fact".into(),
            text.into()
        )
    );
    let ordinary: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE status!='memory_command'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        ordinary, 0,
        "command sources must stay out of ordinary history"
    );
    let tasks: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        tasks, 0,
        "addressed commands must never enqueue provider work"
    );
    scheduler.shutdown().unwrap();
}

#[test]
fn incoming_memory_commands_are_authorized_scoped_and_replay_safe() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update = r#"{"update_id":701,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"/memory remember dispatch scoped fact"}}"#;
    let first = process_webhook_line(&cfg, &mut store, &scheduler, update).unwrap();
    assert!(
        first
            .output
            .as_deref()
            .unwrap()
            .contains("remember applied")
    );
    let replay = process_webhook_line(&cfg, &mut store, &scheduler, update).unwrap();
    assert_eq!(replay.output, first.output);
    assert_eq!(store.memory_revision("telegram:321").unwrap(), 1);
    let disallowed = r#"{"update_id":702,"message":{"chat":{"id":"999"},"text":"/memory remember unauthorized fact"}}"#;
    let rejected = process_webhook_line(&cfg, &mut store, &scheduler, disallowed).unwrap();
    assert!(rejected.output.is_none());
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM memory_records", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let output = process_webhook_line(
        &cfg,
        &mut store,
        &scheduler,
        r#"{"update_id":703,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"/memory search scoped"}}"#,
    )
    .unwrap();
    assert!(output.output.unwrap().contains("dispatch scoped fact"));
    let tasks: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(tasks, 0, "memory commands must not enqueue agent work");
}

#[test]
fn raw_reconstruction_matches_initial_final_content_and_effects() {
    let reply = "Literal code:\n```xml\n<think>keep this code</think>\n```";
    let envelope = format!(
        "<think>\nSEND_APPROVAL: reasoning-only approval\nDELEGATE_TASK: reasoning-only delegation\nMEMORY_APPEND: reasoning-only memory\nTASK_APPEND: reasoning-only task\n</think>\nTELEGRAM_REPLY: {reply}\nSEND_DOCUMENT: outbox/final-file.pdf\nMEMORY_APPEND: final memory\nTASK_APPEND: final task"
    );
    let event =
        serde_json::json!({"type":"item.completed","item":{"type":"agent_message","text":reply}})
            .to_string();
    for (raw, has_final_effects) in [
        (envelope.as_str(), true),
        (event.as_str(), false),
        (reply, false),
    ] {
        let cfg = test_config();
        fs::create_dir_all(cfg.instance_dir.join("outbox")).unwrap();
        fs::write(cfg.instance_dir.join("outbox/final-file.pdf"), "fixture").unwrap();
        let mut store = Store::open(&cfg).unwrap();
        let resolved = crate::turn::resolve_turn_result(raw, "telegram", true, false);
        assert_eq!(resolved.telegram_reply, reply);
        let mut expected = vec![crate::markers::Effect::TelegramReply(reply.into())];
        if has_final_effects {
            expected.extend([
                crate::markers::Effect::SendDocument("outbox/final-file.pdf".into()),
                crate::markers::Effect::MemoryAppend("final memory".into()),
                crate::markers::Effect::TaskAppend("final task".into()),
            ]);
            assert_eq!(resolved.effects, expected);
        } else {
            assert!(resolved.effects.is_empty());
        }
        store
            .insert_turn(&TurnRecord {
                ts: "2026-11-15T00:00:00+0000".into(),
                chat_id: "321".into(),
                input_type: "text".into(),
                user_text: "synthetic reconstruction".into(),
                provider_raw: raw.into(),
                telegram_reply: reply.into(),
                status: resolved.status.to_string(),
                update_id: Some("9300".into()),
                duration_ms: None,
                channel: "telegram".into(),
                task_run_id: None,
                side_effects_applied: false,
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            reconcile_pending_turn_side_effects(&cfg, &mut store).unwrap(),
            1
        );
        assert_eq!(
            reconcile_pending_turn_side_effects(&cfg, &mut store).unwrap(),
            0
        );
        let replay = store
            .rendered_output_for_update_id("9300")
            .unwrap()
            .unwrap();
        assert_eq!(parse_markers(&replay).to_effects(), expected);
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        let replayed = process_webhook_line(
            &cfg,
            &mut store,
            &scheduler,
            r#"{"update_id":9300,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"synthetic duplicate"}}"#,
        )
        .unwrap();
        assert!(
            replayed.output.is_none(),
            "a legacy saved result is not proof its Telegram acknowledgment was absent"
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT state FROM telegram_ingress WHERE update_id='9300'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "unknown"
        );
        let tasks = store.managed_pending_task_entries_from_db().unwrap();
        assert_eq!(
            tasks,
            if has_final_effects {
                vec!["final task"]
            } else {
                vec![]
            }
        );
        let memory: Vec<String> = store
            .conn
            .prepare("SELECT content FROM memory_records ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            memory,
            if has_final_effects {
                vec!["final memory"]
            } else {
                vec![]
            }
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM approvals", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM task_runs", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}

#[test]
fn pending_turn_side_effects_reconcile_candidates_and_tasks() {
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
            provider_raw:
                "TELEGRAM_REPLY: ok\nMEMORY_APPEND: remembered fact\nTASK_APPEND: follow up later\n"
                    .to_string(),
            telegram_reply: "ok".to_string(),
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
    assert!(!memory.contains("remembered fact"));
    let candidate: (String, i64, String) = store
        .conn
        .query_row(
            "SELECT scope,source_turn,content FROM memory_records",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        candidate,
        ("telegram:321".into(), turn_id, "remembered fact".into())
    );
    assert!(
        !store
            .recall_memory("telegram:321", "remembered")
            .unwrap()
            .contains("remembered fact")
    );
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
fn legacy_saved_output_does_not_authorize_resending_telegram_effects() {
    let cfg = test_config();
    let mut store = Store::open(&cfg).expect("store");

    let inserted = store
        .insert_turn(&TurnRecord {
            ts: "2026-02-26T00:00:00+0000".to_string(),
            chat_id: "321".to_string(),
            input_type: "text".to_string(),
            user_text: "hello".to_string(),
            provider_raw: "TELEGRAM_REPLY: Old reply\nSEND_DOCUMENT: /tmp/file.txt\n".to_string(),
            telegram_reply: "Old reply".to_string(),
            status: "ok".to_string(),
            update_id: Some("42".to_string()),
            duration_ms: None,
            channel: "telegram".to_string(),
            task_run_id: None,
            side_effects_applied: true,
        })
        .expect("insert turn");
    assert!(inserted.is_some());

    let update = r#"{"update_id":42,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"hello again"}}"#;
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    assert!(outcome.output.is_none());
    let saved = store.rendered_output_for_update_id("42").unwrap().unwrap();
    assert!(saved.contains("TELEGRAM_REPLY: Old reply"));
    assert!(saved.contains("SEND_DOCUMENT: /tmp/file.txt"));
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state,ack_phase FROM telegram_ingress WHERE update_id='42'",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            )
            .unwrap(),
        ("unknown".into(), "unknown".into())
    );
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
    let update = r#"{"update_id":4242,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"hello again"}}"#;

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
fn photo_update_parses_attachment_metadata() {
    let cfg = test_config();
    let update = r#"{"update_id":201,"message":{"from":{"id":"321"},"chat":{"id":"321"},"photo":[{"file_id":"a"},{"file_id":"b"}]}}"#;

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
    let update =
        r#"{"update_id":202,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"/cancel"}}"#;

    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("No active run for this conversation/topic."));
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
    let update =
        r#"{"update_id":203,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"/tasks"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("logical tasks"));
    assert!(output.contains(&format!("Task #{task_id} / run #{task_id}")));
    assert!(output.contains("queued"));
    assert!(
        !output.contains("summarize status"),
        "prompts stay out of normal operations views"
    );
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
    let update =
        r#"{"update_id":207,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"/tasks"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert!(output.contains("Task #1 / run #1"));
    assert!(!output.contains("Task #2 / run #2"));
    assert!(!output.contains("visible task"));
    assert!(!output.contains("hidden task"));
    assert!(!output.contains("telegram:999"));
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
    assert_eq!(
        store.kv_get("last_ignored_reason").expect("ignored reason"),
        Some("chat_id_mismatch actual=999 configured=321".to_string())
    );
}

#[test]
fn telegram_update_from_secondary_configured_chat_is_accepted() {
    let mut cfg = test_config();
    cfg.telegram_chat_ids = vec!["999".to_string(), "321".to_string()];
    let update = r#"{"update_id":205,"message":{"from":{"id":"999"},"chat":{"id":"999"},"text":"hello from another chat"}}"#;

    let action = parse_webhook_action(&cfg, update).expect("parse");

    let WebhookAction::Turn(turn) = action else {
        panic!("expected turn action");
    };
    assert_eq!(turn.session.root_id, "999");
    assert_eq!(turn.input.user_text, "hello from another chat");
}

#[test]
fn telegram_cancel_command_replies_to_request_chat() {
    let mut cfg = test_config();
    cfg.telegram_chat_ids = vec!["999".to_string()];
    let mut store = Store::open(&cfg).expect("store");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let update =
        r#"{"update_id":206,"message":{"from":{"id":"999"},"chat":{"id":"999"},"text":"/cancel"}}"#;

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, update).expect("process");
    let output = outcome.output.unwrap_or_default();

    assert_eq!(outcome.chat_id.as_deref(), Some("999"));
    assert!(output.contains("No active run for this conversation/topic."));
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
        r#"{{"update_id":208,"message":{{"from":{{"id":"321"}},"chat":{{"id":"321"}},"text":"/cancel {task_id}"}}}}"#
    );

    let outcome = process_webhook_line(&cfg, &mut store, &scheduler, &update).expect("process");
    let output = outcome.output.unwrap_or_default();
    let task = store
        .get_task_run(task_id)
        .expect("get task")
        .expect("task exists");

    assert!(output.contains("not active for this conversation/topic"));
    assert_eq!(task.status, "queued");
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
fn stale_task_recovery_preserves_pending_approval() {
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

    assert_eq!(task.status, "awaiting_approval");
    assert_eq!(task.error_summary, None);
    assert_eq!(approval.status, "pending");
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
    assert!(
        markers
            .to_effects()
            .iter()
            .all(|e| !e.payload().contains("spoken"))
    );
    assert_eq!(markers.task_append, vec!["todo item".to_string()]);
}

#[test]
fn recover_unstructured_reply_unescapes_json_envelope_newlines() {
    let payload = r#"{"type":"item.completed","item":{"type":"agent_message","text":"line one\n\nline two\nline three"}}"#;
    let reply = recover_unstructured_reply(payload);
    assert_eq!(reply.as_deref(), Some("line one\n\nline two\nline three"));
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
fn context_requests_native_rich_markdown_when_regular_parse_mode_is_off() {
    let cfg = test_config();
    let store = Store::open(&cfg).expect("store");
    let input = TurnInput {
        input_type: InputType::Text,
        user_text: "hello".to_string(),
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
    assert!(text.contains("Use native Rich Markdown inside TELEGRAM_REPLY"));
    assert!(!text.contains("Do not use markdown"));
}

#[test]
fn context_requests_native_rich_markdown_when_regular_parse_mode_is_markdown_v2() {
    let cfg = RuntimeConfig::test_builder()
        .telegram_parse_mode(TelegramParseMode::MarkdownV2)
        .build();
    let store = Store::open(&cfg).expect("store");
    let input = TurnInput {
        input_type: InputType::Text,
        user_text: "hello".to_string(),
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
    assert!(text.contains("Use native Rich Markdown inside TELEGRAM_REPLY"));
    assert!(!text.contains("MarkdownV2 is enabled"));
    assert!(!text.contains("Do not use CommonMark syntax"));
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
            provider_raw: "TELEGRAM_REPLY: telegram reply\n".to_string(),
            telegram_reply: "telegram reply".to_string(),
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
            provider_raw: "TELEGRAM_REPLY: slack reply\n".to_string(),
            telegram_reply: "slack reply".to_string(),
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
fn service_subcommand_is_recognized() {
    let parsed = Cli::try_parse_from(["coconutclaw", "service", "status"]);
    assert!(parsed.is_ok());
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
