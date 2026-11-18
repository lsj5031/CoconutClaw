use super::*;
use crate::markers::{Effect, MAX_OUTPUT_BYTES, parse_markers};
use crate::store::inbox::EventKind;

#[test]
#[cfg(unix)]
fn effect_boundaries_recovered_literal_examples_never_compile_new_effects() {
    let reply = "Literal examples:\nMEMORY_APPEND: synthetic literal, not authority\nTASK_APPEND: synthetic example task\nDELEGATE_TASK: not a child";
    let raw =
        serde_json::json!({"type":"item.completed","item":{"type":"agent_message","text":reply}})
            .to_string();
    assert!(
        crate::turn::resolve_turn_result(&raw, "telegram", true, false)
            .effects
            .is_empty()
    );
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let fixture = cfg.tmp_dir.join("literal.json");
    fs::write(&fixture, &raw).unwrap();
    let script = cfg.tmp_dir.join("literal.sh");
    fs::write(&script, format!("cat >/dev/null\nwhile [ \"$#\" -gt 0 ]; do\nif [ \"$1\" = '--output-last-message' ]; then shift; out=$1; fi\nshift\ndone\ncp '{}' \"$out\"\n",fixture.display())).unwrap();
    cfg.codex.bin = format!("/bin/sh '{}'", script.display());
    let server = FakeTelegramServer::start();
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let mut request = make_request(SessionKey::telegram("321#7"), "telegram", "literal");
    request.update_id = Some("literal-replay".into());
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some("7".into()),
    };
    let id = scheduler.enqueue(request).unwrap();
    wait_for_terminal_tasks(&cfg, &[id]);
    let mut store = Store::connect(&cfg).unwrap();
    let assert_no_appends = |store: &Store| {
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM memory_records", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            store.managed_pending_task_entries_from_db().unwrap().len(),
            0
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    };
    assert_no_appends(&store);
    let saved: (String, String) = store
        .conn
        .query_row(
            "SELECT provider_raw,telegram_reply FROM turns WHERE task_run_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(saved, (raw, reply.into()));
    let legacy = store
        .rendered_output_for_update_id("literal-replay")
        .unwrap()
        .unwrap();
    let legacy = crate::markers::CompiledOutput::from_output(&legacy);
    assert_eq!(legacy.reply(), reply);
    assert_eq!(legacy.effects, vec![Effect::TelegramReply(reply.into())]);
    // Repeat both crash cuts: append recovery and scheduler materialization.
    scheduler.shutdown().unwrap();
    store
        .conn
        .execute(
            "UPDATE turns SET side_effects_applied=0 WHERE task_run_id=?1",
            [id],
        )
        .unwrap();
    crate::recovery::reconcile_pending_turn_side_effects(&cfg, &mut store).unwrap();
    assert_no_appends(&store);
    store.conn.execute("UPDATE task_payloads SET output=NULL,delivery_state='[]',delivered=0 WHERE task_run_id=?1",[id]).unwrap();
    store
        .conn
        .execute(
            "DELETE FROM runtime_inbox WHERE event_key=?1",
            [format!("result:{id}")],
        )
        .unwrap();
    let recovered = SessionScheduler::new(cfg, None);
    recovered.recover().unwrap();
    recovered.retry_deliveries().unwrap();
    assert_no_appends(&store);
    assert_eq!(
        super::topics_rich::state(&store, id)["text_plan"]["markdown"],
        reply
    );
    recovered.shutdown().unwrap();
}

#[test]
fn effect_boundaries_exact_provider_reply_with_runtime_parent_and_child_text_is_deliverable() {
    let server = FakeTelegramServer::start();
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.inner.recovering.store(true, Ordering::SeqCst);
    let mut request = make_request(SessionKey::telegram("321#7"), "telegram", "parent-boundary");
    request.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some("7".into()),
    };
    let tx = store.conn.unchecked_transaction().unwrap();
    let (id, _) = store
        .admit_inbox_request(EventKind::Request, Some("parent-boundary"), &request, "now")
        .unwrap();
    tx.commit().unwrap();
    let reply = "x".repeat(262144);
    let raw = format!("TELEGRAM_REPLY: {reply}\nDELEGATE_TASK: bounded child");
    scheduler
        .persist_result(
            &store,
            id,
            &request,
            &request.input,
            &raw,
            crate::TurnStatus::Ok,
        )
        .unwrap();
    let (child, payload): (i64, String) = store
        .conn
        .query_row(
            "SELECT task_run_id,request FROM task_payloads WHERE task_run_id!=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(store.conn.query_row("SELECT count(*) FROM runtime_tasks WHERE parent_task_id=(SELECT task_id FROM runtime_attempts WHERE run_id=?1)",[id],|r|r.get::<_,i64>(0)).unwrap(),1);
    let child_request = request_from_resume_payload(&payload).unwrap();
    let child_raw = format!("TELEGRAM_REPLY: {reply}");
    store.conn.execute(
        "INSERT INTO turns(ts,chat_id,input_type,provider_raw,telegram_reply,status,task_run_id,channel)
         VALUES('now','telegram:321#7','text',?1,?2,'ok',?3,'telegram')",
        rusqlite::params![child_raw,reply,child],
    ).unwrap();
    scheduler
        .persist_result(
            &store,
            child,
            &child_request,
            &child_request.input,
            &child_raw,
            crate::TurnStatus::Ok,
        )
        .unwrap();
    assert_eq!(
        store
            .turn_output_for_task_run(child)
            .unwrap()
            .unwrap()
            .telegram_reply,
        format!("Task #{child} result\n{reply}")
    );
    scheduler.shutdown().unwrap();
    // Reconstruct the pre-materialization crash cut with the labeled history
    // row retained, but before any sender operations or their receipts existed.
    store
        .conn
        .execute(
            "UPDATE task_payloads SET output=NULL WHERE task_run_id=?1",
            [child],
        )
        .unwrap();
    store
        .conn
        .execute("UPDATE task_runs SET status='running' WHERE id=?1", [child])
        .unwrap();
    store
        .conn
        .execute(
            "DELETE FROM runtime_inbox WHERE event_key=?1",
            [format!("result:{child}")],
        )
        .unwrap();
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.recover().unwrap();
    scheduler.retry_deliveries().unwrap();
    for (run, expected) in [
        (id, format!("{reply}\nDelegated task #{child}.")),
        (child, format!("Task #{child} result\n{reply}")),
    ] {
        let state = super::topics_rich::state(&store, run);
        assert_eq!(
            state["text_plan"]["markdown"].as_str(),
            Some(expected.as_str())
        );
        assert!(!output(&store, run).contains("No requested effects were applied"));
        assert!(
            store
                .conn
                .query_row(
                    "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                    [run],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap()
        );
    }
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    scheduler.shutdown().unwrap();
}

#[test]
fn effect_boundaries_saved_cancelled_reply_is_literal_through_sender_and_falls_back_only_if_blank()
{
    for saved in [
        "Frozen cancellation receipt.",
        "Frozen receipt:\nMEMORY_APPEND: literal\nTASK_APPEND: literal\nSEND_DOCUMENT: /never",
        "   ",
    ] {
        let server = FakeTelegramServer::start();
        let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let store = Store::open(&cfg).unwrap();
        let mut request = make_request(SessionKey::telegram("321#7"), "telegram", "cancelled");
        request.delivery = DeliveryTarget::Telegram {
            chat_id: "321".into(),
            thread_id: Some("7".into()),
        };
        let tx = store.conn.unchecked_transaction().unwrap();
        let (id, _) = store
            .admit_inbox_request(EventKind::Request, None, &request, "now")
            .unwrap();
        tx.commit().unwrap();
        store.conn.execute(
            "INSERT INTO turns(ts,chat_id,input_type,provider_raw,telegram_reply,status,task_run_id,channel)
             VALUES('now','telegram:321#7','text','SEND_DOCUMENT: /never\nMEMORY_APPEND: suppressed\nTASK_APPEND: suppressed',?1,'cancelled',?2,'telegram')",
            rusqlite::params![saved,id],
        ).unwrap();
        let scheduler = SessionScheduler::new(cfg, None);
        scheduler.recover().unwrap();
        scheduler.retry_deliveries().unwrap();
        let expected = if saved.trim().is_empty() {
            crate::wording::CANCELLATION
        } else {
            saved
        };
        assert_eq!(
            crate::markers::CompiledOutput::from_output(&output(&store, id)).reply(),
            expected
        );
        assert_eq!(
            super::topics_rich::state(&store, id)["text_plan"]["markdown"],
            expected
        );
        assert_eq!(
            store
                .turn_output_for_task_run(id)
                .unwrap()
                .unwrap()
                .telegram_reply,
            saved
        );
        assert!(
            store
                .managed_pending_task_entries_from_db()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM memory_records", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
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
}

fn output(store: &Store, id: i64) -> String {
    store
        .conn
        .query_row(
            "SELECT output FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn effect_boundaries_exact_counts_and_utf8_payload_bytes() {
    // Literal policy values are independent expected boundaries, not values
    // derived from the implementation's constants.
    for (marker, maximum) in [
        ("DELEGATE_TASK", 4),
        ("SEND_PHOTO", 8),
        ("MEMORY_APPEND", 8),
        ("TASK_APPEND", 16),
    ] {
        let accepted = format!(
            "TELEGRAM_REPLY: ok\n{}",
            format!("{marker}: x\n").repeat(maximum)
        );
        assert!(
            !parse_markers(&accepted)
                .telegram_reply
                .unwrap()
                .contains("rejected")
        );
        let rejected = format!("{accepted}{marker}: extra\n");
        let effects = parse_markers(&rejected).to_effects();
        assert!(matches!(&effects[..], [Effect::TelegramReply(s)] if s.contains("count limit")));
    }
    for (marker, bytes) in [
        ("MEMORY_APPEND", 2048),
        ("TASK_APPEND", 2048),
        ("DELEGATE_TASK", 16384),
        ("SEND_DOCUMENT", 4096),
        ("SEND_APPROVAL", 4096),
        ("WAIT_FOR_TASK", 4096),
        ("TELEGRAM_REPLY", 262144),
    ] {
        let value = "λ".repeat(bytes / 2);
        let accepted = format!("{marker}: {value}");
        assert!(
            !parse_markers(&accepted)
                .telegram_reply
                .as_deref()
                .unwrap_or("")
                .contains("rejected")
        );
        let effects = parse_markers(&format!("{accepted}x")).to_effects();
        assert!(
            matches!(&effects[..], [Effect::TelegramReply(s)] if s.contains("payload byte limit")),
            "{marker}"
        );
    }
    let suffix = "\nTELEGRAM_REPLY: exact";
    let exact = format!("{}{suffix}", "x".repeat(524288 - suffix.len()));
    assert_eq!(
        parse_markers(&exact).telegram_reply.as_deref(),
        Some("exact")
    );
    assert!(
        parse_markers(&(exact + "x"))
            .telegram_reply
            .unwrap()
            .contains("output byte limit")
    );
    let mixed = "SEND_PHOTO: x\n".repeat(4) + &"SEND_VIDEO: x\n".repeat(5);
    assert!(
        parse_markers(&mixed)
            .telegram_reply
            .unwrap()
            .contains("count limit")
    );
}

#[test]
fn effect_boundaries_rejection_is_persisted_before_child_admission_and_replay() {
    for raw in [
        "TELEGRAM_REPLY: ok\nDELEGATE_TASK: child\n".to_string()
            + &"DELEGATE_TASK: child\n".repeat(4),
        format!("TELEGRAM_REPLY: ok\nTASK_APPEND: {}", "x".repeat(2049)),
        "x".repeat(MAX_OUTPUT_BYTES + 1),
    ] {
        let cfg = scheduler_test_config("NEVER-EXECUTE".into());
        let store = Store::open(&cfg).unwrap();
        let (id, request) = graph_admit(&store, "local:limits", "limits");
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        scheduler.inner.recovering.store(true, Ordering::SeqCst);
        for _ in 0..2 {
            scheduler
                .persist_result(
                    &store,
                    id,
                    &request,
                    &request.input,
                    &raw,
                    crate::TurnStatus::Ok,
                )
                .unwrap();
        }
        let saved = output(&store, id);
        assert!(saved.contains("Output rejected:"));
        assert!(saved.len() < 256);
        assert!(!saved.contains("DELEGATE_TASK:"));
        let counts: (i64, i64) = store.conn.query_row(
            "SELECT (SELECT COUNT(*) FROM task_runs),(SELECT COUNT(*) FROM runtime_inbox WHERE event_key LIKE 'result:%')",
            [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!(counts, (1, 1));
        scheduler.shutdown().unwrap();
        drop(store);
        assert_eq!(output(&Store::open(&cfg).unwrap(), id), saved);
    }
}

#[test]
fn effect_boundaries_four_children_cannot_amplify_after_resume_serialization() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (id, request) = graph_admit(&store, "local:limits", "root");
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.inner.recovering.store(true, Ordering::SeqCst);
    let raw = "TELEGRAM_REPLY: root\n".to_string()
        + &format!("DELEGATE_TASK: {}\n", "λ".repeat(8192)).repeat(4);
    scheduler
        .persist_result(
            &store,
            id,
            &request,
            &request.input,
            &raw,
            crate::TurnStatus::Ok,
        )
        .unwrap();
    let children = store
        .conn
        .prepare("SELECT task_run_id,request FROM task_payloads WHERE task_run_id!=?1")
        .unwrap()
        .query_map([id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(children.len(), 4);
    for (child, payload) in children {
        let resumed = request_from_resume_payload(&payload).unwrap();
        assert!(resumed.delegated);
        assert_eq!(resumed.input.user_text.len(), 16384);
        scheduler
            .persist_result(
                &store,
                child,
                &resumed,
                &resumed.input,
                &raw,
                crate::TurnStatus::Ok,
            )
            .unwrap();
        assert!(output(&store, child).contains("delegated tasks cannot delegate"));
    }
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        5
    );
    scheduler.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn effect_boundaries_real_turn_rejects_before_memory_task_and_raw_persistence() {
    for (name, raw) in [
        (
            "memory",
            "TELEGRAM_REPLY: ok\n".to_string() + &"MEMORY_APPEND: candidate\n".repeat(9),
        ),
        (
            "tasks",
            "TELEGRAM_REPLY: ok\n".to_string() + &"TASK_APPEND: task\n".repeat(17),
        ),
        (
            "bytes",
            format!("TELEGRAM_REPLY: ok\nMEMORY_APPEND: {}", "λ".repeat(1025)),
        ),
        ("raw", "x".repeat(MAX_OUTPUT_BYTES + 1)),
        (
            "media",
            "TELEGRAM_REPLY: ok\nMEMORY_APPEND: candidate\nSEND_DOCUMENT: ../outside-secret".into(),
        ),
        (
            "nested",
            "TELEGRAM_REPLY: ok\nMEMORY_APPEND: candidate\nTASK_APPEND: task\nDELEGATE_TASK: child"
                .into(),
        ),
    ] {
        let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
        let fixture = cfg.tmp_dir.join("output.txt");
        fs::write(&fixture, raw).unwrap();
        let script = cfg.tmp_dir.join("mock.sh");
        fs::write(&script, format!("cat >/dev/null\nwhile [ \"$#\" -gt 0 ]; do\nif [ \"$1\" = '--output-last-message' ]; then shift; out=$1; fi\nshift\ndone\ncp '{}' \"$out\"\n", fixture.display())).unwrap();
        cfg.codex.bin = format!("sh '{}'", script.display());
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        let mut request = make_request(
            SessionKey::telegram("321"),
            "telegram",
            "synthetic limit fixture",
        );
        request.update_id = Some(name.into());
        request.delegated = name == "nested";
        let id = scheduler.enqueue(request).unwrap();
        wait_for_terminal_tasks(&cfg, &[id]);
        scheduler.shutdown().unwrap();
        let mut store = Store::open(&cfg).unwrap();
        let saved = output(&store, id);
        assert!(saved.contains("Output rejected:"), "{name}: {saved}");
        let (raw, reply): (String, String) = store
            .conn
            .query_row(
                "SELECT provider_raw,telegram_reply FROM turns WHERE task_run_id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(raw.len() < 256, "{name}");
        assert!(reply.contains("Output rejected:"));
        assert_eq!(
            store.managed_pending_task_entries_from_db().unwrap().len(),
            0
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM memory_records", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        crate::recovery::reconcile_pending_turn_side_effects(&cfg, &mut store).unwrap();
        assert_eq!(output(&store, id), saved);
    }
}

#[test]
fn effect_boundaries_pending_recovery_rejects_entire_legacy_batch_durably() {
    for raw in [
        "TELEGRAM_REPLY: old\n".to_string() + &"MEMORY_APPEND: candidate\n".repeat(9),
        "TELEGRAM_REPLY: old\n".to_string() + &"TASK_APPEND: task\n".repeat(17),
        "TELEGRAM_REPLY: old\nMEMORY_APPEND: candidate\nSEND_DOCUMENT: /never-authorized".into(),
    ] {
        let cfg = scheduler_test_config("NEVER-EXECUTE".into());
        let mut store = Store::open(&cfg).unwrap();
        store.conn.execute(
            "INSERT INTO turns(ts,chat_id,input_type,provider_raw,telegram_reply,status,channel,side_effects_applied)
             VALUES('now','telegram:321','text',?1,'old','ok','telegram',0)", [&raw]).unwrap();
        assert_eq!(
            crate::recovery::reconcile_pending_turn_side_effects(&cfg, &mut store).unwrap(),
            1
        );
        assert_eq!(
            crate::recovery::reconcile_pending_turn_side_effects(&cfg, &mut store).unwrap(),
            0
        );
        assert_eq!(
            store.managed_pending_task_entries_from_db().unwrap().len(),
            0
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM memory_records", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let (raw, reply): (String, String) = store
            .conn
            .query_row("SELECT provider_raw,telegram_reply FROM turns", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert!(raw.contains("Output rejected:") && raw.len() < 256);
        assert!(reply.contains("Output rejected:"));
    }
}

#[test]
#[cfg(unix)]
fn effect_boundaries_real_turn_accepts_exact_memory_and_task_limits() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let fixture = cfg.tmp_dir.join("accepted.txt");
    let raw = "TELEGRAM_REPLY: accepted\n".to_string()
        + &(0..8)
            .map(|i| format!("MEMORY_APPEND: candidate {i} {}\n", "m".repeat(2036)))
            .collect::<String>()
        + &(0..16)
            .map(|i| {
                let prefix = format!("task {i} ");
                format!("TASK_APPEND: {prefix}{}\n", "t".repeat(2048 - prefix.len()))
            })
            .collect::<String>();
    fs::write(&fixture, raw).unwrap();
    let script = cfg.tmp_dir.join("accepted.sh");
    fs::write(&script, format!("cat >/dev/null\nwhile [ \"$#\" -gt 0 ]; do\nif [ \"$1\" = '--output-last-message' ]; then shift; out=$1; fi\nshift\ndone\ncp '{}' \"$out\"\n", fixture.display())).unwrap();
    cfg.codex.bin = format!("sh '{}'", script.display());
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let id = scheduler
        .enqueue(make_request(
            SessionKey::telegram("321"),
            "telegram",
            "synthetic accepted fixture",
        ))
        .unwrap();
    wait_for_terminal_tasks(&cfg, &[id]);
    scheduler.shutdown().unwrap();
    let store = Store::open(&cfg).unwrap();
    assert!(!output(&store, id).contains("rejected"));
    assert_eq!(
        store.managed_pending_task_entries_from_db().unwrap().len(),
        16
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM memory_records", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        8
    );
}

#[test]
fn effect_boundaries_delivery_caps_media_before_any_upload_and_preserves_valid_files() {
    for count in [8, 9] {
        let server = FakeTelegramServer::start();
        let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let path = outbox_fixture(&cfg, "intended.txt");
        fs::write(&path, "legitimate generated attachment").unwrap();
        let raw = "TELEGRAM_REPLY: attachments\n".to_string()
            + &format!("SEND_DOCUMENT: {}\n", path.display()).repeat(count);
        let (store, id) = super::topics_rich::save_output(&cfg, &raw, None);
        let scheduler = SessionScheduler::new(cfg, None);
        scheduler.retry_deliveries().unwrap();
        scheduler.retry_deliveries().unwrap();
        scheduler.shutdown().unwrap();
        let requests = server.full_requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.contains("/sendDocument "))
                .count(),
            if count == 8 { 8 } else { 0 }
        );
        assert_eq!(requests.len(), if count == 8 { 9 } else { 1 });
        if count == 9 {
            assert!(
                super::topics_rich::state(&store, id)["text_plan"]["markdown"]
                    .as_str()
                    .unwrap()
                    .contains("count limit")
            );
        } else {
            assert!(requests[1].contains("legitimate generated attachment"));
        }
    }
}

#[test]
#[cfg(unix)]
fn effect_boundaries_send_rechecks_canonical_paths_and_symlink_swaps() {
    use std::os::unix::fs::symlink;
    let server = FakeTelegramServer::start();
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let good = outbox_fixture(&cfg, "good.txt");
    fs::write(&good, "intended artifact").unwrap();
    let outside = cfg.instance_dir.join("private.txt");
    fs::write(&outside, "synthetic private bytes never send").unwrap();
    let link = outbox_fixture(&cfg, "escape.txt");
    symlink(&outside, &link).unwrap();
    let directory = outbox_fixture(&cfg, "directory");
    symlink(&cfg.tmp_dir, &directory).unwrap();
    fs::write(
        cfg.tmp_dir.join("private.txt"),
        "synthetic private bytes never send",
    )
    .unwrap();
    for path in [
        outside.clone(),
        link,
        directory.join("private.txt"),
        good.parent().unwrap().join("../private.txt"),
    ] {
        assert!(
            crate::telegram::telegram_send_media_file(
                &crate::telegram::build_telegram_client(&cfg).unwrap(),
                &cfg,
                "321",
                "sendDocument",
                "document",
                &path
            )
            .is_err()
        );
    }
    assert!(server.full_requests.lock().unwrap().is_empty());
    // Admission is valid, then the file is replaced before the outbox retries.
    let raw = format!(
        "TELEGRAM_REPLY: artifact\nSEND_DOCUMENT: {}",
        good.display()
    );
    assert!(!crate::delivery::media::admitted_output(&cfg, &raw, false).contains("rejected"));
    fs::remove_file(&good).unwrap();
    symlink(&outside, &good).unwrap();
    let (store, id) = super::topics_rich::save_output(&cfg, &raw, None);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.retry_deliveries().unwrap();
    scheduler.shutdown().unwrap();
    let requests = server.full_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/sendRichMessage "));
    assert!(!requests[0].contains("synthetic private bytes"));
    assert!(
        super::topics_rich::state(&store, id)["text_plan"]["markdown"]
            .as_str()
            .unwrap()
            .contains("Output rejected:")
    );
}

#[test]
fn effect_boundaries_file_bytes_and_outbox_root_symlink() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let path = outbox_fixture(&cfg, "large.bin");
    let file = fs::File::create(&path).unwrap();
    file.set_len(20 * 1024 * 1024).unwrap();
    assert!(crate::delivery::media::open_attachment(&cfg, &path).is_ok());
    file.set_len(20 * 1024 * 1024 + 1).unwrap();
    assert!(crate::delivery::media::open_attachment(&cfg, &path).is_err());
    #[cfg(unix)]
    {
        let root = crate::delivery::media::outbox(&cfg);
        fs::rename(&root, cfg.instance_dir.join("moved-outbox")).unwrap();
        std::os::unix::fs::symlink(cfg.instance_dir.join("moved-outbox"), &root).unwrap();
        assert!(crate::delivery::media::open_attachment(&cfg, &path).is_err());
    }
}

#[test]
#[cfg(unix)]
fn effect_boundaries_partial_delivery_rejection_keeps_receipts_and_unknown() {
    for (unknown, notice_unknown) in [(false, false), (true, false), (false, true)] {
        let server = FakeTelegramServer::start_with_response(move |request| {
            if request.contains("/sendDocument ") {
                if unknown {
                    (
                        500,
                        r#"{"ok":false,"error_code":500,"description":"uncertain"}"#.into(),
                    )
                } else {
                    (
                        400,
                        r#"{"ok":false,"error_code":400,"description":"refused"}"#.into(),
                    )
                }
            } else if notice_unknown && request.contains("/sendMessage ") {
                (
                    500,
                    r#"{"ok":false,"error_code":500,"description":"uncertain notice"}"#.into(),
                )
            } else {
                (200, r#"{"ok":true,"result":{"message_id":123}}"#.into())
            }
        });
        let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
        cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
        let path = outbox_fixture(&cfg, "partial.txt");
        fs::write(&path, "approved artifact").unwrap();
        let raw = format!(
            "TELEGRAM_REPLY: original\nSEND_DOCUMENT: {}",
            path.display()
        );
        let (store, id) = super::topics_rich::save_output(&cfg, &raw, None);
        let scheduler = SessionScheduler::new(cfg.clone(), None);
        scheduler.retry_deliveries().unwrap();
        let initial = super::topics_rich::state(&store, id);
        assert!(
            initial["completed_ops"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "telegram:text")
        );
        fs::remove_file(&path).unwrap();
        let secret = cfg.instance_dir.join("private.txt");
        fs::write(&secret, "synthetic secret").unwrap();
        std::os::unix::fs::symlink(secret, &path).unwrap();
        scheduler.retry_deliveries().unwrap();
        // Rejection is a durable decision, not a transient file check.
        fs::remove_file(&path).unwrap();
        fs::write(&path, "approved again but rejected batch stays rejected").unwrap();
        scheduler.retry_deliveries().unwrap();
        scheduler.shutdown().unwrap();
        let saved = super::topics_rich::state(&store, id);
        assert_eq!(saved["text_plan"], initial["text_plan"]);
        assert_eq!(
            saved["operations"]["telegram:document:0"],
            initial["operations"]["telegram:document:0"]
        );
        assert!(
            saved["operations"]["telegram:output-rejection"]["message"]
                .as_str()
                .unwrap()
                .contains("unknown effects remain unknown")
        );
        let delivered: bool = store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(delivered, !unknown && !notice_unknown);
        let requests = server.full_requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].contains("/sendMessage "));
        assert!(!requests[2].contains("synthetic secret"));
    }
}
