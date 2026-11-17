use super::*;
use crate::store::{InsertTaskRunParams, TaskRunStatus, inbox::EventKind};
use coconutclaw_config::RuntimeConfig;

const NOW: &str = "2026-11-17T12:00:00Z";
const SECRET: &str = "<secret>&private prompt, credential and internal JSON";

fn fixture() -> (tempfile::TempDir, RuntimeConfig, Store) {
    let root = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config_at(root.path().join("runtime"));
    let store = Store::open(&cfg).unwrap();
    (root, cfg, store)
}

fn run(store: &Store, session: &SessionKey) -> i64 {
    store
        .insert_task_run(InsertTaskRunParams {
            session_id: session.id(),
            channel: session.platform.as_str().into(),
            source_chat_id: Some(session.root_id.clone()),
            source_user_id: Some(session.root_id.clone()),
            update_id: None,
            prompt: SECRET.into(),
            created_at: NOW.into(),
            progress_message_id: None,
            scheduled_task_id: None,
        })
        .unwrap()
}

fn ingress(store: &Store, session: &SessionKey, id: i64, state: &str) {
    store
        .conn
        .execute(
            "INSERT INTO telegram_ingress(update_id,payload,session_id,state,created_at)
         VALUES(?1,?2,?3,?4,?5)",
            params![id.to_string(), SECRET, session.id(), state, NOW],
        )
        .unwrap();
}

#[test]
fn empty_snapshot_is_scoped_unknown_health_not_inferred_from_config() {
    let (_root, _cfg, store) = fixture();
    let snapshot = store
        .status_snapshot(&SessionKey::telegram("321#7"), NOW)
        .unwrap();
    assert_eq!(snapshot.observed_at, "2026-11-17T12:00:00+00:00");
    assert_eq!(snapshot.task_count, 0);
    assert_eq!(snapshot.run_count, 0);
    assert_eq!(snapshot.schedule_count, 0);
    assert!(snapshot.tasks.is_empty());
    assert!(snapshot.schedules.is_empty());
    assert_eq!(snapshot.transport.pending, 0);
    assert_eq!(snapshot.transport.latest_received_at, None);
    assert_eq!(snapshot.runtime_health, "unavailable");
    assert_eq!(snapshot.node_health, "unavailable");
    assert!(
        store
            .status_snapshot(&SessionKey::telegram("321"), "not a time")
            .is_err()
    );
}

#[test]
fn exact_chat_topic_platform_scope_and_inconsistent_graph_are_fenced() {
    let (_root, _cfg, store) = fixture();
    let scope = SessionKey::telegram("321#7");
    let visible = run(&store, &scope);
    ingress(&store, &scope, 1, "pending");
    for (i, session) in [
        SessionKey::telegram("321"),
        SessionKey::telegram("321#8"),
        SessionKey::telegram("322#7"),
        SessionKey::local("321#7"),
    ]
    .iter()
    .enumerate()
    {
        run(&store, session);
        ingress(&store, session, i as i64 + 2, "unknown");
    }
    let forged = run(&store, &scope);
    store
        .conn
        .execute(
            "UPDATE runtime_tasks SET session_id='telegram:322#7' WHERE task_id=?1",
            [forged],
        )
        .unwrap();
    let snapshot = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].task_id, visible);
    assert_eq!(snapshot.task_count, 1);
    assert_eq!(snapshot.run_count, 2);
    assert_eq!(snapshot.transport.pending, 1);
    assert_eq!(snapshot.transport.unknown, 0);
}

#[test]
fn persisted_progress_wait_cancellation_and_unknown_are_safe_observations() {
    let (_root, _cfg, store) = fixture();
    let scope = SessionKey::telegram("321#7");
    let id = run(&store, &scope);
    store.update_task_run_started(id, NOW).unwrap();
    store.update_task_run_progress(id, SECRET).unwrap();
    store.conn.execute(
        "INSERT INTO task_payloads(task_run_id,request,delivery_state) VALUES(?1,?2,'broken json')",
        params![id, SECRET],
    ).unwrap();
    store.conn.execute(
        "INSERT INTO runtime_waits(run_id,kind,expected_id,session_id,actor,resume_payload,run_prompt)
         VALUES(?1,'dependency',999,?2,'321',?3,?3)",
        params![id,scope.id(),SECRET],
    ).unwrap();
    store
        .conn
        .execute(
            "UPDATE task_runs SET status='waiting_event',error_summary=?2 WHERE id=?1",
            params![id, format!("outcome unknown: {SECRET}")],
        )
        .unwrap();
    let before = store.status_snapshot(&scope, NOW).unwrap();
    let task = &before.tasks[0];
    assert!(task.progress_recorded && task.attention && task.outcome_unknown);
    assert_eq!(task.wait_kind, Some("dependency"));
    assert_eq!(task.delivery, "unknown");
    assert_eq!(
        task.started_at.as_deref(),
        Some("2026-11-17T12:00:00+00:00")
    );
    assert!(!format!("{before:?}").contains("secret"));
    store.mark_task_run_cancel_requested(id, NOW).unwrap();
    let after = store
        .status_snapshot(&scope, "2026-11-17T12:01:00Z")
        .unwrap();
    assert!(after.tasks[0].cancel_requested);
    assert_eq!(after.tasks[0].status, "cancel_requested");
    assert_eq!(before.tasks[0].status, "waiting_event");
    assert_ne!(before.observed_at, after.observed_at);
}

#[test]
fn continuation_is_one_task_two_runs_and_pending_approval_is_scope_bound() {
    let (_root, _cfg, store) = fixture();
    let scope = SessionKey::telegram("321#7");
    let first = run(&store, &scope);
    let second = run(&store, &scope);
    // Persist a continuation projection; snapshot must not mistake runs for tasks.
    store
        .conn
        .execute(
            "UPDATE runtime_attempts SET task_id=?2,ordinal=2,parent_run_id=?2 WHERE run_id=?1",
            params![second, first],
        )
        .unwrap();
    store
        .conn
        .execute("DELETE FROM runtime_tasks WHERE task_id=?1", [second])
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE runtime_tasks SET current_run_id=?2 WHERE task_id=?1",
            params![first, second],
        )
        .unwrap();
    for (session, actor) in [
        ("telegram:322#7", "321"),
        ("telegram:321#7", "322"),
        ("telegram:321#7", "321"),
    ] {
        store.conn.execute(
            "INSERT INTO approvals(task_run_id,session_id,channel,source_user_id,prompt_text,status,resume_payload,created_at)
             VALUES(?1,?2,'telegram',?3,?4,'pending',?4,?5)",
            params![second,session,actor,SECRET,NOW],
        ).unwrap();
    }
    let approval = store.conn.last_insert_rowid();
    store.mark_task_run_awaiting_approval(second).unwrap();
    let snapshot = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(snapshot.task_count, 1);
    assert_eq!(snapshot.run_count, 2);
    assert_eq!(snapshot.tasks[0].task_id, first);
    assert_eq!(snapshot.tasks[0].run_id, second);
    assert_eq!(snapshot.tasks[0].attempt, 2);
    assert_eq!(snapshot.tasks[0].approval_id, Some(approval));
    store
        .conn
        .execute(
            "UPDATE approvals SET status='approved' WHERE id=?1",
            [approval],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&scope, NOW).unwrap().tasks[0].approval_id,
        None
    );
}

#[test]
fn durable_transport_custody_not_network_health_and_malformed_data_is_explicit() {
    let (_root, _cfg, store) = fixture();
    let scope = SessionKey::telegram("321");
    for (i, state) in ["pending", "applying", "unknown", "done", "done", "done"]
        .iter()
        .enumerate()
    {
        ingress(&store, &scope, i as i64, state);
    }
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET ack_phase='unknown' WHERE update_id='3'",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE telegram_ingress SET delivery_state='invalid' WHERE update_id='4'",
            [],
        )
        .unwrap();
    store.conn.execute(
        "UPDATE telegram_ingress SET progress_state='unknown',created_at=?1 WHERE update_id='5'",
        [SECRET],
    ).unwrap();
    let snapshot = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(snapshot.transport.pending, 1);
    assert_eq!(snapshot.transport.applying, 1);
    assert_eq!(snapshot.transport.unknown, 4);
    assert_eq!(snapshot.transport.latest_received_at, None);
    assert_eq!(snapshot.runtime_health, "unavailable");
}

#[test]
fn actual_inbox_and_calendar_persist_across_reopen_without_snapshot_effects() {
    let (_root, cfg, store) = fixture();
    let scope = SessionKey::telegram("321#7");
    let mut request = crate::commands::schedule::request(&cfg, scope.clone(), SECRET).unwrap();
    request.update_id = Some("snapshot-request".into());
    let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate).unwrap();
    let (run_id, fresh) = store
        .admit_inbox_request(EventKind::Request, Some("snapshot-request"), &request, NOW)
        .unwrap();
    tx.commit().unwrap();
    assert!(fresh);
    let now = DateTime::parse_from_rfc3339(NOW)
        .unwrap()
        .with_timezone(&Utc);
    let id = store
        .create_schedule(
            "snapshot-schedule",
            &request,
            &Timing::parse(None, Some("12:00"), "Pacific/Auckland").unwrap(),
            now,
        )
        .unwrap();
    let other =
        crate::commands::schedule::request(&cfg, SessionKey::telegram("321#8"), SECRET).unwrap();
    store
        .create_schedule(
            "other",
            &other,
            &Timing::parse(None, Some("12:00"), "UTC").unwrap(),
            now,
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO scheduled_tasks(ts,source,prompt,schedule_time,origin_session)
         VALUES(?1,'legacy',?2,'12:00',?3)",
            params![NOW, SECRET, scope.id()],
        )
        .unwrap();
    let first = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(first.tasks[0].run_id, run_id);
    assert_eq!(first.schedule_count, 1);
    assert_eq!(first.schedules[0].id, id);
    assert_eq!(
        first.schedules[0].next_due.as_deref(),
        Some("2026-11-17T23:00:00+00:00")
    );
    let timing = first.schedules[0].timing.as_ref().unwrap();
    assert_eq!(timing.cadence, "daily 12:00");
    assert_eq!(timing.timezone, "Pacific/Auckland");
    assert_eq!(
        (timing.fold, timing.gap, timing.missed),
        ("first", "skip", "coalesce")
    );
    assert!(!first.schedules[0].blocked);
    assert_eq!(first.schedules[0].latest_run_id, None);
    let changes = store.conn.total_changes();
    assert_eq!(first, store.status_snapshot(&scope, NOW).unwrap());
    assert_eq!(changes, store.conn.total_changes());
    drop(store);
    let store = Store::open(&cfg).unwrap();
    assert_eq!(first, store.status_snapshot(&scope, NOW).unwrap());
    let due = now + chrono::Duration::hours(11);
    let runs = store.due_schedule_events(due).unwrap();
    let schedule_run = runs.iter().find(|(_, r)| r.session == scope).unwrap().0;
    let accepted = store.status_snapshot(&scope, NOW).unwrap();
    assert!(accepted.schedules[0].blocked);
    assert_eq!(accepted.schedules[0].latest_run_id, Some(schedule_run));
    assert_eq!(accepted.schedules[0].latest_status, Some("queued"));
    store
        .finish_task_run(schedule_run, TaskRunStatus::Failed, NOW, Some(SECRET), None)
        .unwrap();
    let failed = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(failed.schedules[0].latest_status, Some("failed"));
    assert!(failed.schedules[0].blocked);
    assert!(
        failed
            .tasks
            .iter()
            .any(|task| task.run_id == schedule_run && task.attention)
    );
    assert!(!format!("{failed:?}").contains("secret"));
}

#[test]
fn oversized_lists_have_totals_active_priority_and_safe_unknown_fields() {
    let (_root, _cfg, store) = fixture();
    let scope = SessionKey::telegram("321");
    let active = run(&store, &scope);
    for n in 0..75 {
        let id = run(&store, &scope);
        store
            .finish_task_run(id, TaskRunStatus::Completed, SECRET, None, None)
            .unwrap();
        store.conn.execute(
            "INSERT INTO runtime_schedules(session_id,creation_key,creation_spec,request,timing,state,next_due)
             VALUES(?1,?2,?3,?3,?3,'active',?4)",
            params![scope.id(),n.to_string(),SECRET,i64::MAX],
        ).unwrap();
    }
    store
        .conn
        .execute(
            "UPDATE task_runs SET status=?2 WHERE id=?1",
            params![active, SECRET],
        )
        .unwrap();
    let snapshot = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(
        (
            snapshot.task_count,
            snapshot.run_count,
            snapshot.schedule_count
        ),
        (76, 76, 75)
    );
    assert_eq!((snapshot.tasks.len(), snapshot.schedules.len()), (50, 50));
    assert_eq!(snapshot.tasks[0].task_id, active);
    assert_eq!(snapshot.tasks[0].status, "unknown");
    assert!(snapshot.tasks[0].attention);
    assert!(snapshot.tasks.iter().all(|task| task.finished_at.is_none()));
    assert!(
        snapshot
            .schedules
            .iter()
            .all(|s| s.timing.is_none() && s.next_due.is_none())
    );
    assert!(!format!("{snapshot:?}").contains("secret"));
}

#[test]
fn snapshot_reuses_existing_custody_transaction_without_committing_it() {
    let (_root, _cfg, store) = fixture();
    let scope = SessionKey::telegram("321");
    let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate).unwrap();
    run(&store, &scope);
    assert_eq!(store.status_snapshot(&scope, NOW).unwrap().task_count, 1);
    assert!(!store.conn.is_autocommit());
    tx.rollback().unwrap();
    assert_eq!(store.status_snapshot(&scope, NOW).unwrap().task_count, 0);
}

#[test]
fn schedule_blockers_match_existing_admission_fence_without_granting_retries() {
    let (_root, cfg, store) = fixture();
    let scope = SessionKey::telegram("321#7");
    let request = crate::commands::schedule::request(&cfg, scope.clone(), SECRET).unwrap();
    let now = DateTime::parse_from_rfc3339(NOW)
        .unwrap()
        .with_timezone(&Utc);
    store
        .create_schedule(
            "fence",
            &request,
            &Timing::parse(None, Some("12:00"), "UTC").unwrap(),
            now,
        )
        .unwrap();
    let run = store.due_schedule_events(now).unwrap()[0].0;
    for (status, started, delivery, expected) in [
        ("queued", false, "[]", true),
        ("running", true, "[]", true),
        ("awaiting_approval", true, "[]", true),
        ("waiting_event", true, "[]", true),
        ("waiting_remote", true, "[]", true),
        ("failed", true, "[]", true),
        ("cancelled", false, "[]", false),
        ("cancelled", true, "[]", true),
        ("completed", true, "[]", false),
        ("completed", true, r#"{"text":{"phase":"sending"}}"#, true),
        ("completed", true, r#"{"text":{"phase":"unknown"}}"#, true),
    ] {
        store
            .conn
            .execute(
                "UPDATE task_runs SET status=?2,started_at=?3 WHERE id=?1",
                params![run, status, started.then_some(NOW)],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE task_payloads SET delivery_state=?2 WHERE task_run_id=?1",
                params![run, delivery],
            )
            .unwrap();
        let changes = store.conn.total_changes();
        let snapshot = store.status_snapshot(&scope, NOW).unwrap();
        assert_eq!(
            snapshot.schedules[0].blocked, expected,
            "{status}, {delivery}"
        );
        assert_eq!(
            store.next_schedule_deadline().unwrap().is_none(),
            expected,
            "existing scheduler fence: {status}, {delivery}"
        );
        assert_eq!(store.conn.total_changes(), changes);
    }
    store
        .conn
        .execute(
            "UPDATE task_payloads SET delivery_state='broken' WHERE task_run_id=?1",
            [run],
        )
        .unwrap();
    assert!(store.status_snapshot(&scope, NOW).unwrap().schedules[0].blocked);
}

#[test]
fn terminal_unknown_delivery_stays_ahead_of_large_completed_history() {
    let (_root, _cfg, store) = fixture();
    let scope = SessionKey::telegram("321#7");
    let uncertain = run(&store, &scope);
    store
        .finish_task_run(uncertain, TaskRunStatus::Completed, NOW, None, None)
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO task_payloads(task_run_id,request,delivery_state)
             VALUES(?1,?2,'[{\"phase\":\"unknown\"}]')",
            params![uncertain, SECRET],
        )
        .unwrap();
    for _ in 0..60 {
        let id = run(&store, &scope);
        store
            .finish_task_run(id, TaskRunStatus::Completed, NOW, None, None)
            .unwrap();
    }
    let snapshot = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(snapshot.tasks[0].run_id, uncertain);
    assert!(snapshot.tasks[0].attention);
    assert_eq!(snapshot.tasks[0].delivery, "unknown");
    assert_eq!(snapshot.task_count, 61);
}

#[test]
fn schedule_timing_is_validated_and_explicit_not_raw_json() {
    let (_root, cfg, store) = fixture();
    let scope = SessionKey::telegram("321");
    let request = crate::commands::schedule::request(&cfg, scope.clone(), SECRET).unwrap();
    let now = DateTime::parse_from_rfc3339(NOW)
        .unwrap()
        .with_timezone(&Utc);
    let policies = crate::scheduling::Policies {
        fold: crate::scheduling::Fold::Second,
        gap: crate::scheduling::Gap::NextValid,
        missed: crate::scheduling::Missed::Skip,
    };
    let id = store
        .create_schedule(
            "weekly",
            &request,
            &Timing::parse_rule(
                None,
                None,
                Some("mon,fri"),
                Some("02:30"),
                "America/New_York",
                policies,
            )
            .unwrap(),
            now,
        )
        .unwrap();
    let snapshot = store.status_snapshot(&scope, NOW).unwrap();
    let timing = snapshot.schedules[0].timing.as_ref().unwrap();
    assert_eq!(timing.cadence, "weekly mon,fri 02:30");
    assert_eq!(timing.timezone, "America/New_York");
    assert_eq!(
        (timing.fold, timing.gap, timing.missed),
        ("second", "next-valid", "skip")
    );
    store.set_schedule_state(&scope.id(), id, "paused").unwrap();
    store
        .conn
        .execute(
            "UPDATE runtime_schedules SET timing=?2 WHERE id=?1",
            params![
                id,
                r#"{"daily":"12:00","timezone":"<secret>","gap":"next-valid"}"#
            ],
        )
        .unwrap();
    let malformed = store.status_snapshot(&scope, NOW).unwrap();
    assert_eq!(malformed.schedules[0].state, "paused");
    assert!(malformed.schedules[0].timing.is_none());
    assert!(!format!("{malformed:?}").contains("secret"));
}
