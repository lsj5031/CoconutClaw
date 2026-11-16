use super::*;

fn rule(raw: serde_json::Value) -> Timing {
    Timing::decode(&raw.to_string()).unwrap()
}

#[test]
fn calendar_wake_weekly_dst_fold_gap_and_year_boundaries() {
    // Independently specified civil/UTC transition expectations.
    for (zone, time, start, first, second) in [
        (
            "America/New_York",
            "01:30",
            "2027-11-07T00:00:00Z",
            "2027-11-07T05:30:00Z",
            "2027-11-07T06:30:00Z",
        ),
        (
            "Pacific/Auckland",
            "02:30",
            "2027-04-03T00:00:00Z",
            "2027-04-03T13:30:00Z",
            "2027-04-03T14:30:00Z",
        ),
    ] {
        for (fold, expected) in [("first", first), ("second", second)] {
            let t = rule(json!({"weekly":"sun","time":time,"timezone":zone,"fold":fold}));
            assert_eq!(t.first(at(start)).unwrap(), at(expected));
            assert_eq!(t.occurrence(at(expected)).unwrap(), at(expected));
            assert!(
                t.next(at(expected)).unwrap().unwrap() > at(expected) + chrono::Duration::days(6)
            );
        }
    }
    for (zone, start, shifted, skipped) in [
        (
            "America/New_York",
            "2027-03-14T00:00:00Z",
            "2027-03-14T07:00:00Z",
            "2027-03-21T06:30:00Z",
        ),
        (
            "Pacific/Auckland",
            "2027-09-25T00:00:00Z",
            "2027-09-25T14:00:00Z",
            "2027-10-02T13:30:00Z",
        ),
    ] {
        for (gap, expected) in [("next-valid", shifted), ("skip", skipped)] {
            let t = rule(json!({"weekly":"sun","time":"02:30","timezone":zone,"gap":gap}));
            assert_eq!(t.first(at(start)).unwrap(), at(expected));
        }
    }
    let t = rule(json!({"weekly":"mon","time":"00:05","timezone":"America/New_York"}));
    assert_eq!(
        t.first(at("2026-12-31T23:59:59Z")).unwrap(),
        at("2027-01-04T05:05:00Z")
    );
}

#[test]
fn calendar_wake_skipped_civil_date_and_non_hour_gap() {
    for gap in ["skip", "next-valid"] {
        let t = rule(json!({"weekly":"fri","time":"09:00","timezone":"Pacific/Apia","gap":gap}));
        assert_eq!(
            t.first(at("2011-12-30T00:00:00Z")).unwrap(),
            at("2012-01-05T19:00:00Z")
        );
    }
    let t = rule(
        json!({"weekly":"sun","time":"02:15","timezone":"Australia/Lord_Howe","gap":"next-valid"}),
    );
    let due = at("2027-10-02T15:30:00Z");
    assert_eq!(t.first(at("2027-10-02T00:00:00Z")).unwrap(), due);
    assert_eq!(t.first(due).unwrap(), due);
    assert_eq!(
        t.first(due + chrono::Duration::nanoseconds(1)).unwrap(),
        at("2027-10-09T15:15:00Z")
    );
    assert_eq!(
        t.occurrence(due + chrono::Duration::minutes(1)).unwrap(),
        due
    );
    assert_eq!(t.next(due).unwrap(), Some(at("2027-10-09T15:15:00Z")));
}

#[test]
fn calendar_wake_invalid_rules_and_cli_policy_values_refuse() {
    for raw in [
        json!({"weekly":"","time":"09:00","timezone":"UTC"}),
        json!({"weekly":"mon,mon","time":"09:00","timezone":"UTC"}),
        json!({"weekly":"monday","time":"09:00","timezone":"UTC"}),
        json!({"weekly":"mon,","time":"09:00","timezone":"UTC"}),
        json!({"weekly":"mon","timezone":"UTC"}),
        json!({"weekly":"mon","time":"9:00","timezone":"UTC"}),
        json!({"weekly":"mon","time":"24:00","timezone":"UTC"}),
        json!({"daily":"09:00","weekly":"mon","time":"09:00","timezone":"UTC"}),
        json!({"daily":"09:00","time":"09:00","timezone":"UTC"}),
        json!({"daily":"09:00","timezone":"invalid"}),
        json!({"daily":"09:00","timezone":"UTC","gap":"invent-offset"}),
        json!({"daily":"09:00","timezone":"UTC","fold":"both"}),
        json!({"daily":"09:00","timezone":"UTC","missed":"retry"}),
        json!({"daily":"09:00","timezone":"UTC","missed":null}),
        json!({"daily":4,"timezone":"UTC"}),
        json!({"at":"2027-01-01T00:00:00Z","timezone":"UTC","fold":"second"}),
    ] {
        assert!(Timing::decode(&raw.to_string()).is_err(), "{raw}");
    }
    for flag in ["--fold", "--gap", "--missed"] {
        assert!(
            crate::Cli::try_parse_from([
                "coconutclaw",
                "schedule",
                "--session",
                "local:invalid",
                "create",
                "--key",
                "bad",
                "--prompt",
                "fixture",
                "--daily",
                "09:00",
                "--timezone",
                "UTC",
                flag,
                "invalid",
            ])
            .is_err()
        );
    }
}

#[test]
fn calendar_wake_legacy_defaults_exact_replay_and_weekday_normalization() {
    let (_root, cfg, store) = fixture();
    let now = at("2026-11-16T08:00:00Z");
    let legacy = r#"{"daily":"09:00","timezone":"UTC"}"#;
    let timing = Timing::decode(legacy).unwrap();
    let req = request(&cfg, "local:legacy-policy", "fixture");
    let id = store.create_schedule("legacy", &req, &timing, now).unwrap();
    let mut spec: serde_json::Value = serde_json::from_str(
        &store
            .conn
            .query_row(
                "SELECT creation_spec FROM runtime_schedules WHERE id=?1",
                [id],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
    )
    .unwrap();
    spec["timing"] = json!(legacy);
    store
        .conn
        .execute(
            "UPDATE runtime_schedules SET timing=?2,creation_spec=?3 WHERE id=?1",
            rusqlite::params![id, legacy, spec.to_string()],
        )
        .unwrap();
    drop(store);
    let store = Store::open(&cfg).unwrap();
    assert_eq!(
        store.create_schedule("legacy", &req, &timing, now).unwrap(),
        id
    );
    assert_eq!(
        store
            .due_schedule_events(at("2026-11-20T10:00:00Z"))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        cursor(&store, id).2,
        Some(at("2026-11-21T09:00:00Z").timestamp())
    );
    let a = rule(json!({"weekly":"wed,mon","time":"09:00","timezone":"UTC"}));
    let b = rule(json!({"weekly":"mon,wed","time":"09:00","timezone":"UTC"}));
    let id = store.create_schedule("sorted", &req, &a, now).unwrap();
    assert_eq!(store.create_schedule("sorted", &req, &b, now).unwrap(), id);
}

#[test]
fn calendar_wake_centuries_coalesce_once_or_skip_without_backlog() {
    for missed in ["coalesce", "skip"] {
        let (_root, cfg, store) = fixture();
        let timing =
            rule(json!({"weekly":"mon,wed","time":"09:00","timezone":"UTC","missed":missed}));
        let id = store
            .create_schedule(
                "centuries",
                &request(&cfg, "local:catchup", "fixture"),
                &timing,
                at("1900-01-01T00:00:00Z"),
            )
            .unwrap();
        let now = at("2200-01-01T10:00:00Z");
        let start = Instant::now();
        assert_eq!(
            store.due_schedule_events(now).unwrap().len(),
            usize::from(missed == "coalesce")
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "catchup must not enumerate historical dates"
        );
        assert_eq!(
            cursor(&store, id).2,
            Some(timing.next(now).unwrap().unwrap().timestamp())
        );
        assert!(store.due_schedule_events(now).unwrap().is_empty());
        assert_eq!(
            count(&store, "schedule_occurrences"),
            i64::from(missed == "coalesce")
        );
    }
    let (_root, cfg, store) = fixture();
    let t = rule(json!({"at":"2026-11-16T09:00:00Z","timezone":"UTC","missed":"skip"}));
    store
        .create_schedule(
            "once",
            &request(&cfg, "local:skip", "fixture"),
            &t,
            at("2026-11-16T08:00:00Z"),
        )
        .unwrap();
    assert!(
        store
            .due_schedule_events(at("2026-11-16T09:00:01Z"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(cursor(&store, 1), (1, "done".into(), None));
    assert_eq!(count(&store, "task_runs"), 0);
}

#[test]
fn calendar_wake_skip_current_second_and_batch_rollback() {
    let (_root, cfg, store) = fixture();
    let start = at("2026-11-16T08:00:00Z");
    let req = request(&cfg, "local:atomic-policy", "fixture");
    let skip = rule(json!({"daily":"09:00","timezone":"UTC","missed":"skip"}));
    let one = store.create_schedule("skip", &req, &skip, start).unwrap();
    let two = store
        .create_schedule(
            "coalesce",
            &req,
            &Timing::parse(None, Some("09:00"), "UTC").unwrap(),
            start,
        )
        .unwrap();
    store.conn.execute_batch("CREATE TRIGGER fixture_abort BEFORE INSERT ON schedule_occurrences BEGIN SELECT RAISE(ABORT,'cut'); END;").unwrap();
    assert!(
        store
            .due_schedule_events(at("2026-11-18T10:00:00Z"))
            .is_err()
    );
    assert_eq!(
        cursor(&store, one).2,
        Some(at("2026-11-16T09:00:00Z").timestamp())
    );
    assert_eq!(cursor(&store, two).2, cursor(&store, one).2);
    assert_eq!(count(&store, "task_runs"), 0);
    store
        .conn
        .execute_batch("DROP TRIGGER fixture_abort")
        .unwrap();
    // A stale cursor does not discard today's on-time selected occurrence.
    assert_eq!(
        store
            .due_schedule_events(at("2026-11-19T09:00:00.500Z"))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(count(&store, "schedule_occurrences"), 2);
    assert_eq!(
        cursor(&store, one).2,
        Some(at("2026-11-20T09:00:00Z").timestamp())
    );
}

#[test]
fn calendar_wake_deadline_excludes_blocked_and_reaches_thirty_three() {
    let (_root, cfg, store) = fixture();
    let now = at("2026-11-16T09:00:00Z");
    let timing = Timing::parse(None, Some("09:00"), "UTC").unwrap();
    for n in 0..33 {
        store
            .create_schedule(
                &format!("row-{n}"),
                &request(&cfg, "local:batch", "fixture"),
                &timing,
                now,
            )
            .unwrap();
    }
    assert_eq!(
        store.next_schedule_deadline().unwrap(),
        Some(now.timestamp())
    );
    assert_eq!(store.due_schedule_events(now).unwrap().len(), 32);
    assert_eq!(
        store.next_schedule_deadline().unwrap(),
        Some(now.timestamp())
    );
    assert_eq!(store.due_schedule_events(now).unwrap().len(), 1);
    assert_eq!(
        store.next_schedule_deadline().unwrap(),
        None,
        "unresolved rows cannot create an expired-deadline hot loop"
    );
    for id in 1..=33 {
        store.update_task_run_started(id, "started").unwrap();
        store
            .finish_task_run(id, TaskRunStatus::Failed, "Unknown", None, None)
            .unwrap();
    }
    assert_eq!(store.next_schedule_deadline().unwrap(), None);
    let later = at("2026-11-17T09:00:00Z");
    store
        .create_schedule(
            "fresh",
            &request(&cfg, "local:fresh", "fixture"),
            &timing,
            later,
        )
        .unwrap();
    assert_eq!(
        store.next_schedule_deadline().unwrap(),
        Some(later.timestamp())
    );
    assert_eq!(store.due_schedule_events(later).unwrap().len(), 1);
}

#[test]
fn calendar_wake_edit_revision_concurrent_writers_restart_and_authority() {
    let (_root, cfg, store) = fixture();
    let now = at("2026-11-16T09:00:00Z");
    let t = rule(json!({"weekly":"mon","time":"09:00","timezone":"UTC"}));
    let id = store
        .create_schedule("edit", &request(&cfg, "local:owner", "old"), &t, now)
        .unwrap();
    let run = store.due_schedule_events(now).unwrap()[0].0;
    let changed = rule(
        json!({"weekly":"tue,thu","time":"10:00","timezone":"UTC","fold":"second","gap":"next-valid","missed":"skip"}),
    );
    assert!(
        store
            .edit_schedule("local:other", id, "forged", &changed, now)
            .is_err()
    );
    store
        .edit_schedule("local:owner", id, "future", &changed, now)
        .unwrap();
    assert_eq!(cursor(&store, id).0, 2);
    assert_eq!(store.get_task_run(run).unwrap().unwrap().prompt, "old");
    store
        .finish_task_run(run, TaskRunStatus::Completed, "known", None, None)
        .unwrap();
    let due = at("2026-11-17T10:00:00Z");
    let connections = (0..4)
        .map(|_| Store::open(&cfg).unwrap())
        .collect::<Vec<_>>();
    let gate = Arc::new(std::sync::Barrier::new(4));
    let threads = connections
        .into_iter()
        .map(|s| {
            let g = gate.clone();
            std::thread::spawn(move || {
                g.wait();
                s.due_schedule_events(due).unwrap().len()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .sum::<usize>(),
        1
    );
    let store = Store::open(&cfg).unwrap();
    assert!(store.due_schedule_events(due).unwrap().is_empty());
    assert_eq!(count(&store, "schedule_occurrences"), 2);
}

#[test]
fn calendar_wake_earlier_edit_pause_cancel_and_shutdown() {
    let (_root, mut cfg, store) = fixture();
    cfg.codex.bin = "calendar-fixture-never-execute".into();
    let req = request(&cfg, "local:mutations", "fixture");
    let future = Timing::parse(Some("2099-01-01T00:00:00Z"), None, "UTC").unwrap();
    for key in ["paused", "cancelled", "edited"] {
        store
            .create_schedule(key, &req, &future, Utc::now())
            .unwrap();
    }
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.start_calendar().unwrap();
    scheduler.start_calendar().unwrap();
    let soon = Utc::now() + chrono::Duration::seconds(2);
    let soon = Timing::parse(
        Some(&soon.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        None,
        "UTC",
    )
    .unwrap();
    for id in 1..=3 {
        store
            .edit_schedule("local:mutations", id, "future-only", &soon, Utc::now())
            .unwrap();
    }
    store
        .set_schedule_state("local:mutations", 1, "paused")
        .unwrap();
    store
        .set_schedule_state("local:mutations", 2, "cancelled")
        .unwrap();
    store
        .edit_schedule(
            "local:mutations",
            3,
            "earlier",
            &Timing::parse(Some("2026-01-01T00:00:00Z"), None, "UTC").unwrap(),
            Utc::now(),
        )
        .unwrap();
    eventually(|| count(&store, "schedule_occurrences") == 1);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(count(&store, "schedule_occurrences"), 1);
    assert_eq!(store.get_task_run(1).unwrap().unwrap().prompt, "earlier");
    assert_eq!(store.next_schedule_deadline().unwrap(), None);
    let start = Instant::now();
    scheduler.shutdown().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(scheduler.start_calendar().is_err());
    assert!(
        scheduler.inner.workers.lock().unwrap().is_empty(),
        "owned workers joined"
    );
}

#[test]
fn calendar_wake_transaction_failure_backoff_and_restart() {
    let (_root, mut cfg, store) = fixture();
    cfg.codex.bin = "calendar-fixture-never-execute".into();
    store
        .create_schedule(
            "failure",
            &request(&cfg, "local:failure", "fixture"),
            &Timing::parse(Some("2026-01-01T00:00:00Z"), None, "UTC").unwrap(),
            Utc::now(),
        )
        .unwrap();
    store.conn.execute_batch("CREATE TRIGGER fixture_abort BEFORE INSERT ON runtime_inbox BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.start_calendar().unwrap();
    std::thread::sleep(Duration::from_millis(1200));
    assert_eq!(count(&store, "schedule_occurrences"), 0);
    assert_eq!(count(&store, "task_runs"), 0);
    store
        .conn
        .execute_batch("DROP TRIGGER fixture_abort")
        .unwrap();
    eventually(|| count(&store, "schedule_occurrences") == 1);
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    restarted.start_calendar().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(count(&store, "schedule_occurrences"), 1);
    assert_eq!(count(&store, "task_runs"), 1);
    restarted.shutdown().unwrap();
}
