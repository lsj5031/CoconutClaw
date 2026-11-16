use super::*;
use clap::Parser;

#[path = "calendar_policy_tests.rs"]
mod policy;

#[path = "calendar_native_tests.rs"]
mod native;

#[test]
fn calendar_wake_actual_cli_weekly_and_explicit_policies() {
    let (_root, cfg, store) = fixture();
    let cli = crate::Cli::try_parse_from([
        "coconutclaw",
        "schedule",
        "--session",
        "local:weekly",
        "create",
        "--key",
        "weekly",
        "--prompt",
        "fixture",
        "--weekly",
        "mon,wed",
        "--time",
        "09:00",
        "--timezone",
        "Pacific/Auckland",
        "--fold",
        "second",
        "--gap",
        "next-valid",
        "--missed",
        "skip",
    ])
    .expect("weekly calendar is an administrative CLI operation");
    let crate::Commands::Schedule(args) = cli.command else {
        panic!("schedule");
    };
    crate::commands::schedule::run(&cfg, &store, &args, at("2026-12-31T00:00:00Z")).unwrap();
    let list = store.list_schedules("local:weekly").unwrap();
    assert_eq!(
        list[0]["next_due_utc"],
        at("2027-01-03T20:00:00Z").timestamp()
    );
    let timing: serde_json::Value =
        serde_json::from_str(list[0]["timing"].as_str().unwrap()).unwrap();
    assert_eq!(timing["fold"], "second");
    assert_eq!(timing["gap"], "next-valid");
    assert_eq!(timing["missed"], "skip");
}

#[test]
fn calendar_wake_earlier_create_during_blocked_poll_fetch() {
    let (_root, mut cfg, store) = fixture();
    // A deliberately nonexistent executable, never a real provider.
    cfg.codex.bin = "calendar-fixture-never-execute".into();
    let entered = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let seen = entered.clone();
    let release = released.clone();
    let server = FakeTelegramServer::start_with_response(move |_| {
        seen.store(true, Ordering::SeqCst);
        let until = Instant::now() + Duration::from_secs(4);
        while !release.load(Ordering::SeqCst) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(10));
        }
        (200, r#"{"ok":true,"result":[]}"#.into())
    });
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    cfg.poll_interval_seconds = 60;
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let polling = scheduler.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let poll_stop = stop.clone();
    let poll_cfg = cfg.clone();
    let worker = std::thread::spawn(move || {
        crate::loops::run_poll_loop(
            &poll_cfg,
            &mut Store::open(&poll_cfg).unwrap(),
            &polling,
            &reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            &poll_stop,
        )
    });
    eventually(|| entered.load(Ordering::SeqCst));
    store
        .create_schedule(
            "earlier",
            &request(&cfg, "local:wake", "fixture"),
            &Timing::parse(Some("2026-01-01T00:00:00Z"), None, "UTC").unwrap(),
            Utc::now(),
        )
        .unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    while count(&store, "schedule_occurrences") == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    let admitted_while_fetch_waits =
        count(&store, "schedule_occurrences") == 1 && !released.load(Ordering::SeqCst);
    // Always join owned workers, including on the red path.
    stop.store(true, Ordering::SeqCst);
    released.store(true, Ordering::SeqCst);
    worker.join().unwrap().unwrap();
    scheduler.shutdown().unwrap();
    assert!(
        admitted_while_fetch_waits,
        "earlier create must wake without waiting for Telegram fetch"
    );
}

#[test]
fn calendar_wake_earlier_create_during_fetch_retry_sleep() {
    let (_root, mut cfg, store) = fixture();
    cfg.codex.bin = "calendar-fixture-never-execute".into();
    let server = FakeTelegramServer::start_with_response(|_| (500, "{}".into()));
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    cfg.telegram_api_retry_attempts = 0;
    cfg.poll_interval_seconds = 60;
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let polling = scheduler.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let poll_stop = stop.clone();
    let poll_cfg = cfg.clone();
    let worker = std::thread::spawn(move || {
        crate::loops::run_poll_loop(
            &poll_cfg,
            &mut Store::open(&poll_cfg).unwrap(),
            &polling,
            &reqwest::blocking::Client::new(),
            &poll_stop,
        )
    });
    eventually(|| !server.requests.lock().unwrap().is_empty());
    // Allow the immediate 500 response to enter the existing 60s retry sleep.
    std::thread::sleep(Duration::from_millis(100));
    store
        .create_schedule(
            "retry",
            &request(&cfg, "local:retry-wake", "fixture"),
            &Timing::parse(Some("2026-01-01T00:00:00Z"), None, "UTC").unwrap(),
            Utc::now(),
        )
        .unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    while count(&store, "schedule_occurrences") == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    let admitted = count(&store, "schedule_occurrences") == 1;
    stop.store(true, Ordering::SeqCst);
    worker.join().unwrap().unwrap();
    scheduler.shutdown().unwrap();
    assert!(
        admitted,
        "retry sleep cannot hold the calendar deadline owner"
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[test]
fn calendar_wake_completion_rearms_blocked_deadline_without_idle_writes() {
    let (_root, mut cfg, store) = fixture();
    cfg.codex.bin = "calendar-fixture-never-execute".into();
    let yesterday = (Utc::now() - chrono::Duration::days(1))
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let t = Timing::parse(None, Some("00:00"), "UTC").unwrap();
    store
        .create_schedule(
            "blocked",
            &request(&cfg, "local:unblock", "fixture"),
            &t,
            yesterday,
        )
        .unwrap();
    let run = store.due_schedule_events(yesterday).unwrap()[0].0;
    assert_eq!(store.next_schedule_deadline().unwrap(), None);
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.start_calendar().unwrap();
    std::thread::sleep(Duration::from_millis(250));
    let version = || {
        store
            .conn
            .query_row("PRAGMA data_version", [], |r| r.get::<_, i64>(0))
            .unwrap()
    };
    let before = version();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        version(),
        before,
        "an unresolved expired schedule must stay idle"
    );
    store
        .finish_task_run(run, TaskRunStatus::Completed, "known", None, None)
        .unwrap();
    eventually(|| count(&store, "schedule_occurrences") == 2);
    scheduler.shutdown().unwrap();
    assert_eq!(count(&store, "task_runs"), 2);
}
