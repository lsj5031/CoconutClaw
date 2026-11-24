use super::*;

const FREEZE: &str = "INSERT INTO legacy_admission_freeze VALUES(1)";

#[test]
fn legacy_cutover_blocks_dispatch_and_delayed_worker_start() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = RuntimeConfig::test_config_at(root.path().into());
    cfg.codex.bin = "/nonexistent/never-invoke-a-real-provider".into();
    let store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.inner.recovering.store(true, Ordering::SeqCst);
    let request = make_stdout_request(SessionKey::local("cutover"), "queued before freeze");
    let run = scheduler.enqueue(request.clone()).unwrap();
    Store::connect(&cfg)
        .unwrap()
        .conn
        .execute(FREEZE, [])
        .unwrap();
    scheduler.inner.recovering.store(false, Ordering::SeqCst);
    scheduler.dispatch_ready();
    {
        let lanes = scheduler.inner.lanes.lock().unwrap();
        let lane = lanes.get(&request.session.id()).unwrap();
        assert!(lane.active.is_none());
        assert_eq!(lane.queue.front().unwrap().id, run);
    }
    scheduler
        .run_task(run, request.clone(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    assert!(scheduler.enqueue(request).is_err());
    scheduler.shutdown().unwrap();
    Store::seal_legacy_cutover(&cfg).unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    let record = store.get_task_run(run).unwrap().unwrap();
    assert_eq!(record.status, "queued");
    assert!(record.started_at.is_none());
    assert!(restarted.inner.lanes.lock().unwrap().is_empty());
    restarted.shutdown().unwrap();
}

#[test]
fn legacy_cutover_seal_waits_for_sender_and_preserves_ambiguous_send() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = RuntimeConfig::test_config_at(root.path().into());
    cfg.codex.bin = "/nonexistent/never-invoke-a-real-provider".into();
    let (on_wire, received) = std::sync::mpsc::channel();
    let (release, respond) = std::sync::mpsc::channel();
    let server = FakeTelegramServer::start_with_response(move |_| {
        if on_wire.send(()).is_ok() {
            let _ = respond.recv_timeout(Duration::from_secs(5));
        }
        (0, String::new())
    });
    cfg.telegram_api_base = Some(server.base_url.clone());
    cfg.telegram_topics_enabled = Some(true);
    let store = Store::open(&cfg).unwrap();
    let owner = cfg.acquire_instance_lock().unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let runs = ["first", "second"].map(|text| {
        let mut request = make_request(SessionKey::telegram("321#37"), "telegram", text);
        request.delivery = DeliveryTarget::Telegram {
            chat_id: "321".into(),
            thread_id: Some("37".into()),
        };
        let tx = store.conn.unchecked_transaction().unwrap();
        let (run, _) = store
            .admit_inbox_request(
                crate::store::inbox::EventKind::Request,
                None,
                &request,
                "now",
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE task_payloads SET output=?2 WHERE task_run_id=?1",
                rusqlite::params![run, format!("TELEGRAM_REPLY: {text}")],
            )
            .unwrap();
        tx.commit().unwrap();
        run
    });
    let sender = scheduler.clone();
    let delivery = std::thread::spawn(move || sender.retry_deliveries());
    received.recv_timeout(Duration::from_secs(5)).unwrap();
    let phases = |store: &Store, id: i64| {
        store
            .conn
            .query_row(
                "SELECT json_extract(delivery_state,'$.text_plan.phase'),
                    coalesce(json_extract(delivery_state,'$.text_plan.attempts'),0), delivered
             FROM task_payloads WHERE task_run_id=?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, bool>(2)?,
                    ))
                },
            )
            .unwrap()
    };
    assert_eq!(phases(&store, runs[0]), (Some("sending".into()), 1, false));
    Store::connect(&cfg)
        .unwrap()
        .conn
        .execute(FREEZE, [])
        .unwrap();
    let failed_seal = Store::seal_legacy_cutover(&cfg).unwrap_err();
    assert!(failed_seal.to_string().contains("failed to lock instance"));
    release.send(()).unwrap();
    delivery.join().unwrap().unwrap();
    scheduler.shutdown().unwrap();
    drop(owner);
    Store::seal_legacy_cutover(&cfg).unwrap();
    Store::seal_legacy_cutover(&cfg).unwrap();
    drop(store);

    let mut reopened = Store::connect(&cfg).unwrap();
    assert_eq!(
        reopened
            .conn
            .query_row("SELECT count(*) FROM legacy_cutover_seal", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    restarted.retry_deliveries().unwrap();
    assert_eq!(
        phases(&reopened, runs[0]),
        (Some("unknown".into()), 1, false)
    );
    assert_eq!(phases(&reopened, runs[1]).1, 0);
    let args = crate::TurnArgs {
        inject_text: None,
        inject_file: None,
        chat_id: None,
    };
    assert_eq!(
        crate::commands::run_run(&cfg, &mut reopened, &args)
            .unwrap_err()
            .to_string(),
        "legacy admission is frozen"
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    restarted.shutdown().unwrap();
}
