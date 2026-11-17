use super::*;

#[test]
fn calendar_wake_normal_binary_deadline_during_fetch_and_restart() {
    let (_root, mut cfg, store) = fixture();
    let entered = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let seen = entered.clone();
    let release = released.clone();
    let server = FakeTelegramServer::start_with_response(move |raw| {
        if raw.contains("/getMe ") {
            return (200, json!({"ok":true,"result":{"is_bot":true,"username":"CalendarFixtureBot","has_topics_enabled":true}}).to_string());
        }
        if raw.contains("/getUpdates?") {
            if !seen.swap(true, Ordering::SeqCst) {
                let until = Instant::now() + Duration::from_secs(10);
                while !release.load(Ordering::SeqCst) && Instant::now() < until {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            return (
                500,
                r#"{"ok":false,"description":"fixture fetch failure"}"#.into(),
            );
        }
        (200, r#"{"ok":true,"result":{"message_id":77}}"#.into())
    });
    cfg.codex.bin = write_fake_provider_script(
        &cfg.instance_dir,
        "calendar-provider",
        "#!/bin/sh\nbase=$(dirname \"$0\"); echo call >> \"$base/calendar-calls\"; out=''; while [ \"$#\" -gt 0 ]; do if [ \"$1\" = '--output-last-message' ]; then shift; out=\"$1\"; fi; shift; done; cat > \"$base/calendar-input\"; printf 'TELEGRAM_REPLY: calendar fixture result' > \"$out\"; printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"11111111-1111-4111-8111-111111111111\"}'",
        "$a=$args; $i=[Array]::IndexOf($a,'--output-last-message'); [IO.File]::AppendAllText((Join-Path $PSScriptRoot 'calendar-calls'),\"call`n\"); [IO.File]::WriteAllText((Join-Path $PSScriptRoot 'calendar-input'),[Console]::In.ReadToEnd()); [IO.File]::WriteAllText($a[$i+1],'TELEGRAM_REPLY: calendar fixture result'); Write-Output '{\"type\":\"thread.started\",\"thread_id\":\"11111111-1111-4111-8111-111111111111\"}'",
    );
    struct Evidence(PathBuf);
    impl Drop for Evidence {
        fn drop(&mut self) {
            if let Some(base) = std::env::var_os("COCONUTCLAW_CALENDAR_ARTIFACTS") {
                let base = PathBuf::from(base);
                fs::create_dir_all(&base).unwrap();
                for entry in fs::read_dir(&self.0).unwrap() {
                    let e = entry.unwrap();
                    if e.file_type().unwrap().is_file() {
                        fs::copy(e.path(), base.join(e.file_name())).unwrap();
                    }
                }
            }
        }
    }
    let _evidence = Evidence(cfg.instance_dir.clone());
    let binary = crate::test_runtime::binary();
    let hash = blake3::hash(&fs::read(&binary).unwrap());
    let command = || {
        let mut c = std::process::Command::new(&binary);
        c.arg("--data-dir")
            .arg(&cfg.instance_dir)
            .env_clear()
            .env("HOME", &cfg.instance_dir)
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("FACTORY_AIRGAP_ENABLED", "1")
            .env("TELEGRAM_BOT_TOKEN", "123:token")
            .env("TELEGRAM_CHAT_ID", "321")
            .env(
                "TELEGRAM_API_BASE",
                server.base_url.trim_end_matches("/bot123:token"),
            )
            .env("POLL_INTERVAL_SECONDS", "60")
            .env("TELEGRAM_API_RETRY_ATTEMPTS", "0")
            .env("CODEX_BIN", &cfg.codex.bin);
        c
    };
    let admin = |args: &[&str]| {
        let output = command()
            .args(["schedule", "--session", "telegram:321#7"])
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    };
    struct Owned(std::process::Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
            }
            let _ = self.0.wait();
        }
    }
    let start = |n| {
        Owned(
            command()
                .arg("run")
                .stdout(std::process::Stdio::null())
                .stderr(
                    fs::File::create(cfg.instance_dir.join(format!("calendar-normal-{n}.log")))
                        .unwrap(),
                )
                .spawn()
                .unwrap(),
        )
    };
    let wait = |sql: &str| {
        let until = Instant::now() + Duration::from_secs(20);
        while !store
            .conn
            .query_row(sql, [], |r| r.get::<_, bool>(0))
            .unwrap()
        {
            assert!(
                Instant::now() < until,
                "timeout {sql}; {}",
                cfg.instance_dir.display()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    let first = start(0);
    eventually(|| entered.load(Ordering::SeqCst));
    assert_eq!(
        admin(&[
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
            "UTC",
            "--fold",
            "second",
            "--gap",
            "next-valid",
            "--missed",
            "skip"
        ])["schedule"],
        1
    );
    for key in ["paused", "cancelled"] {
        admin(&[
            "create",
            "--key",
            key,
            "--prompt",
            "never",
            "--at",
            "2099-01-01T00:00:00Z",
            "--timezone",
            "UTC",
        ]);
    }
    admin(&["pause", "2"]);
    admin(&["cancel", "3"]);
    let due = Utc::now() + chrono::Duration::seconds(2);
    let due = at(&due.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    admin(&[
        "edit",
        "1",
        "--prompt",
        "deadline fixture",
        "--at",
        &due.to_rfc3339(),
        "--timezone",
        "UTC",
    ]);
    assert_eq!(
        count(&store, "schedule_occurrences"),
        0,
        "deadline cannot fire early"
    );
    wait("SELECT count(*)=1 FROM schedule_occurrences");
    let latency = (Utc::now() - due).num_milliseconds();
    assert!(
        !released.load(Ordering::SeqCst),
        "must admit while getUpdates is blocked"
    );
    assert!((0..1000).contains(&latency), "deadline latency {latency}ms");
    released.store(true, Ordering::SeqCst);
    wait(
        "SELECT EXISTS(SELECT 1 FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id WHERE r.status='completed' AND p.delivered=1)",
    );
    drop(first);
    let second = start(1);
    // Startup announces bot commands before recovery; wait for recovery/outbox
    // and another failed fetch, not just process creation.
    let until = Instant::now() + Duration::from_secs(5);
    while server
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.contains("/getUpdates"))
        .count()
        < 2
    {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(200));
    drop(second);
    assert_eq!(count(&store, "schedule_occurrences"), 1);
    assert_eq!(count(&store, "task_runs"), 1);
    assert_eq!(
        fs::read_to_string(cfg.instance_dir.join("calendar-calls"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    let sends = server
        .full_requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.contains("/sendRichMessage "))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(sends.len(), 1);
    assert!(sends[0].contains("message_thread_id=7"));
    assert_eq!(blake3::hash(&fs::read(&binary).unwrap()), hash);
    fs::write(cfg.instance_dir.join("calendar-acceptance.json"),json!({
        "binary_blake3":hash.to_hex().to_string(),"starts":2,"provider_calls":1,"target_sends":1,
        "occurrences":1,"deadline_latency_ms":latency,"poll_interval_seconds":60,
        "admitted_during_blocked_fetch":true,"paused_cancelled_inert":true,
        "fixture":"disposable portable provider and loopback HTTP only"
    }).to_string()).unwrap();
}
