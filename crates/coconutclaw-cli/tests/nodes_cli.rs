//! Actual executable admission/control entry points. No provider or Telegram
//! invocation; all configuration and identities are fresh and host-local.
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn command(data: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_coconutclaw"));
    cmd.env_clear();
    #[cfg(windows)]
    for name in [
        "SystemRoot",
        "SYSTEMDRIVE",
        "USERPROFILE",
        "TEMP",
        "TMP",
        "LOCALAPPDATA",
    ] {
        if let Some(value) = std::env::var_os(name) {
            cmd.env(name, value);
        }
    }
    cmd.env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", data)
        .arg("--data-dir")
        .arg(data)
        .args(args);
    cmd
}
fn cli(data: &Path, args: &[&str]) -> Output {
    command(data, args).output().unwrap()
}

#[test]
fn reviewed_pair_checks_exact_bytes_and_serializes_competing_coordinators() {
    let root = tempfile::tempdir().unwrap();
    #[cfg(windows)]
    private(root.path());
    let enroll = |name: &str, role: &str| {
        let data = root.path().join(name);
        fs::create_dir(&data).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let mut args = vec![
            "nodes",
            "enroll",
            "--role",
            role,
            "--bind",
            "127.0.0.1:49731",
        ];
        if role == "runner" {
            args.extend(["--workspace", root.path().to_str().unwrap()]);
        }
        let output = cli(&data, &args);
        assert!(output.status.success(), "{:?}", output);
        let card = root.path().join(format!("{name}.json"));
        fs::write(&card, output.stdout).unwrap();
        (data, card)
    };
    let (runner, own_card) = enroll("runner", "runner");
    let (_, first) = enroll("first", "coordinator");
    let (_, second) = enroll("second", "coordinator");
    let rejected = root.path().join("rejected.json");
    let read_card = |path: &Path| -> serde_json::Value {
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    };
    let card = read_card(&first);
    for (field, value) in [
        ("role", serde_json::json!("runner")),
        ("revision", serde_json::json!("wrong")),
        ("id", read_card(&own_card)["id"].clone()),
        ("address", serde_json::json!("127.0.0.1:0")),
    ] {
        let mut invalid = card.clone();
        invalid[field] = value;
        fs::write(&rejected, serde_json::to_vec(&invalid).unwrap()).unwrap();
        assert!(
            !cli(
                &runner,
                &["nodes", "pair", rejected.to_str().unwrap(), "--preview"]
            )
            .status
            .success()
        );
    }
    let preview = |path: &Path| {
        let output = cli(
            &runner,
            &["nodes", "pair", path.to_str().unwrap(), "--preview"],
        );
        assert!(output.status.success(), "{:?}", output);
        let output = String::from_utf8(output.stdout).unwrap();
        let (shown, confirmation) = output.trim_end().split_once('\n').unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(shown).unwrap(),
            read_card(path)
        );
        confirmation
            .strip_prefix("Confirmation digest ")
            .unwrap()
            .to_owned()
    };
    let first_digest = preview(&first);
    let second_digest = preview(&second);
    assert_ne!(first_digest, second_digest);
    let conn = rusqlite::Connection::open(runner.join("state.db")).unwrap();
    let admissions = || {
        conn.prepare("SELECT id FROM paired_nodes ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert!(admissions().is_empty());

    let confirm = |path: &Path, digest: &str| {
        let mut cmd = command(&runner, &["nodes", "pair"]);
        cmd.arg(path).args(["--confirm", digest]);
        cmd
    };
    let bytes = fs::read(&first).unwrap();
    fs::write(&first, [bytes.as_slice(), b" "].concat()).unwrap();
    assert!(
        !confirm(&first, &first_digest)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(admissions().is_empty());
    fs::write(&first, bytes).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&runner, fs::Permissions::from_mode(0o755)).unwrap();
        let output = confirm(&first, &first_digest).output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("owner-only node directory"));
        assert!(admissions().is_empty());
        fs::set_permissions(&runner, fs::Permissions::from_mode(0o700)).unwrap();
    }

    let lock = conn.unchecked_transaction().unwrap();
    lock.execute("UPDATE paired_nodes SET last_contact=NULL", [])
        .unwrap();
    let mut a = confirm(&first, &first_digest)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut b = confirm(&second, &second_digest)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(250));
    assert!(a.try_wait().unwrap().is_none() && b.try_wait().unwrap().is_none());
    lock.commit().unwrap();
    let first_won = a.wait().unwrap().success();
    assert_ne!(first_won, b.wait().unwrap().success());
    let (winner, digest) = if first_won {
        (&first, &first_digest)
    } else {
        (&second, &second_digest)
    };
    let id = read_card(winner)["id"].as_str().unwrap().to_owned();
    assert_eq!(admissions().as_slice(), std::slice::from_ref(&id));
    assert!(confirm(winner, digest).output().unwrap().status.success());
    assert_eq!(admissions(), [id]);
}
#[test]
fn public_pair_flow_preserves_private_keys_and_runner_refuses_polling() {
    let root = tempfile::tempdir().unwrap();
    #[cfg(windows)]
    private(root.path());
    let core = root.path().join("core");
    let runner = root.path().join("runner");
    let workspace = root.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let core_output = cli(
        &core,
        &[
            "nodes",
            "enroll",
            "--role",
            "coordinator",
            "--bind",
            "127.0.0.1:49731",
        ],
    );
    assert!(
        core_output.status.success(),
        "{}",
        String::from_utf8_lossy(&core_output.stderr)
    );
    let core_card = root.path().join("core-public.json");
    fs::write(&core_card, &core_output.stdout).unwrap();
    let runner_output = cli(
        &runner,
        &[
            "nodes",
            "enroll",
            "--role",
            "runner",
            "--bind",
            "127.0.0.1:49732",
            "--workspace",
            workspace.to_str().unwrap(),
        ],
    );
    assert!(
        runner_output.status.success(),
        "{}",
        String::from_utf8_lossy(&runner_output.stderr)
    );
    let runner_card = root.path().join("runner-public.json");
    fs::write(&runner_card, &runner_output.stdout).unwrap();
    for card in [&core_output.stdout, &runner_output.stdout] {
        let value: serde_json::Value = serde_json::from_slice(card).unwrap();
        assert_eq!(
            value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["version", "role", "id", "address", "revision"])
        );
        assert!(value.get("workspace").is_none());
        assert!(value.get("key").is_none());
    }
    assert!(
        cli(&core, &["nodes", "pair", runner_card.to_str().unwrap()])
            .status
            .success()
    );
    assert!(
        cli(&runner, &["nodes", "pair", core_card.to_str().unwrap()])
            .status
            .success()
    );
    assert!(
        cli(&core, &["nodes", "pair", runner_card.to_str().unwrap()])
            .status
            .success()
    );
    let status = cli(&core, &["nodes", "status"]);
    assert!(status.status.success());
    assert!(
        String::from_utf8(status.stdout)
            .unwrap()
            .contains("Last reached: not recorded")
    );
    let before = cli(&runner, &["nodes", "card"]);
    assert!(before.status.success());
    let poll = cli(&runner, &["run"]);
    assert!(!poll.status.success());
    assert!(
        String::from_utf8(poll.stderr)
            .unwrap()
            .contains("runner identities cannot poll Telegram")
    );
    let after = cli(&runner, &["nodes", "card"]);
    assert_eq!(before.stdout, after.stdout);
}

#[cfg(windows)]
fn private(path: &Path) {
    let account = format!(
        "{}\\{}",
        std::env::var("USERDOMAIN").unwrap(),
        std::env::var("USERNAME").unwrap()
    );
    assert!(
        Command::new("icacls.exe")
            .arg(path)
            .args([
                "/inheritance:r",
                "/grant:r",
                &format!("{account}:(OI)(CI)F"),
                "*S-1-5-18:(OI)(CI)F",
                "*S-1-5-32-544:(OI)(CI)F"
            ])
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[cfg(windows)]
#[test]
fn windows_node_preflight_precedes_layout_and_sqlite_creation() {
    let root = tempfile::tempdir().unwrap();
    private(root.path());
    let data = root.path().join("data");
    let db_dir = root.path().join("custom");
    fs::create_dir(&data).unwrap();
    fs::create_dir(&db_dir).unwrap();
    let config = data.join("config.toml");
    let db = db_dir.join("allowlist.sqlite");
    fs::write(&config, "SQLITE_DB_PATH = '../custom/allowlist.sqlite'\n").unwrap();
    for path in [&db_dir, &config] {
        if path == &config {
            fs::write(&config, "invalid TOML").unwrap();
        }
        assert!(
            Command::new("icacls.exe")
                .arg(path)
                .args(["/grant", "*S-1-1-0:R"])
                .status()
                .unwrap()
                .success()
        );
        let output = cli(&data, &["nodes", "status"]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("unsafe node ACL"));
        assert_eq!(fs::read_dir(&data).unwrap().count(), 1);
        assert_eq!(fs::read_dir(&db_dir).unwrap().count(), 0);
        assert!(
            Command::new("icacls.exe")
                .arg(path)
                .args(["/remove:g", "*S-1-1-0"])
                .status()
                .unwrap()
                .success()
        );
    }
    fs::write(&config, "SQLITE_DB_PATH = '../custom/allowlist.sqlite'\n").unwrap();
    fs::remove_dir(&db_dir).unwrap();
    assert!(!cli(&data, &["nodes", "status"]).status.success());
    assert!(!db_dir.exists());
    assert_eq!(fs::read_dir(&data).unwrap().count(), 1);
    fs::create_dir(&db_dir).unwrap();
    assert!(cli(&data, &["nodes", "status"]).status.success());
    assert!(db.is_file());
}

#[cfg(any(unix, windows))]
#[test]
fn paired_runner_start_dated_probe_disconnect_and_restart_use_native_transport() {
    use std::{
        io::{BufRead, BufReader},
        process::{Child, Stdio},
        sync::mpsc,
        time::Duration,
    };
    struct Owned(Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            if matches!(self.0.try_wait(), Ok(None)) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    let root = tempfile::tempdir().unwrap();
    #[cfg(windows)]
    private(root.path());
    let core = root.path().join("core");
    let node = root.path().join("node");
    let workspace = root.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let core_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let node_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let core_bind = core_socket.local_addr().unwrap().to_string();
    let node_bind = node_socket.local_addr().unwrap().to_string();
    drop((core_socket, node_socket));
    let a = cli(
        &core,
        &[
            "nodes",
            "enroll",
            "--role",
            "coordinator",
            "--bind",
            &core_bind,
        ],
    );
    let b = cli(
        &node,
        &[
            "nodes",
            "enroll",
            "--role",
            "runner",
            "--bind",
            &node_bind,
            "--workspace",
            workspace.to_str().unwrap(),
        ],
    );
    assert!(a.status.success() && b.status.success());
    let core_card = root.path().join("core.json");
    let node_card = root.path().join("node.json");
    fs::write(&core_card, a.stdout).unwrap();
    fs::write(&node_card, b.stdout).unwrap();
    assert!(
        cli(&core, &["nodes", "pair", node_card.to_str().unwrap()])
            .status
            .success()
    );
    let snapshot = || {
        let db = core.join("state.db");
        let output = cli(
            &core,
            &[
                "desktop",
                "--database",
                db.to_str().unwrap(),
                "--session",
                "local:default",
            ],
        );
        assert!(output.status.success(), "{output:?}");
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["jobs"], serde_json::json!([]));
        value
    };
    let unchecked = snapshot();
    assert_eq!(unchecked["nodes"][0]["observation"], "unchecked");
    assert!(unchecked["nodes"][0]["check"].is_null());
    assert!(unchecked["nodes"][0]["last_reached"].is_null());

    let start = || {
        let mut child = Owned(
            command(&node, &["nodes", "start"])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut line = String::new();
            BufReader::new(stdout).read_line(&mut line).unwrap();
            let _ = send.send(line);
        });
        let card: serde_json::Value =
            serde_json::from_slice(&fs::read(&node_card).unwrap()).unwrap();
        assert_eq!(
            receive
                .recv_timeout(Duration::from_secs(20))
                .unwrap()
                .trim_end(),
            format!("Runner ready {}", card["id"].as_str().unwrap())
        );
        reader.join().unwrap();
        assert!(child.0.try_wait().unwrap().is_none());
        child
    };
    let probe = || {
        let probe = cli(&core, &["nodes", "status", "--probe"]);
        assert!(probe.status.success(), "{probe:?}");
        assert!(String::from_utf8_lossy(&probe.stdout).contains("Checked: "));
        snapshot()["nodes"][0].clone()
    };
    let stop = |mut child: Owned| {
        assert!(child.0.try_wait().unwrap().is_none());
        #[cfg(unix)]
        {
            // The retained, unreaped handle identifies this exact fixture child.
            assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGINT) }, 0);
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success(), "{status:?}");
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "runner did not shut down"
                );
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        #[cfg(windows)]
        {
            child.0.kill().unwrap();
            child.0.wait().unwrap();
        }
    };

    let child = start();
    let duplicate = cli(&node, &["nodes", "start"]);
    assert!(!duplicate.status.success());
    assert!(
        String::from_utf8_lossy(&duplicate.stderr).contains("failed to lock instance directory")
    );
    let refused = probe();
    assert_eq!(refused["check"]["reachable"], false);
    assert!(refused["last_reached"].is_null());
    assert!(
        cli(&node, &["nodes", "pair", core_card.to_str().unwrap()])
            .status
            .success()
    );
    let reached = probe();
    assert_eq!(reached["check"]["reachable"], true);
    assert_eq!(reached["check"]["checked_at"], reached["last_reached"]);
    stop(child);
    let disconnected = probe();
    assert_eq!(disconnected["check"]["reachable"], false);
    assert_eq!(disconnected["last_reached"], reached["last_reached"]);
    assert!(disconnected["check"]["checked_at"].as_str() > reached["check"]["checked_at"].as_str());

    let child = start();
    let reconnected = probe();
    assert_eq!(reconnected["check"]["reachable"], true);
    assert!(
        reconnected["check"]["checked_at"].as_str() > disconnected["check"]["checked_at"].as_str()
    );
    stop(child);
    assert!(unchecked["nodes"][0]["check"].is_null());
    assert_eq!(reached["check"]["reachable"], true);
    assert_eq!(disconnected["check"]["reachable"], false);
}

#[cfg(windows)]
#[test]
fn unsafe_key_and_coordinator_allowlist_acl_refuse_actual_cli() {
    let root = tempfile::tempdir().unwrap();
    private(root.path());
    let workspace = root.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let data = root.path().join("runner");
    assert!(
        cli(
            &data,
            &[
                "nodes",
                "enroll",
                "--role",
                "runner",
                "--bind",
                "127.0.0.1:49732",
                "--workspace",
                workspace.to_str().unwrap()
            ]
        )
        .status
        .success()
    );
    for file in [data.join("runtime/nodes/device-key"), data.join("state.db")] {
        assert!(
            Command::new("icacls.exe")
                .arg(&file)
                .args(["/grant", "*S-1-1-0:R"])
                .output()
                .unwrap()
                .status
                .success()
        );
        for args in [&["nodes", "card"][..], &["nodes", "start"][..]] {
            let output = cli(&data, args);
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("unsafe node ACL"));
        }
        assert!(
            Command::new("icacls.exe")
                .arg(&file)
                .args(["/remove:g", "*S-1-1-0"])
                .output()
                .unwrap()
                .status
                .success()
        );
        assert!(cli(&data, &["nodes", "card"]).status.success());
    }
}
