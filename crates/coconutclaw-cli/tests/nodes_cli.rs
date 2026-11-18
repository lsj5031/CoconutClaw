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

#[cfg(windows)]
#[test]
fn windows_private_pair_start_probe_and_restart_use_native_transport() {
    use std::{
        io::{BufRead, BufReader},
        process::{Child, Stdio},
        sync::mpsc,
        time::Duration,
    };
    struct Owned(Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = tempfile::tempdir().unwrap();
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
    assert!(
        cli(&node, &["nodes", "pair", core_card.to_str().unwrap()])
            .status
            .success()
    );
    for _ in 0..2 {
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
        assert!(
            receive
                .recv_timeout(Duration::from_secs(20))
                .unwrap()
                .starts_with("Runner ready ")
        );
        reader.join().unwrap();
        let probe = cli(&core, &["nodes", "status", "--probe"]);
        assert!(probe.status.success());
        let status = String::from_utf8_lossy(&probe.stdout);
        assert!(
            status
                .lines()
                .any(|line| line == "Reachable when last checked."),
            "expected a successful probe, got:\n{status}"
        );
        // Owned drops/kills/reaps this exact listener before the next restart.
    }
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
