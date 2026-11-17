//! Actual executable admission/control entry points. No provider or Telegram
//! invocation; all configuration and identities are fresh and host-local.
#![cfg(not(windows))]
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn cli(data: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_coconutclaw"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", data)
        .arg("--data-dir")
        .arg(data)
        .args(args)
        .output()
        .unwrap()
}
#[cfg(not(windows))]
#[test]
fn public_pair_flow_preserves_private_keys_and_runner_refuses_polling() {
    let root = tempfile::tempdir().unwrap();
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
            .contains("Last contact: never")
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
