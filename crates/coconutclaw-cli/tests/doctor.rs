use coconutclaw_config::RuntimeConfig;
use std::{fs, path::Path, process::Command};

fn doctor(root: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_coconutclaw"))
        .env_clear()
        .env("HOME", root)
        .env("CODEX_BIN", "/nonexistent/synthetic-codex")
        .args(["--data-dir"])
        .arg(root)
        .args(["doctor", "--json"])
        .output()
        .unwrap()
}

#[test]
fn doctor_does_not_create_a_missing_data_directory() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("absent");
    let output = doctor(&root);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !root.exists(),
        "doctor must not initialize layout or SQLite"
    );
}

#[test]
fn doctor_is_available_under_live_instance_and_sqlite_writer_locks() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config_at(temp.path().to_path_buf());
    let _instance = cfg.acquire_instance_lock().unwrap();
    let db = rusqlite::Connection::open(&cfg.sqlite_db_path).unwrap();
    db.execute_batch("CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES('untouched'); BEGIN IMMEDIATE").unwrap();
    let before = fs::read(&cfg.sqlite_db_path).unwrap();
    let output = doctor(temp.path());
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["runtime"], "codex");
    assert_eq!(fs::read(&cfg.sqlite_db_path).unwrap(), before);
    assert!(!temp.path().join("state.db.initialize.lock").exists());
    db.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}
