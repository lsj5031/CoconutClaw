use super::*;

#[test]
fn plain_connection_reads_while_another_connection_owns_writer() {
    let cfg = RuntimeConfig::test_config();
    let writer = Store::open(&cfg).unwrap();
    writer.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = Store::connect(&cfg).and_then(|reader| reader.kv_get("last_update_id"));
        send.send(result).unwrap();
    });
    let read = receive.recv_timeout(std::time::Duration::from_secs(1));
    writer.conn.execute_batch("ROLLBACK").unwrap();
    worker.join().unwrap();
    assert_eq!(
        read.expect("plain connection must not wait for a writer")
            .unwrap(),
        Some("0".into())
    );
}

#[test]
fn plain_connection_does_not_create_a_database_or_layout() {
    let mut cfg = RuntimeConfig::test_config();
    cfg.sqlite_db_path = cfg.instance_dir.join("absent").join("state.db");
    assert!(Store::connect(&cfg).is_err());
    assert!(!cfg.sqlite_db_path.parent().unwrap().exists());
    cfg.sqlite_db_path = cfg.instance_dir.join("absent.db");
    assert!(Store::connect(&cfg).is_err());
    assert!(!cfg.sqlite_db_path.exists());
}

#[test]
fn plain_connection_ignores_initialization_lock_and_preserves_connection_pragmas() {
    let cfg = RuntimeConfig::test_config();
    let initial = Store::open(&cfg).unwrap();
    let mut path = cfg.sqlite_db_path.as_os_str().to_os_string();
    path.push(".initialize.lock");
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    lock.lock().unwrap();
    let schema_version: i64 = initial
        .conn
        .query_row("PRAGMA schema_version", [], |r| r.get(0))
        .unwrap();
    let data_version: i64 = initial
        .conn
        .query_row("PRAGMA data_version", [], |r| r.get(0))
        .unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader_cfg = cfg.clone();
    let worker = std::thread::spawn(move || {
        let reader = Store::connect(&reader_cfg).unwrap();
        let timeout: i64 = reader
            .conn
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        let foreign_keys: i64 = reader
            .conn
            .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
            .unwrap();
        let journal: String = reader
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        send.send((timeout, foreign_keys, journal)).unwrap();
    });
    let result = receive.recv_timeout(std::time::Duration::from_secs(1));
    drop(lock);
    worker.join().unwrap();
    let initial_foreign_keys: i64 = initial
        .conn
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .unwrap();
    assert_eq!(result.unwrap(), (5000, initial_foreign_keys, "wal".into()));
    assert_eq!(
        initial
            .conn
            .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        schema_version
    );
    assert_eq!(
        initial
            .conn
            .query_row("PRAGMA data_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        data_version,
        "plain connect must not perform hidden writes"
    );
}

#[test]
fn initialization_process_helper() {
    let Some(root) = std::env::var_os("COCONUTCLAW_STORE_INIT_TEST_ROOT") else {
        return;
    };
    let cfg = RuntimeConfig::test_config_at(root.into());
    let store = Store::open(&cfg).unwrap();
    assert_eq!(
        store.kv_get("runtime_graph_v1").unwrap().as_deref(),
        Some("1")
    );
    assert!(
        store
            .conn
            .query_row("SELECT count(*) FROM memory_records", [], |r| r
                .get::<_, i64>(0))
            .is_ok()
    );
}

#[test]
fn concurrent_process_startup_and_restart_keep_schema_and_receipts() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config_at(temp.path().to_path_buf());
    let spawn = || {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::startup_tests::initialization_process_helper",
                "--nocapture",
            ])
            .env("COCONUTCLAW_STORE_INIT_TEST_ROOT", temp.path())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap()
    };
    let mut processes = (0..4).map(|_| spawn()).collect::<Vec<_>>();
    for process in &mut processes {
        assert!(process.wait().unwrap().success());
    }
    let store = Store::connect(&cfg).unwrap();
    store.kv_set("startup_test_receipt", "preserved").unwrap();
    let schema: i64 = store
        .conn
        .query_row("PRAGMA schema_version", [], |r| r.get(0))
        .unwrap();
    assert!(spawn().wait().unwrap().success());
    assert_eq!(
        store.kv_get("startup_test_receipt").unwrap().as_deref(),
        Some("preserved")
    );
    assert_eq!(
        store
            .conn
            .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        schema
    );
    assert_eq!(
        store
            .conn
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn connection_busy_timeout_allows_a_contending_write_after_owner_releases() {
    let cfg = RuntimeConfig::test_config();
    let writer = Store::open(&cfg).unwrap();
    writer.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (ready, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let store = Store::connect(&cfg).unwrap();
        ready.send(()).unwrap();
        store.kv_set("contended", "committed").unwrap();
    });
    receive
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
    writer.conn.execute_batch("COMMIT").unwrap();
    worker.join().unwrap();
    assert_eq!(
        writer.kv_get("contended").unwrap().as_deref(),
        Some("committed")
    );
}
