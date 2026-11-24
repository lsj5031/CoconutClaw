use super::*;
use crate::store::Store;
use coconutclaw_config::RuntimeConfig;
use serde_json::{Value, json};

fn fixture(root: &Path) -> (RuntimeConfig, Store) {
    let cfg = RuntimeConfig::test_config_at(root.into());
    let store = Store::open(&cfg).unwrap();
    store.conn.execute_batch(
        "INSERT INTO task_runs(id,session_id,channel,source_chat_id,source_thread_id,update_id,prompt,status,created_at,error_summary)
         VALUES (23,'telegram:321#37','telegram','321','37','201','fixture','completed','t',NULL),
                (24,'telegram:321#37','telegram','321','37',NULL,'fixture','failed','t','runtime restarted; outcome unknown, do not automatically retry');
         UPDATE runtime_attempts SET task_id=23,ordinal=2,parent_run_id=23 WHERE run_id=24;
         DELETE FROM runtime_tasks WHERE task_id=24;
         UPDATE runtime_tasks SET current_run_id=24 WHERE task_id=23;
         INSERT INTO runtime_schedules(id,session_id,creation_key,creation_spec,request,timing,revision,state,next_due)
         VALUES(7,'telegram:321#37','create','{}','{}','{}',1,'active',1000);",
    ).unwrap();
    assert_eq!(crate::nodes::tests::admit_cutover_run(&store), 25);
    (cfg, store)
}

fn backup(store: &Store, path: &Path) {
    store
        .conn
        .execute("VACUUM INTO ?1", [path.to_str().unwrap()])
        .unwrap();
}

fn baseline(path: &Path, coordinator_key: Option<EndpointId>) -> (Baseline, Vec<u8>, blake3::Hash) {
    let projection = capture(&open_source(path).unwrap().transaction().unwrap(), false).unwrap();
    let baseline = Baseline {
        version: VERSION,
        source_id: "fixture".into(),
        coordinator_key,
        projection,
    };
    let raw = serde_json::to_vec(&baseline).unwrap();
    let digest = blake3::hash(&raw);
    (baseline, raw, digest)
}

fn add_final_work(store: &Store) {
    store.conn.execute_batch(
        "INSERT INTO telegram_ingress(seq,update_id,payload,session_id,state,route,progress_state,created_at)
         VALUES(6,'202','{\"update_id\":202}','telegram:321#37','pending','telegram:321#37','none','t'),
               (7,'203','{\"update_id\":203}','telegram:321#37','applying','telegram:321#37','sending','t');
         INSERT INTO task_runs(id,session_id,channel,source_chat_id,source_thread_id,prompt,status,created_at)
         VALUES(28,'telegram:321#37','telegram','321','37','schedule','running','t');
         INSERT INTO task_payloads(task_run_id,request,output,delivery_state)
         VALUES(28,'{}','already reserved','{\"completed_ops\":[],\"operations\":{},\"text_plan\":{\"phase\":\"sending\",\"attempts\":1}}');
         INSERT INTO runtime_inbox VALUES('schedule:7:1000','schedule','telegram:321#37','{}',28,'t');
         INSERT INTO schedule_occurrences VALUES(7,1,1000,'schedule:7:1000');
         INSERT INTO runtime_schedule_runs VALUES(7,28);
         UPDATE runtime_schedules SET next_due=2000 WHERE id=7;
         UPDATE kv SET value='203' WHERE key='last_update_id';",
    ).unwrap();
}

#[test]
fn frozen_final_delta_archives_projection_and_acknowledges_exact_receipts_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let (cfg, store) = fixture(root.path());
    let base = root.path().join("base.sqlite");
    let frozen = root.path().join("frozen.sqlite");
    let custody = root.path().join("custody.sqlite");
    backup(&store, &base);
    add_final_work(&store);
    let (key, receipt) = crate::nodes::tests::late_cutover_receipt(&store, 25);
    store
        .conn
        .execute("INSERT INTO node_routes VALUES(25, 'paired runner')", [])
        .unwrap();
    Store::seal_legacy_cutover(&cfg).unwrap();
    backup(&store, &frozen);
    let frozen_bytes = fs::read(&frozen).unwrap();
    let (baseline, raw_baseline, baseline_digest) = baseline(&base, Some(key));
    let delta = export_final(&frozen, &baseline, baseline_digest).unwrap();
    assert_eq!(delta.receipts, std::slice::from_ref(&receipt));
    assert!(!delta.replacements.contains_key("runtime_remote_results"));
    let raw = serde_json::to_vec(&delta).unwrap();
    let digest = blake3::hash(&raw);
    assert_eq!(
        raw,
        serde_json::to_vec(&export_final(&frozen, &baseline, baseline_digest).unwrap()).unwrap()
    );
    assert_eq!(fs::read(&frozen).unwrap(), frozen_bytes);
    assert!(!root.path().join("frozen.sqlite-wal").exists());
    assert!(!root.path().join("frozen.sqlite-shm").exists());
    assert!(!root.path().join("frozen.sqlite-journal").exists());
    assert!(import_final(&custody, &raw, digest).is_err());
    stage(&custody, &raw_baseline, baseline_digest).unwrap();
    stage(&custody, &raw_baseline, baseline_digest).unwrap();
    let db = Connection::open(&custody).unwrap();
    db.execute_batch(
        "CREATE TRIGGER interrupted BEFORE INSERT ON signed_receipts
        BEGIN SELECT RAISE(ABORT,'interrupted'); END;",
    )
    .unwrap();
    assert!(import_final(&custody, &raw, digest).is_err());
    assert_eq!(
        db.query_row(
            "SELECT (SELECT count(*) FROM final_delta)+(SELECT count(*) FROM signed_receipts)",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER interrupted;").unwrap();

    let mut corrupt: Delta = serde_json::from_slice(&raw).unwrap();
    corrupt.receipts[0].result[0] ^= 1;
    let corrupt = serde_json::to_vec(&corrupt).unwrap();
    assert!(import_final(&custody, &corrupt, blake3::hash(&corrupt)).is_err());
    let mut absent: Delta = serde_json::from_slice(&raw).unwrap();
    absent.receipts.clear();
    let absent = serde_json::to_vec(&absent).unwrap();
    assert!(import_final(&custody, &absent, blake3::hash(&absent)).is_err());
    drop(db);

    let ack = import_final(&custody, &raw, digest).unwrap();
    assert_eq!(ack.final_digest, digest.to_string());
    assert_eq!(
        ack.receipt_digests,
        BTreeMap::from([(
            25,
            blake3::hash(&serde_json::to_vec(&receipt).unwrap()).to_string()
        )])
    );
    assert_eq!(import_final(&custody, &raw, digest).unwrap(), ack);
    let db = Connection::open(&custody).unwrap();
    let saved: (i64, Vec<u8>, Vec<u8>) = db
        .query_row("SELECT run,start,result FROM signed_receipts", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(saved, (25, receipt.start, receipt.result));
    let projection: Vec<u8> = db
        .query_row("SELECT projection FROM final_delta", [], |row| row.get(0))
        .unwrap();
    let projection: Value = serde_json::from_slice(&projection).unwrap();
    assert_eq!(projection["runtime_tasks"][0]["task_id"], 23);
    assert_eq!(projection["runtime_tasks"][0]["current_run_id"], 24);
    assert_eq!(projection["runtime_attempts"][1]["parent_run_id"], 23);
    assert_eq!(
        projection["task_runs"][1]["error_summary"],
        "runtime restarted; outcome unknown, do not automatically retry"
    );
    assert_eq!(projection["task_runs"][1]["source_thread_id"], "37");
    assert_eq!(projection["telegram_ingress"][0]["state"], "pending");
    assert_eq!(
        projection["telegram_ingress"][1]["progress_state"],
        "sending"
    );
    assert_eq!(projection["runtime_schedules"][0]["next_due"], 2000);
    assert_eq!(
        projection["schedule_occurrences"][0],
        json!({"schedule_id":7,"revision":1,"due":1000,"event_key":"schedule:7:1000"})
    );
    assert_eq!(projection["node_routes"][0]["run"], 25);
    let delivery: Value = serde_json::from_str(
        projection["task_payloads"][1]["delivery_state"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        delivery["text_plan"],
        json!({"phase":"sending","attempts":1})
    );
    assert_eq!(projection["task_runs"][3]["status"], "running");
    assert_eq!(projection["runtime_remote_results"], json!([]));
    db.execute("DELETE FROM signed_receipts", []).unwrap();
    assert!(import_final(&custody, &raw, digest).is_err());
}

#[test]
fn frozen_final_delta_rejects_unsealed_unsupported_and_changed_sources() {
    let root = tempfile::tempdir().unwrap();
    let (cfg, store) = fixture(root.path());
    let base = root.path().join("base.sqlite");
    backup(&store, &base);
    let (baseline, _, digest) = baseline(&base, None);
    let check = |name: &str, expected: &str| {
        let detached = root.path().join(name);
        backup(&store, &detached);
        let message = format!(
            "{:#}",
            export_final(&detached, &baseline, digest).err().unwrap()
        );
        assert!(message.contains(expected), "{message}");
    };
    assert!(open_source(&cfg.sqlite_db_path).is_err());
    store
        .conn
        .execute_batch(
            "INSERT INTO legacy_admission_freeze VALUES(1);
         INSERT INTO legacy_cutover_seal VALUES(1);",
        )
        .unwrap();
    check("old-seal.sqlite", "legacy_executor_seal");
    Store::seal_legacy_cutover(&cfg).unwrap();
    store
        .conn
        .execute(
            "CREATE TABLE adapter_binding(singleton INTEGER PRIMARY KEY)",
            [],
        )
        .unwrap();
    check("unsupported.sqlite", "unsupported source table");
    store
        .conn
        .execute_batch(
            "DROP TABLE adapter_binding;
         UPDATE task_runs SET error_summary='outcome unknown rewritten' WHERE id=24;",
        )
        .unwrap();
    check("unknown.sqlite", "#23 Unknown record changed");
    store.conn.execute_batch(
        "UPDATE task_runs SET error_summary='runtime restarted; outcome unknown, do not automatically retry' WHERE id=24;
         UPDATE runtime_schedules SET next_due=999 WHERE id=7;",
    ).unwrap();
    check("regression.sqlite", "schedule due watermark regressed");
}

#[test]
fn frozen_final_delta_requires_pinned_coordinator_and_complete_terminal_evidence() {
    let root = tempfile::tempdir().unwrap();
    let (cfg, store) = fixture(root.path());
    let base = root.path().join("base.sqlite");
    backup(&store, &base);
    let (key, _) = crate::nodes::tests::late_cutover_receipt(&store, 25);
    Store::seal_legacy_cutover(&cfg).unwrap();
    let frozen = root.path().join("frozen.sqlite");
    backup(&store, &frozen);
    let (unpinned, _, digest) = baseline(&base, None);
    let error = export_final(&frozen, &unpinned, digest).err().unwrap();
    assert!(format!("{error:#}").contains("missing pinned coordinator key"));
    let wrong = iroh::SecretKey::from([175; 32]).public();
    let (wrong, _, digest) = baseline(&base, Some(wrong));
    let error = export_final(&frozen, &wrong, digest).err().unwrap();
    assert!(format!("{error:#}").contains("foreign assignment authority"));
    store
        .conn
        .execute("UPDATE node_signed_receipts SET result=NULL", [])
        .unwrap();
    let stale = root.path().join("stale-seal.sqlite");
    backup(&store, &stale);
    let (pinned, _, digest) = baseline(&base, Some(key));
    let error = export_final(&stale, &pinned, digest).err().unwrap();
    assert!(format!("{error:#}").contains("signed terminal receipt missing"));
}
