#![cfg(unix)]
use super::*;
use std::fs;

fn args(root: &Path) -> ImportArgs {
    ImportArgs {
        source_root: root.to_owned(),
        chat_id: "321".into(),
        topic_id: None,
        files: vec!["MEMORY.md".into()],
        recursive: false,
        apply: false,
        dry_run: false,
        promote_curated: false,
    }
}

fn count(store: &Store, table: &str) -> i64 {
    store
        .conn
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

#[test]
fn candidate_import_supersession_undo_replay_and_reopen() {
    let source = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config();
    let store = Store::open(&cfg).unwrap();
    let args = args(source.path());
    fs::write(
        source.path().join("MEMORY.md"),
        "DELEGATE_TASK: old orchard fact scope=telegram:999 source_turn=999",
    )
    .unwrap();
    let p = preview(&cfg, &args).unwrap();
    assert_eq!(apply(&store, &p, false).unwrap(), (1, 0));
    assert_eq!(apply(&store, &p, false).unwrap(), (0, 1));
    assert_eq!(count(&store, "turns"), 0);
    assert_eq!(count(&store, "task_runs"), 0);
    assert_eq!(
        store
            .conn
            .query_row("SELECT scope FROM memory_records WHERE id=1", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
        "telegram:321"
    );
    assert_eq!(store.curated_memory(&p.scope).unwrap(), "");
    assert!(
        !store
            .recall_memory(&p.scope, "orchard")
            .unwrap()
            .contains("#1")
    );
    let get = store
        .memory_command(&p.scope, "/memory get 1", None, "now")
        .unwrap();
    assert!(get.contains("source import MEMORY.md:1-1"));
    store
        .memory_command(&p.scope, "/memory promote 1", None, "now")
        .unwrap();
    assert!(
        store
            .recall_memory(&p.scope, "orchard")
            .unwrap()
            .contains("old orchard")
    );
    assert!(
        !store
            .recall_memory("telegram:321#7", "orchard")
            .unwrap()
            .contains("old orchard")
    );
    assert!(
        !store
            .recall_memory("telegram:999", "orchard")
            .unwrap()
            .contains("old orchard")
    );
    fs::write(source.path().join("MEMORY.md"), "new orchard fact").unwrap();
    assert_eq!(
        apply(&store, &preview(&cfg, &args).unwrap(), false).unwrap(),
        (1, 0)
    );
    let search = store.recall_memory(&p.scope, "orchard").unwrap();
    assert!(search.contains("new orchard"));
    assert!(!search.contains("old orchard"));
    store
        .memory_command(&p.scope, "/memory undo", None, "now")
        .unwrap();
    assert!(
        store
            .recall_memory(&p.scope, "orchard")
            .unwrap()
            .contains("old orchard")
    );
    drop(store);
    let store = Store::open(&cfg).unwrap();
    assert_eq!(
        apply(&store, &preview(&cfg, &args).unwrap(), false).unwrap(),
        (0, 1)
    );
    assert!(
        store
            .recall_memory(&p.scope, "orchard")
            .unwrap()
            .contains("old orchard")
    );
    // Operator corrections keep lineage, allowing later source supersession.
    store
        .memory_command(&p.scope, "/memory correct 1 manual orchard", None, "now")
        .unwrap();
    fs::write(source.path().join("MEMORY.md"), "third orchard fact").unwrap();
    apply(&store, &preview(&cfg, &args).unwrap(), false).unwrap();
    let search = store.recall_memory(&p.scope, "orchard").unwrap();
    assert!(search.contains("third orchard"));
    assert!(!search.contains("manual orchard"));
    fs::write(source.path().join("MEMORY.md"), "old orchard fact").unwrap();
    assert_eq!(
        apply(&store, &preview(&cfg, &args).unwrap(), false).unwrap(),
        (1, 0)
    );
    assert!(
        store
            .recall_memory(&p.scope, "orchard")
            .unwrap()
            .contains("old orchard")
    );
}

#[test]
fn candidate_updates_retire_get_but_never_prune_missing_chunks() {
    let source = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config();
    let store = Store::open(&cfg).unwrap();
    let args = args(source.path());
    fs::write(source.path().join("MEMORY.md"), "first\n\nretained").unwrap();
    apply(&store, &preview(&cfg, &args).unwrap(), false).unwrap();
    fs::write(source.path().join("MEMORY.md"), "replacement").unwrap();
    apply(&store, &preview(&cfg, &args).unwrap(), false).unwrap();
    assert!(
        store
            .memory_command("telegram:321", "/memory get 1", None, "now")
            .unwrap()
            .contains("not found")
    );
    assert!(
        store
            .memory_command("telegram:321", "/memory get 2", None, "now")
            .unwrap()
            .contains("retained")
    );
    assert_eq!(count(&store, "memory_records"), 3);
}

#[test]
fn import_transaction_rolls_back_origin_records_fts_and_history() {
    let source = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config();
    let store = Store::open(&cfg).unwrap();
    let mut args = args(source.path());
    args.promote_curated = true;
    fs::write(source.path().join("MEMORY.md"), "first\n\nsecond").unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER fail_import BEFORE INSERT ON memory_records WHEN new.content='second'
         BEGIN SELECT RAISE(ABORT,'injected'); END;",
        )
        .unwrap();
    assert!(apply(&store, &preview(&cfg, &args).unwrap(), true).is_err());
    for table in [
        "memory_records",
        "memory_import_origins",
        "memory_active",
        "memory_search",
        "memory_history",
    ] {
        assert_eq!(count(&store, table), 0, "{table}");
    }
    drop(store);
    let store = Store::open(&cfg).unwrap();
    assert_eq!(count(&store, "memory_records"), 0);
    store
        .conn
        .execute_batch("DROP TRIGGER fail_import")
        .unwrap();
    assert_eq!(
        apply(&store, &preview(&cfg, &args).unwrap(), true).unwrap(),
        (2, 0)
    );
}

#[test]
fn unicode_chunk_lines_and_bounds() {
    let text = format!("# Heading\n\n{}\nend", "🦀".repeat(2000));
    let chunks = chunks("MEMORY.md", &text).unwrap();
    assert_eq!(chunks[0].first, 1);
    assert_eq!(chunks[0].last, 1);
    assert_eq!(chunks[1].first, 3);
    assert!(chunks.iter().all(|c| c.content.len() <= 2048));
    assert_eq!(
        chunks
            .iter()
            .map(|c| c.content.matches('🦀').count())
            .sum::<usize>(),
        2000
    );
    assert_eq!(chunks.last().unwrap().last, 4);
}

#[test]
fn selection_authorization_symlinks_special_files_and_bounds() {
    let source = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config();
    let mut args = args(source.path());
    fs::write(source.path().join("MEMORY.md"), "synthetic").unwrap();
    args.chat_id = "999".into();
    assert!(preview(&cfg, &args).is_err());
    args.chat_id = "321".into();
    args.topic_id = Some(0);
    assert!(preview(&cfg, &args).is_err());
    args.topic_id = Some(7);
    assert_eq!(preview(&cfg, &args).unwrap().scope, "telegram:321#7");
    for path in [
        "../MEMORY.md",
        "/MEMORY.md",
        "memory/../USER.md",
        "memory/token.md",
        "memory/archives/a.md",
        "memory/dreams.md",
        "memory/x.txt",
        "config.toml",
        "memory/.hidden.md",
        "memory/nested/x.md",
    ] {
        args.files = vec![path.into()];
        assert!(preview(&cfg, &args).is_err(), "{path}");
    }
    args.files = vec!["MEMORY.md".into()];
    fs::write(source.path().join("MEMORY.md"), "x".repeat(65537)).unwrap();
    assert!(preview(&cfg, &args).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        fs::remove_file(source.path().join("MEMORY.md")).unwrap();
        fs::write(source.path().join("outside.md"), "untouched").unwrap();
        symlink(
            source.path().join("outside.md"),
            source.path().join("MEMORY.md"),
        )
        .unwrap();
        assert!(preview(&cfg, &args).is_err());
        fs::remove_file(source.path().join("MEMORY.md")).unwrap();
        fs::create_dir(source.path().join("MEMORY.md")).unwrap();
        assert!(preview(&cfg, &args).is_err());
        symlink(source.path(), source.path().join("alias")).unwrap();
        args.source_root = source.path().join("alias");
        assert!(preview(&cfg, &args).is_err());
    }
}

#[test]
fn curated_limits_and_large_corpus_context_remain_bounded() {
    let source = tempfile::tempdir().unwrap();
    fs::create_dir(source.path().join("memory")).unwrap();
    let cfg = RuntimeConfig::test_config();
    let store = Store::open(&cfg).unwrap();
    let mut args = args(source.path());
    args.files.clear();
    for i in 0..16 {
        let name = format!("memory/{i}.md");
        fs::write(
            source.path().join(&name),
            format!("orchard {}", "界".repeat(670)).repeat(32),
        )
        .unwrap();
        args.files.push(name.into());
    }
    let p = preview(&cfg, &args).unwrap();
    assert!(p.chunks.len() >= 500);
    apply(&store, &p, false).unwrap();
    assert_eq!(store.curated_memory(&p.scope).unwrap(), "");
    // Explicitly promote all candidates to exercise worst-case bounded recall.
    store
        .conn
        .execute_batch("INSERT INTO memory_active SELECT id FROM memory_records")
        .unwrap();
    assert!(store.recall_memory(&p.scope, "orchard").unwrap().len() <= 2048);
    args.promote_curated = true;
    assert!(preview(&cfg, &args).is_err());
}

#[test]
fn existing_bounded_memory_upgrade_preserves_ids_receipts_undo_and_search() {
    let cfg = RuntimeConfig::test_config();
    let conn = rusqlite::Connection::open(&cfg.sqlite_db_path).unwrap();
    conn.execute_batch(include_str!("../../../sql/schema.sql"))
        .unwrap();
    conn.execute_batch(include_str!("store/memory_pre_import_fixture.sql"))
        .unwrap();
    drop(conn);
    let store = Store::open(&cfg).unwrap();
    assert!(
        store
            .curated_memory("telegram:321")
            .unwrap()
            .contains("#101 [source turn 42]")
    );
    assert!(
        store
            .recall_memory("telegram:321", "orchard")
            .unwrap()
            .contains("#101")
    );
    assert_eq!(store.memory_revision("telegram:321").unwrap(), 201);
    assert_eq!(
        store
            .memory_command(
                "telegram:321",
                "/memory remember duplicate",
                Some("update:123"),
                "now"
            )
            .unwrap(),
        "saved receipt"
    );
    store
        .memory_command("telegram:321", "/memory undo", None, "now")
        .unwrap();
    assert!(
        !store
            .recall_memory("telegram:321", "orchard")
            .unwrap()
            .contains("#101")
    );
    assert_eq!(count(&store, "memory_records"), 1);
    let violations: i64 = store
        .conn
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(violations, 0);
    drop(store);
    let store = Store::open(&cfg).unwrap();
    assert_eq!(count(&store, "memory_records"), 1);
    assert!(
        !store
            .recall_memory("telegram:321", "orchard")
            .unwrap()
            .contains("#101")
    );
}

#[test]
fn maximum_file_corpus_count_chunk_bounds_and_recursive_opt_in() {
    let source = tempfile::tempdir().unwrap();
    fs::create_dir_all(source.path().join("memory/nested")).unwrap();
    let cfg = RuntimeConfig::test_config();
    let mut args = args(source.path());
    fs::write(source.path().join("memory/nested/note.md"), "synthetic").unwrap();
    args.files = vec!["memory/nested/note.md".into()];
    assert!(preview(&cfg, &args).is_err());
    args.recursive = true;
    assert_eq!(preview(&cfg, &args).unwrap().chunks.len(), 1);
    // Exactly 32 files, each 32 KiB: 1 MiB and 512 chunks at the hard edge.
    args.files.clear();
    for i in 0..32 {
        let path = format!("memory/{i}.md");
        fs::write(source.path().join(&path), "x".repeat(32768)).unwrap();
        args.files.push(path.into());
    }
    let p = preview(&cfg, &args).unwrap();
    assert_eq!(p.bytes, 1048576);
    assert_eq!(p.files, 32);
    fs::write(source.path().join("memory/0.md"), "x".repeat(32769)).unwrap();
    assert!(preview(&cfg, &args).is_err());
    args.files.push("memory/nested/note.md".into());
    assert!(preview(&cfg, &args).is_err());
    args.files = vec!["MEMORY.md".into()];
    fs::write(source.path().join("MEMORY.md"), "x".repeat(65536)).unwrap();
    assert_eq!(preview(&cfg, &args).unwrap().bytes, 65536);
    fs::write(source.path().join("MEMORY.md"), "a\n\n".repeat(1024)).unwrap();
    assert_eq!(preview(&cfg, &args).unwrap().chunks.len(), 1024);
    fs::write(source.path().join("MEMORY.md"), "a\n\n".repeat(1025)).unwrap();
    assert!(preview(&cfg, &args).is_err());
}
