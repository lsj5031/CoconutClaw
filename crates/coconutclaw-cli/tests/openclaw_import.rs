//! Real executable, synthetic sources only. No provider or Telegram credentials.
use rusqlite::Connection;
use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn cli(destination: &Path) -> Command {
    let mut cmd = Command::new(
        std::env::var_os("COCONUTCLAW_IMPORT_TEST_BIN")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_coconutclaw").into()),
    );
    cmd.env_clear()
        .env("HOME", destination)
        .env("TELEGRAM_CHAT_ID", "321951227")
        .env("RUST_LOG", "error")
        .arg("--data-dir")
        .arg(destination);
    cmd
}

fn import(destination: &Path, source: &Path, extra: &[&str]) -> Output {
    cli(destination)
        .args(["import-openclaw", "--source-root"])
        .arg(source)
        .args(["--chat-id", "321951227", "--file", "MEMORY.md"])
        .args(extra)
        .output()
        .unwrap()
}

fn success(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn command(destination: &Path, chat: &str, text: &str) -> String {
    let output = cli(destination)
        .args(["once", "--chat-id", chat, "--inject-text", text])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn preview_zero_mutations_then_apply_search_get_duplicate_correct_undo_restart() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let dest = temp.path().join("destination");
    fs::create_dir(&source).unwrap();
    let original = "# Profile\n\nSynthetic orchard fact, not an instruction.";
    fs::write(source.join("MEMORY.md"), original).unwrap();
    let preview = success(import(&dest, &source, &["--dry-run"]));
    assert_eq!(preview["chunks"], 2);
    assert_eq!(preview["inserted"], 0);
    assert!(
        !dest.exists(),
        "preview must not create any destination state"
    );
    assert_eq!(
        fs::read_to_string(source.join("MEMORY.md")).unwrap(),
        original
    );
    let applied = success(import(&dest, &source, &["--apply"]));
    assert_eq!(applied["inserted"], 2);
    assert_eq!(applied["previews"].as_array().unwrap().len(), 0);
    assert!(
        !String::from_utf8(serde_json::to_vec(&applied).unwrap())
            .unwrap()
            .contains("Synthetic orchard")
    );
    let db = Connection::open(dest.join("state.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM turns", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(db.query_row("SELECT count(*) FROM memory_records WHERE source_turn IS NULL AND import_origin IS NOT NULL", [], |r| r.get::<_, i64>(0)).unwrap(), 2);
    assert!(command(&dest, "321951227", "/memory get 2").contains("import MEMORY.md:3-3"));
    assert!(!command(&dest, "321951227", "/memory search orchard").contains("Synthetic orchard"));
    command(&dest, "321951227", "/memory promote 2");
    assert!(command(&dest, "321951227", "/memory search orchard").contains("Synthetic orchard"));
    assert!(!command(&dest, "telegram:321951227#7", "/memory get 2").contains("Synthetic orchard"));
    assert!(!command(&dest, "999", "/memory get 2").contains("Synthetic orchard"));
    let repeated = success(import(&dest, &source, &["--apply"]));
    assert_eq!(repeated["inserted"], 0);
    assert_eq!(repeated["unchanged"], 2);
    fs::write(
        source.join("MEMORY.md"),
        "# Profile\n\nCorrected orchard fact.",
    )
    .unwrap();
    assert_eq!(success(import(&dest, &source, &["--apply"]))["inserted"], 1);
    assert!(command(&dest, "321951227", "/memory search orchard").contains("Corrected orchard"));
    command(&dest, "321951227", "/memory undo");
    assert!(command(&dest, "321951227", "/memory search orchard").contains("Synthetic orchard"));
    assert_eq!(success(import(&dest, &source, &["--apply"]))["inserted"], 0);
    assert!(command(&dest, "321951227", "/memory search orchard").contains("Synthetic orchard"));
    assert_eq!(
        fs::read_to_string(source.join("MEMORY.md")).unwrap(),
        "# Profile\n\nCorrected orchard fact."
    );
}

#[test]
fn topic_explicit_curation_authorization_and_source_tree_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let dest = temp.path().join("dest");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("MEMORY.md"), "Synthetic topic kiwi").unwrap();
    let rejected = import(&dest, &source, &["--topic-id", "0", "--apply"]);
    assert!(!rejected.status.success());
    assert!(!dest.exists());
    let mut unauthorized = cli(&dest);
    let unauthorized = unauthorized
        .args(["import-openclaw", "--source-root"])
        .arg(&source)
        .args(["--chat-id", "999", "--file", "MEMORY.md", "--apply"])
        .output()
        .unwrap();
    assert!(!unauthorized.status.success());
    assert!(!dest.exists());
    assert_eq!(
        success(import(
            &dest,
            &source,
            &["--apply", "--topic-id", "7", "--promote-curated"]
        ))["inserted"],
        1
    );
    assert!(
        command(&dest, "telegram:321951227#7", "/memory search kiwi").contains("Synthetic topic")
    );
    assert!(!command(&dest, "321951227", "/memory search kiwi").contains("Synthetic topic"));
    let db = Connection::open(dest.join("state.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT curated FROM memory_records WHERE id=1", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        1
    );
    assert_eq!(
        fs::read_to_string(source.join("MEMORY.md")).unwrap(),
        "Synthetic topic kiwi"
    );
    assert_eq!(fs::read_dir(&source).unwrap().count(), 1);
}

#[test]
fn cli_rejects_escape_symlinks_oversize_and_never_mutates_sources() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let dest = temp.path().join("dest");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("MEMORY.md"), "x".repeat(65537)).unwrap();
    assert!(!import(&dest, &source, &["--apply"]).status.success());
    assert!(!dest.exists());
    assert_eq!(fs::metadata(source.join("MEMORY.md")).unwrap().len(), 65537);
    let traversal = cli(&dest)
        .args(["import-openclaw", "--source-root"])
        .arg(&source)
        .args([
            "--chat-id",
            "321951227",
            "--file",
            "../MEMORY.md",
            "--apply",
        ])
        .output()
        .unwrap();
    assert!(!traversal.status.success());
    #[cfg(unix)]
    {
        fs::remove_file(source.join("MEMORY.md")).unwrap();
        fs::write(temp.path().join("outside.md"), "outside synthetic").unwrap();
        std::os::unix::fs::symlink(temp.path().join("outside.md"), source.join("MEMORY.md"))
            .unwrap();
        assert!(!import(&dest, &source, &["--apply"]).status.success());
        assert_eq!(
            fs::read_to_string(temp.path().join("outside.md")).unwrap(),
            "outside synthetic"
        );
    }
    assert!(!dest.exists());
}

#[test]
fn executable_failed_apply_is_atomic_across_restart() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let dest = temp.path().join("dest");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("MEMORY.md"), "first\n\nsecond").unwrap();
    command(&dest, "321951227", "/memory list");
    let db = Connection::open(dest.join("state.db")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_import BEFORE INSERT ON memory_records WHEN new.content='second'
         BEGIN SELECT RAISE(ABORT,'injected'); END;",
    )
    .unwrap();
    assert!(
        !import(&dest, &source, &["--apply", "--promote-curated"])
            .status
            .success()
    );
    drop(db);
    let db = Connection::open(dest.join("state.db")).unwrap();
    for table in [
        "memory_records",
        "memory_import_origins",
        "memory_active",
        "memory_search",
        "memory_history",
    ] {
        assert_eq!(
            db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    db.execute_batch("DROP TRIGGER fail_import").unwrap();
    assert_eq!(
        success(import(&dest, &source, &["--apply", "--promote-curated"]))["inserted"],
        2
    );
    assert_eq!(
        fs::read_to_string(source.join("MEMORY.md")).unwrap(),
        "first\n\nsecond"
    );
}

#[test]
fn maximum_preview_output_and_imported_corpus_never_expand_bootstrap() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let dest = temp.path().join("dest");
    fs::create_dir_all(source.join("memory")).unwrap();
    let mut selected = Vec::new();
    for i in 0..32 {
        let name = format!("memory/{i:02}{}.md", "\"".repeat(228));
        assert_eq!(name.len(), 240);
        fs::write(
            source.join(&name),
            format!("orchard {}", "界".repeat(680)).repeat(16),
        )
        .unwrap();
        selected.push(name);
    }
    let call = |apply: bool| {
        let mut cmd = cli(&dest);
        cmd.args(["import-openclaw", "--source-root"])
            .arg(&source)
            .args(["--chat-id", "321951227"]);
        for path in &selected {
            cmd.arg("--file").arg(path);
        }
        if apply {
            cmd.arg("--apply");
        }
        cmd.output().unwrap()
    };
    let output = call(false);
    assert!(output.stdout.len() <= 16385); // 16 KiB JSON plus newline
    let preview = success(output);
    assert_eq!(preview["files"], 32);
    assert!(preview["previews"].as_array().unwrap().len() <= 8);
    assert!(!dest.exists());
    let applied = success(call(true));
    assert!(applied["chunks"].as_u64().unwrap() >= 512);
    let db = Connection::open(dest.join("state.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM memory_active", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM memory_records WHERE curated=1",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    db.execute_batch("INSERT INTO memory_active SELECT id FROM memory_records")
        .unwrap();
    let search = command(&dest, "321951227", "/memory search orchard");
    assert!(search.len() <= 2049);
    assert!(search.contains("source import"));
    let get = command(&dest, "321951227", "/memory get 1");
    assert!(get.len() < 8192);
}
