use super::*;
use coconutclaw_config::RuntimeConfig;
use std::fs;

use crate::store::Store;

#[test]
fn rewrite_managed_markdown_file_preserves_manual_content() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("MEMORY.md");
    fs::write(&path, "# Long-Term Memory\nmanual note\n").expect("seed file");

    rewrite_managed_markdown_file(
        &path,
        "# Long-Term Memory\n",
        MEMORY_MANAGED_START,
        MEMORY_MANAGED_END,
        &["- 2026-04-24T10:00:00+0000 | remembered".to_string()],
    )
    .expect("rewrite managed file");

    let rendered = fs::read_to_string(&path).expect("read rendered file");
    assert!(rendered.contains("manual note"));
    assert!(rendered.contains(MEMORY_MANAGED_START));
    assert!(rendered.contains("remembered"));
}

#[test]
fn sync_managed_context_files_renders_db_backed_sections() {
    let cfg = RuntimeConfig::test_config();
    let mut store = Store::open(&cfg).expect("store");
    store
        .insert_memory_and_tasks(
            "2026-04-24T10:00:00+0000",
            "codex",
            Some(7),
            &["remember this".to_string()],
            &["do that".to_string()],
        )
        .expect("insert managed entries");

    sync_managed_context_files(&cfg, &store).expect("sync context files");

    let memory = fs::read_to_string(cfg.instance_dir.join("MEMORY.md")).expect("read memory");
    let tasks = fs::read_to_string(cfg.instance_dir.join("TASKS/pending.md")).expect("read tasks");
    assert!(memory.contains(MEMORY_MANAGED_START));
    assert!(memory.contains("- 2026-04-24T10:00:00+0000 | remember this"));
    assert!(tasks.contains(TASKS_MANAGED_START));
    assert!(tasks.contains("- [ ] do that"));
}
