use super::*;
use std::path::PathBuf;

#[test]
fn read_or_default_missing_file() {
    let path = PathBuf::from("/nonexistent/path/file.txt");
    assert_eq!(read_or_default(&path, "fallback"), "fallback");
}

#[test]
fn read_or_default_existing_file() {
    let dir = std::env::temp_dir().join("coconutclaw_test_read_or_default");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("test_file.txt");
    std::fs::write(&path, "file content").unwrap();
    assert_eq!(read_or_default(&path, "fallback"), "file content");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn managed_replacement_preserves_surrounding_manual_bytes_and_position() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.md");
    let prefix = "  # Manual header\n\nmanual prefix  \n";
    let suffix = "\n  manual suffix\n\n";
    fs::write(
        &path,
        format!("{prefix}{TASKS_MANAGED_START}\nold\n{TASKS_MANAGED_END}{suffix}"),
    )
    .unwrap();
    rewrite_managed_markdown_file(
        &path,
        "",
        TASKS_MANAGED_START,
        TASKS_MANAGED_END,
        &["new".into()],
    )
    .unwrap();
    assert_eq!(
        fs::read_to_string(path).unwrap(),
        format!("{prefix}{TASKS_MANAGED_START}\nnew\n{TASKS_MANAGED_END}{suffix}")
    );
}

#[test]
fn projection_crash_child() {
    let Some(path) = std::env::var_os("COCONUTCLAW_TEST_PROJECTION_PATH") else {
        return;
    };
    rewrite_managed_markdown_file(
        &PathBuf::from(path),
        "",
        TASKS_MANAGED_START,
        TASKS_MANAGED_END,
        &["new complete projection".into()],
    )
    .unwrap();
    panic!("projection crash point did not run");
}

#[test]
fn projection_interruption_is_old_or_new_never_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.md");
    let old =
        format!("manual prefix\n{TASKS_MANAGED_START}\nold\n{TASKS_MANAGED_END}\nmanual suffix\n");
    for (point, code) in [("before", 73), ("after", 74)] {
        fs::write(&path, &old).unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "context::tests::projection_crash_child"])
            .env_clear()
            .env("COCONUTCLAW_TEST_PROJECTION_PATH", &path)
            .env("COCONUTCLAW_TEST_PROJECTION_EXIT", point)
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(code),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let observed = fs::read_to_string(&path).unwrap();
        if point == "before" {
            assert_eq!(observed, old);
        } else {
            assert_eq!(
                observed,
                old.replace("\nold\n", "\nnew complete projection\n")
            );
        }
    }
}

#[test]
fn concurrent_task_projections_and_scoped_memory_stay_separate() {
    use std::sync::{Arc, Barrier};
    let cfg = RuntimeConfig::test_config();
    fs::write(cfg.instance_dir.join("MEMORY.md"), "# Manual\nkeep this\n").unwrap();
    fs::write(cfg.tasks_dir.join("pending.md"), "# Checklist\nkeep that\n").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let workers = (0..2).map(|index| {
        let cfg = cfg.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let mut store = Store::open(&cfg).unwrap();
            store.conn.execute("INSERT INTO turns(ts,chat_id,input_type,provider_raw,status) VALUES('now',?1,'text','','ok')", [index.to_string()]).unwrap();
            let id = store.conn.last_insert_rowid();
            store.insert_memory_and_tasks("now","codex",Some(id), &[format!("memory-{index}")], &[format!("task-{index}")]).unwrap();
            barrier.wait();
            sync_managed_context_files(&cfg, &store).unwrap();
        })
    }).collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    let store = Store::open(&cfg).unwrap();
    sync_managed_context_files(&cfg, &store).unwrap();
    let input =
        crate::turn::resolve_turn_input(&cfg, &store, None, Some("next".into()), None, "telegram")
            .unwrap();
    let context = build_context(
        &cfg,
        &store,
        &input,
        "now",
        "telegram:321",
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )
    .unwrap();
    for expected in ["keep that", "task-0", "task-1"] {
        assert!(context.contains(expected), "{expected}");
    }
    for excluded in ["keep this", "memory-0", "memory-1"] {
        assert!(!context.contains(excluded), "{excluded}");
    }
    assert_eq!(
        fs::read_to_string(cfg.instance_dir.join("MEMORY.md")).unwrap(),
        "# Manual\nkeep this\n"
    );
}
