use super::*;
use std::path::PathBuf;

#[test]
fn truncate_chars_no_truncation() {
    assert_eq!(truncate_chars("hello", 10), "hello");
}

#[test]
fn truncate_chars_truncation() {
    assert_eq!(truncate_chars("hello world", 8), "hello wo...");
}

#[test]
fn truncate_chars_exact_boundary() {
    assert_eq!(truncate_chars("hello", 5), "hello");
}

#[test]
fn truncate_chars_zero_max() {
    assert_eq!(truncate_chars("hello", 0), "");
}

#[test]
fn truncate_chars_empty_input() {
    assert_eq!(truncate_chars("", 5), "");
}

#[test]
fn truncate_chars_multibyte() {
    assert_eq!(truncate_chars("日本語テスト", 3), "日本語...");
}

#[test]
fn truncate_chars_multibyte_no_truncation() {
    let s = "日本語テスト";
    assert_eq!(truncate_chars(s, 10), s);
}

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
fn parse_schedule_prompt_line_normalizes_time_and_once_prefix() {
    assert_eq!(
        parse_schedule_prompt_line("once 9:05|Check backups"),
        Some((false, "09:05".to_string(), "Check backups".to_string()))
    );
}

#[test]
fn strip_managed_section_preserves_manual_content() {
    let existing = format!(
        "# Long-Term Memory\nmanual note\n\n{MEMORY_MANAGED_START}\n- generated\n{MEMORY_MANAGED_END}\n"
    );
    assert_eq!(
        strip_managed_section(&existing, MEMORY_MANAGED_START, MEMORY_MANAGED_END),
        "# Long-Term Memory\nmanual note"
    );
}
