use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn all_numeric_domains_accept_endpoints_and_reject_outside_effective_values() {
    let fields: [(&str, u64, u64); 7] = [
        ("POLL_INTERVAL_SECONDS", 1, 3600),
        ("CONTEXT_TURNS", 1, 10000),
        ("PROVIDER_MAX_RETRIES", 0, 10),
        ("PROGRESS_UPDATE_INTERVAL", 1, 3600),
        ("TELEGRAM_API_TIMEOUT_SECS", 1, 300),
        ("TELEGRAM_API_RETRY_ATTEMPTS", 0, 10),
        ("PROVIDER_TIMEOUT_SECS", 1, 86400),
    ];
    for (key, min, max) in fields {
        for n in [min, max] {
            let dir = unique_dir();
            write_config(&dir, &format!("{key} = {n}"));
            let cfg = load_runtime_config_isolated(dir.clone()).unwrap();
            let effective = match key {
                "POLL_INTERVAL_SECONDS" => cfg.poll_interval_seconds,
                "CONTEXT_TURNS" => cfg.context_turns.into(),
                "PROVIDER_MAX_RETRIES" => cfg.provider_max_retries.into(),
                "PROGRESS_UPDATE_INTERVAL" => cfg.progress_update_interval_secs,
                "TELEGRAM_API_TIMEOUT_SECS" => cfg.telegram_api_timeout_secs,
                "TELEGRAM_API_RETRY_ATTEMPTS" => cfg.telegram_api_retry_attempts.into(),
                "PROVIDER_TIMEOUT_SECS" => cfg.provider_timeout_secs,
                _ => unreachable!(),
            };
            assert_eq!(effective, n);
            fs::remove_dir_all(dir).unwrap();
        }
        for value in ["-1".to_string(), (max + 1).to_string()] {
            let dir = unique_dir();
            write_config(&dir, &format!("{key} = {value}"));
            assert!(load_runtime_config_isolated(dir.clone()).is_err());
            fs::remove_dir_all(dir).unwrap();
        }
    }
}

#[test]
fn effective_invalid_values_fail_before_layout_creation() {
    for assignment in [
        "PROVIDER_TIMEOUT_SECS = false",
        "PROVIDER_TIMEOUT_SECS = -1",
        "PROVIDER_TIMEOUT_SECS = 0",
        "PROVIDER_TIMEOUT_SECS = \"typo\"",
        "PROVIDER_TIMEOUT_SECS = 86401",
        "CONTEXT_TURNS = 0",
        "TELEGRAM_API_RETRY_ATTEMPTS = -1",
        "TELEGRAM_API_RETRY_ATTEMPTS = 11",
        "SQLITE_DB_PATH = false",
        "SQLITE_DB_PATH = \"\"",
        "SQLITE_DB_PATH = \"./config.toml\"",
        "LOG_DIR = \"./config.toml\"",
        "ALLOWLIST_PATH = \"./state.db\"",
        "LOG_DIR = \"\"",
        "CODEX_BIN = \"\"",
        "TELEGRAM_API_TIMEOUT_SECS = 301",
        "TELEGRAM_API_BASE = \"not-a-url\"",
        "TIMEZONE = \"not-a-zone\"",
    ] {
        let dir = unique_dir();
        write_config(&dir, assignment);
        let error = load_runtime_config_isolated(dir.clone()).unwrap_err();
        assert!(
            format!("{error:#}").contains(assignment.split(' ').next().unwrap()),
            "{assignment}"
        );
        assert!(!dir.join("runtime").exists(), "{assignment}");
        assert!(!dir.join("false").exists());
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn configuration_errors_redact_entire_source_chain() {
    for body in [
        "TELEGRAM_BOT_TOKEN = \"SYNTHETIC_SENTINEL\" trailing",
        "TELEGRAM_PARSE_MODE = \"SYNTHETIC_SENTINEL\"",
        "TELEGRAM_PARSE_FALLBACK = \"SYNTHETIC_SENTINEL\"",
        "PROVIDER_TIMEOUT_SECS = \"SYNTHETIC_SENTINEL\"",
    ] {
        let dir = unique_dir();
        write_config(&dir, body);
        let error = load_runtime_config_isolated(dir.clone()).unwrap_err();
        for cause in error.chain() {
            assert!(
                !cause
                    .to_string()
                    .to_uppercase()
                    .contains("SYNTHETIC_SENTINEL")
            );
        }
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn actual_loader_environment_precedence_and_numeric_domains() {
    let _env = TestEnvGuard::clear_config_env();
    let dir = unique_dir();
    let ignored = unique_dir();
    write_config(
        &dir,
        "PROVIDER_TIMEOUT_SECS = 12\nSQLITE_DB_PATH = 'file.db'\n",
    );
    unsafe {
        env::set_var("COCONUTCLAW_DATA_DIR", &ignored);
        env::set_var("PROVIDER_TIMEOUT_SECS", "23");
        env::set_var("SQLITE_DB_PATH", "env.db");
    }
    let overrides = CliOverrides {
        data_dir: Some(dir.clone()),
    };
    let cfg = load_runtime_config(&overrides).unwrap();
    assert_eq!(cfg.instance_dir, dir);
    assert_eq!(cfg.provider_timeout_secs, 23);
    assert_eq!(cfg.sqlite_db_path, dir.join("env.db"));
    assert!(!ignored.exists());
    for value in ["typo", "18446744073709551616", "0", "-1", ""] {
        unsafe {
            env::set_var("PROVIDER_TIMEOUT_SECS", value);
        }
        let error = load_runtime_config(&overrides).unwrap_err();
        assert!(format!("{error:#}").contains("PROVIDER_TIMEOUT_SECS"));
    }
    fs::remove_dir_all(dir).unwrap();
}

fn unique_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "coconutclaw_cfg_test_{}_{}_{}",
        std::process::id(),
        unique,
        sequence
    ))
}

fn write_config(instance_dir: &Path, body: &str) {
    fs::create_dir_all(instance_dir).expect("mkdir instance");
    fs::write(instance_dir.join("config.toml"), body).expect("write config.toml");
}

fn write_legacy_env(instance_dir: &Path, body: &str) {
    fs::create_dir_all(instance_dir).expect("mkdir instance");
    fs::write(instance_dir.join(".env"), body).expect("write .env");
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct TestEnvGuard {
    _lock: MutexGuard<'static, ()>,
    saved: Vec<(String, Option<String>)>,
}

impl TestEnvGuard {
    fn clear_config_env() -> Self {
        let lock = env_lock().lock().expect("lock env");
        let mut keys: Vec<&str> = DEFAULT_CONFIG_TOML
            .lines()
            .filter_map(|line| line.split_once('=').map(|(key, _)| key.trim()))
            .collect();
        keys.push("COCONUTCLAW_INSTANCE");
        keys.push("COCONUTCLAW_DATA_DIR");

        let mut saved = Vec::new();
        for key in keys {
            saved.push((key.to_string(), env::var(key).ok()));
            // SAFETY: Tests in this module hold a global lock while mutating process env.
            unsafe {
                env::remove_var(key);
            }
        }

        Self { _lock: lock, saved }
    }
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            // SAFETY: Tests in this module hold a global lock while mutating process env.
            unsafe {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }
}

fn load_runtime_config_isolated(instance_dir: PathBuf) -> Result<RuntimeConfig> {
    let _guard = TestEnvGuard::clear_config_env();
    load_runtime_config(&CliOverrides {
        data_dir: Some(instance_dir),
    })
}

#[test]
fn telegram_parse_mode_defaults_to_off() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert_eq!(cfg.telegram_parse_mode, TelegramParseMode::Off);
}

#[test]
fn telegram_parse_mode_accepts_markdown_v2() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\nTELEGRAM_PARSE_MODE = \"MarkdownV2\"\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert_eq!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2);
}

#[test]
fn telegram_parse_mode_accepts_html() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\nTELEGRAM_PARSE_MODE = \"Html\"\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert_eq!(cfg.telegram_parse_mode, TelegramParseMode::Html);
}

#[test]
fn telegram_parse_mode_rejects_invalid_value() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\nTELEGRAM_PARSE_MODE = \"Markdown\"\n",
    );

    let err =
        load_runtime_config_isolated(instance_dir).expect_err("invalid parse mode should fail");

    let text = format!("{err:#}");
    assert!(text.contains("invalid TELEGRAM_PARSE_MODE"));
}

#[test]
fn telegram_parse_fallback_defaults_to_plain() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert_eq!(cfg.telegram_parse_fallback, TelegramParseFallback::Plain);
}

#[test]
fn telegram_parse_fallback_accepts_none() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\nTELEGRAM_PARSE_FALLBACK = \"none\"\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert_eq!(cfg.telegram_parse_fallback, TelegramParseFallback::None);
}

#[test]
fn telegram_parse_fallback_rejects_invalid_value() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\nTELEGRAM_PARSE_FALLBACK = \"retry\"\n",
    );

    let err = load_runtime_config_isolated(instance_dir).expect_err("invalid fallback should fail");

    let text = format!("{err:#}");
    assert!(text.contains("invalid TELEGRAM_PARSE_FALLBACK"));
}

#[test]
fn telegram_chat_ids_accepts_csv_allowlist() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\nTELEGRAM_CHAT_IDS = \"999, 555\"\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert_eq!(
        cfg.telegram_chat_ids,
        vec!["999".to_string(), "555".to_string()]
    );
}

#[test]
fn legacy_env_is_preserved_with_actionable_error() {
    let instance_dir = unique_dir();
    write_legacy_env(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN=123:token\nTELEGRAM_CHAT_ID=321\nTELEGRAM_PARSE_MODE=MarkdownV2\n",
    );

    let before = fs::read(instance_dir.join(".env")).unwrap();
    let err = load_runtime_config_isolated(instance_dir.clone()).unwrap_err();
    assert!(err.to_string().contains("docs/migration-phase-0.md"));
    assert!(!instance_dir.join("config.toml").exists());
    assert_eq!(fs::read(instance_dir.join(".env")).unwrap(), before);
}

#[test]
fn ignores_legacy_env_when_config_toml_exists() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\nTELEGRAM_CHAT_ID = \"321\"\nTELEGRAM_PARSE_MODE = \"off\"\n",
    );
    write_legacy_env(&instance_dir, "TELEGRAM_PARSE_MODE=MarkdownV2\n");

    let cfg = load_runtime_config_isolated(instance_dir.clone()).expect("config");

    assert_eq!(cfg.telegram_parse_mode, TelegramParseMode::Off);
    assert!(instance_dir.join(".env").exists());
}

#[test]
fn webhook_native_fields_are_loaded() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\n\
TELEGRAM_CHAT_ID = \"321\"\n\
WEBHOOK_MODE = \"on\"\n\
WEBHOOK_BIND = \"127.0.0.1:8789\"\n\
WEBHOOK_PUBLIC_URL = \"https://claw.example\"\n\
WEBHOOK_SECRET = \"secret-token\"\n\
WEBHOOK_PATH = \"/telegram/webhook\"\n",
    );

    let err = load_runtime_config_isolated(instance_dir).unwrap_err();
    assert!(err.to_string().contains("removed setting WEBHOOK_"));
}

#[test]
fn toml_keys_keep_their_declared_types() {
    for body in [
        "CONTEXT_TURNS = 1.2",
        "CODEX_MODEL = true",
        "CODEX_BIN = ['codex']",
        "[CODEX_BIN]\nvalue='codex'",
    ] {
        let dir = unique_dir();
        write_config(&dir, body);
        assert!(load_runtime_config_isolated(dir.clone()).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn removed_config_is_rejected_even_when_disabled_and_files_are_untouched() {
    for key in [
        "AGENT_PROVIDER",
        "SLACK_APP_TOKEN",
        "SLACK_BOT_TOKEN",
        "ASR_URL",
        "TTS_CMD_TEMPLATE",
        "SCHEDULED_TASKS_ENABLED",
        "NIGHTLY_REFLECTION_PROMPT",
        "PI_BIN",
        "CLAUDE_BIN",
        "OPENCODE_BIN",
        "ANTIGRAVITY_BIN",
        "FACTORY_BIN",
        "WEBHOOK_MODE",
        "VISION_MODEL",
        "HEARTBEAT_PROMPT",
    ] {
        let dir = unique_dir();
        let body = format!("{key} = \"\"\n");
        write_config(&dir, &body);
        fs::write(dir.join("MEMORY.md"), "manual memory").unwrap();
        let error = load_runtime_config_isolated(dir.clone()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("removed setting {key}"))
        );
        assert_eq!(fs::read_to_string(dir.join("config.toml")).unwrap(), body);
        assert_eq!(
            fs::read_to_string(dir.join("MEMORY.md")).unwrap(),
            "manual memory"
        );
        assert!(!dir.join("state.db").exists());
    }
}

#[test]
fn obsolete_environment_recognizes_only_runtime_settings() {
    assert!(obsolete_env_key("FACTORY_BIN"));
    assert!(obsolete_env_key("SLACK_APP_TOKEN"));
    assert!(!obsolete_env_key("FACTORY_SESSION_ID"));
}
