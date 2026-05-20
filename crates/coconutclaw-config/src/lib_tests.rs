use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

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
        let mut keys: Vec<&str> = MIGRATABLE_ENV_KEYS.to_vec();
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
        instance: None,
        data_dir: None,
        instance_dir: Some(instance_dir),
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
fn migrates_legacy_env_to_config_toml() {
    let instance_dir = unique_dir();
    write_legacy_env(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN=123:token\nTELEGRAM_CHAT_ID=321\nTELEGRAM_PARSE_MODE=MarkdownV2\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir.clone()).expect("config");

    assert_eq!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2);
    assert!(instance_dir.join("config.toml").exists());
    assert!(!instance_dir.join(".env").exists());
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

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert!(cfg.webhook_mode);
    assert_eq!(cfg.webhook_bind, "127.0.0.1:8789");
    assert_eq!(
        cfg.webhook_public_url.as_deref(),
        Some("https://claw.example")
    );
    assert_eq!(cfg.webhook_secret.as_deref(), Some("secret-token"));
    assert_eq!(cfg.webhook_path, "/telegram/webhook");
}

#[test]
fn antigravity_provider_loads_from_config() {
    let instance_dir = unique_dir();
    write_config(
        &instance_dir,
        "TELEGRAM_BOT_TOKEN = \"123:token\"\n\
TELEGRAM_CHAT_ID = \"321\"\n\
AGENT_PROVIDER = \"antigravity\"\n\
ANTIGRAVITY_BIN = \"antigravity\"\n\
ANTIGRAVITY_MODEL = \"antigravity-pro\"\n\
ANTIGRAVITY_REASONING_EFFORT = \"high\"\n",
    );

    let cfg = load_runtime_config_isolated(instance_dir).expect("config");

    assert_eq!(cfg.provider, AgentProvider::Antigravity);
    assert_eq!(cfg.antigravity.bin, "antigravity");
    assert_eq!(cfg.antigravity.model.as_deref(), Some("antigravity-pro"));
    assert_eq!(cfg.antigravity.reasoning_effort.as_deref(), Some("high"));
}

#[test]
fn toml_scalar_to_string_works() {
    assert_eq!(
        toml_scalar_to_string(&toml::Value::String("hello".to_string())),
        Some("hello".to_string())
    );
    assert_eq!(
        toml_scalar_to_string(&toml::Value::Integer(123)),
        Some("123".to_string())
    );
    assert_eq!(
        toml_scalar_to_string(&toml::Value::Float(1.23)),
        Some("1.23".to_string())
    );
    assert_eq!(
        toml_scalar_to_string(&toml::Value::Boolean(true)),
        Some("true".to_string())
    );
    assert_eq!(
        toml_scalar_to_string(&toml::Value::Boolean(false)),
        Some("false".to_string())
    );
    assert_eq!(toml_scalar_to_string(&toml::Value::Array(vec![])), None);
    assert_eq!(
        toml_scalar_to_string(&toml::Value::Table(toml::value::Table::new())),
        None
    );
}
