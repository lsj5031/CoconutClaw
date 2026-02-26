use anyhow::{Context, Result, anyhow, bail};
use dotenvy::from_path_iter;
use fs2::FileExt;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProvider {
    Codex,
    Pi,
}

impl AgentProvider {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "codex" => Ok(Self::Codex),
            "pi" => Ok(Self::Pi),
            other => bail!("invalid AGENT_PROVIDER: {other} (expected codex or pi)"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Pi => "pi",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CliOverrides {
    pub instance: Option<String>,
    pub data_dir: Option<PathBuf>,
    pub instance_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct CodexConfig {
    pub bin: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PiConfig {
    pub bin: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub mode: String,
    pub extra_args: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root_dir: PathBuf,
    pub data_dir: PathBuf,
    pub instance_name: String,
    pub instance_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub tasks_dir: PathBuf,
    pub log_dir: PathBuf,
    pub sqlite_db_path: PathBuf,
    pub allowlist_path: PathBuf,
    pub timezone: String,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub telegram_parse_mode: TelegramParseMode,
    pub telegram_parse_fallback: TelegramParseFallback,
    pub webhook_mode: bool,
    pub webhook_bind: String,
    pub webhook_public_url: Option<String>,
    pub webhook_secret: Option<String>,
    pub webhook_path: String,
    pub poll_interval_seconds: u64,
    pub provider: AgentProvider,
    pub exec_policy: String,
    pub asr_url: Option<String>,
    pub asr_cmd_template: Option<String>,
    pub asr_file_field: Option<String>,
    pub asr_text_jq: Option<String>,
    pub asr_preprocess: Option<String>,
    pub asr_sample_rate: Option<String>,
    pub tts_cmd_template: Option<String>,
    pub voice_bitrate: Option<String>,
    pub tts_max_chars: Option<String>,
    pub nightly_reflection_file: PathBuf,
    pub nightly_reflection_skip_agent: bool,
    pub nightly_reflection_prompt: Option<String>,
    pub codex: CodexConfig,
    pub pi: PiConfig,
    pub config_file_path: PathBuf,
}

pub struct InstanceLock {
    _file: File,
    pub path: PathBuf,
}

const DEFAULT_CONFIG_TOML: &str = r#"# CoconutClaw runtime config
TELEGRAM_BOT_TOKEN = "replace_me"
TELEGRAM_CHAT_ID = "replace_me"
TELEGRAM_PARSE_MODE = "off"
TELEGRAM_PARSE_FALLBACK = "plain"

TIMEZONE = "UTC"
SQLITE_DB_PATH = "./state.db"
LOG_DIR = "./LOGS"
ALLOWLIST_PATH = "./config/allowlist.txt"
POLL_INTERVAL_SECONDS = 2
WEBHOOK_MODE = "off"
WEBHOOK_BIND = "127.0.0.1:8787"
WEBHOOK_PUBLIC_URL = ""
WEBHOOK_SECRET = ""
WEBHOOK_PATH = "/webhook"

AGENT_PROVIDER = "codex"
EXEC_POLICY = "yolo"
CODEX_BIN = "codex"
PI_BIN = "pi"
PI_MODE = "text"

ASR_URL = ""
ASR_CMD_TEMPLATE = ""
TTS_CMD_TEMPLATE = ""
VOICE_BITRATE = "32k"
TTS_MAX_CHARS = 260

NIGHTLY_REFLECTION_FILE = "./LOGS/nightly_reflection.md"
NIGHTLY_REFLECTION_SKIP_AGENT = "off"
NIGHTLY_REFLECTION_PROMPT = ""
"#;

const MIGRATABLE_ENV_KEYS: &[&str] = &[
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "TELEGRAM_PARSE_MODE",
    "TELEGRAM_PARSE_FALLBACK",
    "TIMEZONE",
    "SQLITE_DB_PATH",
    "LOG_DIR",
    "ALLOWLIST_PATH",
    "POLL_INTERVAL_SECONDS",
    "WEBHOOK_MODE",
    "WEBHOOK_BIND",
    "WEBHOOK_PUBLIC_URL",
    "WEBHOOK_SECRET",
    "WEBHOOK_PATH",
    "AGENT_PROVIDER",
    "EXEC_POLICY",
    "CODEX_BIN",
    "CODEX_MODEL",
    "CODEX_REASONING_EFFORT",
    "PI_BIN",
    "PI_PROVIDER",
    "PI_MODEL",
    "PI_MODE",
    "PI_EXTRA_ARGS",
    "ASR_URL",
    "ASR_CMD_TEMPLATE",
    "ASR_FILE_FIELD",
    "ASR_TEXT_JQ",
    "ASR_PREPROCESS",
    "ASR_SAMPLE_RATE",
    "TTS_CMD_TEMPLATE",
    "VOICE_BITRATE",
    "TTS_MAX_CHARS",
    "NIGHTLY_REFLECTION_FILE",
    "NIGHTLY_REFLECTION_SKIP_AGENT",
    "NIGHTLY_REFLECTION_PROMPT",
];

impl RuntimeConfig {
    pub fn acquire_instance_lock(&self) -> Result<InstanceLock> {
        let lock_path = self.runtime_dir.join("instance.lock");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("failed to open lock file {}", lock_path.display()))?;

        file.try_lock_exclusive().map_err(|e| {
            anyhow!(
                "failed to lock instance directory {} (another process may be using it): {e}",
                self.instance_dir.display()
            )
        })?;

        Ok(InstanceLock {
            _file: file,
            path: lock_path,
        })
    }
}

pub fn load_runtime_config(overrides: &CliOverrides) -> Result<RuntimeConfig> {
    let cwd = env::current_dir().context("failed to read current directory")?;
    let root_dir = find_project_root(&cwd);

    let explicit_instance_dir = overrides
        .instance_dir
        .clone()
        .or_else(|| env::var("INSTANCE_DIR").ok().map(PathBuf::from));

    let (data_dir, instance_name, instance_dir) = if let Some(path) = explicit_instance_dir {
        let dir = absolutize(path, &cwd);
        let derived_name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .map(ToOwned::to_owned)
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| "default".to_string());
        let parent = dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| dir.clone());
        (parent, derived_name, dir)
    } else {
        let instance_name = overrides
            .instance
            .clone()
            .or_else(|| env::var("COCONUTCLAW_INSTANCE").ok())
            .unwrap_or_else(|| "default".to_string());
        validate_instance_name(&instance_name)?;

        let data_root = overrides
            .data_dir
            .clone()
            .or_else(|| env::var("COCONUTCLAW_DATA_DIR").ok().map(PathBuf::from))
            .map(|value| absolutize(value, &cwd))
            .unwrap_or_else(default_data_dir);
        let instance_dir = data_root.join(&instance_name);
        (data_root, instance_name, instance_dir)
    };

    let config_file_path = instance_dir.join("config.toml");
    let legacy_env_file_path = instance_dir.join(".env");
    migrate_legacy_env_if_needed(&config_file_path, &legacy_env_file_path)?;
    let config_file = load_config_file(&config_file_path)?;

    let sqlite_db_path = resolve_path(
        &instance_dir,
        pick_value("SQLITE_DB_PATH", &config_file).unwrap_or_else(|| "./state.db".to_string()),
    );
    let log_dir = resolve_path(
        &instance_dir,
        pick_value("LOG_DIR", &config_file).unwrap_or_else(|| "./LOGS".to_string()),
    );
    let allowlist_path = resolve_path(
        &instance_dir,
        pick_value("ALLOWLIST_PATH", &config_file)
            .unwrap_or_else(|| "./config/allowlist.txt".to_string()),
    );

    let provider = AgentProvider::parse(
        &pick_value("AGENT_PROVIDER", &config_file).unwrap_or_else(|| "codex".to_string()),
    )?;
    let exec_policy = pick_value("EXEC_POLICY", &config_file)
        .unwrap_or_else(|| "yolo".to_string())
        .to_ascii_lowercase();
    let webhook_mode = parse_on_off(pick_value("WEBHOOK_MODE", &config_file).as_deref(), false);
    let webhook_bind = pick_value("WEBHOOK_BIND", &config_file)
        .unwrap_or_else(|| "127.0.0.1:8787".to_string())
        .trim()
        .to_string();
    let webhook_public_url = normalize_optional(pick_value("WEBHOOK_PUBLIC_URL", &config_file));
    let webhook_secret = normalize_optional(pick_value("WEBHOOK_SECRET", &config_file));
    let webhook_path = normalize_webhook_path(pick_value("WEBHOOK_PATH", &config_file).as_deref());
    let poll_interval_seconds = pick_value("POLL_INTERVAL_SECONDS", &config_file)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2);
    let telegram_parse_mode = TelegramParseMode::parse(
        &pick_value("TELEGRAM_PARSE_MODE", &config_file).unwrap_or_else(|| "off".to_string()),
    )?;
    let telegram_parse_fallback = TelegramParseFallback::parse(
        &pick_value("TELEGRAM_PARSE_FALLBACK", &config_file).unwrap_or_else(|| "plain".to_string()),
    )?;

    let pi_mode = pick_value("PI_MODE", &config_file)
        .unwrap_or_else(|| "text".to_string())
        .to_ascii_lowercase();
    if pi_mode != "text" && pi_mode != "json" {
        bail!("invalid PI_MODE: {pi_mode} (expected text or json)");
    }

    let asr_url = normalize_optional(pick_value("ASR_URL", &config_file));
    let asr_cmd_template = normalize_optional(pick_value("ASR_CMD_TEMPLATE", &config_file));
    let asr_file_field = normalize_optional(pick_value("ASR_FILE_FIELD", &config_file));
    let asr_text_jq = normalize_optional(pick_value("ASR_TEXT_JQ", &config_file));
    let asr_preprocess = normalize_optional(pick_value("ASR_PREPROCESS", &config_file));
    let asr_sample_rate = normalize_optional(pick_value("ASR_SAMPLE_RATE", &config_file));
    let tts_cmd_template = normalize_optional(pick_value("TTS_CMD_TEMPLATE", &config_file));
    let voice_bitrate = normalize_optional(pick_value("VOICE_BITRATE", &config_file));
    let tts_max_chars = normalize_optional(pick_value("TTS_MAX_CHARS", &config_file));
    let nightly_reflection_file = resolve_path(
        &instance_dir,
        pick_value("NIGHTLY_REFLECTION_FILE", &config_file)
            .unwrap_or_else(|| "./LOGS/nightly_reflection.md".to_string()),
    );
    let nightly_reflection_skip_agent = parse_on_off(
        pick_value("NIGHTLY_REFLECTION_SKIP_AGENT", &config_file).as_deref(),
        false,
    );
    let nightly_reflection_prompt =
        normalize_optional(pick_value("NIGHTLY_REFLECTION_PROMPT", &config_file));

    let cfg = RuntimeConfig {
        root_dir,
        data_dir,
        instance_name,
        instance_dir: instance_dir.clone(),
        runtime_dir: instance_dir.join("runtime"),
        tmp_dir: instance_dir.join("tmp"),
        tasks_dir: instance_dir.join("TASKS"),
        log_dir,
        sqlite_db_path,
        allowlist_path,
        timezone: pick_value("TIMEZONE", &config_file).unwrap_or_else(|| "UTC".to_string()),
        telegram_bot_token: pick_value("TELEGRAM_BOT_TOKEN", &config_file),
        telegram_chat_id: pick_value("TELEGRAM_CHAT_ID", &config_file),
        telegram_parse_mode,
        telegram_parse_fallback,
        webhook_mode,
        webhook_bind,
        webhook_public_url,
        webhook_secret,
        webhook_path,
        poll_interval_seconds,
        provider,
        exec_policy,
        asr_url,
        asr_cmd_template,
        asr_file_field,
        asr_text_jq,
        asr_preprocess,
        asr_sample_rate,
        tts_cmd_template,
        voice_bitrate,
        tts_max_chars,
        nightly_reflection_file,
        nightly_reflection_skip_agent,
        nightly_reflection_prompt,
        codex: CodexConfig {
            bin: pick_value("CODEX_BIN", &config_file).unwrap_or_else(|| "codex".to_string()),
            model: pick_value("CODEX_MODEL", &config_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            reasoning_effort: pick_value("CODEX_REASONING_EFFORT", &config_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        },
        pi: PiConfig {
            bin: pick_value("PI_BIN", &config_file).unwrap_or_else(|| "pi".to_string()),
            provider: pick_value("PI_PROVIDER", &config_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            model: pick_value("PI_MODEL", &config_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            mode: pi_mode,
            extra_args: pick_value("PI_EXTRA_ARGS", &config_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        },
        config_file_path,
    };

    ensure_instance_layout(&cfg)?;
    Ok(cfg)
}

pub fn ensure_instance_layout(cfg: &RuntimeConfig) -> Result<()> {
    fs::create_dir_all(&cfg.instance_dir)
        .with_context(|| format!("failed to create {}", cfg.instance_dir.display()))?;
    fs::create_dir_all(&cfg.runtime_dir)
        .with_context(|| format!("failed to create {}", cfg.runtime_dir.display()))?;
    fs::create_dir_all(&cfg.tmp_dir)
        .with_context(|| format!("failed to create {}", cfg.tmp_dir.display()))?;
    fs::create_dir_all(&cfg.log_dir)
        .with_context(|| format!("failed to create {}", cfg.log_dir.display()))?;
    fs::create_dir_all(&cfg.tasks_dir)
        .with_context(|| format!("failed to create {}", cfg.tasks_dir.display()))?;

    ensure_file(
        &cfg.instance_dir.join("SOUL.md"),
        Some(&cfg.root_dir.join("SOUL.md")),
        "You are CoconutClaw, a calm and practical local agent.\n",
    )?;
    ensure_file(
        &cfg.instance_dir.join("MEMORY.md"),
        Some(&cfg.root_dir.join("MEMORY.md")),
        "# Long-Term Memory\n",
    )?;
    ensure_file(
        &cfg.instance_dir.join("TASKS/pending.md"),
        Some(&cfg.root_dir.join("TASKS/pending.md")),
        "# Pending Tasks\n",
    )?;
    ensure_file(
        &cfg.instance_dir.join("USER.md"),
        Some(&cfg.root_dir.join("USER.md")),
        "# User Profile\n",
    )?;
    if !cfg.config_file_path.exists() {
        ensure_file(
            &cfg.config_file_path,
            Some(&cfg.root_dir.join("config.toml.example")),
            DEFAULT_CONFIG_TOML,
        )?;
    }

    Ok(())
}

fn ensure_file(
    target: &Path,
    preferred_source: Option<&Path>,
    fallback_content: &str,
) -> Result<()> {
    if target.exists() {
        return Ok(());
    }

    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    if let Some(source) = preferred_source
        && source.exists()
    {
        fs::copy(source, target).with_context(|| {
            format!(
                "failed to copy template {} -> {}",
                source.display(),
                target.display()
            )
        })?;
        return Ok(());
    }

    fs::write(target, fallback_content)
        .with_context(|| format!("failed to write {}", target.display()))?;
    Ok(())
}

fn migrate_legacy_env_if_needed(
    config_file_path: &Path,
    legacy_env_file_path: &Path,
) -> Result<()> {
    if config_file_path.exists() || !legacy_env_file_path.exists() {
        return Ok(());
    }

    let legacy_env = load_env_file(legacy_env_file_path)?;
    let mut output = String::from("# Migrated from legacy .env by CoconutClaw.\n");
    for key in MIGRATABLE_ENV_KEYS {
        if let Some(value) = legacy_env.get(*key) {
            output.push_str(&format!("{key} = {}\n", toml::Value::String(value.clone())));
        }
    }

    if let Some(parent) = config_file_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(config_file_path, output)
        .with_context(|| format!("failed to write {}", config_file_path.display()))?;

    if let Err(err) = fs::remove_file(legacy_env_file_path) {
        tracing::warn!(
            "migrated legacy .env but failed to delete {}: {err}",
            legacy_env_file_path.display()
        );
    }

    tracing::info!("migrated legacy .env to {}", config_file_path.display());
    Ok(())
}

fn pick_value(key: &str, config_file: &HashMap<String, String>) -> Option<String> {
    if let Ok(value) = env::var(key) {
        return Some(value);
    }
    config_file.get(key).cloned()
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_webhook_path(raw: Option<&str>) -> String {
    let trimmed = raw.map(str::trim).unwrap_or_default();
    if trimmed.is_empty() || trimmed == "/" {
        return "/webhook".to_string();
    }
    if trimmed.starts_with('/') {
        return trimmed.to_string();
    }
    format!("/{trimmed}")
}

fn parse_on_off(value: Option<&str>, default: bool) -> bool {
    let Some(value) = value else {
        return default;
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => true,
        "off" | "false" | "0" | "no" => false,
        _ => default,
    }
}

fn load_config_file(path: &Path) -> Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let body =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let value: toml::Value =
        toml::from_str(&body).with_context(|| format!("failed to parse {}", path.display()))?;
    let table = value
        .as_table()
        .ok_or_else(|| anyhow!("{} must contain a top-level TOML table", path.display()))?;

    let mut output = HashMap::new();
    for (key, node) in table {
        if let Some(value) = toml_scalar_to_string(node) {
            output.insert(key.to_string(), value);
        } else {
            bail!(
                "unsupported value for key {} in {} (only string/int/float/bool are supported)",
                key,
                path.display()
            );
        }
    }
    Ok(output)
}

fn toml_scalar_to_string(value: &toml::Value) -> Option<String> {
    match value {
        toml::Value::String(v) => Some(v.clone()),
        toml::Value::Integer(v) => Some(v.to_string()),
        toml::Value::Float(v) => Some(v.to_string()),
        toml::Value::Boolean(v) => Some(v.to_string()),
        _ => None,
    }
}

fn load_env_file(path: &Path) -> Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let mut output = HashMap::new();
    for entry in
        from_path_iter(path).with_context(|| format!("failed to parse {}", path.display()))?
    {
        let (key, value) = entry.with_context(|| format!("invalid entry in {}", path.display()))?;
        output.insert(key, value);
    }
    Ok(output)
}

fn validate_instance_name(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("instance name cannot be empty");
    }
    if value
        .chars()
        .any(|ch| !matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.'))
    {
        bail!("instance name can only contain [a-zA-Z0-9_.-]");
    }
    Ok(())
}

fn resolve_path(base: &Path, raw: String) -> PathBuf {
    let candidate = PathBuf::from(raw);
    if candidate.is_absolute() {
        candidate
    } else {
        base.join(candidate)
    }
}

fn default_data_dir() -> PathBuf {
    if cfg!(windows) {
        if let Ok(value) = env::var("LOCALAPPDATA") {
            return PathBuf::from(value).join("CoconutClaw");
        }
        if let Some(path) = dirs::data_local_dir() {
            return path.join("CoconutClaw");
        }
    } else {
        if let Ok(value) = env::var("XDG_STATE_HOME") {
            return PathBuf::from(value).join("coconutclaw");
        }
        if let Some(home) = dirs::home_dir() {
            return home.join(".local/state/coconutclaw");
        }
    }

    env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(".coconutclaw/state")
}

fn absolutize(path: PathBuf, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

fn find_project_root(start: &Path) -> PathBuf {
    let mut current = start.to_path_buf();
    loop {
        if current.join("sql/schema.sql").exists() {
            return current;
        }
        if !current.pop() {
            break;
        }
    }
    start.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("coconutclaw_cfg_test_{unique}"))
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

        let err =
            load_runtime_config_isolated(instance_dir).expect_err("invalid fallback should fail");

        let text = format!("{err:#}");
        assert!(text.contains("invalid TELEGRAM_PARSE_FALLBACK"));
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramParseMode {
    Off,
    MarkdownV2,
}

impl TelegramParseMode {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "markdownv2" => Ok(Self::MarkdownV2),
            other => bail!("invalid TELEGRAM_PARSE_MODE: {other} (expected off or MarkdownV2)"),
        }
    }

    pub fn as_api_value(self) -> Option<&'static str> {
        match self {
            Self::Off => None,
            Self::MarkdownV2 => Some("MarkdownV2"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramParseFallback {
    Plain,
    None,
}

impl TelegramParseFallback {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "plain" => Ok(Self::Plain),
            "none" => Ok(Self::None),
            other => bail!("invalid TELEGRAM_PARSE_FALLBACK: {other} (expected plain or none)"),
        }
    }
}
