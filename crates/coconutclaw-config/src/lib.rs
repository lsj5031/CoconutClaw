use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct CliOverrides {
    pub data_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct CodexConfig {
    pub bin: String,
    pub model: Option<String>,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            bin: "codex".to_string(),
            model: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root_dir: PathBuf,
    pub data_dir: PathBuf,
    pub instance_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub tasks_dir: PathBuf,
    pub log_dir: PathBuf,
    pub sqlite_db_path: PathBuf,
    pub allowlist_path: PathBuf,
    pub timezone: String,
    pub telegram_bot_token: Option<String>,
    /// Runtime identity from Telegram getMe, never configuration or message text.
    pub telegram_bot_username: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub telegram_chat_ids: Vec<String>,
    pub telegram_api_base: Option<String>,
    pub telegram_file_base: Option<String>,
    pub telegram_parse_mode: TelegramParseMode,
    pub telegram_parse_fallback: TelegramParseFallback,
    pub poll_interval_seconds: u64,
    pub exec_policy: String,
    pub context_turns: u32,
    pub provider_max_retries: u32,
    pub progress_update_interval_secs: u64,
    pub telegram_api_timeout_secs: u64,
    pub telegram_api_retry_attempts: u32,
    pub provider_timeout_secs: u64,
    pub codex: CodexConfig,
    pub config_file_path: PathBuf,
}

pub struct InstanceLock {
    _file: File,
    pub path: PathBuf,
}

const DEFAULT_CONFIG_TOML: &str = include_str!("../../../config.toml.example");

pub struct RuntimeConfigBuilder(RuntimeConfig);

impl RuntimeConfigBuilder {
    pub fn telegram_bot_token(mut self, token: impl Into<Option<String>>) -> Self {
        self.0.telegram_bot_token = token.into();
        self
    }

    pub fn telegram_chat_id(mut self, chat_id: impl Into<Option<String>>) -> Self {
        self.0.telegram_chat_id = chat_id.into();
        self
    }

    pub fn telegram_chat_ids(mut self, chat_ids: Vec<String>) -> Self {
        self.0.telegram_chat_ids = chat_ids;
        self
    }

    pub fn telegram_parse_mode(mut self, mode: TelegramParseMode) -> Self {
        self.0.telegram_parse_mode = mode;
        self
    }

    pub fn telegram_parse_fallback(mut self, fallback: TelegramParseFallback) -> Self {
        self.0.telegram_parse_fallback = fallback;
        self
    }

    pub fn build(self) -> RuntimeConfig {
        self.0
    }
}

impl RuntimeConfig {
    pub fn builder(root: PathBuf) -> RuntimeConfigBuilder {
        RuntimeConfigBuilder(Self::test_config_at(root))
    }

    pub fn test_builder() -> RuntimeConfigBuilder {
        RuntimeConfigBuilder(Self::test_config())
    }

    pub fn test_config() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let counter = TEST_CONFIG_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = env::temp_dir().join(format!("coconutclaw_test_{unique}_{counter}"));
        Self::test_config_at(root)
    }

    pub fn test_config_at(root: PathBuf) -> Self {
        let cfg = Self {
            root_dir: root.clone(),
            data_dir: root.clone(),
            instance_dir: root.clone(),
            runtime_dir: root.join("runtime"),
            tmp_dir: root.join("tmp"),
            tasks_dir: root.join("TASKS"),
            log_dir: root.join("LOGS"),
            sqlite_db_path: root.join("state.db"),
            allowlist_path: root.join("config/allowlist.txt"),
            timezone: "UTC".to_string(),
            telegram_bot_token: Some("123:token".to_string()),
            telegram_bot_username: None,
            telegram_chat_id: Some("321".to_string()),
            telegram_chat_ids: Vec::new(),
            telegram_api_base: None,
            telegram_file_base: None,
            telegram_parse_mode: TelegramParseMode::Off,
            telegram_parse_fallback: TelegramParseFallback::Plain,
            poll_interval_seconds: 1,
            exec_policy: "safe".to_string(),
            context_turns: 8,
            provider_max_retries: 0,
            progress_update_interval_secs: 3,
            telegram_api_timeout_secs: 30,
            telegram_api_retry_attempts: 3,
            provider_timeout_secs: 300,
            codex: CodexConfig::default(),
            config_file_path: root.join("config.toml"),
        };
        ensure_instance_layout(&cfg).expect("layout");
        cfg
    }

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

    if env::var_os("COCONUTCLAW_INSTANCE").is_some() || env::var_os("INSTANCE_DIR").is_some() {
        bail!(
            "named instances and INSTANCE_DIR were removed; use --data-dir <existing directory>, see docs/migration-phase-0.md"
        );
    }
    let selected_dir = overrides
        .data_dir
        .clone()
        .or_else(|| env::var_os("COCONUTCLAW_DATA_DIR").map(PathBuf::from));
    if let Some(path) = &selected_dir {
        validate_path("COCONUTCLAW_DATA_DIR", path, true)?;
    }
    let instance_dir = selected_dir
        .map(|p| absolutize(p, &cwd))
        .unwrap_or_else(|| default_data_dir().join("default"));
    validate_path("COCONUTCLAW_DATA_DIR", &instance_dir, true)?;
    let data_dir = instance_dir.clone();

    let config_file_path = instance_dir.join("config.toml");
    let legacy_env_file_path = instance_dir.join(".env");
    if !config_file_path.exists() && legacy_env_file_path.exists() {
        bail!(
            "legacy .env is preserved in the selected data directory; convert it to config.toml using docs/migration-phase-0.md"
        );
    }
    let config_file = load_config_file(&config_file_path)?;
    validate_config_keys(&config_file)?;
    for key in DEFAULT_CONFIG_TOML
        .lines()
        .filter_map(|line| line.split_once('=').map(|(key, _)| key.trim()))
    {
        if env::var_os(key).is_some() && env::var(key).is_err() {
            bail!("invalid {key}: expected UTF-8 text");
        }
    }
    let mut obsolete = env::vars_os()
        .map(|(k, _)| k.to_string_lossy().into_owned())
        .filter(|k| obsolete_env_key(k))
        .collect::<Vec<_>>();
    obsolete.sort();
    if !obsolete.is_empty() {
        bail!(
            "removed environment settings: {}; unset them, see docs/migration-phase-0.md",
            obsolete.join(", ")
        );
    }

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

    let exec_policy = pick_value("EXEC_POLICY", &config_file)
        .unwrap_or_else(|| "safe".to_string())
        .to_ascii_lowercase();
    if !matches!(exec_policy.as_str(), "safe" | "yolo") {
        bail!("invalid EXEC_POLICY (expected safe or yolo)");
    }
    let number = |key, default, min, max| numeric_setting(&config_file, key, default, min, max);
    let poll_interval_seconds = number("POLL_INTERVAL_SECONDS", 2, 1, 3600)?;
    let telegram_parse_mode = TelegramParseMode::parse(
        &pick_value("TELEGRAM_PARSE_MODE", &config_file).unwrap_or_else(|| "off".to_string()),
    )?;
    let telegram_parse_fallback = TelegramParseFallback::parse(
        &pick_value("TELEGRAM_PARSE_FALLBACK", &config_file).unwrap_or_else(|| "plain".to_string()),
    )?;

    let context_turns = number("CONTEXT_TURNS", 8, 1, 10000)? as u32;
    let provider_max_retries = number("PROVIDER_MAX_RETRIES", 0, 0, 10)? as u32;
    let progress_update_interval_secs = number("PROGRESS_UPDATE_INTERVAL", 3, 1, 3600)?;
    let telegram_api_timeout_secs = number("TELEGRAM_API_TIMEOUT_SECS", 30, 1, 300)?;
    let telegram_api_retry_attempts = number("TELEGRAM_API_RETRY_ATTEMPTS", 3, 0, 10)? as u32;
    let provider_timeout_secs = number("PROVIDER_TIMEOUT_SECS", 300, 1, 86400)?;
    let cfg = RuntimeConfig {
        root_dir,
        data_dir,
        instance_dir: instance_dir.clone(),
        runtime_dir: instance_dir.join("runtime"),
        tmp_dir: instance_dir.join("tmp"),
        tasks_dir: instance_dir.join("TASKS"),
        log_dir,
        sqlite_db_path,
        allowlist_path,
        timezone: pick_value("TIMEZONE", &config_file).unwrap_or_else(|| "UTC".to_string()),
        telegram_bot_token: pick_value("TELEGRAM_BOT_TOKEN", &config_file),
        telegram_bot_username: None,
        telegram_chat_id: pick_value("TELEGRAM_CHAT_ID", &config_file),
        telegram_chat_ids: parse_csv_list(pick_value("TELEGRAM_CHAT_IDS", &config_file)),
        telegram_api_base: normalize_optional(pick_value("TELEGRAM_API_BASE", &config_file)),
        telegram_file_base: normalize_optional(pick_value("TELEGRAM_FILE_BASE", &config_file)),
        telegram_parse_mode,
        telegram_parse_fallback,
        poll_interval_seconds,
        exec_policy,
        context_turns,
        provider_max_retries,
        progress_update_interval_secs,
        telegram_api_timeout_secs,
        telegram_api_retry_attempts,
        provider_timeout_secs,
        codex: CodexConfig {
            bin: pick_value("CODEX_BIN", &config_file).unwrap_or_else(|| "codex".to_string()),
            model: pick_value("CODEX_MODEL", &config_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        },
        config_file_path,
    };

    validate_effective_config(&cfg)?;
    ensure_instance_layout(&cfg).map_err(|_| anyhow!("failed to initialize configured directories/files; check path permissions (values redacted)"))?;
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

    // Only treat `root_dir` as a template source when it actually points at a
    // CoconutClaw repo checkout. Otherwise `find_project_root` may have fallen
    // back to the user's cwd and we'd silently seed instance files from
    // unrelated documents that happen to share these names.
    let template_root: Option<&Path> = is_project_root(&cfg.root_dir).then_some(&cfg.root_dir);

    ensure_file(
        &cfg.instance_dir.join("SOUL.md"),
        template_root.map(|root| root.join("SOUL.md")).as_deref(),
        "You are CoconutClaw, a calm and practical local agent.\n",
    )?;
    ensure_file(
        &cfg.instance_dir.join("TASKS/pending.md"),
        template_root
            .map(|root| root.join("TASKS/pending.md"))
            .as_deref(),
        "# Pending Tasks\n",
    )?;
    if !cfg.config_file_path.exists() {
        ensure_file(
            &cfg.config_file_path,
            template_root
                .map(|root| root.join("config.toml.example"))
                .as_deref(),
            DEFAULT_CONFIG_TOML,
        )?;
    }

    Ok(())
}

fn is_project_root(path: &Path) -> bool {
    path.join("sql/schema.sql").exists()
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

fn obsolete_key(key: &str) -> bool {
    [
        "SLACK_",
        "WEBHOOK_",
        "PI_",
        "CLAUDE_",
        "OPENCODE_",
        "ANTIGRAVITY_",
        "FACTORY_",
        "ASR_",
        "TTS_",
        "VOICE_",
        "NIGHTLY_",
        "HEARTBEAT_",
        "VISION_",
    ]
    .iter()
    .any(|prefix| key.starts_with(prefix))
        || matches!(
            key,
            "AGENT_PROVIDER" | "SCHEDULED_TASKS_ENABLED" | "COCONUTCLAW_INSTANCE" | "INSTANCE_DIR"
        )
}

fn obsolete_env_key(key: &str) -> bool {
    // Do not mistake host tooling's FACTORY_SESSION_ID, etc. for app settings.
    matches!(
        key,
        "AGENT_PROVIDER"
            | "SCHEDULED_TASKS_ENABLED"
            | "COCONUTCLAW_INSTANCE"
            | "INSTANCE_DIR"
            | "SLACK_BOT_TOKEN"
            | "SLACK_USER_TOKEN"
            | "SLACK_APP_TOKEN"
            | "SLACK_CHANNEL_ID"
            | "SLACK_API_TIMEOUT_SECS"
            | "SLACK_SIGNING_SECRET"
            | "SLACK_ADMIN_USER_IDS"
            | "SLACK_FORMAT_MODE"
            | "SLACK_FORMAT_FALLBACK"
            | "SLACK_API_BASE"
            | "WEBHOOK_MODE"
            | "WEBHOOK_BIND"
            | "WEBHOOK_PUBLIC_URL"
            | "WEBHOOK_SECRET"
            | "WEBHOOK_PATH"
            | "PI_BIN"
            | "PI_MODEL"
            | "PI_REASONING_EFFORT"
            | "PI_NO_EXTENSIONS"
            | "PI_PROVIDER"
            | "PI_EXTRA_ARGS"
            | "CLAUDE_BIN"
            | "CLAUDE_MODEL"
            | "CLAUDE_REASONING_EFFORT"
            | "OPENCODE_BIN"
            | "OPENCODE_MODEL"
            | "OPENCODE_REASONING_EFFORT"
            | "OPENCODE_SKIP_PERMISSIONS"
            | "ANTIGRAVITY_BIN"
            | "ANTIGRAVITY_MODEL"
            | "ANTIGRAVITY_REASONING_EFFORT"
            | "FACTORY_BIN"
            | "FACTORY_MODEL"
            | "FACTORY_REASONING_EFFORT"
            | "ASR_URL"
            | "ASR_CMD_TEMPLATE"
            | "ASR_FILE_FIELD"
            | "ASR_TEXT_JQ"
            | "ASR_PREPROCESS"
            | "ASR_SAMPLE_RATE"
            | "TTS_CMD_TEMPLATE"
            | "TTS_MAX_CHARS"
            | "VOICE_BITRATE"
            | "NIGHTLY_REFLECTION_FILE"
            | "NIGHTLY_REFLECTION_SKIP_AGENT"
            | "NIGHTLY_REFLECTION_PROMPT"
    )
}

fn validate_config_keys(config: &HashMap<String, String>) -> Result<()> {
    for key in config.keys() {
        if obsolete_key(key) {
            bail!(
                "removed setting {key}; remove it from config.toml (data is unchanged), see docs/migration-phase-0.md"
            );
        }
        if !DEFAULT_CONFIG_TOML.lines().any(|line| {
            line.trim_start()
                .strip_prefix(key.as_str())
                .is_some_and(|rest| rest.trim_start().starts_with('='))
        }) {
            bail!("unknown setting {key}; see config.toml.example and docs/migration-phase-0.md");
        }
    }
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

fn parse_csv_list(value: Option<String>) -> Vec<String> {
    value
        .as_deref()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn load_config_file(path: &Path) -> Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let body = fs::read_to_string(path)
        .map_err(|_| anyhow!("failed to read config.toml; expected a readable UTF-8 file"))?;
    // TOML's error carries the source line, including credentials. Never attach
    // it as an anyhow source, even behind an otherwise sanitized outer message.
    let value: toml::Value = toml::from_str(&body).map_err(|error: toml::de::Error| {
        let line = error.span().map(|span| body.as_bytes()[..span.start.min(body.len())].iter().filter(|b| **b == b'\n').count() + 1);
        anyhow!("invalid config.toml syntax at line {}; check TOML quoting and assignments (values redacted)", line.unwrap_or(1))
    })?;
    let table = value
        .as_table()
        .ok_or_else(|| anyhow!("config.toml must contain a top-level TOML table"))?;

    let defaults: toml::Table = toml::from_str(DEFAULT_CONFIG_TOML).expect("typed config template");
    let mut output = HashMap::new();
    for (key, node) in table {
        validate_config_keys(&HashMap::from([(key.clone(), String::new())]))?;
        let expected = &defaults[key];
        let text = match (expected, node) {
            (toml::Value::String(_), toml::Value::String(text)) => text.clone(),
            (toml::Value::Integer(_), toml::Value::Integer(number)) => number.to_string(),
            (toml::Value::Integer(_), _) => bail!("invalid {key}: expected a TOML integer"),
            _ => bail!("invalid {key}: expected a TOML string"),
        };
        output.insert(key.clone(), text);
    }
    Ok(output)
}

fn numeric_setting(
    config: &HashMap<String, String>,
    key: &str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64> {
    match pick_value(key, config) {
        None => Ok(default),
        Some(value) => match value.parse::<u64>() {
            Ok(n) if (min..=max).contains(&n) => Ok(n),
            _ => bail!("invalid {key}: expected an integer in {min}..={max} (value redacted)"),
        },
    }
}

fn validate_path(key: &str, path: &Path, directory: bool) -> Result<()> {
    if path.as_os_str().is_empty()
        || path.as_os_str().to_string_lossy().trim().is_empty()
        || path
            .as_os_str()
            .to_string_lossy()
            .chars()
            .any(char::is_control)
    {
        bail!("invalid {key}: expected a nonempty path without control characters");
    }
    if path.exists()
        && (if directory {
            !path.is_dir()
        } else {
            !path.is_file()
        })
    {
        bail!(
            "invalid {key}: expected a {} path",
            if directory { "directory" } else { "file" }
        );
    }
    for parent in path.ancestors().skip(1) {
        if parent.exists() && !parent.is_dir() {
            bail!("invalid {key}: a parent is not a directory");
        }
    }
    Ok(())
}

fn validate_effective_config(cfg: &RuntimeConfig) -> Result<()> {
    validate_path("SQLITE_DB_PATH", &cfg.sqlite_db_path, false)?;
    validate_path("LOG_DIR", &cfg.log_dir, true)?;
    validate_path("ALLOWLIST_PATH", &cfg.allowlist_path, false)?;
    for dir in [&cfg.runtime_dir, &cfg.tmp_dir, &cfg.tasks_dir] {
        validate_path("COCONUTCLAW_DATA_DIR", dir, true)?;
    }
    let db = path_identity(&cfg.sqlite_db_path);
    let allowlist = path_identity(&cfg.allowlist_path);
    let reserved = [
        "config.toml",
        "SOUL.md",
        "USER.md",
        "MEMORY.md",
        "TASKS/pending.md",
    ]
    .map(|name| path_identity(&cfg.instance_dir.join(name)));
    if reserved
        .iter()
        .any(|file| db.starts_with(file) || file.starts_with(&db))
        || db.starts_with(&allowlist)
        || allowlist.starts_with(&db)
    {
        bail!("invalid SQLITE_DB_PATH / ALLOWLIST_PATH: state files must be distinct");
    }
    for (key, file) in [("SQLITE_DB_PATH", db), ("ALLOWLIST_PATH", allowlist)] {
        if [
            &cfg.instance_dir,
            &cfg.runtime_dir,
            &cfg.tmp_dir,
            &cfg.tasks_dir,
            &cfg.log_dir,
        ]
        .iter()
        .any(|dir| path_identity(dir).starts_with(&file))
        {
            bail!("invalid {key}: file path conflicts with a configured directory");
        }
    }
    if cfg.codex.bin.trim().is_empty() || cfg.codex.bin.chars().any(char::is_control) {
        bail!("invalid CODEX_BIN: expected a nonempty command without control characters");
    }
    if cfg.timezone.parse::<chrono_tz::Tz>().is_err() {
        bail!("invalid TIMEZONE: expected an IANA timezone name");
    }
    for (key, value) in [
        ("TELEGRAM_API_BASE", &cfg.telegram_api_base),
        ("TELEGRAM_FILE_BASE", &cfg.telegram_file_base),
    ] {
        if let Some(value) = value {
            let url = url::Url::parse(value)
                .map_err(|_| anyhow!("invalid {key}: expected an absolute HTTP(S) URL"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.fragment().is_some()
                || url.query().is_some()
            {
                bail!(
                    "invalid {key}: expected an HTTP(S) URL without credentials, query or fragment"
                );
            }
        }
    }
    Ok(())
}

fn path_identity(path: &Path) -> PathBuf {
    if let Ok(canonical) = fs::canonicalize(path) {
        return canonical;
    }
    if let (Some(parent), Some(name)) = (path.parent(), path.file_name()) {
        return path_identity(parent).join(name);
    }
    path.to_path_buf()
}

fn resolve_path(base: &Path, raw: String) -> PathBuf {
    if raw.trim().is_empty() || raw.chars().any(char::is_control) {
        // Leave an invalid sentinel for key-specific pre-layout validation.
        return PathBuf::new();
    }
    let candidate = PathBuf::from(raw);
    let path = if candidate.is_absolute() {
        candidate
    } else {
        base.join(candidate)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other),
        }
    }
    normalized
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

    let fallback = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    tracing::warn!(
        "neither XDG_STATE_HOME nor HOME is set; using {}",
        fallback.display()
    );
    fallback.join(".coconutclaw/state")
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramParseMode {
    Off,
    MarkdownV2,
    Html,
}

impl TelegramParseMode {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "markdownv2" => Ok(Self::MarkdownV2),
            "html" => Ok(Self::Html),
            _ => {
                bail!(
                    "invalid TELEGRAM_PARSE_MODE (expected off, MarkdownV2, or Html; value redacted)"
                )
            }
        }
    }

    pub fn as_api_value(self) -> Option<&'static str> {
        match self {
            Self::Off => None,
            Self::MarkdownV2 => Some("MarkdownV2"),
            Self::Html => Some("HTML"),
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
            _ => bail!("invalid TELEGRAM_PARSE_FALLBACK (expected plain or none; value redacted)"),
        }
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
