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
    pub telegram_chat_id: Option<String>,
    pub webhook_mode: bool,
    pub poll_interval_seconds: u64,
    pub provider: AgentProvider,
    pub exec_policy: String,
    pub codex: CodexConfig,
    pub pi: PiConfig,
    pub env_file_path: PathBuf,
}

pub struct InstanceLock {
    _file: File,
    pub path: PathBuf,
}

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

    let env_file_path = instance_dir.join(".env");
    let env_file = load_env_file(&env_file_path)?;

    let sqlite_db_path = resolve_path(
        &instance_dir,
        pick_value("SQLITE_DB_PATH", &env_file).unwrap_or_else(|| "./state.db".to_string()),
    );
    let log_dir = resolve_path(
        &instance_dir,
        pick_value("LOG_DIR", &env_file).unwrap_or_else(|| "./LOGS".to_string()),
    );
    let allowlist_path = resolve_path(
        &instance_dir,
        pick_value("ALLOWLIST_PATH", &env_file)
            .unwrap_or_else(|| "./config/allowlist.txt".to_string()),
    );

    let provider = AgentProvider::parse(
        &pick_value("AGENT_PROVIDER", &env_file).unwrap_or_else(|| "codex".to_string()),
    )?;
    let exec_policy = pick_value("EXEC_POLICY", &env_file)
        .unwrap_or_else(|| "yolo".to_string())
        .to_ascii_lowercase();
    let webhook_mode = pick_value("WEBHOOK_MODE", &env_file)
        .map(|value| value.eq_ignore_ascii_case("on"))
        .unwrap_or(false);
    let poll_interval_seconds = pick_value("POLL_INTERVAL_SECONDS", &env_file)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2);

    let pi_mode = pick_value("PI_MODE", &env_file)
        .unwrap_or_else(|| "text".to_string())
        .to_ascii_lowercase();
    if pi_mode != "text" && pi_mode != "json" {
        bail!("invalid PI_MODE: {pi_mode} (expected text or json)");
    }

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
        timezone: pick_value("TIMEZONE", &env_file).unwrap_or_else(|| "UTC".to_string()),
        telegram_chat_id: pick_value("TELEGRAM_CHAT_ID", &env_file),
        webhook_mode,
        poll_interval_seconds,
        provider,
        exec_policy,
        codex: CodexConfig {
            bin: pick_value("CODEX_BIN", &env_file).unwrap_or_else(|| "codex".to_string()),
            model: pick_value("CODEX_MODEL", &env_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            reasoning_effort: pick_value("CODEX_REASONING_EFFORT", &env_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        },
        pi: PiConfig {
            bin: pick_value("PI_BIN", &env_file).unwrap_or_else(|| "pi".to_string()),
            provider: pick_value("PI_PROVIDER", &env_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            model: pick_value("PI_MODEL", &env_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            mode: pi_mode,
            extra_args: pick_value("PI_EXTRA_ARGS", &env_file)
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        },
        env_file_path,
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

    if let Some(source) = preferred_source {
        if source.exists() {
            fs::copy(source, target).with_context(|| {
                format!(
                    "failed to copy template {} -> {}",
                    source.display(),
                    target.display()
                )
            })?;
            return Ok(());
        }
    }

    fs::write(target, fallback_content)
        .with_context(|| format!("failed to write {}", target.display()))?;
    Ok(())
}

fn pick_value(key: &str, env_file: &HashMap<String, String>) -> Option<String> {
    if let Ok(value) = env::var(key) {
        return Some(value);
    }
    env_file.get(key).cloned()
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
