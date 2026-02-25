use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use clap::{Args, Parser, Subcommand};
use coconutclaw_config::{CliOverrides, RuntimeConfig, load_runtime_config};
use coconutclaw_provider::run_provider;
use rusqlite::{Connection, params};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

const SCHEMA_SQL: &str = include_str!("../../../sql/schema.sql");

#[derive(Parser, Debug)]
#[command(name = "coconutclaw", version, about = "CoconutClaw Rust CLI")]
struct Cli {
    #[arg(long, global = true)]
    instance: Option<String>,
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[arg(long = "instance-dir", global = true)]
    instance_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Once(TurnArgs),
    Run(RunArgs),
    Doctor,
}

#[derive(Args, Debug, Clone)]
struct TurnArgs {
    #[arg(long)]
    inject_text: Option<String>,
    #[arg(long)]
    inject_file: Option<PathBuf>,
    #[arg(long)]
    chat_id: Option<String>,
}

#[derive(Args, Debug, Clone)]
struct RunArgs {
    #[arg(long)]
    inject_text: Option<String>,
    #[arg(long)]
    inject_file: Option<PathBuf>,
    #[arg(long)]
    chat_id: Option<String>,
}

#[derive(Debug, Clone)]
struct TurnInput {
    input_type: String,
    user_text: String,
    asr_text: String,
    attachment_type: Option<String>,
    attachment_path: Option<PathBuf>,
}

#[derive(Debug, Default)]
struct ParsedMarkers {
    telegram_reply: Option<String>,
    voice_reply: Option<String>,
    send_photo: Vec<String>,
    send_document: Vec<String>,
    send_video: Vec<String>,
    memory_append: Vec<String>,
    task_append: Vec<String>,
}

#[derive(Debug)]
struct TurnRecord {
    ts: String,
    chat_id: String,
    input_type: String,
    user_text: String,
    asr_text: String,
    provider_raw: String,
    telegram_reply: String,
    voice_reply: String,
    status: String,
}

struct Store {
    conn: Connection,
}

impl Store {
    fn open(cfg: &RuntimeConfig) -> Result<Self> {
        if let Some(parent) = cfg.sqlite_db_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let conn = Connection::open(&cfg.sqlite_db_path)
            .with_context(|| format!("failed to open {}", cfg.sqlite_db_path.display()))?;
        conn.execute_batch(SCHEMA_SQL)
            .context("failed to apply sqlite schema")?;
        Ok(Self { conn })
    }

    fn recent_turns_snippet(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts || ' | in=' || COALESCE(REPLACE(user_text, char(10), ' '), '') || ' | out=' || COALESCE(REPLACE(COALESCE(telegram_reply, voice_reply), char(10), ' '), '')
             FROM turns
             WHERE status != 'boundary'
               AND id > COALESCE((SELECT MAX(id) FROM turns WHERE user_text = '---CONTEXT_BOUNDARY---'), 0)
             ORDER BY id DESC
             LIMIT 8",
        )?;

        let mut rows = stmt.query([])?;
        let mut lines = Vec::new();
        while let Some(row) = rows.next()? {
            lines.push(row.get::<_, String>(0)?);
        }
        Ok(lines)
    }

    fn insert_turn(&self, turn: &TurnRecord) -> Result<()> {
        self.conn.execute(
            "INSERT INTO turns(ts, chat_id, input_type, user_text, asr_text, codex_raw, telegram_reply, voice_reply, status, update_id)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)",
            params![
                turn.ts,
                turn.chat_id,
                turn.input_type,
                turn.user_text,
                turn.asr_text,
                turn.provider_raw,
                turn.telegram_reply,
                turn.voice_reply,
                turn.status,
            ],
        )?;
        Ok(())
    }

    fn insert_task(&self, ts: &str, source: &str, content: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO tasks(ts, source, content, done) VALUES(?1, ?2, ?3, 0)",
            params![ts, source, content],
        )?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let cfg = load_runtime_config(&CliOverrides {
        instance: cli.instance.clone(),
        data_dir: cli.data_dir.clone(),
        instance_dir: cli.instance_dir.clone(),
    })?;

    let _instance_lock = cfg.acquire_instance_lock()?;
    let store = Store::open(&cfg)?;

    match cli.command {
        Commands::Once(args) => run_once(&cfg, &store, &args),
        Commands::Run(args) => run_run(&cfg, &store, &args),
        Commands::Doctor => run_doctor(&cfg),
    }
}

fn run_once(cfg: &RuntimeConfig, store: &Store, args: &TurnArgs) -> Result<()> {
    let input = resolve_turn_input(args.inject_text.clone(), args.inject_file.clone(), cfg)?;
    let output = process_turn(cfg, store, input, args.chat_id.clone())?;
    print!("{output}");
    io::stdout().flush().ok();
    Ok(())
}

fn run_run(cfg: &RuntimeConfig, store: &Store, args: &RunArgs) -> Result<()> {
    if args.inject_text.is_some() || args.inject_file.is_some() {
        let input = resolve_turn_input(args.inject_text.clone(), args.inject_file.clone(), cfg)?;
        let output = process_turn(cfg, store, input, args.chat_id.clone())?;
        print!("{output}");
        io::stdout().flush().ok();
        return Ok(());
    }

    let stdin = io::stdin();
    let mut handled = 0usize;
    for line in stdin.lock().lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        handled += 1;
        let input = TurnInput {
            input_type: "text".to_string(),
            user_text: trimmed.to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
        };
        let output = process_turn(cfg, store, input, args.chat_id.clone())?;
        print!("{output}\n");
        io::stdout().flush().ok();
    }

    if handled == 0 {
        bail!("run expects --inject-text/--inject-file or piped text on stdin");
    }

    Ok(())
}

fn run_doctor(cfg: &RuntimeConfig) -> Result<()> {
    println!("CoconutClaw doctor");
    println!("instance_name={}", cfg.instance_name);
    println!("instance_dir={}", cfg.instance_dir.display());
    println!("data_dir={}", cfg.data_dir.display());
    println!("sqlite_db_path={}", cfg.sqlite_db_path.display());
    println!("provider={}", cfg.provider.as_str());
    println!("timezone={}", cfg.timezone);
    println!("env_file={}", cfg.env_file_path.display());

    let codex_ok = command_exists(&cfg.codex.bin);
    let pi_ok = command_exists(&cfg.pi.bin);
    let ffmpeg_ok = command_exists("ffmpeg");

    println!("check_codex_bin={} ({})", yes_no(codex_ok), cfg.codex.bin);
    println!("check_pi_bin={} ({})", yes_no(pi_ok), cfg.pi.bin);
    println!("check_ffmpeg={} (optional)", yes_no(ffmpeg_ok));
    println!(
        "check_asr_script={} (optional)",
        yes_no(cfg.root_dir.join("scripts/asr.sh").exists())
    );
    println!(
        "check_tts_script={} (optional)",
        yes_no(cfg.root_dir.join("scripts/tts.sh").exists())
    );

    Ok(())
}

fn process_turn(
    cfg: &RuntimeConfig,
    store: &Store,
    input: TurnInput,
    chat_id_override: Option<String>,
) -> Result<String> {
    let ts = iso_now(&cfg.timezone);
    let chat_id = chat_id_override
        .or_else(|| cfg.telegram_chat_id.clone())
        .unwrap_or_else(|| "local".to_string());

    let context = build_context(cfg, store, &input, &ts)?;

    let provider_result = run_provider(cfg, &context);
    let (raw_output, provider_success) = match provider_result {
        Ok(result) => {
            let _exit_code = result.exit_code;
            (result.raw_output, result.success)
        }
        Err(err) => (format!("{err:#}"), false),
    };

    let markers = parse_markers(&raw_output);
    let mut telegram_reply = markers.telegram_reply.clone().unwrap_or_default();
    let voice_reply = markers.voice_reply.clone().unwrap_or_default();
    let status: String;

    if telegram_reply.trim().is_empty() && voice_reply.trim().is_empty() {
        if provider_success {
            telegram_reply =
                "I hit a parser issue on my side. Please resend that in text while I recover."
                    .to_string();
            status = "parse_fallback".to_string();
        } else {
            let err_line = raw_output
                .lines()
                .find(|line| !line.trim().is_empty())
                .unwrap_or("Please check local logs and retry.");
            telegram_reply = format!("Agent execution failed locally. {err_line}");
            status = "agent_error".to_string();
        }
    } else if provider_success {
        status = "ok".to_string();
    } else {
        status = "agent_error".to_string();
    }

    append_memory_and_tasks(cfg, store, &ts, &markers)?;

    store.insert_turn(&TurnRecord {
        ts,
        chat_id,
        input_type: input.input_type,
        user_text: input.user_text,
        asr_text: input.asr_text,
        provider_raw: raw_output,
        telegram_reply: telegram_reply.clone(),
        voice_reply: voice_reply.clone(),
        status,
    })?;

    Ok(render_output(&telegram_reply, &voice_reply, &markers))
}

fn resolve_turn_input(
    inject_text: Option<String>,
    inject_file: Option<PathBuf>,
    cfg: &RuntimeConfig,
) -> Result<TurnInput> {
    let user_text = inject_text
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "(empty message)".to_string());

    if let Some(path) = inject_file {
        let resolved = resolve_instance_path(&cfg.instance_dir, path);
        if !resolved.exists() {
            bail!("inject file not found: {}", resolved.display());
        }

        let lower = resolved
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();

        let (input_type, attachment_type) = match lower.as_str() {
            "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" => {
                ("photo".to_string(), Some("photo".to_string()))
            }
            "mp4" | "mkv" | "avi" | "mov" | "webm" => {
                ("video".to_string(), Some("video".to_string()))
            }
            _ => ("document".to_string(), Some("document".to_string())),
        };

        return Ok(TurnInput {
            input_type,
            user_text,
            asr_text: String::new(),
            attachment_type,
            attachment_path: Some(resolved),
        });
    }

    Ok(TurnInput {
        input_type: "text".to_string(),
        user_text,
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
    })
}

fn resolve_instance_path(instance_dir: &Path, raw: PathBuf) -> PathBuf {
    if raw.is_absolute() {
        raw
    } else {
        instance_dir.join(raw)
    }
}

fn append_memory_and_tasks(
    cfg: &RuntimeConfig,
    store: &Store,
    ts: &str,
    markers: &ParsedMarkers,
) -> Result<()> {
    if !markers.memory_append.is_empty() {
        let memory_path = cfg.instance_dir.join("MEMORY.md");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&memory_path)
            .with_context(|| format!("failed to open {}", memory_path.display()))?;

        for line in &markers.memory_append {
            writeln!(file, "- {ts} | {line}")?;
        }
    }

    if !markers.task_append.is_empty() {
        let task_path = cfg.instance_dir.join("TASKS/pending.md");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&task_path)
            .with_context(|| format!("failed to open {}", task_path.display()))?;

        for line in &markers.task_append {
            writeln!(file, "- [ ] {line}")?;
            store.insert_task(ts, cfg.provider.as_str(), line)?;
        }
    }

    Ok(())
}

fn build_context(
    cfg: &RuntimeConfig,
    store: &Store,
    input: &TurnInput,
    ts: &str,
) -> Result<String> {
    let soul = read_or_default(
        &cfg.instance_dir.join("SOUL.md"),
        "You are CoconutClaw, a calm and practical local agent.\n",
    );
    let user = read_or_default(&cfg.instance_dir.join("USER.md"), "(missing USER.md)\n");
    let memory = read_or_default(&cfg.instance_dir.join("MEMORY.md"), "# Long-Term Memory\n");
    let tasks = read_or_default(
        &cfg.instance_dir.join("TASKS/pending.md"),
        "# Pending Tasks\n",
    );

    let mut text = String::new();
    text.push_str("# CoconutClaw Runtime Context\n\n");
    text.push_str(&format!("Timestamp: {ts}\n"));
    text.push_str(&format!("Input type: {}\n", input.input_type));
    text.push_str(&format!("Agent provider: {}\n", cfg.provider.as_str()));
    text.push_str(&format!("Exec policy: {}\n", cfg.exec_policy));
    text.push_str(&format!(
        "Allowlist path: {}\n\n",
        cfg.allowlist_path.display()
    ));

    text.push_str("## SOUL.md\n");
    text.push_str(&soul);
    if !soul.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## USER.md\n");
    text.push_str(&user);
    if !user.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## MEMORY.md\n");
    text.push_str(&memory);
    if !memory.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## TASKS/pending.md\n");
    text.push_str(&tasks);
    if !tasks.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## Recent turns\n");
    for line in store.recent_turns_snippet()? {
        text.push_str(&line);
        text.push('\n');
    }

    text.push_str("\n## Current user input\n");
    text.push_str(&format!("USER_TEXT: {}\n", input.user_text));
    if !input.asr_text.trim().is_empty() {
        text.push_str(&format!("ASR_TEXT: {}\n", input.asr_text));
    }
    if let (Some(attachment_type), Some(attachment_path)) =
        (&input.attachment_type, &input.attachment_path)
    {
        text.push_str(&format!("ATTACHMENT_TYPE: {attachment_type}\n"));
        text.push_str(&format!("ATTACHMENT_PATH: {}\n", attachment_path.display()));
        text.push_str(&format!(
            "The user sent a {attachment_type}. The file has been downloaded to the path above. You can access and analyze it using your tools.\n"
        ));
    }

    text.push_str("\n## Output requirements\n");
    text.push_str("Return only plain text marker lines. No prose before or after markers.\n");
    text.push_str("Required first line format:\n");
    text.push_str("TELEGRAM_REPLY: <reply text>\n");
    text.push_str("Optional additional lines:\n");
    text.push_str("VOICE_REPLY: <spoken reply text>\n");
    text.push_str("SEND_PHOTO: <absolute file path>\n");
    text.push_str("SEND_DOCUMENT: <absolute file path>\n");
    text.push_str("SEND_VIDEO: <absolute file path>\n");
    text.push_str("MEMORY_APPEND: <single memory line>\n");
    text.push_str("TASK_APPEND: <single task line>\n");
    text.push_str("Do not use markdown, code fences, or extra prefixes.\n");

    Ok(text)
}

fn render_output(telegram_reply: &str, voice_reply: &str, markers: &ParsedMarkers) -> String {
    let mut lines = Vec::new();
    lines.push(format!("TELEGRAM_REPLY: {telegram_reply}"));

    if !voice_reply.trim().is_empty() {
        lines.push(format!("VOICE_REPLY: {voice_reply}"));
    }

    for line in &markers.send_photo {
        lines.push(format!("SEND_PHOTO: {line}"));
    }
    for line in &markers.send_document {
        lines.push(format!("SEND_DOCUMENT: {line}"));
    }
    for line in &markers.send_video {
        lines.push(format!("SEND_VIDEO: {line}"));
    }
    for line in &markers.memory_append {
        lines.push(format!("MEMORY_APPEND: {line}"));
    }
    for line in &markers.task_append {
        lines.push(format!("TASK_APPEND: {line}"));
    }

    lines.join("\n") + "\n"
}

fn parse_markers(payload: &str) -> ParsedMarkers {
    ParsedMarkers {
        telegram_reply: first_marker("TELEGRAM_REPLY", payload),
        voice_reply: first_marker("VOICE_REPLY", payload),
        send_photo: all_markers("SEND_PHOTO", payload),
        send_document: all_markers("SEND_DOCUMENT", payload),
        send_video: all_markers("SEND_VIDEO", payload),
        memory_append: all_markers("MEMORY_APPEND", payload),
        task_append: all_markers("TASK_APPEND", payload),
    }
}

fn first_marker(marker: &str, payload: &str) -> Option<String> {
    for line in payload.lines() {
        if let Some(value) = strip_marker(marker, line) {
            return Some(value.to_string());
        }
    }
    None
}

fn all_markers(marker: &str, payload: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in payload.lines() {
        if let Some(value) = strip_marker(marker, line) {
            if !value.trim().is_empty() {
                out.push(value.to_string());
            }
        }
    }
    out
}

fn strip_marker<'a>(marker: &str, line: &'a str) -> Option<&'a str> {
    let prefix = format!("{marker}:");
    if let Some(rest) = line.strip_prefix(&prefix) {
        return Some(rest.trim_start());
    }
    None
}

fn read_or_default(path: &Path, fallback: &str) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| fallback.to_string())
}

fn iso_now(timezone: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    if let Ok(tz) = timezone.parse::<Tz>() {
        return now
            .with_timezone(&tz)
            .format("%Y-%m-%dT%H:%M:%S%z")
            .to_string();
    }
    now.format("%Y-%m-%dT%H:%M:%S%z").to_string()
}

fn command_exists(bin: &str) -> bool {
    let candidate = Path::new(bin);
    if candidate.is_absolute() || bin.contains('/') || bin.contains('\\') {
        return candidate.is_file();
    }

    let Some(paths) = env::var_os("PATH") else {
        return false;
    };

    for dir in env::split_paths(&paths) {
        let full = dir.join(bin);
        if full.is_file() {
            return true;
        }
        if cfg!(windows) {
            let exe = dir.join(format!("{bin}.exe"));
            if exe.is_file() {
                return true;
            }
        }
    }

    false
}

fn yes_no(value: bool) -> &'static str {
    if value { "ok" } else { "missing" }
}
