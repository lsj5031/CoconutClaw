use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use clap::{Args, Parser, Subcommand};
use coconutclaw_config::{CliOverrides, RuntimeConfig, load_runtime_config};
use coconutclaw_provider::run_provider;
use fs2::FileExt;
use rusqlite::{Connection, params};
use serde_json::Value;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

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
    update_id: Option<String>,
}

#[derive(Debug, Clone)]
struct QuotedMessage {
    reply_from: Option<String>,
    reply_text: Option<String>,
}

#[derive(Debug, Clone)]
struct WebhookTurn {
    update_id: Option<String>,
    chat_id: String,
    input: TurnInput,
    quoted: QuotedMessage,
}

#[derive(Debug, Clone, Copy)]
enum SourceMode {
    Webhook,
    WebhookRestore,
}

#[derive(Debug, Clone)]
enum WebhookAction {
    Ignore {
        update_id: Option<String>,
    },
    Fresh {
        update_id: Option<String>,
        chat_id: String,
    },
    Cancel {
        update_id: Option<String>,
    },
    Turn(WebhookTurn),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckStatus {
    Acked,
    Empty,
    HeadMismatch,
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

    fn insert_turn(&self, turn: &TurnRecord) -> Result<bool> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, codex_raw, telegram_reply, voice_reply, status, update_id)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
                turn.update_id,
            ],
        )?;
        Ok(self.conn.changes() > 0)
    }

    fn insert_task(&self, ts: &str, source: &str, content: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO tasks(ts, source, content, done) VALUES(?1, ?2, ?3, 0)",
            params![ts, source, content],
        )?;
        Ok(())
    }

    fn insert_boundary_turn(
        &self,
        ts: &str,
        chat_id: &str,
        update_id: Option<&str>,
    ) -> Result<bool> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, codex_raw, telegram_reply, voice_reply, status, update_id)
             VALUES(?1, ?2, 'system', '---CONTEXT_BOUNDARY---', '', '', '', '', 'boundary', ?3)",
            params![ts, chat_id, update_id],
        )?;
        Ok(self.conn.changes() > 0)
    }

    fn turn_exists_for_update_id(&self, update_id: &str) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare("SELECT 1 FROM turns WHERE update_id = ?1 LIMIT 1")?;
        let mut rows = stmt.query(params![update_id])?;
        Ok(rows.next()?.is_some())
    }

    fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM kv WHERE key = ?1 LIMIT 1")?;
        let mut rows = stmt.query(params![key])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(row.get::<_, String>(0)?));
        }
        Ok(None)
    }

    fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO kv(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    fn clear_inflight(&self) -> Result<()> {
        self.conn.execute(
            "DELETE FROM kv WHERE key IN ('inflight_update_id', 'inflight_update_json', 'inflight_started_at')",
            [],
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
    let output = process_turn(
        cfg,
        store,
        input,
        args.chat_id.clone(),
        None,
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
        },
    )?;
    print!("{output}");
    io::stdout().flush().ok();
    Ok(())
}

fn run_run(cfg: &RuntimeConfig, store: &Store, args: &RunArgs) -> Result<()> {
    if args.inject_text.is_some() || args.inject_file.is_some() {
        let input = resolve_turn_input(args.inject_text.clone(), args.inject_file.clone(), cfg)?;
        let output = process_turn(
            cfg,
            store,
            input,
            args.chat_id.clone(),
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
            },
        )?;
        print!("{output}");
        io::stdout().flush().ok();
        return Ok(());
    }

    let shutdown = install_shutdown_handler()?;

    if cfg.webhook_mode {
        run_webhook_loop(cfg, store, &shutdown)?;
        return Ok(());
    }

    let stdin = io::stdin();
    let mut handled = 0usize;
    for line in stdin.lock().lines() {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
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
        let output = process_turn(
            cfg,
            store,
            input,
            args.chat_id.clone(),
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
            },
        )?;
        print!("{output}\n");
        io::stdout().flush().ok();
    }

    if handled == 0 && !shutdown.load(Ordering::SeqCst) {
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
    println!("webhook_mode={}", yes_no(cfg.webhook_mode));
    println!("poll_interval_seconds={}", cfg.poll_interval_seconds);
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

fn install_shutdown_handler() -> Result<Arc<AtomicBool>> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_flag = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        signal_flag.store(true, Ordering::SeqCst);
    })
    .context("failed to register shutdown signal handler")?;
    Ok(shutdown)
}

fn run_webhook_loop(cfg: &RuntimeConfig, store: &Store, shutdown: &Arc<AtomicBool>) -> Result<()> {
    let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
    if let Some(parent) = queue_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if !queue_path.exists() {
        fs::write(&queue_path, "")
            .with_context(|| format!("failed to initialize {}", queue_path.display()))?;
    }

    if let Err(err) = restore_inflight_update(cfg, store) {
        eprintln!("warn: failed to restore inflight webhook update: {err:#}");
    }

    while !shutdown.load(Ordering::SeqCst) {
        let progressed = drain_webhook_queue(cfg, store, shutdown)?;
        if !progressed {
            thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
        }
    }

    eprintln!("info: shutdown signal received, stopping webhook loop");
    Ok(())
}

fn restore_inflight_update(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    let Some(inflight_json) = store.kv_get("inflight_update_json")? else {
        return Ok(());
    };

    let mut inflight_update_id = store.kv_get("inflight_update_id")?;
    if inflight_update_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        inflight_update_id = extract_update_id_from_json(&inflight_json)?;
    }

    if let Some(update_id) = inflight_update_id.as_deref() {
        if store.turn_exists_for_update_id(update_id)? {
            store.clear_inflight()?;
            match ack_webhook_queue_line(cfg, Some(update_id))? {
                AckStatus::Acked => {
                    eprintln!("info: restored inflight update_id={update_id} (dedup + ack)");
                }
                AckStatus::HeadMismatch => {
                    eprintln!(
                        "warn: inflight restore head mismatch for update_id={update_id}, leaving queue as-is"
                    );
                }
                AckStatus::Empty => {}
            }
            return Ok(());
        }
    }

    let outcome = process_webhook_line(cfg, store, &inflight_json, SourceMode::WebhookRestore)?;
    if outcome.should_ack {
        let expected_id = outcome
            .update_id
            .as_deref()
            .or(inflight_update_id.as_deref());
        match ack_webhook_queue_line(cfg, expected_id)? {
            AckStatus::Acked => {
                store.clear_inflight()?;
                if let Some(output) = outcome.output {
                    print!("{output}\n");
                    io::stdout().flush().ok();
                }
            }
            AckStatus::HeadMismatch => {
                eprintln!("warn: inflight restore ack skipped due queue head mismatch");
            }
            AckStatus::Empty => {
                store.clear_inflight()?;
            }
        }
    }

    Ok(())
}

fn drain_webhook_queue(
    cfg: &RuntimeConfig,
    store: &Store,
    shutdown: &Arc<AtomicBool>,
) -> Result<bool> {
    let mut progressed = false;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let Some(line) = peek_webhook_queue_line(cfg)? else {
            break;
        };
        let expected_update_id = extract_update_id_from_json(&line)?;

        let outcome = match process_webhook_line(cfg, store, &line, SourceMode::Webhook) {
            Ok(outcome) => outcome,
            Err(err) => {
                eprintln!("warn: webhook processing failed (will retry): {err:#}");
                break;
            }
        };

        if !outcome.should_ack {
            break;
        }

        match ack_webhook_queue_line(cfg, expected_update_id.as_deref())? {
            AckStatus::Acked => {
                store.clear_inflight()?;
                progressed = true;
                if let Some(output) = outcome.output {
                    print!("{output}\n");
                    io::stdout().flush().ok();
                }
            }
            AckStatus::HeadMismatch => {
                eprintln!(
                    "warn: webhook ack skipped due queue head mismatch update_id={}",
                    expected_update_id.as_deref().unwrap_or("unknown")
                );
                break;
            }
            AckStatus::Empty => {
                break;
            }
        }
    }

    Ok(progressed)
}

#[derive(Debug)]
struct ProcessOutcome {
    should_ack: bool,
    update_id: Option<String>,
    output: Option<String>,
}

fn process_webhook_line(
    cfg: &RuntimeConfig,
    store: &Store,
    line: &str,
    _mode: SourceMode,
) -> Result<ProcessOutcome> {
    let action = parse_webhook_action(cfg, line)?;

    match action {
        WebhookAction::Ignore { update_id } => {
            if let Some(update_id) = update_id.as_ref() {
                store.kv_set("last_update_id", update_id)?;
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                output: None,
            })
        }
        WebhookAction::Cancel { update_id } => {
            if let Some(update_id) = update_id.as_ref() {
                store.kv_set("last_update_id", update_id)?;
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                output: None,
            })
        }
        WebhookAction::Fresh { update_id, chat_id } => {
            let ts = iso_now(&cfg.timezone);
            let inserted = store.insert_boundary_turn(&ts, &chat_id, update_id.as_deref())?;
            if inserted {
                eprintln!("info: inserted context boundary for chat_id={chat_id}");
            }
            if let Some(update_id) = update_id.as_ref() {
                store.kv_set("last_update_id", update_id)?;
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                output: None,
            })
        }
        WebhookAction::Turn(turn) => {
            if let Some(update_id) = turn.update_id.as_deref() {
                if store.turn_exists_for_update_id(update_id)? {
                    store.kv_set("last_update_id", update_id)?;
                    return Ok(ProcessOutcome {
                        should_ack: true,
                        update_id: Some(update_id.to_string()),
                        output: None,
                    });
                }
            }

            set_inflight_update(
                store,
                turn.update_id.as_deref().unwrap_or(""),
                line,
                &cfg.timezone,
            )?;

            let output = process_turn(
                cfg,
                store,
                turn.input,
                Some(turn.chat_id),
                turn.update_id.clone(),
                &turn.quoted,
            )?;

            if let Some(update_id) = turn.update_id.as_ref() {
                store.kv_set("last_update_id", update_id)?;
            }

            Ok(ProcessOutcome {
                should_ack: true,
                update_id: turn.update_id,
                output: Some(output.trim_end().to_string()),
            })
        }
    }
}

fn parse_webhook_action(cfg: &RuntimeConfig, line: &str) -> Result<WebhookAction> {
    let value: Value = serde_json::from_str(line).context("invalid webhook JSON payload")?;
    let update_id = extract_update_id_from_value(&value);

    if let Some(callback_query) = value.get("callback_query") {
        let data = callback_query
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if data.eq_ignore_ascii_case("cancel") {
            let chat_id = callback_query
                .get("message")
                .and_then(|node| node.get("chat"))
                .and_then(|node| node.get("id"))
                .map(value_to_string);
            if is_allowed_chat(cfg, chat_id.as_deref()) {
                return Ok(WebhookAction::Cancel { update_id });
            }
        }
        return Ok(WebhookAction::Ignore { update_id });
    }

    let Some(message) = value.get("message") else {
        return Ok(WebhookAction::Ignore { update_id });
    };

    let chat_id = message
        .get("chat")
        .and_then(|node| node.get("id"))
        .map(value_to_string)
        .unwrap_or_default();
    if chat_id.trim().is_empty() || !is_allowed_chat(cfg, Some(&chat_id)) {
        return Ok(WebhookAction::Ignore { update_id });
    }

    let message_text = message
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| message.get("caption").and_then(Value::as_str))
        .unwrap_or_default();

    if message_text.trim().eq_ignore_ascii_case("/fresh") {
        return Ok(WebhookAction::Fresh { update_id, chat_id });
    }
    if message_text.trim().eq_ignore_ascii_case("/cancel") {
        return Ok(WebhookAction::Cancel { update_id });
    }

    let reply_from = message
        .get("reply_to_message")
        .and_then(|node| node.get("from"))
        .and_then(|node| node.get("first_name"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let reply_text = message
        .get("reply_to_message")
        .and_then(|node| node.get("text"))
        .and_then(Value::as_str)
        .or_else(|| {
            message
                .get("reply_to_message")
                .and_then(|node| node.get("caption"))
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned);

    let input = TurnInput {
        input_type: "text".to_string(),
        user_text: {
            let trimmed = message_text.trim();
            if trimmed.is_empty() {
                "(empty message)".to_string()
            } else {
                trimmed.to_string()
            }
        },
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
    };

    Ok(WebhookAction::Turn(WebhookTurn {
        update_id,
        chat_id,
        input,
        quoted: QuotedMessage {
            reply_from,
            reply_text,
        },
    }))
}

fn is_allowed_chat(cfg: &RuntimeConfig, chat_id: Option<&str>) -> bool {
    match (cfg.telegram_chat_id.as_deref(), chat_id) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(expected), Some(actual)) => expected == actual,
    }
}

fn set_inflight_update(
    store: &Store,
    update_id: &str,
    payload_json: &str,
    timezone: &str,
) -> Result<()> {
    store.kv_set("inflight_update_id", update_id)?;
    store.kv_set("inflight_update_json", payload_json)?;
    store.kv_set("inflight_started_at", &iso_now(timezone))?;
    Ok(())
}

fn peek_webhook_queue_line(cfg: &RuntimeConfig) -> Result<Option<String>> {
    with_webhook_lock(cfg, || {
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        if !queue_path.exists() {
            return Ok(None);
        }
        let payload = fs::read_to_string(&queue_path)
            .with_context(|| format!("failed to read {}", queue_path.display()))?;
        for line in payload.lines() {
            if !line.trim().is_empty() {
                return Ok(Some(line.to_string()));
            }
        }
        Ok(None)
    })
}

fn ack_webhook_queue_line(
    cfg: &RuntimeConfig,
    expected_update_id: Option<&str>,
) -> Result<AckStatus> {
    with_webhook_lock(cfg, || {
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        if !queue_path.exists() {
            return Ok(AckStatus::Empty);
        }

        let payload = fs::read_to_string(&queue_path)
            .with_context(|| format!("failed to read {}", queue_path.display()))?;
        let mut lines: Vec<&str> = payload
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        if lines.is_empty() {
            return Ok(AckStatus::Empty);
        }

        let head = lines.remove(0);
        if let Some(expected) = expected_update_id {
            let head_update_id = extract_update_id_from_json(head)?;
            if head_update_id.as_deref() != Some(expected) {
                return Ok(AckStatus::HeadMismatch);
            }
        }

        let mut rewritten = lines.join("\n");
        if !rewritten.is_empty() {
            rewritten.push('\n');
        }
        fs::write(&queue_path, rewritten)
            .with_context(|| format!("failed to write {}", queue_path.display()))?;
        Ok(AckStatus::Acked)
    })
}

fn with_webhook_lock<T, F>(cfg: &RuntimeConfig, op: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let lock_path = cfg.runtime_dir.join("webhook_queue.lock");
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open {}", lock_path.display()))?;
    lock_file
        .lock_exclusive()
        .with_context(|| format!("failed to lock {}", lock_path.display()))?;

    let output = op();
    let _ = lock_file.unlock();
    output
}

fn extract_update_id_from_json(payload: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(payload).context("invalid update JSON")?;
    Ok(extract_update_id_from_value(&value))
}

fn extract_update_id_from_value(value: &Value) -> Option<String> {
    value.get("update_id").map(value_to_string).and_then(|id| {
        let trimmed = id.trim();
        if trimmed.is_empty() || trimmed == "0" {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn value_to_string(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    if let Some(num) = value.as_i64() {
        return num.to_string();
    }
    if let Some(num) = value.as_u64() {
        return num.to_string();
    }
    value.to_string()
}

fn process_turn(
    cfg: &RuntimeConfig,
    store: &Store,
    input: TurnInput,
    chat_id_override: Option<String>,
    update_id: Option<String>,
    quoted: &QuotedMessage,
) -> Result<String> {
    let ts = iso_now(&cfg.timezone);
    let chat_id = chat_id_override
        .or_else(|| cfg.telegram_chat_id.clone())
        .unwrap_or_else(|| "local".to_string());

    let context = build_context(cfg, store, &input, &ts, quoted)?;

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

    let inserted = store.insert_turn(&TurnRecord {
        ts: ts.clone(),
        chat_id,
        input_type: input.input_type,
        user_text: input.user_text,
        asr_text: input.asr_text,
        provider_raw: raw_output,
        telegram_reply: telegram_reply.clone(),
        voice_reply: voice_reply.clone(),
        status,
        update_id,
    })?;

    if inserted {
        append_memory_and_tasks(cfg, store, &ts, &markers)?;
    }

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
    quoted: &QuotedMessage,
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

    if let Some(reply_text) = quoted.reply_text.as_ref() {
        if !reply_text.trim().is_empty() {
            text.push_str("\n## Quoted/replied-to message\n");
            let reply_from = quoted.reply_from.as_deref().unwrap_or("someone");
            text.push_str(&format!("REPLY_FROM: {reply_from}\n"));
            text.push_str(&format!("REPLY_TEXT: {reply_text}\n"));
            text.push_str(
                "The user is replying to the above message. Use it as context for understanding their intent.\n",
            );
        }
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
