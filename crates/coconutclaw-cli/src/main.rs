use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use clap::{Args, Parser, Subcommand};
use coconutclaw_config::{
    CliOverrides, RuntimeConfig, TelegramParseFallback, TelegramParseMode, load_runtime_config,
};
use coconutclaw_provider::run_provider;
use fs2::FileExt;
use reqwest::blocking::{Client, multipart};
use rusqlite::{Connection, params};
use serde_json::Value;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant};

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
    Run(TurnArgs),
    Heartbeat,
    NightlyReflection,
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

#[derive(Debug, Clone)]
struct TurnInput {
    input_type: String,
    user_text: String,
    asr_text: String,
    attachment_type: Option<String>,
    attachment_path: Option<PathBuf>,
    attachment_owned: bool,
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
    media: Option<IncomingMedia>,
    quoted: QuotedMessage,
}

#[derive(Debug, Clone)]
enum IncomingMedia {
    Voice {
        file_id: String,
    },
    Photo {
        file_id: String,
    },
    Document {
        file_id: String,
        file_name: Option<String>,
    },
    Video {
        file_id: String,
    },
    VideoNote {
        file_id: String,
    },
}

#[derive(Debug, Clone)]
struct CancelSignal {
    callback_query_id: Option<String>,
}

#[derive(Debug, Clone)]
enum WebhookAction {
    Ignore {
        update_id: Option<String>,
        reason: String,
    },
    Fresh {
        update_id: Option<String>,
        chat_id: String,
    },
    Cancel {
        update_id: Option<String>,
    },
    Turn(Box<WebhookTurn>),
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

    fn rendered_output_for_update_id(&self, update_id: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT codex_raw, telegram_reply, voice_reply
             FROM turns
             WHERE update_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![update_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let codex_raw: String = row.get(0)?;
        let telegram_reply: String = row.get(1)?;
        let voice_reply: String = row.get(2)?;
        let mut markers = parse_markers(&codex_raw);
        if !telegram_reply.trim().is_empty() {
            markers.telegram_reply = Some(telegram_reply.clone());
        }
        if !voice_reply.trim().is_empty() {
            markers.voice_reply = Some(voice_reply.clone());
        }

        let rendered = render_output(
            markers.telegram_reply.as_deref().unwrap_or_default(),
            markers.voice_reply.as_deref().unwrap_or_default(),
            &markers,
        );
        Ok(Some(rendered.trim_end().to_string()))
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

    fn max_turn_id(&self) -> Result<i64> {
        let id: i64 = self
            .conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM turns", [], |row| {
                row.get(0)
            })?;
        Ok(id)
    }

    fn latest_turn_for_prompt_after_id(
        &self,
        after_id: i64,
        prompt: &str,
    ) -> Result<Option<(String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, COALESCE(NULLIF(telegram_reply, ''), NULLIF(voice_reply, ''), ''), status
             FROM turns
             WHERE id > ?1
               AND user_text = ?2
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![after_id, prompt])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
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
        Commands::Heartbeat => run_heartbeat(&cfg, &store),
        Commands::NightlyReflection => run_nightly_reflection(&cfg, &store),
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

fn run_run(cfg: &RuntimeConfig, store: &Store, args: &TurnArgs) -> Result<()> {
    if args.inject_text.is_some() || args.inject_file.is_some() {
        let input = resolve_turn_input(args.inject_text.clone(), args.inject_file.clone(), cfg)?;
        let output = process_turn(
            cfg,
            store,
            input,
            args.chat_id.clone(),
            None,
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
    let telegram_client = build_telegram_client(cfg)?;

    if cfg.webhook_mode {
        run_webhook_loop(cfg, store, &telegram_client, &shutdown)?;
        return Ok(());
    }

    run_poll_loop(cfg, store, &telegram_client, &shutdown)
}

fn run_heartbeat(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    let prompt = "Daily heartbeat for CoconutClaw. Summarize today, surface urgent tasks from TASKS/pending.md, and suggest next 1-3 actions.";
    let output = process_turn(
        cfg,
        store,
        TurnInput {
            input_type: "text".to_string(),
            user_text: prompt.to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
        },
        cfg.telegram_chat_id.clone(),
        None,
        None,
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
        },
    )?;

    let client = build_telegram_client(cfg)?;
    dispatch_telegram_output(&client, cfg, cfg.telegram_chat_id.as_deref(), &output, None)?;
    print!("{output}");
    io::stdout().flush().ok();
    Ok(())
}

fn run_nightly_reflection(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    let reflection_path = nightly_reflection_file_path(cfg);
    let local_day = local_day(&cfg.timezone);
    let marker = nightly_reflection_marker(&local_day);
    let now_iso = iso_now(&cfg.timezone);

    if let Some(parent) = reflection_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if !reflection_path.exists() {
        fs::write(&reflection_path, "")
            .with_context(|| format!("failed to initialize {}", reflection_path.display()))?;
    }

    let existing = fs::read_to_string(&reflection_path)
        .with_context(|| format!("failed to read {}", reflection_path.display()))?;
    if existing.contains(&marker) {
        println!("{now_iso} [INFO] nightly reflection already exists for {local_day}");
        return Ok(());
    }

    let prompt = nightly_reflection_prompt(cfg);
    let before_id = store.max_turn_id()?;
    let skip_agent = cfg.nightly_reflection_skip_agent;
    if !skip_agent {
        let output = process_turn(
            cfg,
            store,
            TurnInput {
                input_type: "text".to_string(),
                user_text: prompt.clone(),
                asr_text: String::new(),
                attachment_type: None,
                attachment_path: None,
                attachment_owned: false,
            },
            cfg.telegram_chat_id.clone(),
            None,
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
            },
        )?;
        let client = build_telegram_client(cfg)?;
        dispatch_telegram_output(&client, cfg, cfg.telegram_chat_id.as_deref(), &output, None)?;
    }

    let (turn_ts, reflection_text, status) = if let Some((turn_ts, text, status)) =
        store.latest_turn_for_prompt_after_id(before_id, &prompt)?
    {
        if !text.trim().is_empty() {
            (turn_ts, text, status)
        } else {
            (
                "<none>".to_string(),
                "- Today outcomes:\n- Today insights:\n- Most important thing tomorrow:"
                    .to_string(),
                "template_only".to_string(),
            )
        }
    } else {
        (
            "<none>".to_string(),
            "- Today outcomes:\n- Today insights:\n- Most important thing tomorrow:".to_string(),
            "template_only".to_string(),
        )
    };

    let block = format!(
        "{marker}\n## {local_day} nightly reflection\n- generated_at: {now_iso}\n- source: coconutclaw nightly-reflection\n- turn_ts: {turn_ts}\n- status: {status}\n\n{reflection_text}\n\n"
    );
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&reflection_path)
        .with_context(|| format!("failed to open {}", reflection_path.display()))?;
    file.write_all(block.as_bytes())
        .with_context(|| format!("failed to write {}", reflection_path.display()))?;

    println!(
        "{now_iso} [INFO] nightly reflection appended to {}",
        reflection_path.display()
    );
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
    println!(
        "webhook_mode={}",
        if cfg.webhook_mode { "on" } else { "off" }
    );
    println!(
        "telegram_parse_mode={}",
        cfg.telegram_parse_mode.as_api_value().unwrap_or("off")
    );
    println!(
        "telegram_parse_fallback={}",
        match cfg.telegram_parse_fallback {
            TelegramParseFallback::Plain => "plain",
            TelegramParseFallback::None => "none",
        }
    );
    println!("poll_interval_seconds={}", cfg.poll_interval_seconds);
    println!("config_file={}", cfg.config_file_path.display());

    let codex_ok = command_exists(&cfg.codex.bin);
    let pi_ok = command_exists(&cfg.pi.bin);
    let ffmpeg_ok = command_exists("ffmpeg");
    let bash_ok = command_exists("bash");
    let curl_ok = command_exists("curl");
    let jq_ok = command_exists("jq");
    let telegram_token_ok = valid_telegram_token(cfg).is_some();
    let telegram_chat_id_ok = valid_telegram_chat_id(cfg).is_some();
    let asr_script_ok = cfg.root_dir.join("scripts/asr.sh").exists();
    let tts_script_ok = cfg.root_dir.join("scripts/tts.sh").exists();
    let asr_enabled = asr_feature_enabled(cfg);
    let asr_uses_http = cfg.asr_cmd_template.is_none() && cfg.asr_url.is_some();
    let asr_preprocess = parse_on_like(cfg.asr_preprocess.as_deref(), true);
    let tts_enabled = cfg.tts_cmd_template.is_some();

    println!("check_codex_bin={} ({})", yes_no(codex_ok), cfg.codex.bin);
    println!("check_pi_bin={} ({})", yes_no(pi_ok), cfg.pi.bin);
    println!(
        "check_telegram_token={} (required for run)",
        yes_no(telegram_token_ok)
    );
    println!(
        "check_telegram_chat_id={} (required for run)",
        yes_no(telegram_chat_id_ok)
    );
    println!("feature_asr={}", if asr_enabled { "on" } else { "off" });
    println!("feature_tts={}", if tts_enabled { "on" } else { "off" });

    if asr_enabled {
        println!(
            "check_asr_script={} (required when ASR is enabled)",
            yes_no(asr_script_ok)
        );
        println!(
            "check_bash={} (required when ASR is enabled)",
            yes_no(bash_ok)
        );
        if asr_preprocess {
            println!(
                "check_ffmpeg={} (required when ASR_PREPROCESS is enabled)",
                yes_no(ffmpeg_ok)
            );
        } else {
            println!("check_ffmpeg={} (ASR_PREPROCESS is off)", yes_no(ffmpeg_ok));
        }
        if asr_uses_http {
            println!("check_curl={} (required for ASR_URL mode)", yes_no(curl_ok));
            println!("check_jq={} (required for ASR_URL mode)", yes_no(jq_ok));
        } else {
            println!("check_curl={} (optional)", yes_no(curl_ok));
            println!("check_jq={} (optional)", yes_no(jq_ok));
        }
    } else {
        println!("check_asr_script={} (optional)", yes_no(asr_script_ok));
        println!("check_bash={} (optional)", yes_no(bash_ok));
        println!("check_ffmpeg={} (optional)", yes_no(ffmpeg_ok));
        println!("check_curl={} (optional)", yes_no(curl_ok));
        println!("check_jq={} (optional)", yes_no(jq_ok));
    }

    if tts_enabled {
        println!(
            "check_tts_script={} (required when TTS is enabled)",
            yes_no(tts_script_ok)
        );
        println!(
            "check_bash={} (required when TTS is enabled)",
            yes_no(bash_ok)
        );
        println!(
            "check_ffmpeg={} (required when TTS is enabled)",
            yes_no(ffmpeg_ok)
        );
    } else {
        println!("check_tts_script={} (optional)", yes_no(tts_script_ok));
    }

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

fn run_poll_loop(
    cfg: &RuntimeConfig,
    store: &Store,
    telegram_client: &Client,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    let mut offset = store
        .kv_get("last_update_id")?
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.saturating_add(1));

    while !shutdown.load(Ordering::SeqCst) {
        let updates = match fetch_poll_updates(telegram_client, cfg, offset) {
            Ok(updates) => updates,
            Err(err) => {
                eprintln!("warn: telegram polling failed: {err:#}");
                thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
                continue;
            }
        };

        if updates.is_empty() {
            thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
            continue;
        }

        for update in updates {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            let update_id =
                extract_update_id_from_value(&update).and_then(|value| value.parse::<u64>().ok());
            let line =
                serde_json::to_string(&update).context("failed to serialize polled update JSON")?;
            let outcome = process_webhook_line(cfg, store, &line)?;

            if let Some(output) = outcome.output.as_deref() {
                dispatch_telegram_output(
                    telegram_client,
                    cfg,
                    outcome.chat_id.as_deref(),
                    output,
                    outcome.progress_message_id.as_deref(),
                )?;
                println!("{output}");
                io::stdout().flush().ok();
            }
            if let Some(path) = outcome.cleanup_path.as_deref() {
                let _ = fs::remove_file(path);
            }

            store.clear_inflight()?;

            if let Some(update_id) = update_id {
                offset = Some(update_id.saturating_add(1));
            }
        }
    }

    eprintln!("info: shutdown signal received, stopping poll loop");
    Ok(())
}

fn run_webhook_loop(
    cfg: &RuntimeConfig,
    store: &Store,
    telegram_client: &Client,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
    if let Some(parent) = queue_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if !queue_path.exists() {
        fs::write(&queue_path, "")
            .with_context(|| format!("failed to initialize {}", queue_path.display()))?;
    }

    if let Err(err) = restore_inflight_update(cfg, store, telegram_client) {
        eprintln!("warn: failed to restore inflight webhook update: {err:#}");
    }

    while !shutdown.load(Ordering::SeqCst) {
        let progressed = drain_webhook_queue(cfg, store, telegram_client, shutdown)?;
        if !progressed {
            thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
        }
    }

    eprintln!("info: shutdown signal received, stopping webhook loop");
    Ok(())
}

fn restore_inflight_update(
    cfg: &RuntimeConfig,
    store: &Store,
    telegram_client: &Client,
) -> Result<()> {
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

    if let Some(update_id) = inflight_update_id.as_deref()
        && store.turn_exists_for_update_id(update_id)?
    {
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

    let outcome = process_webhook_line(cfg, store, &inflight_json)?;
    if outcome.should_ack {
        let expected_id = outcome
            .update_id
            .as_deref()
            .or(inflight_update_id.as_deref());
        match ack_webhook_queue_line(cfg, expected_id)? {
            AckStatus::Acked => {
                store.clear_inflight()?;
                if let Some(output) = outcome.output {
                    dispatch_telegram_output(
                        telegram_client,
                        cfg,
                        outcome.chat_id.as_deref(),
                        &output,
                        outcome.progress_message_id.as_deref(),
                    )?;
                    println!("{output}");
                    io::stdout().flush().ok();
                }
                if let Some(path) = outcome.cleanup_path.as_deref() {
                    let _ = fs::remove_file(path);
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
    telegram_client: &Client,
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

        let outcome = match process_webhook_line(cfg, store, &line) {
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
                    dispatch_telegram_output(
                        telegram_client,
                        cfg,
                        outcome.chat_id.as_deref(),
                        &output,
                        outcome.progress_message_id.as_deref(),
                    )?;
                    println!("{output}");
                    io::stdout().flush().ok();
                }
                if let Some(path) = outcome.cleanup_path.as_deref() {
                    let _ = fs::remove_file(path);
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
    chat_id: Option<String>,
    output: Option<String>,
    cleanup_path: Option<PathBuf>,
    progress_message_id: Option<String>,
}

fn process_webhook_line(cfg: &RuntimeConfig, store: &Store, line: &str) -> Result<ProcessOutcome> {
    let action = parse_webhook_action(cfg, line)?;

    match action {
        WebhookAction::Ignore { update_id, reason } => {
            let update_id_text = update_id.as_deref().unwrap_or_default().to_string();
            let ignored_at = iso_now(&cfg.timezone);
            store.kv_set("last_ignored_update_id", &update_id_text)?;
            store.kv_set("last_ignored_reason", &reason)?;
            store.kv_set("last_ignored_at", &ignored_at)?;
            eprintln!(
                "info: ignored telegram update_id={} reason={reason}",
                if update_id_text.trim().is_empty() {
                    "unknown"
                } else {
                    update_id_text.as_str()
                }
            );
            if let Some(update_id) = update_id.as_ref() {
                store.kv_set("last_update_id", update_id)?;
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: None,
                output: None,
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Cancel { update_id } => {
            if let Err(err) = signal_cancel_marker(cfg) {
                eprintln!("warn: failed to set cancel marker: {err:#}");
            }
            if let Some(update_id) = update_id.as_ref() {
                store.kv_set("last_update_id", update_id)?;
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: None,
                output: None,
                cleanup_path: None,
                progress_message_id: None,
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
                chat_id: Some(chat_id),
                output: Some(
                    render_output(
                        "Context cleared. Fresh start!",
                        "",
                        &ParsedMarkers::default(),
                    )
                    .trim_end()
                    .to_string(),
                ),
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Turn(turn) => {
            let chat_id = turn.chat_id.clone();
            if let Some(update_id) = turn.update_id.as_deref()
                && store.turn_exists_for_update_id(update_id)?
            {
                store.kv_set("last_update_id", update_id)?;
                let replay_output = store.rendered_output_for_update_id(update_id)?;
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(update_id.to_string()),
                    chat_id: Some(chat_id),
                    output: replay_output,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }

            set_inflight_update(
                store,
                turn.update_id.as_deref().unwrap_or(""),
                line,
                &cfg.timezone,
            )?;

            let progress_message_id = send_progress_message(cfg, &chat_id)
                .map_err(|err| {
                    eprintln!("warn: failed to send progress message: {err:#}");
                    err
                })
                .ok()
                .flatten();

            let (hydrated_input, cleanup_path) =
                hydrate_turn_input(cfg, turn.update_id.as_deref(), turn.input, turn.media)?;

            let output = process_turn(
                cfg,
                store,
                hydrated_input,
                Some(chat_id.clone()),
                turn.update_id.clone(),
                progress_message_id.as_deref(),
                &turn.quoted,
            )?;

            if let Some(update_id) = turn.update_id.as_ref() {
                store.kv_set("last_update_id", update_id)?;
            }

            Ok(ProcessOutcome {
                should_ack: true,
                update_id: turn.update_id,
                chat_id: Some(chat_id),
                output: Some(output.trim_end().to_string()),
                cleanup_path,
                progress_message_id,
            })
        }
    }
}

fn cancel_marker_path(cfg: &RuntimeConfig) -> PathBuf {
    cfg.runtime_dir.join("cancel")
}

fn signal_cancel_marker(cfg: &RuntimeConfig) -> Result<()> {
    let path = cancel_marker_path(cfg);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&path, "").with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn clear_cancel_marker(cfg: &RuntimeConfig) {
    let path = cancel_marker_path(cfg);
    let _ = fs::remove_file(path);
}

fn hydrate_turn_input(
    cfg: &RuntimeConfig,
    update_id: Option<&str>,
    mut input: TurnInput,
    media: Option<IncomingMedia>,
) -> Result<(TurnInput, Option<PathBuf>)> {
    let Some(media) = media else {
        return Ok((input, None));
    };

    let client = match build_telegram_client(cfg) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("warn: telegram media fetch disabled: {err:#}");
            return Ok((input, None));
        }
    };

    let suffix = update_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("0")
        .to_string();

    match media {
        IncomingMedia::Voice { file_id } => {
            let voice_path = cfg.tmp_dir.join(format!("in_{suffix}.oga"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &voice_path) {
                eprintln!("warn: failed to download voice attachment: {err:#}");
                return Ok((input, None));
            }

            input.input_type = "voice".to_string();
            input.attachment_type = None;
            input.attachment_path = None;
            input.attachment_owned = false;

            if asr_feature_enabled(cfg) {
                match run_asr_script(cfg, &voice_path) {
                    Ok(asr_text) => {
                        let asr_text = asr_text.trim().to_string();
                        if !asr_text.is_empty() {
                            input.asr_text = asr_text.clone();
                            input.user_text = asr_text;
                        }
                    }
                    Err(err) => {
                        eprintln!("warn: ASR failed for voice attachment: {err:#}");
                    }
                }
            } else {
                eprintln!("info: voice attachment received but ASR is disabled in config.toml");
            }

            let _ = fs::remove_file(voice_path);
            Ok((input, None))
        }
        IncomingMedia::Photo { file_id } => {
            let path = cfg.tmp_dir.join(format!("photo_{suffix}.jpg"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                eprintln!("warn: failed to download photo attachment: {err:#}");
                return Ok((input, None));
            }
            input.input_type = "photo".to_string();
            input.attachment_type = Some("photo".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::Document { file_id, file_name } => {
            let ext = file_name
                .as_deref()
                .and_then(|name| Path::new(name).extension().and_then(|ext| ext.to_str()))
                .unwrap_or("bin");
            let path = cfg.tmp_dir.join(format!("doc_{suffix}.{ext}"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                eprintln!("warn: failed to download document attachment: {err:#}");
                return Ok((input, None));
            }
            input.input_type = "document".to_string();
            input.attachment_type = Some("document".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::Video { file_id } => {
            let path = cfg.tmp_dir.join(format!("video_{suffix}.mp4"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                eprintln!("warn: failed to download video attachment: {err:#}");
                return Ok((input, None));
            }
            input.input_type = "video".to_string();
            input.attachment_type = Some("video".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::VideoNote { file_id } => {
            let path = cfg.tmp_dir.join(format!("video_note_{suffix}.mp4"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                eprintln!("warn: failed to download video_note attachment: {err:#}");
                return Ok((input, None));
            }
            input.input_type = "video_note".to_string();
            input.attachment_type = Some("video_note".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
    }
}

fn run_asr_script(cfg: &RuntimeConfig, audio_path: &Path) -> Result<String> {
    let script = cfg.root_dir.join("scripts/asr.sh");
    if !script.is_file() {
        bail!("ASR script not found: {}", script.display());
    }
    if !command_exists("bash") {
        bail!("bash not found; cannot run ASR script");
    }

    let mut cmd = Command::new("bash");
    cmd.arg(script)
        .arg(audio_path)
        .current_dir(&cfg.root_dir)
        .env("INSTANCE_DIR", &cfg.instance_dir);
    if let Some(value) = cfg.asr_url.as_deref() {
        cmd.env("ASR_URL", value);
    }
    if let Some(value) = cfg.asr_cmd_template.as_deref() {
        cmd.env("ASR_CMD_TEMPLATE", value);
    }
    if let Some(value) = cfg.asr_file_field.as_deref() {
        cmd.env("ASR_FILE_FIELD", value);
    }
    if let Some(value) = cfg.asr_text_jq.as_deref() {
        cmd.env("ASR_TEXT_JQ", value);
    }
    if let Some(value) = cfg.asr_preprocess.as_deref() {
        cmd.env("ASR_PREPROCESS", value);
    }
    if let Some(value) = cfg.asr_sample_rate.as_deref() {
        cmd.env("ASR_SAMPLE_RATE", value);
    }

    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to execute ASR script")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        bail!("ASR script failed: {stderr}");
    }

    let text = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(text.trim().to_string())
}

fn telegram_file_base(cfg: &RuntimeConfig) -> Result<String> {
    let token = valid_telegram_token(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_BOT_TOKEN is missing; set it in instance config.toml")
    })?;
    Ok(format!("https://api.telegram.org/file/bot{token}"))
}

fn telegram_download_file(
    client: &Client,
    cfg: &RuntimeConfig,
    file_id: &str,
    out_path: &Path,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let file_base = telegram_file_base(cfg)?;
    let response = client
        .post(format!("{base}/getFile"))
        .form(&[("file_id", file_id)])
        .send()
        .context("failed to call telegram getFile")?;
    let value = parse_telegram_response(response, "getFile")?;
    let file_path = value
        .get("result")
        .and_then(|node| node.get("file_path"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("telegram getFile returned empty file_path"))?;

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let response = client
        .get(format!("{file_base}/{file_path}"))
        .send()
        .context("failed to download telegram file")?;
    if !response.status().is_success() {
        bail!("telegram file download HTTP {}", response.status().as_u16());
    }
    let bytes = response
        .bytes()
        .context("failed to read downloaded telegram file")?;
    fs::write(out_path, &bytes)
        .with_context(|| format!("failed to write {}", out_path.display()))?;
    Ok(())
}

fn valid_telegram_token(cfg: &RuntimeConfig) -> Option<&str> {
    cfg.telegram_bot_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty() && *token != "replace_me")
}

fn valid_telegram_chat_id(cfg: &RuntimeConfig) -> Option<&str> {
    cfg.telegram_chat_id
        .as_deref()
        .map(str::trim)
        .filter(|chat_id| !chat_id.is_empty() && *chat_id != "replace_me")
}

fn telegram_api_base(cfg: &RuntimeConfig) -> Result<String> {
    let token = valid_telegram_token(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_BOT_TOKEN is missing; set it in instance config.toml")
    })?;
    Ok(format!("https://api.telegram.org/bot{token}"))
}

fn build_telegram_client(cfg: &RuntimeConfig) -> Result<Client> {
    let _ = telegram_api_base(cfg)?;
    let _ = valid_telegram_chat_id(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_CHAT_ID is missing; set it in instance config.toml")
    })?;
    Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to build telegram HTTP client")
}

fn fetch_poll_updates(
    client: &Client,
    cfg: &RuntimeConfig,
    offset: Option<u64>,
) -> Result<Vec<Value>> {
    fetch_updates(client, cfg, offset, 25, r#"["message","callback_query"]"#)
}

fn fetch_cancel_updates(
    client: &Client,
    cfg: &RuntimeConfig,
    offset: Option<u64>,
) -> Result<Vec<Value>> {
    fetch_updates(client, cfg, offset, 0, r#"["message","callback_query"]"#)
}

fn fetch_updates(
    client: &Client,
    cfg: &RuntimeConfig,
    offset: Option<u64>,
    timeout_seconds: u64,
    allowed_updates: &str,
) -> Result<Vec<Value>> {
    let base = telegram_api_base(cfg)?;
    let url = format!("{base}/getUpdates");

    let mut query: Vec<(&str, String)> = vec![
        ("timeout", timeout_seconds.to_string()),
        ("allowed_updates", allowed_updates.to_string()),
    ];
    if let Some(offset) = offset {
        query.push(("offset", offset.to_string()));
    }

    let response = client
        .get(url)
        .query(&query)
        .send()
        .context("failed to call telegram getUpdates")?;
    let value = parse_telegram_response(response, "getUpdates")?;
    let updates = value
        .get("result")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(updates)
}

fn cancel_signal_from_update(value: &Value, expected_chat_id: &str) -> Option<CancelSignal> {
    if let Some(callback_query) = value.get("callback_query") {
        let data = callback_query
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let chat_id = callback_query
            .get("message")
            .and_then(|node| node.get("chat"))
            .and_then(|node| node.get("id"))
            .map(value_to_string)
            .unwrap_or_default();
        if data.eq_ignore_ascii_case("cancel") && chat_id == expected_chat_id {
            let callback_query_id = callback_query
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            return Some(CancelSignal { callback_query_id });
        }
    }

    if let Some(message) = value.get("message") {
        let chat_id = message
            .get("chat")
            .and_then(|node| node.get("id"))
            .map(value_to_string)
            .unwrap_or_default();
        let text = message
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if chat_id == expected_chat_id && text.trim().eq_ignore_ascii_case("/cancel") {
            return Some(CancelSignal {
                callback_query_id: None,
            });
        }
    }

    None
}

fn maybe_spawn_cancel_watcher(
    cfg: &RuntimeConfig,
    store: &Store,
    cancel_flag: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
) -> Result<Option<std::thread::JoinHandle<()>>> {
    let expected_chat = valid_telegram_chat_id(cfg)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("missing TELEGRAM_CHAT_ID"))?;
    if valid_telegram_token(cfg).is_none() {
        return Ok(None);
    }

    let mut offset = store
        .kv_get("last_update_id")?
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.saturating_add(1));

    let cfg_clone = cfg.clone();
    let handle = thread::spawn(move || {
        let client = match build_telegram_client(&cfg_clone) {
            Ok(client) => client,
            Err(_) => return,
        };

        while !stop_flag.load(Ordering::SeqCst) && !cancel_flag.load(Ordering::SeqCst) {
            let updates = match fetch_cancel_updates(&client, &cfg_clone, offset) {
                Ok(updates) => updates,
                Err(_) => {
                    thread::sleep(Duration::from_millis(500));
                    continue;
                }
            };

            for update in updates {
                if let Some(update_id) = extract_update_id_from_value(&update)
                    .and_then(|value| value.parse::<u64>().ok())
                {
                    offset = Some(update_id.saturating_add(1));
                }

                if let Some(signal) = cancel_signal_from_update(&update, &expected_chat) {
                    if let Some(callback_id) = signal.callback_query_id.as_deref() {
                        let _ = telegram_answer_callback(&client, &cfg_clone, callback_id);
                    }
                    cancel_flag.store(true, Ordering::SeqCst);
                    let _ = signal_cancel_marker(&cfg_clone);
                    break;
                }
            }

            thread::sleep(Duration::from_millis(500));
        }
    });

    Ok(Some(handle))
}

fn dispatch_telegram_output(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id_override: Option<&str>,
    output: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    let chat_id = chat_id_override
        .or(cfg.telegram_chat_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("TELEGRAM_CHAT_ID missing and update chat_id unavailable")
        })?;

    let markers = parse_markers(output);
    if let Some(reply) = markers.telegram_reply.as_deref() {
        let reply = reply.trim();
        if !reply.is_empty() {
            send_or_edit_text(client, cfg, chat_id, reply, progress_message_id)?;
        } else if let Some(message_id) = progress_message_id {
            let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
        }
    } else if let Some(message_id) = progress_message_id {
        let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
    }

    if let Some(voice_reply) = markers.voice_reply.as_deref() {
        let voice_reply = voice_reply.trim();
        if !voice_reply.is_empty()
            && let Err(err) = send_voice_reply(client, cfg, chat_id, voice_reply)
        {
            eprintln!("warn: failed to send voice reply: {err:#}");
        }
    }

    for item in markers.send_photo {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendPhoto", "photo", &path)
        {
            eprintln!("warn: failed to send photo {}: {err:#}", path.display());
        }
    }
    for item in markers.send_document {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendDocument", "document", &path)
        {
            eprintln!("warn: failed to send document {}: {err:#}", path.display());
        }
    }
    for item in markers.send_video {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendVideo", "video", &path)
        {
            eprintln!("warn: failed to send video {}: {err:#}", path.display());
        }
    }

    Ok(())
}

fn send_progress_message(cfg: &RuntimeConfig, chat_id: &str) -> Result<Option<String>> {
    let client = build_telegram_client(cfg)?;
    let base = telegram_api_base(cfg)?;
    let reply_markup = progress_reply_markup();
    let params = [
        ("chat_id", chat_id.to_string()),
        ("text", "Thinking...".to_string()),
        ("reply_markup", reply_markup.to_string()),
    ];
    let response = client
        .post(format!("{base}/sendMessage"))
        .form(&params)
        .send()
        .context("failed to send progress message")?;
    let value = parse_telegram_response(response, "sendMessage")?;
    let message_id = value
        .get("result")
        .and_then(|node| node.get("message_id"))
        .map(value_to_string)
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty());
    Ok(message_id)
}

fn progress_reply_markup() -> &'static str {
    r#"{"inline_keyboard":[[{"text":"Cancel","callback_data":"cancel"}]]}"#
}

fn progress_status_text(elapsed_secs: u64) -> String {
    format!("Thinking...\nElapsed: {elapsed_secs}s\nTap Cancel to stop.")
}

fn progress_status_with_events(elapsed_secs: u64, statuses: &[String]) -> String {
    let mut text = progress_status_text(elapsed_secs);
    if statuses.is_empty() {
        return text;
    }
    text.push_str("\n\n");
    for status in statuses {
        text.push_str("- ");
        text.push_str(status);
        text.push('\n');
    }
    text.trim_end().to_string()
}

fn spawn_progress_updater(
    cfg: RuntimeConfig,
    chat_id: String,
    message_id: String,
    progress_rx: mpsc::Receiver<String>,
    stop_flag: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    thread::spawn(move || {
        let client = match build_telegram_client(&cfg) {
            Ok(client) => client,
            Err(_) => return,
        };
        let started = Instant::now();
        let mut last_bucket = 0u64;
        let mut last_edit = Instant::now()
            .checked_sub(Duration::from_secs(5))
            .unwrap_or_else(Instant::now);
        let mut statuses: Vec<String> = Vec::new();
        let mut saw_event = false;
        let mut channel_closed = false;

        loop {
            match progress_rx.recv_timeout(Duration::from_millis(400)) {
                Ok(status) => {
                    let status = status.trim().to_string();
                    if !status.is_empty() && statuses.last() != Some(&status) {
                        statuses.push(status);
                        if statuses.len() > 5 {
                            statuses.remove(0);
                        }
                        saw_event = true;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    channel_closed = true;
                }
            }

            let elapsed = started.elapsed().as_secs();
            let bucket = elapsed / 3;
            let elapsed_tick = bucket > last_bucket;
            if elapsed_tick {
                last_bucket = bucket;
            }

            if (elapsed_tick || saw_event) && last_edit.elapsed() >= Duration::from_secs(1) {
                let text = progress_status_with_events(elapsed, &statuses);
                let _ =
                    telegram_edit_message_text(&client, &cfg, &chat_id, &message_id, &text, true);
                saw_event = false;
                last_edit = Instant::now();
            }

            if stop_flag.load(Ordering::SeqCst) && channel_closed {
                break;
            }
        }
    })
}

fn send_or_edit_text(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    text: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    let chunks = split_text_chunks(text, 4096);
    if chunks.is_empty() {
        return Ok(());
    }

    let mut first_done = false;
    if let Some(message_id) = progress_message_id
        && telegram_edit_message_text(client, cfg, chat_id, message_id, &chunks[0], false).is_ok()
    {
        first_done = true;
    }

    if !first_done {
        let _ = telegram_send_message(client, cfg, chat_id, &chunks[0])?;
    }

    for chunk in chunks.iter().skip(1) {
        let _ = telegram_send_message(client, cfg, chat_id, chunk)?;
    }

    Ok(())
}

fn split_text_chunks(text: &str, max_chars: usize) -> Vec<String> {
    if max_chars == 0 {
        return vec![text.to_string()];
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let end = (start + max_chars).min(chars.len());
        let chunk: String = chars[start..end].iter().collect();
        chunks.push(chunk);
        start = end;
    }
    chunks
}

fn telegram_edit_message_text(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: &str,
    text: &str,
    keep_cancel_button: bool,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let reply_markup = if keep_cancel_button {
        progress_reply_markup()
    } else {
        r#"{"inline_keyboard":[]}"#
    };
    let params =
        telegram_text_form_params(cfg, chat_id, Some(message_id), text, Some(reply_markup));
    let url = format!("{base}/editMessageText");
    match telegram_post_form(client, &url, &params, "editMessageText") {
        Ok(_) => Ok(()),
        Err(_err) if should_retry_plain_text(cfg) => {
            let retry = strip_parse_mode_param(&params);
            telegram_post_form(client, &url, &retry, "editMessageText").map(|_| ())
        }
        Err(err) => Err(err),
    }
}

fn telegram_remove_keyboard(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: &str,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let reply_markup = r#"{"inline_keyboard":[]}"#;
    let params = [
        ("chat_id", chat_id.to_string()),
        ("message_id", message_id.to_string()),
        ("reply_markup", reply_markup.to_string()),
    ];
    let response = client
        .post(format!("{base}/editMessageReplyMarkup"))
        .form(&params)
        .send()
        .context("failed to call telegram editMessageReplyMarkup")?;
    parse_telegram_response(response, "editMessageReplyMarkup")?;
    Ok(())
}

fn telegram_answer_callback(
    client: &Client,
    cfg: &RuntimeConfig,
    callback_query_id: &str,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let params = [("callback_query_id", callback_query_id.to_string())];
    let response = client
        .post(format!("{base}/answerCallbackQuery"))
        .form(&params)
        .send()
        .context("failed to call telegram answerCallbackQuery")?;
    parse_telegram_response(response, "answerCallbackQuery")?;
    Ok(())
}

fn send_voice_reply(client: &Client, cfg: &RuntimeConfig, chat_id: &str, text: &str) -> Result<()> {
    let script = cfg.root_dir.join("scripts/tts.sh");
    if !script.is_file() {
        bail!("TTS script not found: {}", script.display());
    }
    if !command_exists("bash") {
        bail!("bash not found; cannot run TTS script");
    }

    let output_voice = cfg.tmp_dir.join(format!(
        "reply_{}.ogg",
        chrono::Utc::now().timestamp_millis()
    ));
    if let Some(parent) = output_voice.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut cmd = Command::new("bash");
    cmd.arg(script)
        .arg(text)
        .arg(&output_voice)
        .current_dir(&cfg.root_dir)
        .env("INSTANCE_DIR", &cfg.instance_dir);
    if let Some(value) = cfg.tts_cmd_template.as_deref() {
        cmd.env("TTS_CMD_TEMPLATE", value);
    }
    if let Some(value) = cfg.voice_bitrate.as_deref() {
        cmd.env("VOICE_BITRATE", value);
    }
    if let Some(value) = cfg.tts_max_chars.as_deref() {
        cmd.env("TTS_MAX_CHARS", value);
    }

    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to execute TTS script")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let _ = fs::remove_file(&output_voice);
        bail!("TTS script failed: {stderr}");
    }

    let result =
        telegram_send_media_file(client, cfg, chat_id, "sendVoice", "voice", &output_voice);
    let _ = fs::remove_file(&output_voice);
    result
}

fn telegram_send_message(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    text: &str,
) -> Result<Option<String>> {
    let base = telegram_api_base(cfg)?;
    let params = telegram_text_form_params(cfg, chat_id, None, text, None);
    let url = format!("{base}/sendMessage");
    let value = match telegram_post_form(client, &url, &params, "sendMessage") {
        Ok(value) => value,
        Err(_err) if should_retry_plain_text(cfg) => {
            let retry = strip_parse_mode_param(&params);
            telegram_post_form(client, &url, &retry, "sendMessage")?
        }
        Err(err) => return Err(err),
    };
    let message_id = value
        .get("result")
        .and_then(|node| node.get("message_id"))
        .map(value_to_string)
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty());
    Ok(message_id)
}

fn telegram_text_form_params(
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: Option<&str>,
    text: &str,
    reply_markup: Option<&str>,
) -> Vec<(String, String)> {
    let mut params = Vec::new();
    params.push(("chat_id".to_string(), chat_id.to_string()));
    if let Some(message_id) = message_id {
        params.push(("message_id".to_string(), message_id.to_string()));
    }
    params.push(("text".to_string(), text.to_string()));
    if let Some(reply_markup) = reply_markup {
        params.push(("reply_markup".to_string(), reply_markup.to_string()));
    }
    if let Some(parse_mode) = cfg.telegram_parse_mode.as_api_value() {
        params.push(("parse_mode".to_string(), parse_mode.to_string()));
    }
    params
}

fn strip_parse_mode_param(params: &[(String, String)]) -> Vec<(String, String)> {
    params
        .iter()
        .filter(|(key, _)| key != "parse_mode")
        .cloned()
        .collect()
}

fn should_retry_plain_text(cfg: &RuntimeConfig) -> bool {
    matches!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2)
        && matches!(cfg.telegram_parse_fallback, TelegramParseFallback::Plain)
}

fn telegram_post_form(
    client: &Client,
    url: &str,
    params: &[(String, String)],
    action: &str,
) -> Result<Value> {
    let response = client
        .post(url)
        .form(params)
        .send()
        .with_context(|| format!("failed to call telegram {action}"))?;
    parse_telegram_response(response, action)
}

fn telegram_send_media_file(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    method: &str,
    field: &str,
    path: &Path,
) -> Result<()> {
    if !path.is_file() {
        bail!(
            "{} marker path not found: {}",
            field.to_ascii_uppercase(),
            path.display()
        );
    }

    let base = telegram_api_base(cfg)?;
    let form = multipart::Form::new()
        .text("chat_id", chat_id.to_string())
        .file(field.to_string(), path)
        .with_context(|| format!("failed to prepare multipart upload for {}", path.display()))?;

    let response = client
        .post(format!("{base}/{method}"))
        .multipart(form)
        .send()
        .with_context(|| format!("failed to call telegram {method}"))?;
    parse_telegram_response(response, method)?;
    Ok(())
}

fn parse_telegram_response(response: reqwest::blocking::Response, action: &str) -> Result<Value> {
    let status = response.status();
    let body = response
        .text()
        .with_context(|| format!("failed to read telegram {action} response body"))?;
    if !status.is_success() {
        bail!("telegram {action} HTTP {}: {body}", status.as_u16());
    }

    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("telegram {action} returned invalid JSON: {body}"))?;
    let ok = value.get("ok").and_then(Value::as_bool).unwrap_or(false);
    if !ok {
        let description = value
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("unknown telegram error");
        bail!("telegram {action} failed: {description}");
    }

    Ok(value)
}

fn parse_webhook_action(cfg: &RuntimeConfig, line: &str) -> Result<WebhookAction> {
    let value: Value = serde_json::from_str(line).context("invalid webhook JSON payload")?;
    let update_id = extract_update_id_from_value(&value);
    let configured_chat = cfg.telegram_chat_id.as_deref().unwrap_or("<any>");

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
            let actual_chat = chat_id.unwrap_or_else(|| "<missing>".to_string());
            return Ok(WebhookAction::Ignore {
                update_id,
                reason: format!(
                    "callback_cancel_chat_id_mismatch actual={actual_chat} configured={configured_chat}"
                ),
            });
        }
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: format!(
                "callback_query_ignored data={}",
                shorten_log_text(data.trim(), 64)
            ),
        });
    }

    let Some(message) = value.get("message") else {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "missing_message_payload".to_string(),
        });
    };

    let chat_id = message
        .get("chat")
        .and_then(|node| node.get("id"))
        .map(value_to_string)
        .unwrap_or_default();
    if chat_id.trim().is_empty() {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "missing_chat_id".to_string(),
        });
    }
    if !is_allowed_chat(cfg, Some(&chat_id)) {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: format!("chat_id_mismatch actual={chat_id} configured={configured_chat}"),
        });
    }

    let message_text = message
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| message.get("caption").and_then(Value::as_str))
        .unwrap_or_default();
    let media = extract_incoming_media(message);

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

    let (input_type, attachment_type) = match media.as_ref() {
        Some(IncomingMedia::Voice { .. }) => ("voice".to_string(), None),
        Some(IncomingMedia::Photo { .. }) => ("photo".to_string(), Some("photo".to_string())),
        Some(IncomingMedia::Document { .. }) => {
            ("document".to_string(), Some("document".to_string()))
        }
        Some(IncomingMedia::Video { .. }) => ("video".to_string(), Some("video".to_string())),
        Some(IncomingMedia::VideoNote { .. }) => {
            ("video_note".to_string(), Some("video_note".to_string()))
        }
        None => ("text".to_string(), None),
    };

    let input = TurnInput {
        input_type,
        user_text: {
            let trimmed = message_text.trim();
            if trimmed.is_empty() {
                "(empty message)".to_string()
            } else {
                trimmed.to_string()
            }
        },
        asr_text: String::new(),
        attachment_type,
        attachment_path: None,
        attachment_owned: false,
    };

    Ok(WebhookAction::Turn(Box::new(WebhookTurn {
        update_id,
        chat_id,
        input,
        media,
        quoted: QuotedMessage {
            reply_from,
            reply_text,
        },
    })))
}

fn shorten_log_text(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out: String = chars[..keep].iter().collect();
    out.push_str("...");
    out
}

fn extract_incoming_media(message: &Value) -> Option<IncomingMedia> {
    if let Some(file_id) = message
        .get("voice")
        .and_then(|node| node.get("file_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(IncomingMedia::Voice {
            file_id: file_id.to_string(),
        });
    }

    if let Some(photo_array) = message.get("photo").and_then(Value::as_array) {
        for item in photo_array.iter().rev() {
            if let Some(file_id) = item
                .get("file_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                return Some(IncomingMedia::Photo {
                    file_id: file_id.to_string(),
                });
            }
        }
    }

    if let Some(document) = message.get("document")
        && let Some(file_id) = document
            .get("file_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    {
        let file_name = document
            .get("file_name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        return Some(IncomingMedia::Document {
            file_id: file_id.to_string(),
            file_name,
        });
    }

    if let Some(file_id) = message
        .get("video")
        .and_then(|node| node.get("file_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(IncomingMedia::Video {
            file_id: file_id.to_string(),
        });
    }

    if let Some(file_id) = message
        .get("video_note")
        .and_then(|node| node.get("file_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(IncomingMedia::VideoNote {
            file_id: file_id.to_string(),
        });
    }

    None
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
        .truncate(false)
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
    progress_message_id: Option<&str>,
    quoted: &QuotedMessage,
) -> Result<String> {
    clear_cancel_marker(cfg);
    let ts = iso_now(&cfg.timezone);
    let chat_id = chat_id_override
        .or_else(|| cfg.telegram_chat_id.clone())
        .unwrap_or_else(|| "local".to_string());

    let context = build_context(cfg, store, &input, &ts, quoted)?;

    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancel_watcher_stop = Arc::new(AtomicBool::new(false));
    let cancel_watcher = if update_id.is_some() {
        maybe_spawn_cancel_watcher(
            cfg,
            store,
            Arc::clone(&cancel_flag),
            Arc::clone(&cancel_watcher_stop),
        )
        .ok()
        .flatten()
    } else {
        None
    };
    let progress_updater_stop = Arc::new(AtomicBool::new(false));
    let (progress_sender, progress_updater) = if let Some(message_id) = progress_message_id {
        let (progress_tx, progress_rx) = mpsc::channel::<String>();
        (
            Some(progress_tx),
            Some(spawn_progress_updater(
                cfg.clone(),
                chat_id.clone(),
                message_id.to_string(),
                progress_rx,
                Arc::clone(&progress_updater_stop),
            )),
        )
    } else {
        (None, None)
    };

    let provider_result = run_provider(cfg, &context, Some(&cancel_flag), progress_sender.as_ref());
    cancel_watcher_stop.store(true, Ordering::SeqCst);
    if let Some(handle) = cancel_watcher {
        let _ = handle.join();
    }
    drop(progress_sender);
    progress_updater_stop.store(true, Ordering::SeqCst);
    if let Some(handle) = progress_updater {
        let _ = handle.join();
    }

    let (raw_output, provider_success, exit_code) = match provider_result {
        Ok(result) => (result.raw_output, result.success, result.exit_code),
        Err(err) => (format!("{err:#}"), false, 1),
    };
    let cancelled = cancel_flag.load(Ordering::SeqCst) || exit_code == 130;

    let mut markers = parse_markers(&raw_output);
    let mut telegram_reply = markers.telegram_reply.clone().unwrap_or_default();
    let mut voice_reply = markers.voice_reply.clone().unwrap_or_default();
    let status: String;

    if cancelled {
        markers = ParsedMarkers::default();
        telegram_reply = "❌ Cancelled.".to_string();
        voice_reply = String::new();
        status = "cancelled".to_string();
    } else if telegram_reply.trim().is_empty() && voice_reply.trim().is_empty() {
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
        status: status.clone(),
        update_id,
    })?;

    if inserted && status != "cancelled" {
        append_memory_and_tasks(cfg, store, &ts, &markers)?;
    }

    clear_cancel_marker(cfg);
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
            attachment_owned: false,
        });
    }

    Ok(TurnInput {
        input_type: "text".to_string(),
        user_text,
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
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

    if let Some(reply_text) = quoted.reply_text.as_ref()
        && !reply_text.trim().is_empty()
    {
        text.push_str("\n## Quoted/replied-to message\n");
        let reply_from = quoted.reply_from.as_deref().unwrap_or("someone");
        text.push_str(&format!("REPLY_FROM: {reply_from}\n"));
        text.push_str(&format!("REPLY_TEXT: {reply_text}\n"));
        text.push_str(
            "The user is replying to the above message. Use it as context for understanding their intent.\n",
        );
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
    if matches!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2) {
        text.push_str("MarkdownV2 is enabled for Telegram replies.\n");
        text.push_str("You may use Telegram MarkdownV2 formatting inside marker values only.\n");
        text.push_str("Keep marker prefixes plain and unchanged.\n");
        text.push_str("Do not use code fences or extra prefixes.\n");
    } else {
        text.push_str("Do not use markdown, code fences, or extra prefixes.\n");
    }

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
        if let Some(value) = strip_marker(marker, line)
            && !value.trim().is_empty()
        {
            out.push(value.to_string());
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

fn local_day(timezone: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    if let Ok(tz) = timezone.parse::<Tz>() {
        return now.with_timezone(&tz).format("%Y-%m-%d").to_string();
    }
    now.format("%Y-%m-%d").to_string()
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

fn nightly_reflection_marker(day: &str) -> String {
    format!("<!-- nightly-reflection:{day} -->")
}

fn nightly_reflection_prompt(cfg: &RuntimeConfig) -> String {
    cfg.nightly_reflection_prompt.clone().unwrap_or_else(|| {
        "Do a brief nightly reflection in first person: include today outcomes, today lessons, and the single most important thing for tomorrow in under 120 words.".to_string()
    })
}

fn nightly_reflection_file_path(cfg: &RuntimeConfig) -> PathBuf {
    cfg.nightly_reflection_file.clone()
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

fn asr_feature_enabled(cfg: &RuntimeConfig) -> bool {
    cfg.asr_cmd_template.is_some() || cfg.asr_url.is_some()
}

fn parse_on_like(value: Option<&str>, default: bool) -> bool {
    let Some(value) = value else {
        return default;
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => true,
        "off" | "false" | "0" | "no" => false,
        _ => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coconutclaw_config::{AgentProvider, CodexConfig, PiConfig, RuntimeConfig};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_config() -> RuntimeConfig {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("coconutclaw_test_{unique}"));
        let cfg = RuntimeConfig {
            root_dir: root.clone(),
            data_dir: root.clone(),
            instance_name: "default".to_string(),
            instance_dir: root.clone(),
            runtime_dir: root.join("runtime"),
            tmp_dir: root.join("tmp"),
            tasks_dir: root.join("TASKS"),
            log_dir: root.join("LOGS"),
            sqlite_db_path: root.join("state.db"),
            allowlist_path: root.join("config/allowlist.txt"),
            timezone: "UTC".to_string(),
            telegram_bot_token: Some("123:token".to_string()),
            telegram_chat_id: Some("321".to_string()),
            telegram_parse_mode: TelegramParseMode::Off,
            telegram_parse_fallback: TelegramParseFallback::Plain,
            webhook_mode: false,
            poll_interval_seconds: 1,
            provider: AgentProvider::Codex,
            exec_policy: "yolo".to_string(),
            asr_url: None,
            asr_cmd_template: None,
            asr_file_field: None,
            asr_text_jq: None,
            asr_preprocess: None,
            asr_sample_rate: None,
            tts_cmd_template: None,
            voice_bitrate: None,
            tts_max_chars: None,
            nightly_reflection_file: root.join("LOGS/nightly_reflection.md"),
            nightly_reflection_skip_agent: false,
            nightly_reflection_prompt: None,
            codex: CodexConfig {
                bin: "codex".to_string(),
                model: None,
                reasoning_effort: None,
            },
            pi: PiConfig {
                bin: "pi".to_string(),
                provider: None,
                model: None,
                mode: "text".to_string(),
                extra_args: None,
            },
            config_file_path: root.join("config.toml"),
        };
        coconutclaw_config::ensure_instance_layout(&cfg).expect("layout");
        cfg
    }

    #[test]
    fn fresh_command_returns_confirmation_output() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");
        let update = r#"{"update_id":100,"message":{"chat":{"id":"321"},"text":"/fresh"}}"#;

        let outcome = process_webhook_line(&cfg, &store, update).expect("process");
        let output = outcome.output.unwrap_or_default();

        assert!(output.contains("TELEGRAM_REPLY:"));
        assert!(output.contains("Context cleared"));
    }

    #[test]
    fn dedup_replays_previous_output_from_store() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");

        let inserted = store
            .insert_turn(&TurnRecord {
                ts: "2026-02-26T00:00:00+0000".to_string(),
                chat_id: "321".to_string(),
                input_type: "text".to_string(),
                user_text: "hello".to_string(),
                asr_text: String::new(),
                provider_raw: "TELEGRAM_REPLY: Old reply\nSEND_DOCUMENT: /tmp/file.txt\n"
                    .to_string(),
                telegram_reply: "Old reply".to_string(),
                voice_reply: String::new(),
                status: "ok".to_string(),
                update_id: Some("42".to_string()),
            })
            .expect("insert turn");
        assert!(inserted);

        let update = r#"{"update_id":42,"message":{"chat":{"id":"321"},"text":"hello again"}}"#;
        let outcome = process_webhook_line(&cfg, &store, update).expect("process");
        let output = outcome.output.unwrap_or_default();

        assert!(output.contains("TELEGRAM_REPLY: Old reply"));
        assert!(output.contains("SEND_DOCUMENT: /tmp/file.txt"));
    }

    #[test]
    fn missing_media_marker_path_does_not_fail_dispatch() {
        let cfg = test_config();
        let client = build_telegram_client(&cfg).expect("telegram client");
        let output = "TELEGRAM_REPLY: \nSEND_DOCUMENT: Z:/definitely-missing-file.txt\n";

        let dispatch = dispatch_telegram_output(&client, &cfg, Some("321"), output, None);
        assert!(dispatch.is_ok());
    }

    #[test]
    fn voice_update_parses_as_voice_input_type() {
        let cfg = test_config();
        let update =
            r#"{"update_id":200,"message":{"chat":{"id":"321"},"voice":{"file_id":"abc123"}}}"#;

        let action = parse_webhook_action(&cfg, update).expect("parse");
        let WebhookAction::Turn(turn) = action else {
            panic!("expected turn action");
        };
        assert_eq!(turn.input.input_type, "voice");
    }

    #[test]
    fn photo_update_parses_attachment_metadata() {
        let cfg = test_config();
        let update = r#"{"update_id":201,"message":{"chat":{"id":"321"},"photo":[{"file_id":"a"},{"file_id":"b"}]}}"#;

        let action = parse_webhook_action(&cfg, update).expect("parse");
        let WebhookAction::Turn(turn) = action else {
            panic!("expected turn action");
        };
        assert_eq!(turn.input.input_type, "photo");
        assert_eq!(turn.input.attachment_type.as_deref(), Some("photo"));
    }

    #[test]
    fn cancel_command_sets_runtime_cancel_marker() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");
        let update = r#"{"update_id":202,"message":{"chat":{"id":"321"},"text":"/cancel"}}"#;
        let marker_path = cfg.runtime_dir.join("cancel");
        if marker_path.exists() {
            fs::remove_file(&marker_path).expect("cleanup stale marker");
        }

        let _ = process_webhook_line(&cfg, &store, update).expect("process");

        assert!(marker_path.exists());
    }

    #[test]
    fn cancel_signal_detects_message_and_callback_query() {
        let message_update: Value = serde_json::from_str(
            r#"{"update_id":300,"message":{"chat":{"id":"321"},"text":"/cancel"}}"#,
        )
        .expect("json");
        let callback_update: Value = serde_json::from_str(
            r#"{"update_id":301,"callback_query":{"id":"cb1","data":"cancel","message":{"chat":{"id":"321"}}}}"#,
        )
        .expect("json");

        let message_signal = cancel_signal_from_update(&message_update, "321");
        assert!(message_signal.is_some());
        let callback_signal = cancel_signal_from_update(&callback_update, "321");
        assert_eq!(
            callback_signal.and_then(|signal| signal.callback_query_id),
            Some("cb1".to_string())
        );
    }

    #[test]
    fn progress_text_includes_elapsed_time() {
        let text = progress_status_text(12);
        assert!(text.contains("Thinking"));
        assert!(text.contains("12s"));
    }

    #[test]
    fn progress_text_includes_event_lines() {
        let statuses = vec![
            "Processing...".to_string(),
            "Running: cargo test".to_string(),
        ];
        let text = progress_status_with_events(9, &statuses);
        assert!(text.contains("Elapsed: 9s"));
        assert!(text.contains("- Processing..."));
        assert!(text.contains("- Running: cargo test"));
    }

    #[test]
    fn text_params_include_parse_mode_for_markdown_v2() {
        let mut cfg = test_config();
        cfg.telegram_parse_mode = TelegramParseMode::MarkdownV2;
        let params = telegram_text_form_params(
            &cfg,
            "321",
            Some("42"),
            "hello",
            Some(r#"{"inline_keyboard":[]}"#),
        );
        assert!(params.contains(&("parse_mode".to_string(), "MarkdownV2".to_string())));
    }

    #[test]
    fn context_requires_plain_text_when_parse_mode_off() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");
        let input = TurnInput {
            input_type: "text".to_string(),
            user_text: "hello".to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
        };
        let text = build_context(
            &cfg,
            &store,
            &input,
            "2026-01-01T00:00:00+0000",
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
            },
        )
        .expect("context");
        assert!(text.contains("Do not use markdown"));
    }

    #[test]
    fn context_allows_markdown_v2_when_parse_mode_enabled() {
        let mut cfg = test_config();
        cfg.telegram_parse_mode = TelegramParseMode::MarkdownV2;
        let store = Store::open(&cfg).expect("store");
        let input = TurnInput {
            input_type: "text".to_string(),
            user_text: "hello".to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
        };
        let text = build_context(
            &cfg,
            &store,
            &input,
            "2026-01-01T00:00:00+0000",
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
            },
        )
        .expect("context");
        assert!(text.contains("MarkdownV2"));
        assert!(!text.contains("Do not use markdown"));
    }

    #[test]
    fn heartbeat_subcommand_is_recognized() {
        let parsed = Cli::try_parse_from(["coconutclaw", "heartbeat"]);
        assert!(parsed.is_ok());
    }

    #[test]
    fn nightly_reflection_subcommand_is_recognized() {
        let parsed = Cli::try_parse_from(["coconutclaw", "nightly-reflection"]);
        assert!(parsed.is_ok());
    }

    #[test]
    fn nightly_reflection_marker_format_is_stable() {
        assert_eq!(
            nightly_reflection_marker("2026-02-25"),
            "<!-- nightly-reflection:2026-02-25 -->"
        );
    }
}
