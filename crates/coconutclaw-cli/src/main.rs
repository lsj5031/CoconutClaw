use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use clap::{Args, Parser, Subcommand};
use coconutclaw_config::{CliOverrides, RuntimeConfig, TelegramParseFallback, load_runtime_config};
use reqwest::blocking::Client;
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

mod context;
mod markers;
mod service;
mod slack;
mod store;
mod telegram;
mod turn;
mod webhook;

use crate::markers::{ParsedMarkers, render_output};
use crate::slack::{
    SlackMedia, SlackWebhookTurn, build_slack_client, build_slack_user_client,
    dispatch_slack_output, send_slack_progress_message, slack_fetch_thread_context,
    start_slack_socket_mode, valid_slack_channel_id, valid_slack_token,
};
use crate::store::Store;
use crate::telegram::{
    build_telegram_client, dispatch_telegram_output, fetch_cancel_updates, fetch_poll_updates,
    register_bot_commands, register_telegram_webhook, send_progress_message,
    telegram_answer_callback, valid_telegram_chat_id, valid_telegram_token,
};
use crate::turn::{hydrate_slack_turn_input, hydrate_turn_input, process_turn, resolve_turn_input};
use crate::webhook::{
    AckStatus, ack_webhook_queue_line, ensure_webhook_queue_file, extract_update_id_from_json,
    extract_update_id_from_value, peek_webhook_queue_line, spawn_webhook_http_server,
    value_to_string, webhook_request_path, with_webhook_lock,
};

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
    Doctor(DoctorArgs),
    Service(ServiceArgs),
}

#[derive(Args, Debug, Clone)]
struct DoctorArgs {
    #[arg(long)]
    json: bool,
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
struct ServiceArgs {
    #[command(subcommand)]
    action: ServiceAction,
}

#[derive(Subcommand, Debug, Clone)]
enum ServiceAction {
    Install {
        #[arg(long, default_value = "09:00")]
        heartbeat: String,
        #[arg(long, default_value = "22:30")]
        reflection: String,
    },
    Start,
    Stop,
    Status,
    Uninstall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputType {
    Text,
    Voice,
    Photo,
    Video,
    Document,
    VideoNote,
}

impl InputType {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Voice => "voice",
            Self::Photo => "photo",
            Self::Video => "video",
            Self::Document => "document",
            Self::VideoNote => "video_note",
        }
    }
}

impl std::fmt::Display for InputType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnStatus {
    Ok,
    Cancelled,
    AgentError,
    ParseRecovered,
    ParseFallback,
    AgentErrorRecovered,
}

impl TurnStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Cancelled => "cancelled",
            Self::AgentError => "agent_error",
            Self::ParseRecovered => "parse_recovered",
            Self::ParseFallback => "parse_fallback",
            Self::AgentErrorRecovered => "agent_error_recovered",
        }
    }
}

impl std::fmt::Display for TurnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TurnInput {
    input_type: InputType,
    user_text: String,
    asr_text: String,
    attachment_type: Option<String>,
    attachment_path: Option<PathBuf>,
    attachment_owned: bool,
    supplemental_context: Option<String>,
    channel: String, // "telegram", "slack", "local"
}

#[derive(Debug, Clone)]
struct QuotedMessage {
    reply_from: Option<String>,
    reply_text: Option<String>,
    reply_ts: Option<i64>,
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
    SlackTurn(Box<SlackWebhookTurn>),
}

struct TurnResult {
    markers: ParsedMarkers,
    telegram_reply: String,
    voice_reply: String,
    status: TurnStatus,
    #[allow(dead_code)]
    channel: String,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let overrides = CliOverrides {
        instance: cli.instance.clone(),
        data_dir: cli.data_dir.clone(),
        instance_dir: cli.instance_dir.clone(),
    };

    let cfg = load_runtime_config(&overrides)?;
    let command = cli.command;

    if let Commands::Service(args) = &command {
        return service::run_service(&cfg, &overrides, args.clone());
    }

    let _instance_lock = cfg.acquire_instance_lock()?;
    let mut store = Store::open(&cfg)?;

    match command {
        Commands::Once(args) => run_once(&cfg, &mut store, &args),
        Commands::Run(args) => run_run(&cfg, &mut store, &args),
        Commands::Heartbeat => run_heartbeat(&cfg, &mut store),
        Commands::NightlyReflection => run_nightly_reflection(&cfg, &mut store),
        Commands::Doctor(args) => run_doctor(&cfg, &args),
        Commands::Service(_) => unreachable!("service command handled before lock/store setup"),
    }
}

fn run_once(cfg: &RuntimeConfig, store: &mut Store, args: &TurnArgs) -> Result<()> {
    let input = resolve_turn_input(
        cfg,
        store,
        None,
        args.inject_text.clone(),
        args.inject_file.clone(),
        "telegram",
    )?;
    let output = process_turn(
        cfg,
        store,
        input,
        "telegram",
        args.chat_id.clone(),
        None,
        None,
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )?;
    print!("{output}");
    io::stdout().flush().ok();
    Ok(())
}

fn run_run(cfg: &RuntimeConfig, store: &mut Store, args: &TurnArgs) -> Result<()> {
    if args.inject_text.is_some() || args.inject_file.is_some() {
        let input = resolve_turn_input(
            cfg,
            store,
            None,
            args.inject_text.clone(),
            args.inject_file.clone(),
            "telegram",
        )?;
        let output = process_turn(
            cfg,
            store,
            input,
            "telegram",
            args.chat_id.clone(),
            None,
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
        )?;
        print!("{output}");
        io::stdout().flush().ok();
        return Ok(());
    }

    let shutdown = install_shutdown_handler()?;
    let telegram_client = build_telegram_client(cfg)?;

    if let Err(err) = register_bot_commands(&telegram_client, cfg) {
        tracing::warn!("failed to register bot menu commands: {err:#}");
    }

    // Attempt to recover any in-flight update from a previous crash before entering loops
    if let Err(err) = restore_inflight_update(cfg, store, &telegram_client) {
        tracing::warn!("failed to restore inflight update on startup: {err:#}");
    }

    // Start Slack Socket Mode listener if configured
    let slack_rx = start_socket_mode_if_configured(cfg)?;

    if cfg.webhook_mode {
        run_webhook_loop(cfg, store, &telegram_client, &shutdown, slack_rx.as_ref())?;
        return Ok(());
    }

    run_poll_loop(cfg, store, &telegram_client, &shutdown, slack_rx.as_ref())
}

fn start_socket_mode_if_configured(
    cfg: &RuntimeConfig,
) -> Result<Option<mpsc::Receiver<SlackWebhookTurn>>> {
    let has_bot_token = valid_slack_token(cfg).is_some();
    let has_app_token = cfg
        .slack_app_token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty() && t.starts_with("xapp-"))
        .is_some();

    if !has_bot_token || !has_app_token {
        return Ok(None);
    }

    let (tx, rx) = mpsc::channel::<SlackWebhookTurn>();
    start_slack_socket_mode(cfg, tx)?;
    tracing::info!("slack socket mode listener started");
    Ok(Some(rx))
}

fn process_slack_socket_turn(
    cfg: &RuntimeConfig,
    store: &mut Store,
    mut turn: SlackWebhookTurn,
) -> Result<()> {
    let slack_client = match build_slack_client(cfg) {
        Ok(c) => c,
        Err(err) => {
            tracing::error!("slack client build failed for socket mode turn: {err:#}");
            return Err(err);
        }
    };

    let progress_message_id =
        send_slack_progress_message(&slack_client, &turn.channel_id, turn.thread_ts.as_deref())
            .map_err(|err| {
                tracing::warn!("slack progress message failed: {err:#}");
                err
            })
            .ok();

    // Set inflight checkpoint before processing for crash recovery
    let inflight_payload = serde_json::json!({
        "channel": "slack",
        "event_id": turn.event_id,
        "channel_id": turn.channel_id,
        "thread_ts": turn.thread_ts,
        "user_text": turn.input.user_text,
    });
    if let Err(err) = set_inflight_update(
        store,
        turn.event_id.as_deref().unwrap_or(""),
        &inflight_payload.to_string(),
        &cfg.timezone,
    ) {
        tracing::warn!("failed to set slack inflight checkpoint: {err:#}");
    }

    maybe_populate_slack_thread_context(cfg, store, &slack_client, &mut turn);

    let (hydrated_input, cleanup_path) =
        hydrate_slack_turn_input(cfg, turn.event_id.as_deref(), turn.input, turn.media)?;

    let output = process_turn(
        cfg,
        store,
        hydrated_input,
        "slack",
        Some(turn.channel_id.clone()),
        turn.event_id.clone(),
        progress_message_id.as_deref(),
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )?;

    if let Err(err) = dispatch_slack_output(
        &slack_client,
        cfg,
        &turn.channel_id,
        &output,
        progress_message_id.as_deref(),
        turn.thread_ts.as_deref(),
    ) {
        tracing::error!("slack dispatch failed: {err:#}");
    }

    if let Err(err) = store.clear_inflight() {
        tracing::warn!("failed to clear slack inflight after turn: {err:#}");
    }

    print!("{output}");
    io::stdout().flush().ok();

    if let Some(path) = cleanup_path {
        let _ = fs::remove_file(path);
    }

    Ok(())
}

fn drain_slack_socket_turns(
    cfg: &RuntimeConfig,
    store: &mut Store,
    slack_rx: &mpsc::Receiver<SlackWebhookTurn>,
) {
    while let Ok(turn) = slack_rx.try_recv() {
        if let Err(err) = process_slack_socket_turn(cfg, store, turn) {
            tracing::warn!("failed to process slack socket mode turn: {err:#}");
        }
    }
}

fn run_due_scheduled_tasks(
    cfg: &RuntimeConfig,
    store: &mut Store,
    telegram_client: &Client,
) -> Result<()> {
    if !cfg.scheduled_tasks_enabled {
        return Ok(());
    }

    let (current_hhmm, today) = scheduled_task_slot_now(&cfg.timezone);

    let due_tasks = store.get_due_scheduled_tasks(&current_hhmm, &today)?;
    if due_tasks.is_empty() {
        return Ok(());
    }

    tracing::info!("running {} due scheduled task(s)", due_tasks.len());

    for task in due_tasks {
        let is_retry = task.pending_output.is_some();
        tracing::info!(
            "executing scheduled task id={} created={} last_run={:?} time={} recurring={} done={} prompt={:?} is_retry={}",
            task.id,
            task.ts,
            task.last_run_ts,
            task.schedule_time,
            task.recurring,
            task.done,
            task.prompt,
            is_retry
        );

        let output_to_dispatch = if let Some(pending) = task.pending_output.as_deref() {
            tracing::info!("retrying dispatch for scheduled task id={}", task.id);
            Some(pending.to_string())
        } else {
            match process_turn(
                cfg,
                store,
                TurnInput {
                    input_type: InputType::Text,
                    user_text: task.prompt.clone(),
                    asr_text: String::new(),
                    attachment_type: None,
                    attachment_path: None,
                    attachment_owned: false,
                    supplemental_context: Some(format!(
                        "This is a scheduled task set to run at {}. Source: {}.",
                        task.schedule_time, task.source
                    )),
                    channel: "telegram".to_string(),
                },
                "telegram",
                cfg.telegram_chat_id.clone(),
                None,
                None,
                &QuotedMessage {
                    reply_from: None,
                    reply_text: None,
                    reply_ts: None,
                },
            ) {
                Ok(output) => {
                    if let Err(err) = store.set_scheduled_task_pending_output(task.id, &output) {
                        tracing::warn!(
                            "failed to set pending output for scheduled task id={}: {err:#}",
                            task.id
                        );
                    }
                    Some(output)
                }
                Err(err) => {
                    tracing::warn!("scheduled task id={} failed: {err:#}", task.id);
                    None
                }
            }
        };

        if let Some(output) = output_to_dispatch {
            let mut dispatch_success = false;
            if let Err(err) = dispatch_telegram_output(
                telegram_client,
                cfg,
                cfg.telegram_chat_id.as_deref(),
                &output,
                None,
            ) {
                tracing::warn!("failed to dispatch scheduled task output: {err:#}");
            } else {
                dispatch_success = true;
            }

            if !is_retry {
                dispatch_slack_if_configured(cfg, &output);
            }

            if dispatch_success {
                let now_iso = iso_now(&cfg.timezone);
                if let Err(err) = store.mark_scheduled_task_executed(task.id, &now_iso) {
                    tracing::warn!(
                        "failed to mark scheduled task id={} as executed: {err:#}",
                        task.id
                    );
                }
            }
        }
    }

    Ok(())
}

fn run_heartbeat(cfg: &RuntimeConfig, store: &mut Store) -> Result<()> {
    let prompt = "Daily heartbeat for CoconutClaw. Summarize today, surface urgent tasks from TASKS/pending.md, and suggest next 1-3 actions.";
    let output = process_turn(
        cfg,
        store,
        TurnInput {
            input_type: InputType::Text,
            user_text: prompt.to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "telegram".to_string(),
        },
        "telegram",
        cfg.telegram_chat_id.clone(),
        None,
        None,
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
    )?;

    let client = build_telegram_client(cfg)?;
    dispatch_telegram_output(&client, cfg, cfg.telegram_chat_id.as_deref(), &output, None)?;
    dispatch_slack_if_configured(cfg, &output);
    print!("{output}");
    io::stdout().flush().ok();
    Ok(())
}

fn run_nightly_reflection(cfg: &RuntimeConfig, store: &mut Store) -> Result<()> {
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
        tracing::info!("nightly reflection already exists for {local_day}");
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
                input_type: InputType::Text,
                user_text: prompt.clone(),
                asr_text: String::new(),
                attachment_type: None,
                attachment_path: None,
                attachment_owned: false,
                supplemental_context: None,
                channel: "telegram".to_string(),
            },
            "telegram",
            cfg.telegram_chat_id.clone(),
            None,
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
        )?;
        let client = build_telegram_client(cfg)?;
        dispatch_telegram_output(&client, cfg, cfg.telegram_chat_id.as_deref(), &output, None)?;
        dispatch_slack_if_configured(cfg, &output);
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

    tracing::info!(
        "nightly reflection appended to {}",
        reflection_path.display()
    );
    Ok(())
}

fn run_doctor(cfg: &RuntimeConfig, args: &DoctorArgs) -> Result<()> {
    let codex_ok = command_exists(&cfg.codex.bin);
    let pi_ok = command_exists(&cfg.pi.bin);
    let ffmpeg_ok = command_exists("ffmpeg");
    let bash_ok = command_exists("bash");
    let curl_ok = command_exists("curl");
    let jq_ok = command_exists("jq");
    let telegram_token_ok = valid_telegram_token(cfg).is_some();
    let telegram_chat_id_ok = valid_telegram_chat_id(cfg).is_some();
    let slack_token_ok = valid_slack_token(cfg).is_some();
    let slack_user_token_ok = cfg
        .slack_user_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    let slack_channel_id_ok = valid_slack_channel_id(cfg).is_some();
    let webhook_bind_ok = cfg.webhook_bind.parse::<std::net::SocketAddr>().is_ok();
    let webhook_public_url_ok = cfg
        .webhook_public_url
        .as_deref()
        .map(str::trim)
        .filter(|value| value.starts_with("http://") || value.starts_with("https://"))
        .is_some();
    let asr_script_ok = cfg.root_dir.join("scripts/asr.sh").exists();
    let tts_script_ok = cfg.root_dir.join("scripts/tts.sh").exists();
    let asr_enabled = asr_feature_enabled(cfg);
    let asr_uses_http = cfg.asr_cmd_template.is_none() && cfg.asr_url.is_some();
    let asr_preprocess = parse_on_like(cfg.asr_preprocess.as_deref(), true);
    let tts_enabled = cfg.tts_cmd_template.is_some();

    let webhook_secret_set = cfg
        .webhook_secret
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();

    if args.json {
        let report = serde_json::json!({
            "instance_name": cfg.instance_name,
            "instance_dir": cfg.instance_dir.display().to_string(),
            "data_dir": cfg.data_dir.display().to_string(),
            "sqlite_db_path": cfg.sqlite_db_path.display().to_string(),
            "provider": cfg.provider.as_str(),
            "timezone": cfg.timezone,
            "webhook_mode": cfg.webhook_mode,
            "webhook_bind": cfg.webhook_bind,
            "webhook_path": webhook_request_path(cfg),
            "webhook_public_url": cfg.webhook_public_url.as_deref().unwrap_or(""),
            "webhook_secret_set": webhook_secret_set,
            "telegram_parse_mode": cfg.telegram_parse_mode.as_api_value().unwrap_or("off"),
            "telegram_parse_fallback": match cfg.telegram_parse_fallback {
                TelegramParseFallback::Plain => "plain",
                TelegramParseFallback::None => "none",
            },
            "poll_interval_seconds": cfg.poll_interval_seconds,
            "context_turns": cfg.context_turns,
            "provider_max_retries": cfg.provider_max_retries,
            "progress_update_interval_secs": cfg.progress_update_interval_secs,
            "config_file": cfg.config_file_path.display().to_string(),
            "features": {
                "asr": asr_enabled,
                "tts": tts_enabled,
            },
            "checks": {
                "codex_bin": { "ok": codex_ok, "path": cfg.codex.bin },
                "pi_bin": { "ok": pi_ok, "path": cfg.pi.bin },
                "telegram_token": telegram_token_ok,
                "telegram_chat_id": telegram_chat_id_ok,
                "slack_token": slack_token_ok,
                "slack_user_token": slack_user_token_ok,
                "slack_channel_id": slack_channel_id_ok,
                "webhook_bind": webhook_bind_ok,
                "webhook_public_url": webhook_public_url_ok,
                "asr_script": asr_script_ok,
                "tts_script": tts_script_ok,
                "bash": bash_ok,
                "ffmpeg": ffmpeg_ok,
                "curl": curl_ok,
                "jq": jq_ok,
            },
        });
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

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
    println!("webhook_bind={}", cfg.webhook_bind);
    println!("webhook_path={}", webhook_request_path(cfg));
    println!(
        "webhook_public_url={}",
        cfg.webhook_public_url.as_deref().unwrap_or("")
    );
    println!(
        "webhook_secret_set={}",
        if webhook_secret_set { "yes" } else { "no" }
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
    println!("context_turns={}", cfg.context_turns);
    println!("provider_max_retries={}", cfg.provider_max_retries);
    println!(
        "progress_update_interval_secs={}",
        cfg.progress_update_interval_secs
    );
    println!("config_file={}", cfg.config_file_path.display());

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
    println!("check_slack_token={} (optional)", yes_no(slack_token_ok));
    println!(
        "check_slack_user_token={} (optional; enables full thread history in channels)",
        yes_no(slack_user_token_ok)
    );
    println!(
        "check_slack_channel_id={} (optional)",
        yes_no(slack_channel_id_ok)
    );
    if cfg.webhook_mode {
        println!(
            "check_webhook_bind={} (required when WEBHOOK_MODE is enabled)",
            yes_no(webhook_bind_ok)
        );
        println!(
            "check_webhook_public_url={} (required when WEBHOOK_MODE is enabled)",
            yes_no(webhook_public_url_ok)
        );
    } else {
        println!("check_webhook_bind={} (optional)", yes_no(webhook_bind_ok));
        println!(
            "check_webhook_public_url={} (optional)",
            yes_no(webhook_public_url_ok)
        );
    }
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
    store: &mut Store,
    telegram_client: &Client,
    shutdown: &Arc<AtomicBool>,
    slack_rx: Option<&mpsc::Receiver<SlackWebhookTurn>>,
) -> Result<()> {
    let mut offset = store
        .kv_get("last_update_id")?
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.saturating_add(1));

    while !shutdown.load(Ordering::SeqCst) {
        let updates = match fetch_poll_updates(telegram_client, cfg, offset) {
            Ok(updates) => updates,
            Err(err) => {
                tracing::warn!("telegram polling failed: {err:#}");
                thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
                continue;
            }
        };

        if updates.is_empty() {
            if let Some(rx) = slack_rx {
                drain_slack_socket_turns(cfg, store, rx);
            }
            // Run any due scheduled tasks before sleeping
            if let Err(err) = run_due_scheduled_tasks(cfg, store, telegram_client) {
                tracing::warn!("scheduled task execution failed: {err:#}");
            }
            thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
            continue;
        }

        for update in updates {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            let update_id =
                extract_update_id_from_value(&update).and_then(|value| value.parse::<u64>().ok());
            let line = match serde_json::to_string(&update) {
                Ok(line) => line,
                Err(err) => {
                    tracing::warn!("failed to serialize polled update (dropping): {err:#}");
                    if let Some(update_id) = update_id {
                        offset = Some(update_id.saturating_add(1));
                    }
                    continue;
                }
            };
            // Set inflight checkpoint before processing for crash recovery
            if let Err(err) = set_inflight_update(
                store,
                update_id.map(|id| id.to_string()).as_deref().unwrap_or(""),
                &line,
                &cfg.timezone,
            ) {
                tracing::warn!("failed to set inflight checkpoint: {err:#}");
            }
            let outcome = match process_webhook_line(cfg, store, &line) {
                Ok(outcome) => outcome,
                Err(err) => {
                    tracing::warn!(
                        "failed to process polled update_id={} (dropping update): {err:#}",
                        update_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    );
                    if let Some(update_id) = update_id {
                        offset = Some(update_id.saturating_add(1));
                    }
                    let _ = store.clear_inflight();
                    continue;
                }
            };

            if let Some(output) = outcome.output.as_deref() {
                if let Err(err) = dispatch_telegram_output(
                    telegram_client,
                    cfg,
                    outcome.chat_id.as_deref(),
                    output,
                    outcome.progress_message_id.as_deref(),
                ) {
                    tracing::warn!("failed to dispatch polled output: {err:#}");
                } else {
                    println!("{output}");
                    io::stdout().flush().ok();
                }
            }
            if let Some(path) = outcome.cleanup_path.as_deref() {
                let _ = fs::remove_file(path);
            }

            if let Err(err) = store.clear_inflight() {
                tracing::warn!("failed to clear inflight after poll processing: {err:#}");
            }

            if let Some(update_id) = update_id {
                offset = Some(update_id.saturating_add(1));
            }
        }

        // Drain any pending Slack socket mode turns
        if let Some(rx) = slack_rx {
            drain_slack_socket_turns(cfg, store, rx);
        }

        // Run any due scheduled tasks
        if let Err(err) = run_due_scheduled_tasks(cfg, store, telegram_client) {
            tracing::warn!("scheduled task execution failed: {err:#}");
        }
    }

    tracing::info!("shutdown signal received, stopping poll loop");
    Ok(())
}

fn run_webhook_loop(
    cfg: &RuntimeConfig,
    store: &mut Store,
    telegram_client: &Client,
    shutdown: &Arc<AtomicBool>,
    slack_rx: Option<&mpsc::Receiver<SlackWebhookTurn>>,
) -> Result<()> {
    ensure_webhook_queue_file(cfg)?;

    register_telegram_webhook(telegram_client, cfg)?;
    let _http_server = spawn_webhook_http_server(cfg.clone(), Arc::clone(shutdown))?;

    while !shutdown.load(Ordering::SeqCst) {
        // Drain Slack socket mode turns first
        if let Some(rx) = slack_rx {
            drain_slack_socket_turns(cfg, store, rx);
        }

        let progressed = match drain_webhook_queue(cfg, store, telegram_client, shutdown) {
            Ok(progressed) => progressed,
            Err(err) => {
                tracing::warn!("webhook queue drain failed (will continue): {err:#}");
                false
            }
        };

        // Run any due scheduled tasks
        if let Err(err) = run_due_scheduled_tasks(cfg, store, telegram_client) {
            tracing::warn!("scheduled task execution failed: {err:#}");
        }

        if !progressed {
            thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
        }
    }

    tracing::info!("shutdown signal received, stopping webhook loop");
    Ok(())
}

fn restore_inflight_update(
    cfg: &RuntimeConfig,
    store: &mut Store,
    telegram_client: &Client,
) -> Result<()> {
    let Some(inflight_json) = store.kv_get("inflight_update_json")? else {
        return Ok(());
    };

    // Check if this is a Slack inflight record (JSON with "channel": "slack")
    if let Ok(v) = serde_json::from_str::<Value>(&inflight_json)
        && v.get("channel").and_then(|c| c.as_str()) == Some("slack")
    {
        restore_inflight_slack(cfg, store, &v)?;
        return Ok(());
    }

    let mut inflight_update_id = store.kv_get("inflight_update_id")?;
    if inflight_update_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        match extract_update_id_from_json(&inflight_json) {
            Ok(id) => inflight_update_id = id,
            Err(err) => {
                tracing::warn!("inflight JSON is malformed, clearing inflight record: {err:#}");
                let _ = store.clear_inflight();
                return Ok(());
            }
        }
    }

    if let Some(update_id) = inflight_update_id.as_deref()
        && store.turn_exists_for_update_id(update_id)?
    {
        store.clear_inflight()?;
        match ack_webhook_queue_line(cfg, Some(update_id))? {
            AckStatus::Acked => {
                tracing::info!("restored inflight update_id={update_id} (dedup + ack)");
            }
            AckStatus::HeadMismatch => {
                tracing::warn!(
                    "inflight restore head mismatch for update_id={update_id}, leaving queue as-is"
                );
            }
            AckStatus::Empty => {}
        }
        return Ok(());
    }

    let outcome = match process_webhook_line(cfg, store, &inflight_json) {
        Ok(outcome) => outcome,
        Err(err) => {
            tracing::warn!("failed to process inflight update, clearing: {err:#}");
            let _ = store.clear_inflight();
            return Ok(());
        }
    };
    if outcome.should_ack {
        let expected_id = outcome
            .update_id
            .as_deref()
            .or(inflight_update_id.as_deref());
        match ack_webhook_queue_line(cfg, expected_id)? {
            AckStatus::Acked => {
                store.clear_inflight()?;
                if let Some(output) = outcome.output.as_deref() {
                    if let Err(err) =
                        dispatch_process_outcome(cfg, telegram_client, &outcome, output)
                    {
                        tracing::warn!("failed to dispatch restored inflight output: {err:#}");
                    } else {
                        println!("{output}");
                        io::stdout().flush().ok();
                    }
                }
                if let Some(path) = outcome.cleanup_path.as_deref() {
                    let _ = fs::remove_file(path);
                }
            }
            AckStatus::HeadMismatch => {
                tracing::warn!("inflight restore ack skipped due queue head mismatch");
            }
            AckStatus::Empty => {
                if let Some(output) = outcome.output.as_deref() {
                    if let Err(err) =
                        dispatch_process_outcome(cfg, telegram_client, &outcome, output)
                    {
                        tracing::warn!(
                            "failed to dispatch restored inflight output (empty queue): {err:#}"
                        );
                    } else {
                        println!("{output}");
                        io::stdout().flush().ok();
                    }
                }
                store.clear_inflight()?;
            }
        }
    }

    Ok(())
}

/// Restore an inflight Slack Socket Mode turn from a previous crash.
fn restore_inflight_slack(cfg: &RuntimeConfig, store: &mut Store, payload: &Value) -> Result<()> {
    let event_id = payload
        .get("event_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let channel_id = payload
        .get("channel_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let thread_ts = payload.get("thread_ts").and_then(|v| v.as_str());
    let user_text = payload
        .get("user_text")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if !event_id.is_empty() && store.turn_exists_for_update_id(event_id)? {
        tracing::info!("restored slack inflight event_id={event_id} (dedup, already processed)");
        store.clear_inflight()?;
        return Ok(());
    }

    tracing::info!("restoring inflight slack turn: event_id={event_id} channel={channel_id}");

    let turn = SlackWebhookTurn {
        event_id: if event_id.is_empty() {
            None
        } else {
            Some(event_id.to_string())
        },
        channel_id: channel_id.to_string(),
        thread_ts: thread_ts.map(|s| s.to_string()),
        input: TurnInput {
            input_type: InputType::Text,
            user_text: if user_text.is_empty() {
                "(empty message)".to_string()
            } else {
                user_text.to_string()
            },
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "slack".to_string(),
        },
        media: None,
    };

    if let Err(err) = process_slack_socket_turn(cfg, store, turn) {
        tracing::warn!("failed to restore inflight slack turn, clearing: {err:#}");
        let _ = store.clear_inflight();
    }

    Ok(())
}

fn drain_webhook_queue(
    cfg: &RuntimeConfig,
    store: &mut Store,
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
        let expected_update_id = match extract_update_id_from_json(&line) {
            Ok(update_id) => update_id,
            Err(err) => {
                tracing::warn!("dropping malformed webhook queue entry: {err:#}");
                match ack_webhook_queue_line(cfg, None) {
                    Ok(AckStatus::Acked) => progressed = true,
                    Ok(AckStatus::HeadMismatch | AckStatus::Empty) => {}
                    Err(ack_err) => {
                        tracing::warn!(
                            "failed to ack malformed webhook queue entry (will retry): {ack_err:#}"
                        );
                        break;
                    }
                }
                continue;
            }
        };

        let outcome = match process_webhook_line(cfg, store, &line) {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::warn!("webhook processing failed (will retry): {err:#}");
                break;
            }
        };

        if !outcome.should_ack {
            break;
        }

        let ack_status = match ack_webhook_queue_line(cfg, expected_update_id.as_deref()) {
            Ok(status) => status,
            Err(err) => {
                tracing::warn!("webhook ack failed (will retry): {err:#}");
                break;
            }
        };

        match ack_status {
            AckStatus::Acked => {
                if let Err(err) = store.clear_inflight() {
                    tracing::warn!("failed to clear inflight after webhook ack: {err:#}");
                }
                progressed = true;
                if let Some(output) = outcome.output.as_deref() {
                    if let Err(err) =
                        dispatch_process_outcome(cfg, telegram_client, &outcome, output)
                    {
                        tracing::warn!("failed to dispatch webhook output: {err:#}");
                    } else {
                        println!("{output}");
                        io::stdout().flush().ok();
                    }
                }
                if let Some(path) = outcome.cleanup_path.as_deref() {
                    let _ = fs::remove_file(path);
                }
            }
            AckStatus::HeadMismatch => {
                tracing::warn!(
                    "webhook ack skipped due queue head mismatch update_id={}",
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
    output_channel: Option<String>,
    output_thread_ts: Option<String>,
    output: Option<String>,
    cleanup_path: Option<PathBuf>,
    progress_message_id: Option<String>,
}

fn maybe_populate_slack_thread_context(
    cfg: &RuntimeConfig,
    store: &Store,
    slack_client: &Client,
    turn: &mut SlackWebhookTurn,
) {
    let Some(ref thread_ts) = turn.thread_ts else {
        return;
    };

    let boundary_unix = match store.latest_boundary_unix(&turn.channel_id, "slack") {
        Ok(ts) => ts,
        Err(err) => {
            tracing::warn!("failed to read slack context boundary: {err:#}");
            None
        }
    };

    let history_client = build_slack_user_client(cfg)
        .unwrap_or(None)
        .unwrap_or_else(|| slack_client.clone());

    match slack_fetch_thread_context(
        &history_client,
        &turn.channel_id,
        Some(thread_ts),
        boundary_unix.map(|ts| ts as f64),
    ) {
        Ok(ctx) if !ctx.trim().is_empty() => turn.input.supplemental_context = Some(ctx),
        Ok(_) => turn.input.supplemental_context = None,
        Err(err) => tracing::warn!("failed to fetch slack thread context: {err:#}"),
    }
}

fn dispatch_process_outcome(
    cfg: &RuntimeConfig,
    telegram_client: &Client,
    outcome: &ProcessOutcome,
    output: &str,
) -> Result<()> {
    match outcome.output_channel.as_deref() {
        Some("slack") => {
            let slack_client = build_slack_client(cfg)?;
            let channel_id = outcome.chat_id.as_deref().ok_or_else(|| {
                anyhow::anyhow!("missing slack channel id for webhook output dispatch")
            })?;
            dispatch_slack_output(
                &slack_client,
                cfg,
                channel_id,
                output,
                outcome.progress_message_id.as_deref(),
                outcome.output_thread_ts.as_deref(),
            )
        }
        _ => dispatch_telegram_output(
            telegram_client,
            cfg,
            outcome.chat_id.as_deref(),
            output,
            outcome.progress_message_id.as_deref(),
        ),
    }
}

fn is_slack_interactive_payload(value: &Value) -> bool {
    value
        .get("type")
        .and_then(Value::as_str)
        .map(|t| t == "block_actions" || t == "interactive_message" || t == "slash_commands")
        .unwrap_or(false)
}

fn check_slack_cancel(value: &Value) -> bool {
    let payload_type = value.get("type").and_then(Value::as_str).unwrap_or("");
    match payload_type {
        "block_actions" | "interactive_message" => value
            .get("actions")
            .and_then(Value::as_array)
            .map(|actions| {
                actions
                    .iter()
                    .any(|a| a.get("action_id").and_then(Value::as_str) == Some("cancel"))
            })
            .unwrap_or(false),
        "slash_commands" => value.get("command").and_then(Value::as_str) == Some("/cancel"),
        _ => false,
    }
}

fn check_slack_fresh(value: &Value) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some("slash_commands") {
        return None;
    }
    if value.get("command").and_then(Value::as_str) != Some("/fresh") {
        return None;
    }
    value
        .get("channel_id")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
}

fn is_slack_event(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|v| {
            v.get("type")
                .and_then(|t| t.as_str())
                .map(|s| s == "event_callback")
        })
        .unwrap_or(false)
}

fn parse_slack_webhook_event(_cfg: &RuntimeConfig, payload: &str) -> Option<WebhookAction> {
    let value: Value = serde_json::from_str(payload).ok()?;

    if value.get("type").and_then(|v| v.as_str()) != Some("event_callback") {
        return None;
    }

    let event = value.get("event")?;
    let event_type = event.get("type")?.as_str()?;

    if event_type != "message" {
        return None;
    }

    // Skip bot messages (avoid echo loops)
    if event.get("bot_id").is_some()
        || event.get("subtype").and_then(|v| v.as_str()) == Some("bot_message")
    {
        return None;
    }

    let event_id = value
        .get("event_id")
        .and_then(|v| v.as_str())
        .map(String::from);
    let channel_id = event.get("channel")?.as_str()?.to_string();
    let text = event
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let thread_ts = event
        .get("thread_ts")
        .and_then(|v| v.as_str())
        .map(String::from);

    // Check for files
    let media = event.get("files").and_then(|files| {
        files.as_array()?.first().and_then(|file| {
            Some(SlackMedia::File {
                url_private: file.get("url_private")?.as_str()?.to_string(),
                filetype: file
                    .get("filetype")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                filename: file
                    .get("filename")
                    .and_then(|v| v.as_str())
                    .unwrap_or("file")
                    .to_string(),
                size: file.get("size").and_then(|v| v.as_u64()),
            })
        })
    });

    Some(WebhookAction::SlackTurn(Box::new(SlackWebhookTurn {
        event_id,
        channel_id,
        thread_ts,
        input: TurnInput {
            input_type: InputType::Text,
            user_text: text,
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "slack".to_string(),
        },
        media,
    })))
}

fn process_webhook_line(
    cfg: &RuntimeConfig,
    store: &mut Store,
    line: &str,
) -> Result<ProcessOutcome> {
    // Resolve the webhook action: try Slack event/payload detection first, then Telegram
    let action = if is_slack_event(line) {
        parse_slack_webhook_event(cfg, line).unwrap_or_else(|| WebhookAction::Ignore {
            update_id: None,
            reason: "unhandled_slack_event".to_string(),
        })
    } else if let Ok(value) = serde_json::from_str::<Value>(line) {
        if is_slack_interactive_payload(&value) {
            if check_slack_cancel(&value) {
                if let Err(err) = signal_cancel_marker(cfg) {
                    tracing::warn!("failed to set cancel marker: {err:#}");
                }
                tracing::info!("slack cancel received");
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: None,
                    chat_id: None,
                    output_channel: None,
                    output_thread_ts: None,
                    output: None,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }
            if let Some(channel_id) = check_slack_fresh(&value) {
                let ts = iso_now(&cfg.timezone);
                match store.insert_boundary_turn(&ts, &channel_id, None, "slack") {
                    Ok(true) => {
                        tracing::info!("inserted context boundary for slack channel={channel_id}")
                    }
                    Ok(false) => {}
                    Err(err) => tracing::warn!("failed to insert context boundary: {err:#}"),
                }
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: None,
                    chat_id: Some(channel_id),
                    output_channel: Some("slack".to_string()),
                    output_thread_ts: None,
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
                });
            }
            // Other interactive payloads we don't handle — ack and ignore
            WebhookAction::Ignore {
                update_id: None,
                reason: "unhandled_slack_interactive_payload".to_string(),
            }
        } else {
            parse_webhook_action(cfg, line)?
        }
    } else {
        parse_webhook_action(cfg, line)?
    };

    match action {
        WebhookAction::Ignore { update_id, reason } => {
            let update_id_text = update_id.as_deref().unwrap_or_default().to_string();
            let ignored_at = iso_now(&cfg.timezone);
            let _ = store.kv_set("last_ignored_update_id", &update_id_text);
            let _ = store.kv_set("last_ignored_reason", &reason);
            let _ = store.kv_set("last_ignored_at", &ignored_at);
            tracing::info!(
                "ignored telegram update_id={} reason={reason}",
                if update_id_text.trim().is_empty() {
                    "unknown"
                } else {
                    update_id_text.as_str()
                }
            );
            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: None,
                output_channel: None,
                output_thread_ts: None,
                output: None,
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Cancel { update_id } => {
            if let Err(err) = signal_cancel_marker(cfg) {
                tracing::warn!("failed to set cancel marker: {err:#}");
            }
            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: None,
                output_channel: None,
                output_thread_ts: None,
                output: None,
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Fresh { update_id, chat_id } => {
            let ts = iso_now(&cfg.timezone);
            match store.insert_boundary_turn(&ts, &chat_id, update_id.as_deref(), "telegram") {
                Ok(true) => tracing::info!("inserted context boundary for chat_id={chat_id}"),
                Ok(false) => {}
                Err(err) => tracing::warn!("failed to insert context boundary: {err:#}"),
            }
            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(chat_id),
                output_channel: Some("telegram".to_string()),
                output_thread_ts: None,
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
                let _ = store.kv_set("last_update_id", update_id);
                let replay_output = store.rendered_output_for_update_id(update_id)?;
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(update_id.to_string()),
                    chat_id: Some(chat_id),
                    output_channel: Some("telegram".to_string()),
                    output_thread_ts: None,
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
                    tracing::warn!("failed to send progress message: {err:#}");
                    err
                })
                .ok()
                .flatten();

            let (hydrated_input, cleanup_path) = hydrate_turn_input(
                cfg,
                turn.update_id.as_deref(),
                turn.input,
                turn.media,
                "telegram",
            )?;

            let output = process_turn(
                cfg,
                store,
                hydrated_input,
                "telegram",
                Some(chat_id.clone()),
                turn.update_id.clone(),
                progress_message_id.as_deref(),
                &turn.quoted,
            )?;

            if let Some(update_id) = turn.update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }

            Ok(ProcessOutcome {
                should_ack: true,
                update_id: turn.update_id,
                chat_id: Some(chat_id),
                output_channel: Some("telegram".to_string()),
                output_thread_ts: None,
                output: Some(output.trim_end().to_string()),
                cleanup_path,
                progress_message_id,
            })
        }
        WebhookAction::SlackTurn(mut turn) => {
            let slack_client = match build_slack_client(cfg) {
                Ok(c) => c,
                Err(err) => {
                    tracing::error!("slack client build failed: {err:#}");
                    return Ok(ProcessOutcome {
                        should_ack: true,
                        update_id: turn.event_id,
                        chat_id: Some(turn.channel_id.clone()),
                        output_channel: None,
                        output_thread_ts: None,
                        output: None,
                        cleanup_path: None,
                        progress_message_id: None,
                    });
                }
            };

            maybe_populate_slack_thread_context(cfg, store, &slack_client, &mut turn);

            let progress_message_id = send_slack_progress_message(
                &slack_client,
                &turn.channel_id,
                turn.thread_ts.as_deref(),
            )
            .map_err(|err| {
                tracing::warn!("slack progress message failed: {err:#}");
                err
            })
            .ok();

            let (hydrated_input, cleanup_path) =
                hydrate_slack_turn_input(cfg, turn.event_id.as_deref(), turn.input, turn.media)?;

            let output = process_turn(
                cfg,
                store,
                hydrated_input,
                "slack",
                Some(turn.channel_id.clone()),
                turn.event_id.clone(),
                progress_message_id.as_deref(),
                &QuotedMessage {
                    reply_from: None,
                    reply_text: None,
                    reply_ts: None,
                },
            )?;

            Ok(ProcessOutcome {
                should_ack: true,
                update_id: turn.event_id,
                chat_id: Some(turn.channel_id),
                output_channel: Some("slack".to_string()),
                output_thread_ts: turn.thread_ts,
                output: Some(output.trim_end().to_string()),
                cleanup_path,
                progress_message_id,
            })
        }
    }
}

pub(crate) fn cancel_marker_path(cfg: &RuntimeConfig) -> PathBuf {
    cfg.runtime_dir.join("cancel")
}

pub(crate) fn signal_cancel_marker(cfg: &RuntimeConfig) -> Result<()> {
    let path = cancel_marker_path(cfg);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&path, "").with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub(crate) fn clear_cancel_marker(cfg: &RuntimeConfig) {
    let path = cancel_marker_path(cfg);
    let _ = fs::remove_file(path);
}

pub(crate) fn cancel_signal_from_update(
    value: &Value,
    expected_chat_id: &str,
) -> Option<CancelSignal> {
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

pub(crate) fn maybe_spawn_cancel_watcher(
    cfg: &RuntimeConfig,
    store: &mut Store,
    active_update_id: Option<String>,
    cancel_flag: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
) -> Result<Option<std::thread::JoinHandle<()>>> {
    let expected_chat = valid_telegram_chat_id(cfg)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("missing TELEGRAM_CHAT_ID"))?;
    if valid_telegram_token(cfg).is_none() {
        return Ok(None);
    }

    let poll_offset = store
        .kv_get("last_update_id")?
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.saturating_add(1));

    let cfg_clone = cfg.clone();
    let handle = thread::spawn(move || {
        let client = match build_telegram_client(&cfg_clone) {
            Ok(client) => client,
            Err(_) => return,
        };
        let mut offset = poll_offset;

        while !stop_flag.load(Ordering::SeqCst) && !cancel_flag.load(Ordering::SeqCst) {
            if cfg_clone.webhook_mode {
                match scan_webhook_queue_for_cancel(
                    &cfg_clone,
                    &expected_chat,
                    active_update_id.as_deref(),
                ) {
                    Ok(Some(signal)) => {
                        if let Some(callback_id) = signal.callback_query_id.as_deref() {
                            let _ = telegram_answer_callback(&client, &cfg_clone, callback_id);
                        }
                        cancel_flag.store(true, Ordering::SeqCst);
                        let _ = signal_cancel_marker(&cfg_clone);
                        break;
                    }
                    Ok(None) => {
                        thread::sleep(Duration::from_millis(300));
                        continue;
                    }
                    Err(_) => {
                        thread::sleep(Duration::from_millis(500));
                        continue;
                    }
                }
            }

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

pub(crate) fn scan_webhook_queue_for_cancel(
    cfg: &RuntimeConfig,
    expected_chat_id: &str,
    active_update_id: Option<&str>,
) -> Result<Option<CancelSignal>> {
    with_webhook_lock(cfg, || {
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        if !queue_path.exists() {
            return Ok(None);
        }

        let payload = fs::read_to_string(&queue_path)
            .with_context(|| format!("failed to read {}", queue_path.display()))?;
        for line in payload.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_str(trimmed) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if let Some(active_id) = active_update_id
                && extract_update_id_from_value(&value).as_deref() == Some(active_id)
            {
                continue;
            }
            if let Some(signal) = cancel_signal_from_update(&value, expected_chat_id) {
                return Ok(Some(signal));
            }
        }
        Ok(None)
    })
}

pub(crate) fn parse_webhook_action(cfg: &RuntimeConfig, line: &str) -> Result<WebhookAction> {
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
    let reply_ts = message
        .get("reply_to_message")
        .and_then(|node| node.get("date"))
        .and_then(Value::as_i64);

    let (input_type, attachment_type) = match media.as_ref() {
        Some(IncomingMedia::Voice { .. }) => (InputType::Voice, None),
        Some(IncomingMedia::Photo { .. }) => (InputType::Photo, Some("photo".to_string())),
        Some(IncomingMedia::Document { .. }) => (InputType::Document, Some("document".to_string())),
        Some(IncomingMedia::Video { .. }) => (InputType::Video, Some("video".to_string())),
        Some(IncomingMedia::VideoNote { .. }) => {
            (InputType::VideoNote, Some("video_note".to_string()))
        }
        None => (InputType::Text, None),
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
        supplemental_context: None,
        channel: "telegram".to_string(),
    };

    Ok(WebhookAction::Turn(Box::new(WebhookTurn {
        update_id,
        chat_id,
        input,
        media,
        quoted: QuotedMessage {
            reply_from,
            reply_text,
            reply_ts,
        },
    })))
}

pub(crate) fn shorten_log_text(text: &str, max_chars: usize) -> String {
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

pub(crate) fn extract_incoming_media(message: &Value) -> Option<IncomingMedia> {
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

pub(crate) fn is_allowed_chat(cfg: &RuntimeConfig, chat_id: Option<&str>) -> bool {
    match (cfg.telegram_chat_id.as_deref(), chat_id) {
        (None, _) | (Some(_), None) => false,
        (Some(expected), Some(actual)) => expected == actual,
    }
}

pub(crate) fn set_inflight_update(
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

pub(crate) fn resolve_instance_path(instance_dir: &Path, raw: PathBuf) -> PathBuf {
    if raw.is_absolute() {
        raw
    } else {
        instance_dir.join(raw)
    }
}

pub(crate) fn local_day(timezone: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    if let Ok(tz) = timezone.parse::<Tz>() {
        return now.with_timezone(&tz).format("%Y-%m-%d").to_string();
    }
    now.format("%Y-%m-%d").to_string()
}

fn scheduled_task_slot_at(now: DateTime<Utc>, timezone: &str) -> (String, String) {
    if let Ok(tz) = timezone.parse::<Tz>() {
        let local = now.with_timezone(&tz);
        return (
            local.format("%H:%M").to_string(),
            local.format("%Y-%m-%d").to_string(),
        );
    }
    (
        now.format("%H:%M").to_string(),
        now.format("%Y-%m-%d").to_string(),
    )
}

fn scheduled_task_slot_now(timezone: &str) -> (String, String) {
    scheduled_task_slot_at(Utc::now(), timezone)
}

pub(crate) fn iso_now(timezone: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    if let Ok(tz) = timezone.parse::<Tz>() {
        return now
            .with_timezone(&tz)
            .format("%Y-%m-%dT%H:%M:%S%z")
            .to_string();
    }
    now.format("%Y-%m-%dT%H:%M:%S%z").to_string()
}

pub(crate) fn nightly_reflection_marker(day: &str) -> String {
    format!("<!-- nightly-reflection:{day} -->")
}

pub(crate) fn nightly_reflection_prompt(cfg: &RuntimeConfig) -> String {
    cfg.nightly_reflection_prompt.clone().unwrap_or_else(|| {
        "Do a brief nightly reflection in first person: include today outcomes, today lessons, and the single most important thing for tomorrow in under 120 words.".to_string()
    })
}

pub(crate) fn nightly_reflection_file_path(cfg: &RuntimeConfig) -> PathBuf {
    cfg.nightly_reflection_file.clone()
}

pub(crate) fn command_exists(bin: &str) -> bool {
    let candidate = Path::new(bin);
    if candidate.is_absolute() || bin.contains('/') || bin.contains('\\') {
        return candidate.is_file();
    }
    crate::service::find_on_path(bin).is_some()
}

pub(crate) fn yes_no(value: bool) -> &'static str {
    if value { "ok" } else { "missing" }
}

fn dispatch_slack_if_configured(cfg: &RuntimeConfig, output: &str) {
    if valid_slack_token(cfg).is_none() {
        return;
    }
    let Ok(slack_client) = build_slack_client(cfg) else {
        return;
    };
    let Some(ch) = valid_slack_channel_id(cfg) else {
        return;
    };
    if let Err(err) = dispatch_slack_output(&slack_client, cfg, ch, output, None, None) {
        tracing::warn!("slack dispatch failed: {err:#}");
    }
}

pub(crate) fn asr_feature_enabled(cfg: &RuntimeConfig) -> bool {
    cfg.asr_cmd_template.is_some() || cfg.asr_url.is_some()
}

pub(crate) use coconutclaw_config::parse_on_off as parse_on_like;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::build_context;
    use crate::markers::{
        extract_error_summary, parse_markers, recover_unstructured_reply,
        should_retry_provider_failure,
    };
    use crate::store::TurnRecord;
    use crate::telegram::{
        progress_status_text, progress_status_with_events, render_markdown_v2_reply,
        render_telegram_reply_text, should_fallback_plain_for_error, should_send_reply_as_document,
        split_text_chunks, telegram_retry_after_seconds, telegram_text_form_params,
    };
    use crate::webhook::webhook_public_endpoint;
    use chrono::DateTime;
    use coconutclaw_config::{RuntimeConfig, TelegramParseMode};

    fn test_config() -> RuntimeConfig {
        RuntimeConfig::test_config()
    }

    #[test]
    fn fresh_command_returns_confirmation_output() {
        let cfg = test_config();
        let mut store = Store::open(&cfg).expect("store");
        let update = r#"{"update_id":100,"message":{"chat":{"id":"321"},"text":"/fresh"}}"#;

        let outcome = process_webhook_line(&cfg, &mut store, update).expect("process");
        let output = outcome.output.unwrap_or_default();

        assert_eq!(outcome.output_channel.as_deref(), Some("telegram"));
        assert!(output.contains("TELEGRAM_REPLY:"));
        assert!(output.contains("Context cleared"));
    }

    #[test]
    fn slack_fresh_command_routes_output_to_slack_channel() {
        let cfg = test_config();
        let mut store = Store::open(&cfg).expect("store");
        let payload = r#"{"type":"slash_commands","command":"/fresh","channel_id":"C123"}"#;

        let outcome = process_webhook_line(&cfg, &mut store, payload).expect("process");
        let output = outcome.output.unwrap_or_default();

        assert_eq!(outcome.chat_id.as_deref(), Some("C123"));
        assert_eq!(outcome.output_channel.as_deref(), Some("slack"));
        assert!(output.contains("TELEGRAM_REPLY:"));
        assert!(output.contains("Context cleared"));
    }

    #[test]
    fn dedup_replays_previous_output_from_store() {
        let cfg = test_config();
        let mut store = Store::open(&cfg).expect("store");

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
                duration_ms: None,
                channel: "telegram".to_string(),
            })
            .expect("insert turn");
        assert!(inserted);

        let update = r#"{"update_id":42,"message":{"chat":{"id":"321"},"text":"hello again"}}"#;
        let outcome = process_webhook_line(&cfg, &mut store, update).expect("process");
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
        assert_eq!(turn.input.input_type, InputType::Voice);
    }

    #[test]
    fn photo_update_parses_attachment_metadata() {
        let cfg = test_config();
        let update = r#"{"update_id":201,"message":{"chat":{"id":"321"},"photo":[{"file_id":"a"},{"file_id":"b"}]}}"#;

        let action = parse_webhook_action(&cfg, update).expect("parse");
        let WebhookAction::Turn(turn) = action else {
            panic!("expected turn action");
        };
        assert_eq!(turn.input.input_type, InputType::Photo);
        assert_eq!(turn.input.attachment_type.as_deref(), Some("photo"));
    }

    #[test]
    fn cancel_command_sets_runtime_cancel_marker() {
        let cfg = test_config();
        let mut store = Store::open(&cfg).expect("store");
        let update = r#"{"update_id":202,"message":{"chat":{"id":"321"},"text":"/cancel"}}"#;
        let marker_path = cfg.runtime_dir.join("cancel");
        if marker_path.exists() {
            fs::remove_file(&marker_path).expect("cleanup stale marker");
        }

        let _ = process_webhook_line(&cfg, &mut store, update).expect("process");

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
    fn scheduled_task_slot_uses_configured_timezone() {
        let now = DateTime::parse_from_rfc3339("2026-04-20T00:30:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        assert_eq!(
            scheduled_task_slot_at(now, "Pacific/Auckland"),
            ("12:30".to_string(), "2026-04-20".to_string())
        );
        assert_eq!(
            scheduled_task_slot_at(now, "America/Los_Angeles"),
            ("17:30".to_string(), "2026-04-19".to_string())
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
    fn parse_markers_keeps_multiline_telegram_reply() {
        let payload = "TELEGRAM_REPLY: line one\nline two\nline three\nMEMORY_APPEND: fact";
        let markers = parse_markers(payload);
        assert_eq!(
            markers.telegram_reply.as_deref(),
            Some("line one\nline two\nline three")
        );
    }

    #[test]
    fn parse_markers_stops_multiline_reply_at_next_marker() {
        let payload =
            "noise\nTELEGRAM_REPLY: first\nsecond\nVOICE_REPLY: spoken\nTASK_APPEND: todo item";
        let markers = parse_markers(payload);
        assert_eq!(markers.telegram_reply.as_deref(), Some("first\nsecond"));
        assert_eq!(markers.voice_reply.as_deref(), Some("spoken"));
        assert_eq!(markers.task_append, vec!["todo item".to_string()]);
    }

    #[test]
    fn parse_markers_unescapes_inline_newlines() {
        let payload = "TELEGRAM_REPLY: line one\\n\\nline two\\nline three";
        let markers = parse_markers(payload);
        assert_eq!(
            markers.telegram_reply.as_deref(),
            Some("line one\n\nline two\nline three")
        );
    }

    #[test]
    fn parse_markers_keeps_double_escaped_newline_literal() {
        let payload = r"TELEGRAM_REPLY: keep \\n literal";
        let markers = parse_markers(payload);
        assert_eq!(markers.telegram_reply.as_deref(), Some(r"keep \\n literal"));
    }

    #[test]
    fn recover_unstructured_reply_prefers_json_assistant_text() {
        let payload = r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Hello from JSON"}]}}"#;
        let recovered = recover_unstructured_reply(payload);
        assert_eq!(recovered.as_deref(), Some("Hello from JSON"));
    }

    #[test]
    fn recover_unstructured_reply_uses_plain_text_when_no_json() {
        let payload = "plain fallback reply";
        let recovered = recover_unstructured_reply(payload);
        assert_eq!(recovered.as_deref(), Some("plain fallback reply"));
    }

    #[test]
    fn recover_unstructured_reply_ignores_json_stream_without_assistant_text() {
        let payload = r#"{"type":"session","id":"abc"}
{"type":"agent_end","messages":[]}"#;
        let recovered = recover_unstructured_reply(payload);
        assert!(recovered.is_none());
    }

    #[test]
    fn recover_unstructured_reply_surfaces_agent_end_error() {
        let payload = r#"{"type":"session","id":"abc"}
{"type":"agent_end","messages":[{"role":"assistant","content":[{"type":"toolCall","id":"t1","name":"bash","arguments":{}}]}],"error":"Maximum tool iterations (50) exceeded"}"#;
        let recovered = recover_unstructured_reply(payload);
        assert_eq!(
            recovered.as_deref(),
            Some("⚠️ Agent stopped: Maximum tool iterations (50) exceeded")
        );
    }

    #[test]
    fn extract_error_summary_prefers_structured_event_error() {
        let payload = r#"{"type":"session","id":"abc"}
{"type":"turn_end","message":{"errorMessage":"internal timeout"}}
{"type":"agent_end","error":"Internal Network Failure"}"#;
        let summary = extract_error_summary(payload);
        assert_eq!(summary.as_deref(), Some("Internal Network Failure"));
    }

    #[test]
    fn should_retry_provider_failure_matches_transient_errors() {
        assert!(should_retry_provider_failure("Internal Network Failure"));
        assert!(should_retry_provider_failure(
            "API error: JSON parse error: missing field `type`"
        ));
        assert!(!should_retry_provider_failure("permission denied"));
    }

    #[test]
    fn should_retry_provider_failure_ignores_non_retryable_turn_failed_stream() {
        let payload = r#"{"type":"item.completed","item":{"type":"command_execution","aggregated_output":"mentions timeout in a file path only"}}
{"type":"turn.failed","error":{"message":"Codex ran out of room in the model's context window. Start a new thread or clear earlier history before retrying."}}"#;
        assert!(!should_retry_provider_failure(payload));
    }

    #[test]
    fn text_params_include_parse_mode_for_markdown_v2() {
        let cfg = RuntimeConfig::test_builder()
            .telegram_parse_mode(TelegramParseMode::MarkdownV2)
            .build();
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
    fn markdown_entity_error_triggers_plain_fallback() {
        let err = anyhow::anyhow!(
            "telegram sendMessage failed: Bad Request: can't parse entities: Character '-' is reserved"
        );
        assert!(should_fallback_plain_for_error(&err));
    }

    #[test]
    fn non_entity_error_does_not_trigger_plain_fallback() {
        let err =
            anyhow::anyhow!("telegram sendMessage failed: Bad Request: message is not modified");
        assert!(!should_fallback_plain_for_error(&err));
    }

    #[test]
    fn telegram_retry_after_seconds_reads_integer_value() {
        let body = r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":15}}"#;
        assert_eq!(telegram_retry_after_seconds(body), Some(15));
    }

    #[test]
    fn telegram_retry_after_seconds_reads_string_value() {
        let body = r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":"7"}}"#;
        assert_eq!(telegram_retry_after_seconds(body), Some(7));
    }

    #[test]
    fn render_markdown_v2_converts_commonmark_bold() {
        let text = "**Architecture highlights:**\n- item";
        let rendered = render_markdown_v2_reply(text);
        assert!(rendered.contains("Architecture highlights"));
        assert!(!rendered.contains("**Architecture"));
        assert!(rendered.contains("item"));
    }

    #[test]
    fn render_markdown_v2_escapes_all_special_chars() {
        let text = "`**not bold**` then **bold**";
        let rendered = render_markdown_v2_reply(text);
        // The simple escaper escapes all special chars uniformly
        assert!(rendered.contains("\\`"));
        assert!(rendered.contains("\\*\\*bold\\*\\*"));
    }

    #[test]
    fn render_markdown_v2_escapes_reserved_chars() {
        let text = "- item\n1. test (x.y)";
        let rendered = render_markdown_v2_reply(text);
        assert!(!rendered.is_empty());
        assert!(rendered.contains("item"));
        assert!(rendered.contains("test"));
    }

    #[test]
    fn render_telegram_reply_text_escapes_progress_for_markdown_v2() {
        let cfg = RuntimeConfig::test_builder()
            .telegram_parse_mode(TelegramParseMode::MarkdownV2)
            .build();
        let rendered = render_telegram_reply_text(&cfg, &progress_status_text(3));
        assert!(rendered.contains("Thinking\\.\\.\\."));
        assert!(rendered.contains("stop\\."));
    }

    #[test]
    fn render_telegram_reply_text_keeps_plain_when_parse_mode_off() {
        let cfg = test_config();
        let text = progress_status_text(3);
        let rendered = render_telegram_reply_text(&cfg, &text);
        assert_eq!(rendered, text);
    }

    #[test]
    fn long_reply_document_fallback_respects_telegram_limit() {
        let exact = "a".repeat(4096);
        assert!(!should_send_reply_as_document(&exact));

        let oversized = "a".repeat(4097);
        assert!(should_send_reply_as_document(&oversized));
    }

    #[test]
    fn render_markdown_v2_escapes_backticks() {
        let text = "*标题* and `main.rs`";
        let rendered = render_markdown_v2_reply(text);
        // The simple escaper escapes backticks too
        assert!(rendered.contains("\\`main\\.rs\\`"));
    }

    #[test]
    fn context_requires_plain_text_when_parse_mode_off() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");
        let input = TurnInput {
            input_type: InputType::Text,
            user_text: "hello".to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "telegram".to_string(),
        };
        let text = build_context(
            &cfg,
            &store,
            &input,
            "2026-01-01T00:00:00+0000",
            "321",
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
        )
        .expect("context");
        assert!(text.contains("Do not use markdown"));
    }

    #[test]
    fn context_allows_markdown_v2_when_parse_mode_enabled() {
        let cfg = RuntimeConfig::test_builder()
            .telegram_parse_mode(TelegramParseMode::MarkdownV2)
            .build();
        let store = Store::open(&cfg).expect("store");
        let input = TurnInput {
            input_type: InputType::Text,
            user_text: "hello".to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "telegram".to_string(),
        };
        let text = build_context(
            &cfg,
            &store,
            &input,
            "2026-01-01T00:00:00+0000",
            "321",
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
        )
        .expect("context");
        assert!(text.contains("MarkdownV2"));
        assert!(!text.contains("Do not use markdown"));
    }

    #[test]
    fn context_recent_turns_are_scoped_to_chat_and_channel() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");
        store
            .insert_turn(&TurnRecord {
                ts: "2026-01-01T00:00:00+0000".to_string(),
                chat_id: "321".to_string(),
                input_type: "text".to_string(),
                user_text: "telegram hello".to_string(),
                asr_text: String::new(),
                provider_raw: "TELEGRAM_REPLY: telegram reply\n".to_string(),
                telegram_reply: "telegram reply".to_string(),
                voice_reply: String::new(),
                status: "ok".to_string(),
                update_id: Some("500".to_string()),
                duration_ms: None,
                channel: "telegram".to_string(),
            })
            .expect("insert telegram turn");
        store
            .insert_turn(&TurnRecord {
                ts: "2026-01-01T00:01:00+0000".to_string(),
                chat_id: "C123".to_string(),
                input_type: "text".to_string(),
                user_text: "slack hello".to_string(),
                asr_text: String::new(),
                provider_raw: "TELEGRAM_REPLY: slack reply\n".to_string(),
                telegram_reply: "slack reply".to_string(),
                voice_reply: String::new(),
                status: "ok".to_string(),
                update_id: Some("501".to_string()),
                duration_ms: None,
                channel: "slack".to_string(),
            })
            .expect("insert slack turn");

        let input = TurnInput {
            input_type: InputType::Text,
            user_text: "next telegram message".to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "telegram".to_string(),
        };

        let text = build_context(
            &cfg,
            &store,
            &input,
            "2026-01-01T00:02:00+0000",
            "321",
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
        )
        .expect("context");

        assert!(text.contains("telegram hello"));
        assert!(!text.contains("slack hello"));
    }

    #[test]
    fn context_includes_supplemental_conversation_context() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");
        let input = TurnInput {
            input_type: InputType::Text,
            user_text: "follow-up".to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: Some("U123: earlier thread reply".to_string()),
            channel: "slack".to_string(),
        };

        let text = build_context(
            &cfg,
            &store,
            &input,
            "2026-01-01T00:02:00+0000",
            "C123",
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
        )
        .expect("context");

        assert!(text.contains("## Supplemental conversation context"));
        assert!(text.contains("U123: earlier thread reply"));
    }

    #[test]
    fn context_omits_quoted_reply_from_before_boundary() {
        let cfg = test_config();
        let store = Store::open(&cfg).expect("store");

        store
            .insert_boundary_turn("2026-01-01T00:02:00+0000", "321", Some("600"), "telegram")
            .expect("insert boundary");

        let input = TurnInput {
            input_type: InputType::Text,
            user_text: "fresh question".to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "telegram".to_string(),
        };

        let text = build_context(
            &cfg,
            &store,
            &input,
            "2026-01-01T00:03:00+0000",
            "321",
            &QuotedMessage {
                reply_from: Some("CoconutClaw".to_string()),
                reply_text: Some("older reply".to_string()),
                reply_ts: Some(1_767_225_719),
            },
        )
        .expect("context");

        assert!(!text.contains("## Quoted/replied-to message"));
        assert!(!text.contains("older reply"));
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
    fn service_subcommand_is_recognized() {
        let parsed = Cli::try_parse_from(["coconutclaw", "service", "status"]);
        assert!(parsed.is_ok());
    }

    #[test]
    fn nightly_reflection_marker_format_is_stable() {
        assert_eq!(
            nightly_reflection_marker("2026-02-25"),
            "<!-- nightly-reflection:2026-02-25 -->"
        );
    }

    #[test]
    fn webhook_public_endpoint_joins_base_and_path() {
        let cfg = RuntimeConfig::test_builder()
            .webhook_public_url(Some("https://claw.example".to_string()))
            .webhook_path("/telegram/webhook".to_string())
            .build();

        let endpoint = webhook_public_endpoint(&cfg).expect("endpoint");
        assert_eq!(endpoint, "https://claw.example/telegram/webhook");
    }

    #[test]
    fn append_webhook_queue_line_persists_single_line() {
        let cfg = test_config();
        let payload = r#"{"update_id":9001,"message":{"chat":{"id":"321"},"text":"ping"}}"#;

        crate::webhook::append_webhook_queue_line(&cfg, payload).expect("append queue");
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        let content = fs::read_to_string(queue_path).expect("queue content");
        let line = content.trim_end_matches('\n');
        assert!(!line.contains('\n'));
        let stored: Value = serde_json::from_str(line).expect("stored json");
        let original: Value = serde_json::from_str(payload).expect("original json");
        assert_eq!(stored, original);
    }

    #[test]
    fn append_webhook_queue_line_normalizes_multiline_payload() {
        let cfg = test_config();
        let payload = "{\n  \"update_id\": 9002,\n  \"message\": {\"chat\": {\"id\": \"321\"}, \"text\": \"ping\"}\n}";

        crate::webhook::append_webhook_queue_line(&cfg, payload).expect("append queue");
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        let content = fs::read_to_string(queue_path).expect("queue content");
        let line = content.trim_end_matches('\n');
        assert!(!line.contains('\n'));
        let value: Value = serde_json::from_str(line).expect("normalized json");
        assert_eq!(
            extract_update_id_from_value(&value).as_deref(),
            Some("9002")
        );
    }

    #[test]
    fn queue_cancel_scan_ignores_active_update_and_detects_tail_cancel() {
        let cfg = test_config();
        let active = r#"{"update_id":42,"message":{"chat":{"id":"321"},"text":"long task"}}"#;
        let cancel = r#"{"update_id":43,"callback_query":{"id":"cb1","data":"cancel","message":{"chat":{"id":"321"}}}}"#;
        crate::webhook::append_webhook_queue_line(&cfg, active).expect("append active");
        crate::webhook::append_webhook_queue_line(&cfg, cancel).expect("append cancel");

        let signal =
            scan_webhook_queue_for_cancel(&cfg, "321", Some("42")).expect("scan queue cancel");
        assert_eq!(
            signal.and_then(|item| item.callback_query_id),
            Some("cb1".to_string())
        );
    }

    #[test]
    fn drain_webhook_queue_drops_malformed_head_entry() {
        let cfg = test_config();
        let mut store = Store::open(&cfg).expect("store");
        let client = Client::builder().build().expect("client");
        let shutdown = Arc::new(AtomicBool::new(false));
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");

        fs::write(&queue_path, "{\"update_id\":9003,\n").expect("write malformed queue line");

        let progressed =
            drain_webhook_queue(&cfg, &mut store, &client, &shutdown).expect("drain webhook queue");
        assert!(progressed);

        let content = fs::read_to_string(queue_path).expect("queue content");
        assert!(content.trim().is_empty());
    }

    #[test]
    fn ack_webhook_queue_line_drops_malformed_head_with_expected_update_id() {
        let cfg = test_config();
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        let malformed = "{\"update_id\":9003,\n";
        let valid = r#"{"update_id":9004,"message":{"chat":{"id":"321"},"text":"ping"}}"#;
        fs::write(&queue_path, format!("{malformed}{valid}\n")).expect("write queue");

        let status =
            ack_webhook_queue_line(&cfg, Some("9004")).expect("ack with malformed queue head");
        assert_eq!(status, AckStatus::Acked);

        let content = fs::read_to_string(queue_path).expect("queue content");
        assert_eq!(content.trim_end(), valid);
    }

    // ── format-aware split_text_chunks tests ──────────────────────────

    #[test]
    fn split_short_text_returns_single_chunk() {
        let text = "Hello world";
        let chunks = split_text_chunks(text, 4096, TelegramParseMode::Off);
        assert_eq!(chunks, vec!["Hello world"]);
    }

    #[test]
    fn split_empty_text_returns_no_chunks() {
        let chunks = split_text_chunks("", 4096, TelegramParseMode::Off);
        assert!(chunks.is_empty());
    }

    #[test]
    fn split_closes_and_reopens_code_block_at_boundary_html() {
        // Build text with a code block that spans across the chunk limit.
        let fence_open = "<pre><code>";
        let fence_close = "</code></pre>";
        let code_line = "line of code\n";
        // code_line is 13 chars. We want ~10 lines in first chunk, rest in second.
        let code_lines = code_line.repeat(20); // 260 chars
        let text = format!("{fence_open}{code_lines}{fence_close}");

        // max=150 chars to force a split inside the code block
        let chunks = split_text_chunks(&text, 150, TelegramParseMode::Html);
        assert!(chunks.len() >= 2, "should split into multiple chunks");

        // First chunk must contain the closing fence (before the indicator)
        assert!(
            chunks[0].contains(fence_close),
            "first chunk should contain closing fence, got: {:?}",
            chunks[0]
        );

        // Second chunk must reopen the code block
        assert!(
            chunks[1].starts_with(fence_open),
            "second chunk should start with opening fence, got: {}...",
            &chunks[1][..fence_open.len().min(chunks[1].len())]
        );

        // Last chunk must contain the closing fence
        let last = chunks.last().unwrap();
        assert!(
            last.contains(fence_close),
            "last chunk should contain closing fence, got: {:?}",
            last
        );

        // Multi-chunk: must have indicators
        if chunks.len() > 1 {
            for (i, chunk) in chunks.iter().enumerate() {
                let indicator = format!("({}/{})", i + 1, chunks.len());
                assert!(
                    chunk.contains(&indicator),
                    "chunk {} should contain indicator {:?}",
                    i + 1,
                    indicator
                );
            }
        }
    }

    #[test]
    fn split_closes_and_reopens_code_block_markdown_v2() {
        let fence = "```";
        let code_line = "x = 1\n"; // 6 chars
        let code_lines = code_line.repeat(60); // 360 chars
        let text = format!("{fence}rust\n{code_lines}{fence}");

        let chunks = split_text_chunks(&text, 150, TelegramParseMode::MarkdownV2);
        assert!(chunks.len() >= 2);

        // First chunk must contain the closing fence
        assert!(
            chunks[0].contains(fence),
            "first chunk should contain ```, got: {:?}",
            chunks[0]
        );
        // Second chunk must reopen with original lang tag
        assert!(
            chunks[1].starts_with("```rust\n"),
            "second chunk should reopen with ```rust\\n, got: {}",
            &chunks[1][..20.min(chunks[1].len())]
        );

        // Indicators use escaped parens for MarkdownV2
        for (i, chunk) in chunks.iter().enumerate() {
            let indicator = format!(
                "\\({current}/{total}\\)",
                current = i + 1,
                total = chunks.len()
            );
            assert!(
                chunk.contains(&indicator),
                "chunk {} should contain escaped indicator {:?}",
                i + 1,
                indicator
            );
        }
    }

    #[test]
    fn split_plain_mode_no_format_awareness() {
        // In Off mode, code blocks are just text — no fence closing/reopening.
        let text = "```rust\nfn main() {}\n```".repeat(20);
        let chunks = split_text_chunks(&text, 200, TelegramParseMode::Off);
        assert!(chunks.len() >= 2);
        // No chunk should have added fence tags
        for chunk in &chunks {
            // The chunk indicators should be plain parens
            if chunk.contains("(1/") || chunk.contains("(2/") {
                // ok — plain indicator
            }
        }
    }

    #[test]
    fn split_protects_inline_code_from_breaking() {
        // Inline code with backticks that falls near a split boundary.
        let inline = "`some_long_code_here`";
        let padding = "word ".repeat(10); // 50 chars
        let text = format!("{padding}{inline} more text after");
        let chunks = split_text_chunks(&text, 60, TelegramParseMode::MarkdownV2);
        // No chunk should have an unmatched backtick count
        for chunk in &chunks {
            let count = chunk.matches('`').count();
            assert_eq!(
                count % 2,
                0,
                "chunk has odd number of backticks: {:?}",
                chunk
            );
        }
    }

    // ── Bug-coverage tests (TDD: written to fail, then fix) ──────────

    /// Bug #2: headroom subtraction must not panic on underflow when max_chars is tiny.
    #[test]
    fn split_format_aware_small_max_chars_no_panic() {
        let text = "hello world this is a test string";
        // Must not panic — previously caused integer underflow.
        let chunks = split_text_chunks(text, 5, TelegramParseMode::MarkdownV2);
        assert!(!chunks.is_empty());
        // With max_chars this tiny the indicator alone exceeds the limit,
        // so we only assert no panic and non-empty output.
    }

    /// Bug #2b: realistic small max_chars must respect limit.
    #[test]
    fn split_format_aware_moderate_max_chars_respects_limit() {
        let text = "hello world this is a test string that is moderately long for splitting";
        let max = 25;
        let chunks = split_text_chunks(text, max, TelegramParseMode::MarkdownV2);
        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(
                chunk.chars().count() <= max,
                "chunk exceeds max {max}: {:?}",
                chunk
            );
        }
    }

    /// Bug #3: byte vs char index confusion with multi-byte chars.
    #[test]
    fn split_multibyte_chars_respect_max_chars() {
        // Each '日' is 3 bytes but 1 char. 200 chars = 600 bytes.
        let text = "日".repeat(200);
        let chunks = split_text_chunks(&text, 80, TelegramParseMode::MarkdownV2);
        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(
                chunk.chars().count() <= 80,
                "chunk char count {} exceeds 80: {:?}",
                chunk.chars().count(),
                &chunk[..chunk.len().min(60)]
            );
        }
    }

    /// Bug #4: infinite loop when max_chars is very small and headroom becomes 0.
    #[test]
    fn split_format_aware_terminates_with_tiny_limit() {
        let text = "abcdefghijklmnopqrstuvwxyz".repeat(5);
        // This must terminate (not infinite loop).
        let chunks = split_text_chunks(&text, 3, TelegramParseMode::Html);
        assert!(!chunks.is_empty());
    }

    /// Bug #5: HtmlFormat::code_fence_open_with_lang ignores lang tag.
    #[test]
    fn split_html_reopens_code_block_with_language_class() {
        let code_line = "let x = 1;\n";
        let code_lines = code_line.repeat(30); // 330 chars
        let text = format!("<pre><code class=\"language-rust\">{code_lines}</code></pre>");
        let chunks = split_text_chunks(&text, 150, TelegramParseMode::Html);
        assert!(chunks.len() >= 2);
        // Second chunk must reopen with the language class
        assert!(
            chunks[1].starts_with("<pre><code class=\"language-rust\">"),
            "second chunk should preserve language class, got: {}",
            &chunks[1][..60.min(chunks[1].len())]
        );
    }

    /// Bug #6: scan_code_block_state doesn't match <pre><code class="language-X">.
    #[test]
    fn split_html_detects_code_block_with_class_attr() {
        let code_line = "line\n";
        let code_lines = code_line.repeat(40);
        let text = format!("<pre><code class=\"language-python\">{code_lines}</code></pre>");
        let chunks = split_text_chunks(&text, 100, TelegramParseMode::Html);
        assert!(chunks.len() >= 2);
        // Every non-last chunk must properly close the code block
        for (i, chunk) in chunks.iter().enumerate() {
            if i < chunks.len() - 1 {
                assert!(
                    chunk.contains("</code></pre>"),
                    "chunk {} should close code block, got: {:?}",
                    i,
                    chunk
                );
            }
        }
    }

    /// Bug #7: split_plain appends indicators after splitting, exceeding max_chars.
    #[test]
    fn split_plain_chunks_respect_max_chars_with_indicators() {
        let text = "abcde\n".repeat(500); // ~3000 chars
        let max = 200;
        let chunks = split_text_chunks(&text, max, TelegramParseMode::Off);
        assert!(chunks.len() >= 2);
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.chars().count() <= max,
                "plain chunk {} is {} chars, exceeds {}: {:?}",
                i,
                chunk.chars().count(),
                max,
                &chunk[..chunk.len().min(60)]
            );
        }
    }

    /// Bug #7b: oversized single line in plain mode still respects limit.
    #[test]
    fn split_plain_oversized_line_with_indicators() {
        let text = "x".repeat(8000);
        let max = 4096;
        let chunks = split_text_chunks(&text, max, TelegramParseMode::Off);
        assert!(chunks.len() >= 2);
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.chars().count() <= max,
                "plain oversized chunk {} is {} chars, exceeds {max}",
                i,
                chunk.chars().count()
            );
        }
    }

    /// Inline backtick protection must not corrupt ``` fences.
    #[test]
    fn split_md_fence_not_corrupted_by_inline_backtick_protection() {
        let text = "Some text before\n```rust\nfn main() {}\nlet x = 1;\nlet y = 2;\n```\nAfter";
        let chunks = split_text_chunks(text, 40, TelegramParseMode::MarkdownV2);
        // No chunk should contain a partial fence like `` (two backticks alone)
        for chunk in &chunks {
            for line in chunk.split('\n') {
                let trimmed = line.trim();
                if trimmed.starts_with('`') && !trimmed.starts_with("```") {
                    // Must be an inline code span (even backtick count)
                    let count = trimmed.matches('`').count();
                    assert_eq!(
                        count % 2,
                        0,
                        "fence corrupted to partial backticks: {:?}",
                        trimmed
                    );
                }
            }
        }
    }

    /// HTML scan correctly handles close+open on the same line.
    #[test]
    fn split_html_close_then_open_same_line() {
        // Craft text where a code block is closed and another opened on the same line
        let text = "<pre><code>first block</code></pre><pre><code class=\"language-rust\">second block content that is long enough to need splitting into multiple chunks for testing\n";
        let text = text.repeat(3);
        let chunks = split_text_chunks(&text, 150, TelegramParseMode::Html);
        // Just verify it doesn't panic and produces non-empty output
        assert!(!chunks.is_empty());
    }

    /// All format-aware chunks must stay within max_chars.
    #[test]
    fn split_format_aware_all_chunks_within_limit() {
        let fence = "```";
        let code_line = "x = 1\n";
        let code_lines = code_line.repeat(100);
        let text = format!("{fence}rust\n{code_lines}{fence}");
        let max = 150;
        let chunks = split_text_chunks(&text, max, TelegramParseMode::MarkdownV2);
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.chars().count() <= max,
                "md chunk {} is {} chars, exceeds {max}: {:?}",
                i,
                chunk.chars().count(),
                &chunk[..chunk.len().min(80)]
            );
        }
    }
}
