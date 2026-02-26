use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use clap::{Args, Parser, Subcommand};
use coconutclaw_config::{
    CliOverrides, RuntimeConfig, TelegramParseFallback, TelegramParseMode, load_runtime_config,
};
use coconutclaw_provider::run_provider;
use reqwest::blocking::{Client, multipart};
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
use telegram_markdown_v2::{UnsupportedTagsStrategy, convert_with_strategy};

mod markers;
mod store;
mod webhook;

use crate::markers::{
    ParsedMarkers, extract_assistant_text_from_json_stream, extract_error_summary, parse_markers,
    recover_unstructured_reply, render_output, should_retry_provider_failure,
};
use crate::store::{Store, TurnRecord};
use crate::webhook::{
    AckStatus, ack_webhook_queue_line, ensure_webhook_queue_file, extract_update_id_from_json,
    extract_update_id_from_value, peek_webhook_queue_line, spawn_webhook_http_server,
    value_to_string, webhook_public_endpoint, webhook_request_path, with_webhook_lock,
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

struct TurnResult {
    markers: ParsedMarkers,
    telegram_reply: String,
    voice_reply: String,
    status: String,
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

    if let Err(err) = register_bot_commands(&telegram_client, cfg) {
        tracing::warn!("failed to register bot menu commands: {err:#}");
    }

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

    tracing::info!(
        "nightly reflection appended to {}",
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
    println!("webhook_bind={}", cfg.webhook_bind);
    println!("webhook_path={}", webhook_request_path(cfg));
    println!(
        "webhook_public_url={}",
        cfg.webhook_public_url.as_deref().unwrap_or("")
    );
    println!(
        "webhook_secret_set={}",
        if cfg
            .webhook_secret
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        {
            "yes"
        } else {
            "no"
        }
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
                tracing::warn!("telegram polling failed: {err:#}");
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
    }

    tracing::info!("shutdown signal received, stopping poll loop");
    Ok(())
}

fn run_webhook_loop(
    cfg: &RuntimeConfig,
    store: &Store,
    telegram_client: &Client,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    ensure_webhook_queue_file(cfg)?;

    register_telegram_webhook(telegram_client, cfg)?;
    let _http_server = spawn_webhook_http_server(cfg.clone(), Arc::clone(shutdown))?;

    if let Err(err) = restore_inflight_update(cfg, store, telegram_client) {
        tracing::warn!("failed to restore inflight webhook update: {err:#}");
    }

    while !shutdown.load(Ordering::SeqCst) {
        let progressed = match drain_webhook_queue(cfg, store, telegram_client, shutdown) {
            Ok(progressed) => progressed,
            Err(err) => {
                tracing::warn!("webhook queue drain failed (will continue): {err:#}");
                false
            }
        };
        if !progressed {
            thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
        }
    }

    tracing::info!("shutdown signal received, stopping webhook loop");
    Ok(())
}

fn register_bot_commands(client: &Client, cfg: &RuntimeConfig) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let commands = serde_json::json!([
        {"command": "fresh", "description": "Start a fresh conversation"},
        {"command": "cancel", "description": "Cancel the current task"},
    ]);
    let params = vec![("commands".to_string(), commands.to_string())];
    telegram_post_form(
        client,
        &format!("{base}/setMyCommands"),
        &params,
        "setMyCommands",
    )?;
    Ok(())
}

fn register_telegram_webhook(client: &Client, cfg: &RuntimeConfig) -> Result<()> {
    let webhook_url = webhook_public_endpoint(cfg)?;
    let base = telegram_api_base(cfg)?;

    let mut params: Vec<(String, String)> = vec![
        ("url".to_string(), webhook_url),
        (
            "allowed_updates".to_string(),
            r#"["message","callback_query"]"#.to_string(),
        ),
        ("drop_pending_updates".to_string(), "false".to_string()),
    ];
    if let Some(secret) = cfg.webhook_secret.as_deref().map(str::trim)
        && !secret.is_empty()
    {
        params.push(("secret_token".to_string(), secret.to_string()));
    }

    let response = client
        .post(format!("{base}/setWebhook"))
        .form(&params)
        .send()
        .context("failed to call telegram setWebhook")?;
    parse_telegram_response(response, "setWebhook")?;
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
                if let Some(output) = outcome.output {
                    if let Err(err) = dispatch_telegram_output(
                        telegram_client,
                        cfg,
                        outcome.chat_id.as_deref(),
                        &output,
                        outcome.progress_message_id.as_deref(),
                    ) {
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
                if let Some(output) = outcome.output {
                    if let Err(err) = dispatch_telegram_output(
                        telegram_client,
                        cfg,
                        outcome.chat_id.as_deref(),
                        &output,
                        outcome.progress_message_id.as_deref(),
                    ) {
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
                output: None,
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Fresh { update_id, chat_id } => {
            let ts = iso_now(&cfg.timezone);
            match store.insert_boundary_turn(&ts, &chat_id, update_id.as_deref()) {
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
                let _ = store.kv_set("last_update_id", update_id);
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
            tracing::warn!("telegram media fetch disabled: {err:#}");
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
                tracing::warn!("failed to download voice attachment: {err:#}");
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
                        tracing::warn!("ASR failed for voice attachment: {err:#}");
                    }
                }
            } else {
                tracing::info!("voice attachment received but ASR is disabled in config.toml");
            }

            let _ = fs::remove_file(voice_path);
            Ok((input, None))
        }
        IncomingMedia::Photo { file_id } => {
            let path = cfg.tmp_dir.join(format!("photo_{suffix}.jpg"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                tracing::warn!("failed to download photo attachment: {err:#}");
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
                tracing::warn!("failed to download document attachment: {err:#}");
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
                tracing::warn!("failed to download video attachment: {err:#}");
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
                tracing::warn!("failed to download video_note attachment: {err:#}");
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

fn scan_webhook_queue_for_cancel(
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

fn dispatch_telegram_output(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id_override: Option<&str>,
    output: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    let Some(chat_id) = chat_id_override
        .or(cfg.telegram_chat_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        tracing::warn!("cannot dispatch telegram output: no chat_id available");
        return Ok(());
    };

    let markers = parse_markers(output);
    if let Some(reply) = markers.telegram_reply.as_deref() {
        let reply = reply.trim();
        if !reply.is_empty() {
            let rendered_reply = if matches!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2)
            {
                render_markdown_v2_reply(reply)
            } else {
                reply.to_string()
            };
            send_or_edit_text(client, cfg, chat_id, &rendered_reply, progress_message_id)?;
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
            tracing::warn!("failed to send voice reply: {err:#}");
        }
    }

    for item in markers.send_photo {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendPhoto", "photo", &path)
        {
            tracing::warn!("failed to send photo {}: {err:#}", path.display());
        }
    }
    for item in markers.send_document {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendDocument", "document", &path)
        {
            tracing::warn!("failed to send document {}: {err:#}", path.display());
        }
    }
    for item in markers.send_video {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendVideo", "video", &path)
        {
            tracing::warn!("failed to send video {}: {err:#}", path.display());
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
                    if !status.is_empty() {
                        if let Some(existing) = statuses.iter().position(|item| item == &status) {
                            statuses.remove(existing);
                        }
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

    if let Some(message_id) = progress_message_id {
        let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
    }

    let mut last_err: Option<anyhow::Error> = None;
    for chunk in chunks {
        if let Err(err) = telegram_send_message(client, cfg, chat_id, &chunk) {
            tracing::warn!("failed to send text chunk: {err:#}");
            last_err = Some(err);
        }
    }

    match last_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
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

fn render_markdown_v2_reply(text: &str) -> String {
    match convert_with_strategy(text, UnsupportedTagsStrategy::Escape) {
        Ok(rendered) => rendered.trim_end_matches('\n').to_string(),
        Err(err) => {
            tracing::warn!("markdown conversion failed, sending original text: {err:#}");
            text.to_string()
        }
    }
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
    let plain_params = strip_parse_mode_param(&params);
    telegram_post_form(client, &url, &plain_params, "editMessageText").map(|_| ())
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
        Err(err) if should_retry_plain_text(cfg) && should_fallback_plain_for_error(&err) => {
            tracing::warn!("sendMessage markdown parse failed, retrying plain text: {err:#}");
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

fn should_fallback_plain_for_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}").to_ascii_lowercase();
    text.contains("can't parse entities")
        || text.contains("can't find end of")
        || text.contains("character '-' is reserved")
        || text.contains("character '.' is reserved")
        || text.contains("character '!' is reserved")
}

fn telegram_post_form(
    client: &Client,
    url: &str,
    params: &[(String, String)],
    action: &str,
) -> Result<Value> {
    const MAX_429_RETRY_ATTEMPTS: usize = 1;
    const MAX_429_SLEEP_SECS: u64 = 60;

    let mut attempts = 0usize;
    loop {
        let response = client
            .post(url)
            .form(params)
            .send()
            .with_context(|| format!("failed to call telegram {action}"))?;

        if response.status().as_u16() == 429 {
            let body = response
                .text()
                .with_context(|| format!("failed to read telegram {action} response body"))?;
            if attempts < MAX_429_RETRY_ATTEMPTS
                && let Some(retry_after) = telegram_retry_after_seconds(&body)
            {
                let sleep_secs = retry_after.clamp(1, MAX_429_SLEEP_SECS);
                attempts += 1;
                tracing::warn!("telegram {action} rate limited, retrying in {sleep_secs}s");
                thread::sleep(Duration::from_secs(sleep_secs));
                continue;
            }
            bail!("telegram {action} HTTP 429: {body}");
        }

        return parse_telegram_response(response, action);
    }
}

fn telegram_retry_after_seconds(body: &str) -> Option<u64> {
    let value: Value = serde_json::from_str(body).ok()?;
    value
        .get("parameters")
        .and_then(|node| node.get("retry_after"))
        .and_then(|node| {
            node.as_u64()
                .or_else(|| node.as_i64().and_then(|value| u64::try_from(value).ok()))
                .or_else(|| node.as_str().and_then(|value| value.parse::<u64>().ok()))
        })
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
            update_id.clone(),
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

    let mut provider_result =
        run_provider(cfg, &context, Some(&cancel_flag), progress_sender.as_ref());
    if let Ok(result) = &provider_result
        && !result.success
        && !cancel_flag.load(Ordering::SeqCst)
        && should_retry_provider_failure(&result.raw_output)
    {
        tracing::warn!("provider failed with retryable error, retrying once");
        provider_result = run_provider(cfg, &context, Some(&cancel_flag), progress_sender.as_ref());
    }
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
    let turn_result = resolve_turn_result(&raw_output, provider_success, cancelled);
    let markers = turn_result.markers;
    let telegram_reply = turn_result.telegram_reply;
    let voice_reply = turn_result.voice_reply;
    let status = turn_result.status;

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

fn resolve_turn_result(raw_output: &str, provider_success: bool, cancelled: bool) -> TurnResult {
    if cancelled {
        return TurnResult {
            markers: ParsedMarkers::default(),
            telegram_reply: "❌ Cancelled.".to_string(),
            voice_reply: String::new(),
            status: "cancelled".to_string(),
        };
    }

    let markers = parse_markers(raw_output);
    let telegram_reply = markers.telegram_reply.clone().unwrap_or_default();
    let voice_reply = markers.voice_reply.clone().unwrap_or_default();
    if !telegram_reply.trim().is_empty() || !voice_reply.trim().is_empty() {
        return TurnResult {
            markers,
            telegram_reply,
            voice_reply,
            status: if provider_success {
                "ok".to_string()
            } else {
                "agent_error".to_string()
            },
        };
    }

    if provider_success {
        if let Some(recovered) = recover_unstructured_reply(raw_output) {
            return TurnResult {
                markers,
                telegram_reply: recovered,
                voice_reply: String::new(),
                status: "parse_recovered".to_string(),
            };
        }
        return TurnResult {
            markers,
            telegram_reply: "I could not parse structured markers from the model output."
                .to_string(),
            voice_reply: String::new(),
            status: "parse_fallback".to_string(),
        };
    }

    if let Some(recovered) = extract_assistant_text_from_json_stream(raw_output) {
        return TurnResult {
            markers,
            telegram_reply: recovered,
            voice_reply: String::new(),
            status: "agent_error_recovered".to_string(),
        };
    }

    let err_line = extract_error_summary(raw_output)
        .or_else(|| {
            raw_output
                .lines()
                .find(|line| !line.trim().is_empty())
                .map(|line| line.to_string())
        })
        .unwrap_or_else(|| "Please check local logs and retry.".to_string());
    TurnResult {
        markers,
        telegram_reply: format!("Agent execution failed locally. {err_line}"),
        voice_reply: String::new(),
        status: "agent_error".to_string(),
    }
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
        text.push_str("Use Telegram MarkdownV2 formatting only inside marker values.\n");
        text.push_str("Use `*bold*`, `_italic_`, and `` `code` `` syntax.\n");
        text.push_str("Do not use CommonMark syntax like `**bold**` or fenced code blocks.\n");
        text.push_str("Keep marker prefixes plain and unchanged.\n");
        text.push_str("Do not use code fences or extra prefixes.\n");
    } else {
        text.push_str("Do not use markdown, code fences, or extra prefixes.\n");
    }

    Ok(text)
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
            webhook_bind: "127.0.0.1:8787".to_string(),
            webhook_public_url: None,
            webhook_secret: None,
            webhook_path: "/webhook".to_string(),
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
    fn resolve_turn_result_marks_cancelled() {
        let result = resolve_turn_result("TELEGRAM_REPLY: hi", true, true);
        assert_eq!(result.status, "cancelled");
        assert_eq!(result.telegram_reply, "❌ Cancelled.");
        assert!(result.markers.telegram_reply.is_none());
    }

    #[test]
    fn resolve_turn_result_marks_parse_recovered() {
        let result = resolve_turn_result("plain reply", true, false);
        assert_eq!(result.status, "parse_recovered");
        assert_eq!(result.telegram_reply, "plain reply");
    }

    #[test]
    fn resolve_turn_result_marks_agent_error_recovered() {
        let payload = r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Recovered"}]}}"#;
        let result = resolve_turn_result(payload, false, false);
        assert_eq!(result.status, "agent_error_recovered");
        assert_eq!(result.telegram_reply, "Recovered");
    }

    #[test]
    fn resolve_turn_result_marks_agent_error_when_unrecoverable() {
        let result = resolve_turn_result("network timeout", false, false);
        assert_eq!(result.status, "agent_error");
        assert!(
            result
                .telegram_reply
                .contains("Agent execution failed locally")
        );
    }

    #[test]
    fn resolve_turn_result_uses_turn_failed_error_message() {
        let payload = r#"{"type":"thread.started","thread_id":"abc"}
{"type":"turn.failed","error":{"message":"Codex ran out of room in the model's context window."}}"#;
        let result = resolve_turn_result(payload, false, false);
        assert_eq!(result.status, "agent_error");
        assert!(result.telegram_reply.contains("context window"));
        assert!(
            !result
                .telegram_reply
                .contains(r#"{"type":"thread.started""#)
        );
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
    fn render_markdown_v2_keeps_commonmark_bold_inside_code() {
        let text = "`**not bold**` then **bold**";
        let rendered = render_markdown_v2_reply(text);
        assert!(rendered.contains("`**not bold**`"));
        assert!(rendered.contains("*bold*"));
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
    fn render_markdown_v2_leaves_already_valid_snippet_reasonable() {
        let text = "*标题* and `main.rs`";
        let rendered = render_markdown_v2_reply(text);
        assert!(rendered.contains("`main.rs`"));
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

    #[test]
    fn webhook_public_endpoint_joins_base_and_path() {
        let mut cfg = test_config();
        cfg.webhook_public_url = Some("https://claw.example".to_string());
        cfg.webhook_path = "/telegram/webhook".to_string();

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
        let store = Store::open(&cfg).expect("store");
        let client = Client::builder().build().expect("client");
        let shutdown = Arc::new(AtomicBool::new(false));
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");

        fs::write(&queue_path, "{\"update_id\":9003,\n").expect("write malformed queue line");

        let progressed =
            drain_webhook_queue(&cfg, &store, &client, &shutdown).expect("drain webhook queue");
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
}
