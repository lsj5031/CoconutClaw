use anyhow::Result;
use coconutclaw_config::{RuntimeConfig, TelegramParseFallback};

use crate::slack::{valid_slack_channel_id, valid_slack_token};
use crate::telegram::{valid_telegram_chat_id, valid_telegram_token};
use crate::util::{asr_feature_enabled, command_exists, yes_no};
use crate::webhook::webhook_request_path;

pub(crate) fn run_doctor(cfg: &RuntimeConfig, args: &crate::DoctorArgs) -> Result<()> {
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
    let asr_preprocess = crate::parse_on_like(cfg.asr_preprocess.as_deref(), true);
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
