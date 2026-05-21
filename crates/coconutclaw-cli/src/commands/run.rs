use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use std::io::Write;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::context::sync_managed_context_files;
use crate::store::Store;
use crate::types::QuotedMessage;
use crate::util::iso_now;

fn telegram_configured(cfg: &RuntimeConfig) -> bool {
    crate::telegram::valid_telegram_token(cfg).is_some()
}

fn slack_socket_configured(cfg: &RuntimeConfig) -> bool {
    crate::slack::valid_slack_token(cfg).is_some()
        && cfg
            .slack_app_token
            .as_deref()
            .map(str::trim)
            .is_some_and(|token| !token.is_empty() && token.starts_with("xapp-"))
}

fn slack_webhook_configured(cfg: &RuntimeConfig) -> bool {
    cfg.webhook_mode && crate::slack::valid_slack_token(cfg).is_some()
}

pub(crate) fn install_shutdown_handler() -> Result<Arc<AtomicBool>> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_flag = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        signal_flag.store(true, Ordering::SeqCst);
    })
    .context("failed to register shutdown signal handler")?;
    Ok(shutdown)
}

pub(crate) fn start_socket_mode_if_configured(
    cfg: &RuntimeConfig,
) -> Result<Option<tokio::sync::mpsc::UnboundedReceiver<crate::slack::SlackWebhookTurn>>> {
    let has_bot_token = crate::slack::valid_slack_token(cfg).is_some();
    let has_app_token = cfg
        .slack_app_token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty() && t.starts_with("xapp-"))
        .is_some();

    if !has_bot_token || !has_app_token {
        return Ok(None);
    }

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<crate::slack::SlackWebhookTurn>();
    crate::slack::start_slack_socket_mode(cfg, tx)?;
    tracing::info!("slack socket mode listener started");
    Ok(Some(rx))
}

pub(crate) fn run_run(
    cfg: &RuntimeConfig,
    store: &mut Store,
    args: &crate::TurnArgs,
) -> Result<()> {
    if args.inject_text.is_some() || args.inject_file.is_some() {
        let input = crate::turn::resolve_turn_input(
            cfg,
            store,
            None,
            args.inject_text.clone(),
            args.inject_file.clone(),
            "telegram",
        )?;
        let output = crate::turn::process_turn(
            cfg,
            store,
            input,
            &crate::delivery::TaskSource::Telegram,
            args.chat_id.clone(),
            None,
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
            None,
        )?;
        print!("{output}");
        std::io::stdout().flush().ok();
        return Ok(());
    }

    let shutdown = install_shutdown_handler()?;

    let has_telegram = telegram_configured(cfg);
    let has_slack_socket = slack_socket_configured(cfg);
    let has_slack_webhook = slack_webhook_configured(cfg);
    let has_any_transport = has_telegram || has_slack_socket || has_slack_webhook;

    if !has_any_transport {
        anyhow::bail!(
            "no transport configured; set TELEGRAM_BOT_TOKEN or Slack settings such as SLACK_BOT_TOKEN + SLACK_APP_TOKEN"
        );
    }

    let telegram_client = if has_telegram {
        let client = crate::telegram::build_telegram_client(cfg)?;
        if let Err(err) = crate::telegram::register_bot_commands(&client, cfg) {
            tracing::warn!("failed to register bot menu commands: {err:#}");
        }
        Some(client)
    } else {
        tracing::info!("telegram transport disabled: TELEGRAM_BOT_TOKEN not configured");
        None
    };

    // Start the long-lived CancelRouter. If it fails, proceed without it —
    // cancel-by-marker simply won't work, but the runtime is still usable.
    let cancel_router = match crate::cancel::CancelRouter::start(cfg) {
        Ok(router) => Some(router),
        Err(err) => {
            tracing::warn!("failed to start cancel router (cancel-by-marker disabled): {err:#}");
            None
        }
    };
    let scheduler = crate::scheduler::SessionScheduler::new(cfg.clone(), cancel_router);
    let startup_ts = iso_now(&cfg.timezone);
    if let Err(err) = store.mark_stale_task_runs_failed(&startup_ts) {
        tracing::warn!("failed to mark stale task runs on startup: {err:#}");
    }
    match store.reconcile_scheduled_tasks_from_completed_runs(&startup_ts) {
        Ok(count) if count > 0 => {
            tracing::info!(
                recovered = count,
                "reconciled scheduled tasks from completed task runs on startup"
            );
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!("failed to reconcile scheduled task state on startup: {err:#}");
        }
    }
    match crate::recovery::reconcile_pending_turn_side_effects(cfg, store) {
        Ok(count) if count > 0 => {
            tracing::info!(
                recovered = count,
                "reconciled pending turn side effects on startup"
            );
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!("failed to reconcile pending turn side effects on startup: {err:#}");
        }
    }
    if let Err(err) = sync_managed_context_files(cfg, store) {
        tracing::warn!("failed to sync managed context files on startup: {err:#}");
    }
    if let Err(err) = scheduler.cleanup_expired_approval_attachments() {
        tracing::warn!("failed to cleanup expired approval attachments on startup: {err:#}");
    }

    // Attempt to recover any in-flight update from a previous crash before entering loops
    if let Err(err) =
        crate::loops::restore_inflight_update(cfg, store, &scheduler, telegram_client.as_ref())
    {
        tracing::warn!("failed to restore inflight update on startup: {err:#}");
    }

    // Start Slack Socket Mode listener if configured
    let mut slack_rx = start_socket_mode_if_configured(cfg)?;

    if cfg.webhook_mode {
        let (webhook_tx, webhook_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        // Clone the sender for the drain loop so it can re-queue messages on transient errors.
        let drain_tx = webhook_tx.clone();
        let _http_server = crate::webhook::spawn_webhook_http_server(
            cfg.clone(),
            Arc::clone(&shutdown),
            webhook_tx,
        )?;
        crate::loops::run_webhook_loop(
            cfg,
            store,
            &scheduler,
            telegram_client.as_ref(),
            &shutdown,
            webhook_rx,
            &drain_tx,
            slack_rx.as_mut(),
        )?;
        return Ok(());
    }

    match (telegram_client.as_ref(), slack_rx.as_mut()) {
        (Some(client), slack_rx) => {
            crate::loops::run_poll_loop(cfg, store, &scheduler, client, &shutdown, slack_rx)
        }
        (None, Some(slack_rx)) => {
            crate::loops::run_slack_socket_loop(cfg, store, &scheduler, &shutdown, slack_rx)
        }
        (None, None) => {
            unreachable!("no transport configured but passed initial check");
        }
    }
}
