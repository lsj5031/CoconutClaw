use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use std::io::Write;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

use crate::scheduler::SessionScheduler;
use crate::slack::SlackWebhookTurn;
use crate::store::Store;
use crate::telegram::{dispatch_telegram_output, fetch_poll_updates};
use crate::util::set_inflight_update;
use crate::webhook::extract_update_id_from_value;

use super::slack_socket::drain_slack_socket_turns;
use super::webhook::process_webhook_line;

pub(crate) fn run_poll_loop(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    telegram_client: &Client,
    shutdown: &Arc<AtomicBool>,
    mut slack_rx: Option<&mut tokio::sync::mpsc::UnboundedReceiver<SlackWebhookTurn>>,
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
            if let Some(ref mut rx) = slack_rx {
                drain_slack_socket_turns(cfg, store, scheduler, rx);
            }
            // Run any due scheduled tasks before sleeping
            if let Err(err) = crate::scheduling::run_due_scheduled_tasks(
                cfg,
                store,
                scheduler,
                Some(telegram_client),
            ) {
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
            let outcome = match process_webhook_line(cfg, store, scheduler, &line) {
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
                    std::io::stdout().flush().ok();
                }
            }
            if let Some(path) = outcome.cleanup_path.as_deref() {
                let _ = std::fs::remove_file(path);
            }

            if let Err(err) = store.clear_inflight() {
                tracing::warn!("failed to clear inflight after poll processing: {err:#}");
            }

            if let Some(update_id) = update_id {
                offset = Some(update_id.saturating_add(1));
            }
        }

        // Drain any pending Slack socket mode turns
        if let Some(ref mut rx) = slack_rx {
            drain_slack_socket_turns(cfg, store, scheduler, rx);
        }

        // Run any due scheduled tasks
        if let Err(err) =
            crate::scheduling::run_due_scheduled_tasks(cfg, store, scheduler, Some(telegram_client))
        {
            tracing::warn!("scheduled task execution failed: {err:#}");
        }
    }

    tracing::info!("shutdown signal received, stopping poll loop");
    Ok(())
}
