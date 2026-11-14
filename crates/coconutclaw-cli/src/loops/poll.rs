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
use crate::store::Store;
use crate::telegram::{dispatch_telegram_output, fetch_poll_updates};
use crate::util::extract_update_id_from_value;
use crate::util::set_inflight_update;

use super::webhook::process_webhook_line;

pub(crate) fn run_poll_loop(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
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
                sleep_until_poll_or_shutdown(cfg.poll_interval_seconds, shutdown);
                continue;
            }
        };

        if updates.is_empty() {
            scheduler.retry_deliveries()?;
            sleep_until_poll_or_shutdown(cfg.poll_interval_seconds, shutdown);
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
            set_inflight_update(
                store,
                update_id.map(|id| id.to_string()).as_deref().unwrap_or(""),
                &line,
                &cfg.timezone,
            )?;
            let outcome = match process_webhook_line(cfg, store, scheduler, &line) {
                Ok(outcome) => outcome,
                Err(err) => {
                    tracing::warn!(
                        "failed to process polled update_id={} (preserved for retry): {err:#}",
                        update_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    );
                    sleep_until_poll_or_shutdown(cfg.poll_interval_seconds, shutdown);
                    break;
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

        scheduler.retry_deliveries()?;
    }

    tracing::info!("shutdown signal received, stopping poll loop");
    Ok(())
}

fn sleep_until_poll_or_shutdown(seconds: u64, shutdown: &AtomicBool) {
    let until = std::time::Instant::now() + Duration::from_secs(seconds.max(1));
    while !shutdown.load(Ordering::SeqCst) && std::time::Instant::now() < until {
        thread::sleep(
            Duration::from_millis(25)
                .min(until.saturating_duration_since(std::time::Instant::now())),
        );
    }
}
