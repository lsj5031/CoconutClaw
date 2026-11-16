use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

use crate::scheduler::SessionScheduler;
use crate::store::Store;
use crate::telegram::fetch_poll_updates;
use crate::util::extract_update_id_from_value;

use super::webhook::{drain_telegram_ingress, process_webhook_line};

pub(crate) fn run_poll_loop(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    telegram_client: &Client,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    scheduler.start_calendar()?;
    while !shutdown.load(Ordering::SeqCst) {
        drain_telegram_ingress(cfg, store, scheduler)?;
        scheduler.resume_runtime_results()?;
        let offset = store
            .kv_get("last_update_id")?
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.saturating_add(1));
        scheduler.wake_delivery();
        let updates = match fetch_poll_updates(telegram_client, cfg, offset) {
            Ok(updates) => updates,
            Err(err) => {
                tracing::warn!("telegram polling failed: {err:#}");
                sleep_until_poll_or_shutdown(cfg.poll_interval_seconds, shutdown);
                continue;
            }
        };

        if updates.is_empty() {
            scheduler.wake_delivery();
            sleep_until_poll_or_shutdown(cfg.poll_interval_seconds, shutdown);
            continue;
        }

        for update in updates {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            let update_id =
                extract_update_id_from_value(&update).and_then(|value| value.parse::<u64>().ok());
            let line = serde_json::to_string(&update)?;
            match process_webhook_line(cfg, store, scheduler, &line) {
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(
                        "failed to process polled update_id={} (custody and effect boundary preserved): {err:#}",
                        update_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    );
                    // Do not acknowledge an identity that failed admission. A
                    // pending SQL control, however, is already in durable custody.
                    sleep_until_poll_or_shutdown(cfg.poll_interval_seconds, shutdown);
                    break;
                }
            }
        }

        scheduler.wake_delivery();
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
