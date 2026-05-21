use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

use crate::commands::helpers::process_slack_socket_turn;
use crate::scheduler::SessionScheduler;
use crate::slack::SlackWebhookTurn;
use crate::store::Store;

pub(crate) fn drain_slack_socket_turns(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    slack_rx: &mut tokio::sync::mpsc::UnboundedReceiver<SlackWebhookTurn>,
) {
    while let Ok(turn) = slack_rx.try_recv() {
        if let Err(err) = process_slack_socket_turn(cfg, store, scheduler, turn) {
            tracing::warn!("failed to process slack socket mode turn: {err:#}");
        }
    }
}

pub(crate) fn run_slack_socket_loop(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    shutdown: &Arc<AtomicBool>,
    slack_rx: &mut tokio::sync::mpsc::UnboundedReceiver<SlackWebhookTurn>,
) -> Result<()> {
    while !shutdown.load(Ordering::SeqCst) {
        // Drain all available slack turns non-blockingly
        drain_slack_socket_turns(cfg, store, scheduler, slack_rx);

        // Run any due scheduled tasks
        if let Err(err) = crate::scheduling::run_due_scheduled_tasks(cfg, store, scheduler, None) {
            tracing::warn!("scheduled task execution failed: {err:#}");
        }

        thread::sleep(Duration::from_secs(cfg.poll_interval_seconds.max(1)));
    }

    tracing::info!("shutdown signal received, stopping slack socket loop");
    Ok(())
}
