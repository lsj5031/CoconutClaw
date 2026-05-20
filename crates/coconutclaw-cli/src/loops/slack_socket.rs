use coconutclaw_config::RuntimeConfig;

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
