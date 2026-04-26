use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

use crate::delivery::{
    ScheduledDeliveryResult, ScheduledDeliveryState, ScheduledTaskDispatch,
    persist_scheduled_delivery_state,
};
use crate::slack::{build_slack_client, dispatch_slack_output, slack_fetch_thread_context};
use crate::store::Store;

pub(crate) fn dispatch_output(
    client: &reqwest::blocking::Client,
    cfg: &RuntimeConfig,
    channel_id: &str,
    output: &str,
    progress_message_id: Option<&str>,
    thread_ts: Option<&str>,
) -> Result<()> {
    dispatch_slack_output(
        client,
        cfg,
        channel_id,
        output,
        progress_message_id,
        thread_ts,
    )
}

pub(crate) fn dispatch_scheduled_output(
    store: &Store,
    cfg: &RuntimeConfig,
    request: ScheduledTaskDispatch<'_>,
    state: &mut ScheduledDeliveryState,
    channel_id: &str,
    thread_ts: Option<&str>,
) -> Result<ScheduledDeliveryResult> {
    if state.slack_completed() {
        return Ok(ScheduledDeliveryResult::Delivered);
    }

    let client = match build_slack_client(cfg) {
        Ok(client) => client,
        Err(err) => {
            tracing::warn!("failed to build slack client for scheduled task output: {err:#}");
            return Ok(ScheduledDeliveryResult::RetryableFailure);
        }
    };

    match dispatch_slack_output(
        &client,
        cfg,
        channel_id,
        request.output,
        request.progress_message_id,
        thread_ts,
    ) {
        Ok(()) => {
            state.mark_slack_completed();
            persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
            Ok(ScheduledDeliveryResult::Delivered)
        }
        Err(err) => {
            tracing::warn!("failed to deliver scheduled task output to slack: {err:#}");
            Ok(ScheduledDeliveryResult::RetryableFailure)
        }
    }
}

pub(crate) fn hydrate_thread_context(
    cfg: &RuntimeConfig,
    store: &Store,
    channel_id: &str,
    thread_ts: Option<&str>,
    session_id: &str,
) -> Option<String> {
    let thread_ts = thread_ts?;
    let boundary_unix = match store.latest_boundary_unix(session_id, "slack") {
        Ok(ts) => ts,
        Err(err) => {
            tracing::warn!("failed to read slack context boundary: {err:#}");
            None
        }
    };
    let history_client = crate::slack::build_slack_user_client(cfg)
        .unwrap_or(None)
        .unwrap_or_else(|| match build_slack_client(cfg) {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!("failed to build slack history client: {err:#}");
                reqwest::blocking::Client::new()
            }
        });
    match slack_fetch_thread_context(
        &history_client,
        channel_id,
        Some(thread_ts),
        boundary_unix.map(|ts| ts as f64),
    ) {
        Ok(ctx) if !ctx.trim().is_empty() => Some(ctx),
        Ok(_) => None,
        Err(err) => {
            tracing::warn!("failed to fetch slack thread context: {err:#}");
            None
        }
    }
}
