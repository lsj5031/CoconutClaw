use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;

use crate::delivery::{
    DeliveryTarget, ScheduledDeliveryResult, ScheduledTaskDispatch, TaskSource,
    dispatch_scheduled_task_output, parse_delivery_target, serialize_delivery_target,
};
use crate::recovery::pending::recover_scheduled_task_output_from_task_run;
use crate::scheduler::{SessionScheduler, TaskRequest};
use crate::session::{SessionKey, SessionPlatform};
use crate::slack::{valid_slack_channel_id, valid_slack_token};
use crate::store::Store;
use crate::telegram::valid_telegram_chat_id;
use crate::types::{InputType, QuotedMessage, TurnInput};
use crate::util::{iso_now, scheduled_task_slot_now};

fn delivery_target_from_session_id(session_id: &str) -> Option<DeliveryTarget> {
    let session = SessionKey::from_id(session_id.to_string());
    match session.platform {
        SessionPlatform::Telegram => Some(DeliveryTarget::Telegram {
            chat_id: session.root_id,
        }),
        SessionPlatform::Slack => Some(DeliveryTarget::Slack {
            channel_id: session.root_id,
            thread_ts: session.thread_id,
        }),
        SessionPlatform::Local => Some(DeliveryTarget::Stdout),
        SessionPlatform::Scheduled => None,
    }
}

fn delivery_target_from_task_run(task_run: &crate::store::TaskRun) -> Option<DeliveryTarget> {
    delivery_target_from_session_id(&task_run.session_id).or_else(|| {
        match task_run.channel.as_str() {
            "telegram" => {
                task_run
                    .source_chat_id
                    .as_ref()
                    .map(|chat_id| DeliveryTarget::Telegram {
                        chat_id: chat_id.clone(),
                    })
            }
            "slack" => task_run
                .source_chat_id
                .as_ref()
                .map(|channel_id| DeliveryTarget::Slack {
                    channel_id: channel_id.clone(),
                    thread_ts: None,
                }),
            "local" => Some(DeliveryTarget::Stdout),
            _ => None,
        }
    })
}

pub(crate) fn append_routing_for_task_run(
    store: &Store,
    task_run_id: Option<i64>,
) -> Result<(Option<String>, Option<String>)> {
    let Some(task_run_id) = task_run_id else {
        return Ok((None, None));
    };
    let Some(task_run) = store.get_task_run(task_run_id)? else {
        return Ok((None, None));
    };

    let scheduled_task = match task_run.scheduled_task_id {
        Some(scheduled_task_id) => store.get_scheduled_task(scheduled_task_id)?,
        None => None,
    };
    let origin_session = scheduled_task
        .as_ref()
        .and_then(|task| task.origin_session.clone())
        .or_else(|| {
            (!matches!(
                SessionKey::from_id(task_run.session_id.clone()).platform,
                SessionPlatform::Scheduled
            ))
            .then(|| task_run.session_id.clone())
        });
    let delivery_target_json = scheduled_task
        .as_ref()
        .and_then(|task| task.delivery_target.clone())
        .or_else(|| {
            delivery_target_from_task_run(&task_run)
                .map(|target| serialize_delivery_target(&target))
        });

    Ok((origin_session, delivery_target_json))
}

fn infer_legacy_scheduled_delivery_target(
    task: &crate::store::ScheduledTask,
    latest_task_run: Option<&crate::store::TaskRun>,
) -> Option<DeliveryTarget> {
    task.origin_session
        .as_deref()
        .and_then(delivery_target_from_session_id)
        .or_else(|| {
            latest_task_run
                .and_then(|task_run| delivery_target_from_session_id(&task_run.session_id))
        })
        .or_else(|| latest_task_run.and_then(delivery_target_from_task_run))
}

fn unique_configured_delivery_target(cfg: &RuntimeConfig) -> Option<DeliveryTarget> {
    let telegram = valid_telegram_chat_id(cfg).map(|chat_id| DeliveryTarget::Telegram {
        chat_id: chat_id.to_string(),
    });
    let slack = match (valid_slack_token(cfg), valid_slack_channel_id(cfg)) {
        (Some(_), Some(channel_id)) => Some(DeliveryTarget::Slack {
            channel_id: channel_id.to_string(),
            thread_ts: None,
        }),
        _ => None,
    };

    match (telegram, slack) {
        (Some(telegram), None) => Some(telegram),
        (None, Some(slack)) => Some(slack),
        (None, None) => Some(DeliveryTarget::Stdout),
        (Some(_), Some(_)) => None,
    }
}

fn infer_legacy_origin_session(
    latest_task_run: Option<&crate::store::TaskRun>,
    delivery_target: Option<&DeliveryTarget>,
) -> Option<String> {
    latest_task_run
        .and_then(|task_run| {
            (!matches!(
                SessionKey::from_id(task_run.session_id.clone()).platform,
                SessionPlatform::Scheduled
            ))
            .then(|| task_run.session_id.clone())
        })
        .or_else(|| {
            delivery_target.map(|target| match target {
                DeliveryTarget::Telegram { chat_id } => SessionKey::telegram(chat_id).id(),
                DeliveryTarget::Slack {
                    channel_id,
                    thread_ts,
                } => SessionKey::slack(channel_id, thread_ts.as_deref()).id(),
                DeliveryTarget::Stdout => SessionKey::local("scheduled").id(),
            })
        })
}

pub(crate) fn scheduled_task_context_channel(
    task: &crate::store::ScheduledTask,
    resolved_delivery_target: Option<&DeliveryTarget>,
    latest_task_run: Option<&crate::store::TaskRun>,
) -> &'static str {
    if let Some(target) = resolved_delivery_target {
        return target.transport_name();
    }

    if let Some(origin_session) = task.origin_session.as_deref() {
        return match SessionKey::from_id(origin_session.to_string()).platform {
            SessionPlatform::Telegram => "telegram",
            SessionPlatform::Slack => "slack",
            SessionPlatform::Local => "local",
            SessionPlatform::Scheduled => "local",
        };
    }

    if let Some(task_run) = latest_task_run {
        return match SessionKey::from_id(task_run.session_id.clone()).platform {
            SessionPlatform::Telegram => "telegram",
            SessionPlatform::Slack => "slack",
            SessionPlatform::Local => "local",
            SessionPlatform::Scheduled => match task_run.channel.as_str() {
                "telegram" => "telegram",
                "slack" => "slack",
                "local" => "local",
                _ => "local",
            },
        };
    }

    "local"
}

pub(crate) fn run_due_scheduled_tasks(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    telegram_client: Option<&Client>,
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
        let session = if let Some(ref origin_session) = task.origin_session {
            SessionKey::from_id(origin_session.clone())
        } else {
            SessionKey::scheduled(&format!("task-{}", task.id))
        };
        let latest_task_run = store.latest_task_run_for_scheduled_task(task.id)?;
        let resolved_delivery_target = parse_delivery_target(task.delivery_target.as_deref())
            .or_else(|| infer_legacy_scheduled_delivery_target(&task, latest_task_run.as_ref()))
            .or_else(|| unique_configured_delivery_target(cfg));
        let Some(delivery_target) = resolved_delivery_target.clone() else {
            tracing::warn!(
                scheduled_task_id = task.id,
                prompt = %task.prompt,
                "scheduled task has no recoverable routing metadata and multiple configured delivery surfaces; leaving it pending instead of misrouting"
            );
            continue;
        };
        if task.delivery_target.is_none() || task.origin_session.is_none() {
            let inferred_origin_session = task.origin_session.clone().or_else(|| {
                infer_legacy_origin_session(latest_task_run.as_ref(), Some(&delivery_target))
            });
            let inferred_delivery_target = task
                .delivery_target
                .clone()
                .or_else(|| Some(serialize_delivery_target(&delivery_target)));
            if let Err(err) = store.set_scheduled_task_routing(
                task.id,
                inferred_origin_session.as_deref(),
                inferred_delivery_target.as_deref(),
            ) {
                tracing::warn!(
                    scheduled_task_id = task.id,
                    "failed to persist inferred scheduled task routing: {err:#}"
                );
            }
        }
        let context_channel = scheduled_task_context_channel(
            &task,
            resolved_delivery_target.as_ref(),
            latest_task_run.as_ref(),
        );
        let is_retry = task.pending_output.is_some();
        tracing::info!(
            "executing scheduled task id={} created={} last_run={:?} time={} recurring={} done={} prompt={:?} is_retry={} origin_session={:?} delivery_target={:?}",
            task.id,
            task.ts,
            task.last_run_ts,
            task.schedule_time,
            task.recurring,
            task.done,
            task.prompt,
            is_retry,
            task.origin_session,
            task.delivery_target
        );

        if let Some(active_task) = store.find_active_task_for_session(&session.id())? {
            tracing::info!(
                scheduled_task_id = task.id,
                active_task_run_id = active_task.id,
                session = %session.id(),
                is_retry,
                "scheduled task already has an active run; skipping duplicate enqueue or retry"
            );
            continue;
        }

        let progress_message_id = latest_task_run
            .as_ref()
            .and_then(|task_run| task_run.progress_message_id.clone());

        let output_to_dispatch = if let Some(pending) = task.pending_output.as_deref() {
            tracing::info!("retrying dispatch for scheduled task id={}", task.id);
            Some(pending.to_string())
        } else if let Some(task_run_id) = latest_task_run.as_ref().map(|task_run| task_run.id) {
            match recover_scheduled_task_output_from_task_run(cfg, store, task.id, task_run_id)? {
                Some(output) => {
                    tracing::info!(
                        scheduled_task_id = task.id,
                        task_run_id,
                        "recovered scheduled task output from persisted turn without rerunning provider"
                    );
                    Some(output)
                }
                None => None,
            }
        } else {
            None
        };

        if let Some(output) = output_to_dispatch {
            let delivery_state = if task.pending_output.is_some() {
                task.delivery_state.as_deref()
            } else {
                None
            };
            match dispatch_scheduled_task_output(
                store,
                cfg,
                telegram_client,
                ScheduledTaskDispatch {
                    scheduled_task_id: task.id,
                    delivery_target: resolved_delivery_target.as_ref(),
                    output: &output,
                    progress_message_id: progress_message_id.as_deref(),
                    delivery_state_raw: delivery_state,
                },
            ) {
                Ok(ScheduledDeliveryResult::Delivered) => {
                    let now_iso = iso_now(&cfg.timezone);
                    if let Err(err) = store.mark_scheduled_task_executed(task.id, &now_iso) {
                        tracing::warn!(
                            "failed to mark scheduled task id={} as executed: {err:#}",
                            task.id
                        );
                    }
                }
                Ok(ScheduledDeliveryResult::SkippedPermanent) => {
                    let now_iso = iso_now(&cfg.timezone);
                    if let Err(err) = store.mark_scheduled_task_executed(task.id, &now_iso) {
                        tracing::warn!("failed to mark scheduled task skipped: {err:#}");
                    }
                }
                Ok(ScheduledDeliveryResult::RetryableFailure) => {
                    tracing::warn!("scheduled task id={} delivery failed, will retry", task.id);
                }
                Err(err) => {
                    tracing::warn!("failed to dispatch scheduled task output: {err:#}");
                }
            }
            continue;
        }

        let progress_message_id = delivery_target.send_placeholder(
            cfg,
            telegram_client,
            &format!("scheduled task id={}", task.id),
        );
        let enqueue_result = scheduler.enqueue(TaskRequest {
            session: session.clone(),
            source: TaskSource::Scheduled,
            input: TurnInput {
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
                channel: context_channel.to_string(),
            },
            update_id: None,
            media: None,
            quoted: QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
            delivery: delivery_target.clone(),
            persisted_delivery_target: resolved_delivery_target,
            source_user_id: None,
            progress_message_id,
            scheduled_task_id: Some(task.id),
        });
        match enqueue_result {
            Ok(task_run_id) => {
                tracing::info!(task_run_id, session = %session.id(), scheduled_task_id = task.id, "queued scheduled task");
            }
            Err(err) => {
                tracing::warn!("scheduled task id={} failed to queue: {err:#}", task.id);
            }
        }
    }

    Ok(())
}
