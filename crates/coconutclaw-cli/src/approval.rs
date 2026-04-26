use std::path::PathBuf;

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::delivery::{DeliveryTarget, TaskSource, cleanup_attachment_path, parse_delivery_target};
use crate::scheduler::TaskRequest;
use crate::session::{SessionKey, SessionPlatform};
use crate::slack::send_slack_approval_request;
use crate::store::{CreateApprovalParams, Store};
use crate::{InputType, QuotedMessage, TurnInput, iso_now};

pub(crate) fn cleanup_attachment_from_resume_payload(payload: &str) -> Result<()> {
    let value: Value = serde_json::from_str(payload).context("parse approval resume payload")?;
    let attachment_owned = value
        .get("attachment_owned")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let attachment_path = value
        .get("attachment_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);

    if attachment_owned && let Some(path) = attachment_path.as_deref() {
        cleanup_attachment_path(path);
    }

    Ok(())
}

pub(crate) fn build_resume_payload(request: &TaskRequest, input: &TurnInput) -> Result<String> {
    Ok(json!({
        "session_id": request.session.id(),
        "channel": input.channel.as_str(),
        "source": request.source.channel_name(),
        "input_type": input.input_type.as_str(),
        "user_text": input.user_text.as_str(),
        "asr_text": input.asr_text.as_str(),
        "attachment_type": input.attachment_type.as_deref(),
        "attachment_path": input.attachment_path.as_ref().map(|path| path.display().to_string()),
        "attachment_owned": input.attachment_owned,
        "supplemental_context": input.supplemental_context.as_deref(),
        "dispatch": serde_json::from_str::<Value>(&crate::delivery::serialize_delivery_target(&request.delivery))
            .unwrap_or_else(|_| json!({"kind":"stdout"})),
        "source_user_id": request.source_user_id.as_deref(),
        "quoted": {
            "reply_from": request.quoted.reply_from.as_deref(),
            "reply_text": request.quoted.reply_text.as_deref(),
            "reply_ts": request.quoted.reply_ts,
        },
    })
    .to_string())
}

pub(crate) fn request_from_resume_payload(payload: &str) -> Result<TaskRequest> {
    let value: Value = serde_json::from_str(payload).context("invalid approval resume payload")?;
    let session_id = value
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or("slack:unknown");
    let dispatch_raw = value.get("dispatch").map(Value::to_string);
    let dispatch = parse_delivery_target(dispatch_raw.as_deref()).unwrap_or(DeliveryTarget::Stdout);

    let session = SessionKey::from_id(session_id.to_string());
    let mut input = TurnInput {
        input_type: match value
            .get("input_type")
            .and_then(Value::as_str)
            .unwrap_or("text")
        {
            "voice" => InputType::Voice,
            "photo" => InputType::Photo,
            "video" => InputType::Video,
            "document" => InputType::Document,
            "video_note" => InputType::VideoNote,
            _ => InputType::Text,
        },
        user_text: value
            .get("user_text")
            .and_then(Value::as_str)
            .unwrap_or("(empty message)")
            .to_string(),
        asr_text: value
            .get("asr_text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        attachment_type: value
            .get("attachment_type")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        attachment_path: value
            .get("attachment_path")
            .and_then(Value::as_str)
            .map(PathBuf::from),
        attachment_owned: value
            .get("attachment_owned")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        supplemental_context: value
            .get("supplemental_context")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        channel: value
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or(match session.platform {
                SessionPlatform::Telegram => "telegram",
                SessionPlatform::Slack => "slack",
                SessionPlatform::Scheduled => "scheduled",
                SessionPlatform::Local => "local",
            })
            .to_string(),
    };
    let approval_note = "Administrator approval granted for the previous pending request. Continue the approved work without asking for approval again.";
    input.supplemental_context = Some(match input.supplemental_context.take() {
        Some(existing) if !existing.trim().is_empty() => format!("{existing}\n{approval_note}"),
        _ => approval_note.to_string(),
    });

    let source = match (&session.platform, &dispatch) {
        (
            SessionPlatform::Slack,
            DeliveryTarget::Slack {
                channel_id,
                thread_ts,
            },
        ) => TaskSource::Slack {
            channel_id: channel_id.clone(),
            thread_ts: thread_ts.clone(),
        },
        (SessionPlatform::Telegram, _) => TaskSource::Telegram,
        (SessionPlatform::Scheduled, _) => TaskSource::Scheduled,
        _ => TaskSource::Local,
    };

    Ok(TaskRequest {
        session,
        source,
        input,
        update_id: None,
        media: None,
        quoted: QuotedMessage {
            reply_from: value
                .get("quoted")
                .and_then(|quoted| quoted.get("reply_from"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            reply_text: value
                .get("quoted")
                .and_then(|quoted| quoted.get("reply_text"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            reply_ts: value
                .get("quoted")
                .and_then(|quoted| quoted.get("reply_ts"))
                .and_then(Value::as_i64),
        },
        delivery: dispatch.clone(),
        persisted_delivery_target: Some(dispatch),
        source_user_id: value
            .get("source_user_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        progress_message_id: None,
        scheduled_task_id: None,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_slack_approval_request(
    store: &Store,
    client: &Client,
    request: &TaskRequest,
    input: &TurnInput,
    task_run_id: i64,
    channel_id: &str,
    thread_ts: Option<&str>,
    prompt: &str,
    timezone: &str,
) -> Result<()> {
    let resume_payload = build_resume_payload(request, input)?;
    store.mark_task_run_awaiting_approval(task_run_id)?;
    let approval_id = store.create_approval(CreateApprovalParams {
        task_run_id,
        session_id: request.session.id(),
        channel: request.source.channel_name().to_string(),
        source_user_id: request.source_user_id.clone(),
        channel_id: Some(channel_id.to_string()),
        thread_ts: thread_ts.map(ToOwned::to_owned),
        prompt_text: prompt.to_string(),
        request_message_ts: None,
        resume_payload,
        created_at: iso_now(timezone),
    })?;
    let approval_message_ts =
        match send_slack_approval_request(client, channel_id, thread_ts, approval_id, prompt) {
            Ok(ts) => ts,
            Err(err) => {
                let now = iso_now(timezone);
                let _ = store.resolve_approval(approval_id, "expired", &now, None);
                return Err(err);
            }
        };
    store.update_approval_request_message_ts(approval_id, &approval_message_ts)?;
    Ok(())
}
