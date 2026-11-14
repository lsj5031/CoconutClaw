use std::path::PathBuf;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::delivery::{TaskSource, cleanup_attachment_path, parse_delivery_target};
use crate::scheduler::{TaskMedia, TaskRequest};
use crate::session::SessionKey;
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
    let media = if input.attachment_path.is_none() {
        request.media.as_ref().map(|media| match media {
            TaskMedia::Telegram(crate::IncomingMedia::Photo { file_id }) => {
                json!({"kind":"photo","file_id":file_id})
            }
            TaskMedia::Telegram(crate::IncomingMedia::Document { file_id, file_name }) => {
                json!({"kind":"document","file_id":file_id,"file_name":file_name})
            }
            TaskMedia::Telegram(crate::IncomingMedia::Video { file_id }) => {
                json!({"kind":"video","file_id":file_id})
            }
            TaskMedia::Telegram(crate::IncomingMedia::VideoNote { file_id }) => {
                json!({"kind":"video_note","file_id":file_id})
            }
        })
    } else {
        None
    };
    Ok(json!({
        "session_id": request.session.id(),
        "channel": input.channel.as_str(),
        "source": request.source.channel_name(),
        "input_type": input.input_type.as_str(),
        "user_text": input.user_text.as_str(),
        "delegated": request.delegated,
        "media": media,
        "update_id": request.update_id,
        "attachment_type": input.attachment_type.as_deref(),
        "attachment_path": input.attachment_path.as_ref().map(|path| path.display().to_string()),
        "attachment_owned": input.attachment_owned,
        "supplemental_context": input.supplemental_context.as_deref(),
        "dispatch": serde_json::from_str::<Value>(&crate::delivery::serialize_delivery_target(&request.delivery))
            .unwrap_or_else(|_| json!({"kind":"stdout"})),
        "source_user_id": request.source_user_id.as_deref(),
        "progress_message_id": request.progress_message_id.as_deref(),
        "quoted": {
            "reply_from": request.quoted.reply_from.as_deref(),
            "reply_text": request.quoted.reply_text.as_deref(),
            "reply_ts": request.quoted.reply_ts,
        },
    })
    .to_string())
}

pub(crate) fn request_from_resume_payload(payload: &str) -> Result<TaskRequest> {
    request_from_payload(payload, true)
}

pub(crate) fn request_from_payload(payload: &str, approved: bool) -> Result<TaskRequest> {
    let value: Value = serde_json::from_str(payload).context("invalid approval resume payload")?;
    let session_id = value
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing approval session"))?;
    if !session_id.starts_with("telegram:") && !session_id.starts_with("local:") {
        anyhow::bail!("removed session transport preserved; see docs/migration-phase-0.md");
    }
    let dispatch_raw = value.get("dispatch").map(Value::to_string);
    let dispatch = parse_delivery_target(dispatch_raw.as_deref())
        .ok_or_else(|| anyhow::anyhow!("unsupported approval delivery target; data preserved"))?;

    let session = SessionKey::from_id(session_id.to_string());
    let mut input = TurnInput {
        input_type: match value
            .get("input_type")
            .and_then(Value::as_str)
            .unwrap_or("text")
        {
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
            .unwrap_or("telegram")
            .to_string(),
    };
    let approval_note = "Administrator approval granted for the previous pending request. Continue the approved work without asking for approval again.";
    if approved {
        input.supplemental_context = Some(match input.supplemental_context.take() {
            Some(existing) if !existing.trim().is_empty() => format!("{existing}\n{approval_note}"),
            _ => approval_note.to_string(),
        });
    }

    let source = if input.channel == "telegram" {
        TaskSource::Telegram
    } else if input.channel == "local" {
        TaskSource::Local
    } else {
        anyhow::bail!("removed request channel preserved; see docs/migration-phase-0.md");
    };
    let media = value
        .get("media")
        .filter(|v| !v.is_null())
        .map(|media| -> Result<TaskMedia> {
            let id = media
                .get("file_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing persisted Telegram file ID"))?
                .to_string();
            let media = match media.get("kind").and_then(Value::as_str) {
                Some("photo") => crate::IncomingMedia::Photo { file_id: id },
                Some("document") => crate::IncomingMedia::Document {
                    file_id: id,
                    file_name: media
                        .get("file_name")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                },
                Some("video") => crate::IncomingMedia::Video { file_id: id },
                Some("video_note") => crate::IncomingMedia::VideoNote { file_id: id },
                _ => anyhow::bail!("unsupported persisted Telegram media; data preserved"),
            };
            Ok(TaskMedia::Telegram(media))
        })
        .transpose()?;
    Ok(TaskRequest {
        session,
        source,
        input,
        update_id: if approved {
            None
        } else {
            value
                .get("update_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        },
        media,
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
        source_user_id: value
            .get("source_user_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        progress_message_id: value
            .get("progress_message_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        delegated: value
            .get("delegated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

pub(crate) fn create_approval_request(
    store: &Store,
    request: &TaskRequest,
    input: &TurnInput,
    task_run_id: i64,
    prompt: &str,
    timezone: &str,
) -> Result<i64> {
    let payload = build_resume_payload(request, input)?;
    let id = store.create_approval(CreateApprovalParams {
        task_run_id,
        session_id: request.session.id(),
        channel: request.source.channel_name().to_string(),
        source_user_id: request.source_user_id.clone(),
        channel_id: Some(request.delivery.display_id().to_string()),
        thread_ts: None,
        prompt_text: prompt.to_string(),
        request_message_ts: None,
        resume_payload: payload,
        created_at: iso_now(timezone),
    })?;
    store.mark_task_run_awaiting_approval(task_run_id)?;
    Ok(id)
}
