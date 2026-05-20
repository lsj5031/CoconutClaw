use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use coconutclaw_config::RuntimeConfig;
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;
use url::form_urlencoded;

use crate::signal_cancel_marker;

#[derive(Clone)]
struct WebhookHttpState {
    cfg: RuntimeConfig,
    tx: mpsc::UnboundedSender<String>,
}

pub(crate) fn webhook_request_path(cfg: &RuntimeConfig) -> &str {
    cfg.webhook_path.as_str()
}

pub(crate) fn webhook_public_endpoint(cfg: &RuntimeConfig) -> Result<String> {
    let base = cfg
        .webhook_public_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("WEBHOOK_PUBLIC_URL is required when WEBHOOK_MODE is on"))?;

    let base = base.trim_end_matches('/');
    let path = webhook_request_path(cfg).trim();
    if path == "/" {
        return Ok(base.to_string());
    }

    Ok(format!("{base}/{}", path.trim_start_matches('/')))
}

pub(crate) fn spawn_webhook_http_server(
    cfg: RuntimeConfig,
    shutdown: Arc<AtomicBool>,
    tx: mpsc::UnboundedSender<String>,
) -> Result<std::thread::JoinHandle<()>> {
    let bind_addr: SocketAddr = cfg
        .webhook_bind
        .parse()
        .with_context(|| format!("failed to parse webhook bind address {}", cfg.webhook_bind))?;
    let route_path = normalize_route_path(cfg.webhook_path.clone());

    Ok(thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                tracing::warn!("failed to build webhook runtime: {err:#}");
                return;
            }
        };

        runtime.block_on(async move {
            let state = WebhookHttpState { cfg: cfg.clone(), tx: tx.clone() };
            let mut router = Router::new()
                .route(&route_path, post(webhook_post_handler));

            if cfg.slack_signing_secret.is_some() || cfg.slack_bot_token.is_some() {
                if cfg.slack_signing_secret.as_deref().map(str::trim).unwrap_or("").is_empty() {
                    tracing::warn!("SLACK_SIGNING_SECRET is not set — Slack webhook requests will not be verified");
                }
                let slack_path = normalize_route_path("/slack/events".to_string());
                tracing::info!("slack events route: {slack_path}");
                router = router.route(&slack_path, post(slack_events_post_handler));
            }

            let app = router.with_state(state);

            let listener = match tokio::net::TcpListener::bind(bind_addr).await {
                Ok(listener) => listener,
                Err(err) => {
                    tracing::warn!("failed to bind webhook listener at {bind_addr}: {err}");
                    return;
                }
            };

            // Log the actual bound address (os-assigned port when config has :0)
            if let Ok(addr) = listener.local_addr() {
                tracing::info!("webhook server listening on {}{}", addr, route_path);
            }

            let shutdown_wait = async move {
                while !shutdown.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };

            if let Err(err) = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_wait)
                .await
            {
                tracing::warn!("webhook server error: {err}");
            }
        });
    }))
}

async fn webhook_post_handler(
    State(state): State<WebhookHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let cfg = &state.cfg;

    if let Some(expected_secret) = cfg.webhook_secret.as_deref().map(str::trim)
        && !expected_secret.is_empty()
    {
        let provided = headers
            .get("x-telegram-bot-api-secret-token")
            .and_then(|value| value.to_str().ok())
            .map(str::trim);

        let is_match = if let Some(provided) = provided {
            provided.as_bytes().ct_eq(expected_secret.as_bytes()).into()
        } else {
            false
        };

        if !is_match {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"ok":false,"error":"forbidden"})),
            )
                .into_response();
        }
    }

    let body_text = match std::str::from_utf8(&body) {
        Ok(text) => text.trim(),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"ok":false,"error":"invalid_utf8"})),
            )
                .into_response();
        }
    };

    if serde_json::from_str::<Value>(body_text).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok":false,"error":"invalid_json"})),
        )
            .into_response();
    }

    // Detect cancel commands inline and signal the cancel marker.
    if let Ok(value) = serde_json::from_str::<Value>(body_text)
        && telegram_cancel_requested(&value)
        && let Err(err) = signal_cancel_marker(cfg)
    {
        tracing::warn!("failed to set cancel marker from telegram webhook: {err:#}");
    }

    if let Err(err) = state.tx.send(body_text.to_string()) {
        tracing::warn!("failed to send webhook update via channel (receiver dropped): {err:#}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok":false,"error":"channel_send_failed"})),
        )
            .into_response();
    }

    (StatusCode::OK, Json(json!({"ok":true}))).into_response()
}

pub(crate) fn extract_update_id_from_json(payload: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(payload).context("invalid update JSON")?;
    Ok(extract_update_id_from_value(&value))
}

pub(crate) fn extract_update_id_from_value(value: &Value) -> Option<String> {
    value.get("update_id").map(value_to_string).and_then(|id| {
        let trimmed = id.trim();
        if trimmed.is_empty() || trimmed == "0" {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub(crate) fn value_to_string(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    if let Some(num) = value.as_i64() {
        return num.to_string();
    }
    if let Some(num) = value.as_u64() {
        return num.to_string();
    }
    value.to_string()
}

fn normalize_route_path(raw: String) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "/" {
        "/".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn verify_slack_signature(
    signing_secret: &str,
    timestamp: &str,
    body: &[u8],
    signature: &str,
) -> bool {
    let ts: i64 = match timestamp.parse() {
        Ok(t) => t,
        Err(_) => return false,
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if (now - ts).abs() > 300 {
        return false;
    }

    let basestring = format!("v0:{timestamp}:{}", String::from_utf8_lossy(body));
    let mut mac = match Hmac::<Sha256>::new_from_slice(signing_secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(basestring.as_bytes());
    let result = mac.finalize().into_bytes();
    let computed = format!("v0={}", hex_encode(&result));

    computed.as_bytes().ct_eq(signature.as_bytes()).into()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn slack_events_post_handler(
    State(state): State<WebhookHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let cfg = &state.cfg;

    if let Some(secret) = cfg.slack_signing_secret.as_deref().map(str::trim)
        && !secret.is_empty()
    {
        let timestamp = headers
            .get("X-Slack-Request-Timestamp")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let signature = headers
            .get("X-Slack-Signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !verify_slack_signature(secret, timestamp, &body, signature) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"ok":false,"error":"invalid_signature"})),
            )
                .into_response();
        }
    }

    let body_text = match std::str::from_utf8(&body) {
        Ok(text) => text.trim(),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"ok":false,"error":"invalid_utf8"})),
            )
                .into_response();
        }
    };

    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let value = match normalize_slack_request_payload(content_type, body_text) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!("failed to parse slack request body: {err:#}");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"ok":false,"error":"invalid_payload"})),
            )
                .into_response();
        }
    };

    // Handle URL verification challenge
    if value.get("type").and_then(|v| v.as_str()) == Some("url_verification") {
        let challenge = value
            .get("challenge")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return Json(json!({"challenge": challenge})).into_response();
    }

    if slack_cancel_requested(&value)
        && let Err(err) = signal_cancel_marker(cfg)
    {
        tracing::warn!("failed to set cancel marker from slack webhook: {err:#}");
    }

    let normalized = match serde_json::to_string(&value) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!("failed to serialize normalized slack payload: {err:#}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"ok":false,"error":"queue_write_failed"})),
            )
                .into_response();
        }
    };

    // Forward to the channel
    if let Err(err) = state.tx.send(normalized) {
        tracing::warn!("failed to send slack event via channel (receiver dropped): {err:#}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok":false,"error":"channel_send_failed"})),
        )
            .into_response();
    }

    (StatusCode::OK, Json(json!({"ok":true}))).into_response()
}

fn normalize_slack_request_payload(content_type: &str, body_text: &str) -> Result<Value> {
    if content_type.contains("application/x-www-form-urlencoded") {
        let fields: Vec<(String, String)> = form_urlencoded::parse(body_text.as_bytes())
            .into_owned()
            .collect();
        if let Some(payload) = fields
            .iter()
            .find_map(|(key, value)| (key == "payload").then_some(value))
        {
            return serde_json::from_str(payload).context("invalid slack interactive payload JSON");
        }

        let mut value = serde_json::Map::new();
        value.insert(
            "type".to_string(),
            Value::String("slash_commands".to_string()),
        );
        for (key, field) in fields {
            value.insert(key, Value::String(field));
        }
        if value.get("command").and_then(Value::as_str).is_none() {
            anyhow::bail!("missing slack command field");
        }
        return Ok(Value::Object(value));
    }

    serde_json::from_str(body_text).context("invalid slack JSON payload")
}

fn telegram_cancel_requested(value: &Value) -> bool {
    if let Some(message) = value.get("message") {
        let text = message
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        return text.trim().eq_ignore_ascii_case("/cancel");
    }
    if let Some(callback_query) = value.get("callback_query") {
        let data = callback_query
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default();
        return data.eq_ignore_ascii_case("cancel");
    }
    false
}

fn slack_cancel_requested(value: &Value) -> bool {
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "slash_commands" => value.get("command").and_then(Value::as_str) == Some("/cancel"),
        "block_actions" | "interactive_message" => value
            .get("actions")
            .and_then(Value::as_array)
            .map(|actions| {
                actions
                    .iter()
                    .any(|action| action.get("action_id").and_then(Value::as_str) == Some("cancel"))
            })
            .unwrap_or(false),
        _ => false,
    }
}

#[cfg(test)]
#[path = "webhook_tests.rs"]
mod tests;
