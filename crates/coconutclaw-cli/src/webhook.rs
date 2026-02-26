use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use coconutclaw_config::RuntimeConfig;
use fs2::FileExt;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckStatus {
    Acked,
    Empty,
    HeadMismatch,
}

#[derive(Clone)]
struct WebhookHttpState {
    cfg: RuntimeConfig,
}

pub(crate) fn ensure_webhook_queue_file(cfg: &RuntimeConfig) -> Result<()> {
    let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
    if let Some(parent) = queue_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if !queue_path.exists() {
        fs::write(&queue_path, "")
            .with_context(|| format!("failed to initialize {}", queue_path.display()))?;
    }
    Ok(())
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
) -> Result<std::thread::JoinHandle<()>> {
    let bind_addr: SocketAddr = cfg
        .webhook_bind
        .parse()
        .with_context(|| format!("failed to parse webhook bind address {}", cfg.webhook_bind))?;
    let route_path = normalize_route_path(cfg.webhook_path.clone());

    tracing::info!(
        "webhook server listening on {}{}",
        cfg.webhook_bind,
        route_path
    );

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
            let state = WebhookHttpState { cfg: cfg.clone() };
            let app = Router::new()
                .route(&route_path, post(webhook_post_handler))
                .with_state(state);

            let listener = match tokio::net::TcpListener::bind(bind_addr).await {
                Ok(listener) => listener,
                Err(err) => {
                    tracing::warn!("failed to bind webhook listener at {bind_addr}: {err}");
                    return;
                }
            };

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
        if provided != Some(expected_secret) {
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

    if let Err(err) = append_webhook_queue_line(cfg, body_text) {
        tracing::warn!("failed to append webhook update to queue: {err:#}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok":false,"error":"queue_write_failed"})),
        )
            .into_response();
    }

    (StatusCode::OK, Json(json!({"ok":true}))).into_response()
}

pub(crate) fn append_webhook_queue_line(cfg: &RuntimeConfig, payload_line: &str) -> Result<()> {
    with_webhook_lock(cfg, || {
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        if let Some(parent) = queue_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let normalized_payload = normalize_webhook_payload_line(payload_line)?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&queue_path)
            .with_context(|| format!("failed to open {}", queue_path.display()))?;
        file.write_all(normalized_payload.as_bytes())
            .with_context(|| format!("failed to append {}", queue_path.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("failed to append {}", queue_path.display()))?;
        file.flush()
            .with_context(|| format!("failed to flush {}", queue_path.display()))?;
        Ok(())
    })
}

fn normalize_webhook_payload_line(payload_line: &str) -> Result<String> {
    let value: Value =
        serde_json::from_str(payload_line).context("invalid webhook JSON payload")?;
    serde_json::to_string(&value).context("failed to serialize webhook JSON payload")
}

pub(crate) fn peek_webhook_queue_line(cfg: &RuntimeConfig) -> Result<Option<String>> {
    with_webhook_lock(cfg, || {
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        if !queue_path.exists() {
            return Ok(None);
        }
        let payload = fs::read_to_string(&queue_path)
            .with_context(|| format!("failed to read {}", queue_path.display()))?;
        for line in payload.lines() {
            if !line.trim().is_empty() {
                return Ok(Some(line.to_string()));
            }
        }
        Ok(None)
    })
}

pub(crate) fn ack_webhook_queue_line(
    cfg: &RuntimeConfig,
    expected_update_id: Option<&str>,
) -> Result<AckStatus> {
    with_webhook_lock(cfg, || {
        let queue_path = cfg.runtime_dir.join("webhook_updates.jsonl");
        if !queue_path.exists() {
            return Ok(AckStatus::Empty);
        }

        let payload = fs::read_to_string(&queue_path)
            .with_context(|| format!("failed to read {}", queue_path.display()))?;
        let mut lines: Vec<&str> = payload
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        if lines.is_empty() {
            return Ok(AckStatus::Empty);
        }

        let head = lines.remove(0);
        if let Some(expected) = expected_update_id {
            match extract_update_id_from_json(head) {
                Ok(head_update_id) => {
                    if head_update_id.as_deref() != Some(expected) {
                        return Ok(AckStatus::HeadMismatch);
                    }
                }
                Err(err) => {
                    // If the queue head is malformed, drop it so queue draining can recover.
                    tracing::warn!("dropping malformed webhook queue head during ack: {err:#}");
                }
            }
        }

        let mut rewritten = lines.join("\n");
        if !rewritten.is_empty() {
            rewritten.push('\n');
        }
        fs::write(&queue_path, rewritten)
            .with_context(|| format!("failed to write {}", queue_path.display()))?;
        Ok(AckStatus::Acked)
    })
}

pub(crate) fn with_webhook_lock<T, F>(cfg: &RuntimeConfig, op: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let lock_path = cfg.runtime_dir.join("webhook_queue.lock");
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("failed to open {}", lock_path.display()))?;
    lock_file
        .lock_exclusive()
        .with_context(|| format!("failed to lock {}", lock_path.display()))?;

    let output = op();
    let _ = lock_file.unlock();
    output
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
