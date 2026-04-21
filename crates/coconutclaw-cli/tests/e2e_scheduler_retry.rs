mod support;

use axum::{Json, Router, body::Bytes, response::IntoResponse, routing::post};
use serde_json::json;
use std::fs;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use support::write_fake_provider_script;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[allow(clippy::zombie_processes)]
#[tokio::test]
async fn test_scheduler_retry() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let instance_dir = tmp_dir.path().join("test_instance");
    fs::create_dir_all(&instance_dir).unwrap();

    let config_path = instance_dir.join("config.toml");
    fs::write(
        &config_path,
        r#"
TELEGRAM_BOT_TOKEN = "fake_token"
TELEGRAM_CHAT_ID = "123456"
WEBHOOK_MODE = "on"
WEBHOOK_BIND = "127.0.0.1:0"
WEBHOOK_PUBLIC_URL = "https://example.com"
SCHEDULED_TASKS_ENABLED = true
"#,
    )
    .unwrap();

    let state_path = tmp_dir.path().join("provider_state");
    let invocations_path = tmp_dir.path().join("invocations");
    let provider_path = write_fake_provider_script(
        tmp_dir.path(),
        "fake_provider",
        format!(
            r#"#!/bin/bash
echo "1" >> "{invocations}"
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --output-last-message) OUT_FILE="$2"; shift ;;
    esac
    shift
done

if [ ! -f "{state}" ]; then
    touch "{state}"
    TIME=$(date -u +%H:%M)
    cat << EOF > "$OUT_FILE"
TELEGRAM_REPLY: Scheduled!
SCHEDULE_PROMPT: once $TIME|Check backups
EOF
else
    cat << EOF > "$OUT_FILE"
TELEGRAM_REPLY: Backup complete
EOF
fi
"#,
            invocations = invocations_path.display(),
            state = state_path.display()
        ),
        format!(
            r#"@echo off
echo 1>> "{invocations}"
if not exist "{state}" (
    type nul > "{state}"
    for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToUniversalTime().ToString('HH:mm')"') do set "SCHEDULE_TIME=%%i"
    echo TELEGRAM_REPLY: Scheduled!
    echo SCHEDULE_PROMPT: once %%SCHEDULE_TIME%%^|Check backups
) else (
    echo TELEGRAM_REPLY: Backup complete
)
"#,
            invocations = invocations_path.display(),
            state = state_path.display()
        ),
    );

    let (tx, mut rx) = mpsc::channel(100);
    let fail_first_backup = Arc::new(AtomicBool::new(true));

    let app = Router::new()
        .route(
            "/botfake_token/setWebhook",
            post({
                let tx = tx.clone();
                move |body: Bytes| async move {
                    tx.send(("setWebhook".to_string(), body.to_vec()))
                        .await
                        .unwrap();
                    Json(json!({"ok": true, "result": true})).into_response()
                }
            }),
        )
        .route(
            "/botfake_token/setMyCommands",
            post({
                let tx = tx.clone();
                move |body: Bytes| async move {
                    tx.send(("setMyCommands".to_string(), body.to_vec()))
                        .await
                        .unwrap();
                    Json(json!({"ok": true, "result": true})).into_response()
                }
            }),
        )
        .route(
            "/botfake_token/sendMessage",
            post({
                let tx = tx.clone();
                let fail_first_backup = fail_first_backup.clone();
                move |body: Bytes| async move {
                    let text = String::from_utf8_lossy(&body);
                    if (text.contains("Backup complete") || text.contains("Backup+complete"))
                        && fail_first_backup.swap(false, Ordering::SeqCst)
                    {
                        tx.send(("sendMessage_fail".to_string(), body.to_vec()))
                            .await
                            .unwrap();
                        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "error")
                            .into_response();
                    }
                    tx.send(("sendMessage".to_string(), body.to_vec()))
                        .await
                        .unwrap();
                    Json(json!({"ok": true, "result": {"message_id": 42}})).into_response()
                }
            }),
        )
        .route(
            "/botfake_token/editMessageText",
            post({
                let tx = tx.clone();
                move |body: Bytes| async move {
                    tx.send(("editMessageText".to_string(), body.to_vec()))
                        .await
                        .unwrap();
                    Json(json!({"ok": true, "result": true})).into_response()
                }
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let base_url = format!("http://127.0.0.1:{}", port);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_coconutclaw"))
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("run")
        .env("TELEGRAM_API_BASE", &base_url)
        .env("TELEGRAM_FILE_BASE", &base_url)
        .env("AGENT_PROVIDER", "codex")
        .env("CODEX_BIN", &provider_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut webhook_set = false;
    let mut my_commands_set = false;
    for _ in 0..50 {
        if let Ok(Some((endpoint, _))) =
            tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
        {
            if endpoint == "setWebhook" {
                webhook_set = true;
            }
            if endpoint == "setMyCommands" {
                my_commands_set = true;
            }
        }
        if webhook_set && my_commands_set {
            break;
        }
    }
    if !(webhook_set && my_commands_set) {
        let output = child.wait_with_output().unwrap();
        panic!(
            "Expected setWebhook to be called. \nSTDOUT: {}\nSTDERR: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let runtime_dir = instance_dir.join("runtime");
    fs::create_dir_all(&runtime_dir).unwrap();
    let updates_file = runtime_dir.join("webhook_updates.jsonl");
    fs::write(
        &updates_file,
        r#"{"update_id":1,"message":{"message_id":1,"chat":{"id":123456},"text":"hello"}}
"#,
    )
    .unwrap();

    let mut initial_reply_sent = false;
    let mut scheduler_reply_failed = false;
    let mut scheduler_reply_sent = false;

    for _ in 0..150 {
        if let Ok(Some((endpoint, body))) =
            tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
        {
            if endpoint == "sendMessage" || endpoint == "editMessageText" {
                let text = String::from_utf8_lossy(&body);
                if text.contains("Scheduled") {
                    initial_reply_sent = true;
                } else if text.contains("Backup complete") || text.contains("Backup+complete") {
                    scheduler_reply_sent = true;
                }
            } else if endpoint == "sendMessage_fail" {
                scheduler_reply_failed = true;
            }
        }
        if initial_reply_sent && scheduler_reply_failed && scheduler_reply_sent {
            break;
        }
    }

    if !initial_reply_sent || !scheduler_reply_failed || !scheduler_reply_sent {
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        panic!(
            "Missing messages. Initial: {}, Failed: {}, Sent: {}.\nSTDOUT: {}\nSTDERR: {}",
            initial_reply_sent,
            scheduler_reply_failed,
            scheduler_reply_sent,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let invocations = fs::read_to_string(&invocations_path).unwrap_or_default();
    let lines: Vec<&str> = invocations.lines().collect();

    if lines.len() != 2 {
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        panic!(
            "Provider should be called exactly twice, but was called {} times.\nSTDOUT: {}\nSTDERR: {}",
            lines.len(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    child.kill().unwrap();
    let _ = child.wait();
}
