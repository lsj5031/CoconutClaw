mod support;

use axum::{Json, Router, body::Bytes, response::IntoResponse, routing::post};
use serde_json::json;
use std::fs;
use std::process::{Command, Stdio};
use std::time::Duration;
use support::write_fake_provider_script;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[allow(clippy::zombie_processes)]
#[tokio::test]
async fn test_webhook_smoke() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let instance_dir = tmp_dir.path().join("test_instance");
    fs::create_dir_all(&instance_dir).unwrap();

    // config.toml
    let config_path = instance_dir.join("config.toml");
    fs::write(
        &config_path,
        r#"
TELEGRAM_BOT_TOKEN = "fake_token"
TELEGRAM_CHAT_ID = "123456"
WEBHOOK_MODE = "on"
WEBHOOK_BIND = "127.0.0.1:0"
WEBHOOK_PUBLIC_URL = "https://example.com"
"#,
    )
    .unwrap();

    // Fake provider
    let provider_path = write_fake_provider_script(
        tmp_dir.path(),
        "fake_provider",
        r#"#!/bin/bash
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --output-last-message) OUT_FILE="$2"; shift ;;
    esac
    shift
done

cat << 'EOF' > "$OUT_FILE"
<message>
<telegram_reply>Hello from fake provider</telegram_reply>
</message>
EOF
"#
        .to_string(),
        r#"@echo off
setlocal EnableDelayedExpansion

:parse
if "%~1"=="" goto after_args
shift
if /I "%~1"=="--output-last-message" goto set_out
goto parse

:set_out
set "OUT_FILE=%~2"
shift
shift
goto parse

:after_args
> "%OUT_FILE%" (
    echo ^<message^>
    echo ^<telegram_reply^>Hello from fake provider^</telegram_reply^>
    echo ^</message^>
)
"#
        .to_string(),
    );

    // Setup local fake Telegram server
    let (tx, mut rx) = mpsc::channel(100);

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
                move |body: Bytes| async move {
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

    // Spawn CLI
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

    // Wait for setWebhook
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

    // Feed a fake update
    let runtime_dir = instance_dir.join("runtime");
    fs::create_dir_all(&runtime_dir).unwrap();
    let updates_file = runtime_dir.join("webhook_updates.jsonl");
    fs::write(
        &updates_file,
        r#"{"update_id":1,"message":{"message_id":1,"chat":{"id":123456},"text":"hello"}}
"#,
    )
    .unwrap();

    // Wait for progress message then final reply
    let mut progress_sent = false;
    let mut final_sent = false;
    for _ in 0..50 {
        if let Ok(Some((endpoint, body))) =
            tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
            && (endpoint == "sendMessage" || endpoint == "editMessageText")
        {
            let text = String::from_utf8_lossy(&body);
            if text.contains("Thinking...") {
                progress_sent = true;
            } else if text.contains("Hello from fake provider")
                || text.contains("Hello+from+fake+provider")
            {
                final_sent = true;
            }
        }
        if progress_sent && final_sent {
            break;
        }
    }

    if !progress_sent || !final_sent {
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        panic!(
            "Missing messages. Progress: {}, Final: {}.\nSTDOUT: {}\nSTDERR: {}",
            progress_sent,
            final_sent,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    child.kill().unwrap();
    let _ = child.wait();
}
