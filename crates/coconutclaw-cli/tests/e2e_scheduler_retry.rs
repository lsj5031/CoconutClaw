mod support;

use axum::{
    Json, Router,
    body::Bytes,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::json;
use std::fs;
use std::io::{BufRead, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use support::{wait_for_http_ready, write_fake_provider_script};
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
            r#"
Add-Content -Path "{invocations}" -Value '1'
if (-not (Test-Path "{state}")) {{
    New-Item -ItemType File -Path "{state}" | Out-Null
    $scheduleTime = [DateTime]::UtcNow.ToString('HH:mm')
    Write-Output 'TELEGRAM_REPLY: Scheduled!'
    Write-Output "SCHEDULE_PROMPT: once $scheduleTime|Check backups"
}} else {{
    Write-Output 'TELEGRAM_REPLY: Backup complete'
}}
"#,
            invocations = invocations_path.display(),
            state = state_path.display()
        ),
    );

    let (tx, mut rx) = mpsc::channel(100);
    let fail_first_backup = Arc::new(AtomicBool::new(true));

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
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
                let fail_first_backup = fail_first_backup.clone();
                move |body: Bytes| async move {
                    let text = String::from_utf8_lossy(&body);
                    if (text.contains("Backup complete") || text.contains("Backup+complete"))
                        && fail_first_backup.swap(false, Ordering::SeqCst)
                    {
                        tx.send(("editMessageText_fail".to_string(), body.to_vec()))
                            .await
                            .unwrap();
                        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "error")
                            .into_response();
                    }
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
    wait_for_http_ready(&base_url).await;

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

    // Spawn a background thread to scan stderr for the webhook listening address.
    let stderr = child.stderr.take().unwrap();
    let (url_tx, url_rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stderr);
        let mut sent_url = false;
        for line in reader.lines().map_while(Result::ok) {
            let marker = "webhook server listening on ";
            if !sent_url && let Some(pos) = line.find(marker) {
                let rest = &line[pos + marker.len()..];
                let _ = url_tx.send(format!("http://{rest}"));
                sent_url = true;
            }
        }
    });

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

    // Get the webhook URL from the stderr reader thread.
    let webhook_url = url_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("could not find webhook listening address in stderr");

    // Parse host:port from the URL for raw TCP connection.
    let host_port = webhook_url
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap()
        .to_string();
    let webhook_path = webhook_url
        .trim_start_matches("http://")
        .find('/')
        .map(|i| &webhook_url.trim_start_matches("http://")[i..])
        .unwrap_or("/");

    // POST webhook update via raw TCP (replaces dead webhook_updates.jsonl file write).
    let body = r#"{"update_id":1,"message":{"message_id":1,"chat":{"id":123456},"text":"hello"}}"#;
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        webhook_path,
        host_port,
        body.len(),
        body
    );

    let host_port_clone = host_port.clone();
    tokio::task::spawn_blocking(move || {
        // Retry connecting for up to 10 seconds
        let mut stream = None;
        for _ in 0..100 {
            match TcpStream::connect(&host_port_clone) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
        let mut stream = stream.expect("could not connect to webhook server");
        stream
            .write_all(request.as_bytes())
            .expect("write webhook request");
        let mut buf = [0u8; 512];
        let _ = stream.read(&mut buf);
    })
    .await
    .unwrap();

    let mut initial_reply_sent = false;
    let mut scheduler_reply_failed = false;
    let mut scheduler_reply_sent = false;

    for _ in 0..300 {
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
            } else if endpoint == "editMessageText_fail" {
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
