use anyhow::{Context, Result, bail};
use coconutclaw_config::{AgentProvider, RuntimeConfig};
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct ProviderOutput {
    pub raw_output: String,
    pub success: bool,
    pub exit_code: i32,
}

pub fn run_provider(config: &RuntimeConfig, context: &str) -> Result<ProviderOutput> {
    match config.provider {
        AgentProvider::Codex => run_codex(config, context),
        AgentProvider::Pi => run_pi(config, context),
    }
}

fn run_codex(config: &RuntimeConfig, context: &str) -> Result<ProviderOutput> {
    let out_file = config
        .tmp_dir
        .join(format!("codex_last_{}.txt", now_nanos()));

    let mut cmd = Command::new(&config.codex.bin);
    cmd.arg("exec")
        .arg("--cd")
        .arg(&config.instance_dir)
        .arg("--skip-git-repo-check")
        .arg("--output-last-message")
        .arg(&out_file);

    match config.exec_policy.as_str() {
        "yolo" => {
            cmd.arg("--dangerously-bypass-approvals-and-sandbox");
        }
        "allowlist" => {
            cmd.arg("--full-auto");
        }
        "strict" => {}
        other => bail!("invalid EXEC_POLICY: {other}"),
    }

    if let Some(model) = &config.codex.model {
        cmd.arg("--model").arg(model);
    }
    if let Some(reasoning_effort) = &config.codex.reasoning_effort {
        cmd.arg("-c")
            .arg(format!("model_reasoning_effort=\"{reasoning_effort}\""));
    }

    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to start {}", config.codex.bin))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(context.as_bytes())
            .context("failed to write context to codex stdin")?;
    }

    let output = child
        .wait_with_output()
        .context("failed waiting for codex command")?;
    let exit_code = output.status.code().unwrap_or(1);

    let stdout_text = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr_text = String::from_utf8_lossy(&output.stderr).to_string();
    let final_message = fs::read_to_string(&out_file).unwrap_or_default();
    let _ = fs::remove_file(&out_file);

    let raw_output = if output.status.success() && !final_message.trim().is_empty() {
        final_message
    } else if !stdout_text.trim().is_empty() {
        stdout_text
    } else {
        stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: output.status.success(),
        exit_code,
    })
}

fn run_pi(config: &RuntimeConfig, context: &str) -> Result<ProviderOutput> {
    let mut cmd = Command::new(&config.pi.bin);
    cmd.arg("-p").arg("--mode").arg(&config.pi.mode);

    if let Some(provider) = &config.pi.provider {
        cmd.arg("--provider").arg(provider);
    }
    if let Some(model) = &config.pi.model {
        cmd.arg("--model").arg(model);
    }
    if let Some(extra) = &config.pi.extra_args {
        let parts = shlex::split(extra).ok_or_else(|| anyhow::anyhow!("invalid PI_EXTRA_ARGS"))?;
        cmd.args(parts);
    }

    cmd.arg(context)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = cmd
        .output()
        .with_context(|| format!("failed to start {}", config.pi.bin))?;
    let exit_code = output.status.code().unwrap_or(1);

    let stdout_text = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr_text = String::from_utf8_lossy(&output.stderr).to_string();

    let raw_output = if output.status.success() && config.pi.mode == "json" {
        extract_pi_json_final(&stdout_text).unwrap_or_else(|| {
            if !stdout_text.trim().is_empty() {
                stdout_text.clone()
            } else {
                stderr_text.clone()
            }
        })
    } else if !stdout_text.trim().is_empty() {
        stdout_text
    } else {
        stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: output.status.success(),
        exit_code,
    })
}

fn extract_pi_json_final(payload: &str) -> Option<String> {
    let mut final_text: Option<String> = None;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };

        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        let candidate = match event_type {
            "message_end" => {
                let role = value
                    .get("message")
                    .and_then(|node| node.get("role"))
                    .and_then(Value::as_str);
                if role == Some("assistant") {
                    value
                        .get("message")
                        .and_then(|node| node.get("content"))
                        .and_then(join_text_blocks)
                } else {
                    None
                }
            }
            "message_update" => {
                let update_type = value
                    .get("assistantMessageEvent")
                    .and_then(|node| node.get("type"))
                    .and_then(Value::as_str);
                if update_type == Some("done") {
                    value
                        .get("assistantMessageEvent")
                        .and_then(|node| node.get("message"))
                        .and_then(|node| node.get("content"))
                        .and_then(join_text_blocks)
                } else {
                    None
                }
            }
            "agent_end" => value
                .get("messages")
                .and_then(Value::as_array)
                .and_then(|messages| {
                    let mut chunks = Vec::new();
                    for message in messages {
                        let role = message.get("role").and_then(Value::as_str);
                        if role != Some("assistant") {
                            continue;
                        }
                        if let Some(content) = message.get("content").and_then(join_text_blocks) {
                            chunks.push(content);
                        }
                    }
                    if chunks.is_empty() {
                        None
                    } else {
                        Some(chunks.join("\n"))
                    }
                }),
            _ => None,
        };

        if let Some(text) = candidate {
            if !text.trim().is_empty() {
                final_text = Some(text);
            }
        }
    }

    final_text
}

fn join_text_blocks(node: &Value) -> Option<String> {
    let array = node.as_array()?;
    let mut chunks = Vec::new();
    for item in array {
        let item_type = item.get("type").and_then(Value::as_str);
        if item_type == Some("text") {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                chunks.push(text.to_string());
            }
        }
    }

    if chunks.is_empty() {
        None
    } else {
        Some(chunks.join("\n"))
    }
}

fn now_nanos() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    }
}
