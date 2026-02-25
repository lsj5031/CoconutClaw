use anyhow::{Context, Result, bail};
use coconutclaw_config::{AgentProvider, RuntimeConfig};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc::Sender;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct ProviderOutput {
    pub raw_output: String,
    pub success: bool,
    pub exit_code: i32,
}

pub fn run_provider(
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
) -> Result<ProviderOutput> {
    match config.provider {
        AgentProvider::Codex => run_codex(config, context, cancel_flag, progress_tx),
        AgentProvider::Pi => run_pi(config, context, cancel_flag, progress_tx),
    }
}

fn run_codex(
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
) -> Result<ProviderOutput> {
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
    if progress_tx.is_some() {
        cmd.arg("--json");
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

    let progress_sender = progress_tx.cloned();
    let stdout_handle = child.stdout.take().map(|stdout| {
        thread::spawn(move || -> String {
            let mut collected = String::new();
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let Ok(line) = line else {
                    break;
                };
                if !line.is_empty() {
                    collected.push_str(&line);
                    collected.push('\n');
                }
                if let Some(status) = parse_codex_progress_line(&line)
                    && let Some(sender) = progress_sender.as_ref()
                {
                    let _ = sender.send(status);
                }
            }
            collected
        })
    });
    let stderr_handle = child.stderr.take().map(|mut stderr| {
        thread::spawn(move || -> String {
            let mut stderr_buf = Vec::new();
            let _ = stderr.read_to_end(&mut stderr_buf);
            String::from_utf8_lossy(&stderr_buf).to_string()
        })
    });

    let mut cancelled = false;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .context("failed waiting for codex command")?
        {
            break status;
        }
        if let Some(cancel_flag) = cancel_flag
            && cancel_flag.load(Ordering::SeqCst)
        {
            cancelled = true;
            let _ = child.kill();
            let status = child.wait().context("failed waiting after codex kill")?;
            break status;
        }
        thread::sleep(Duration::from_millis(200));
    };

    let stdout_text = stdout_handle
        .map(|handle| handle.join().unwrap_or_default())
        .unwrap_or_default();
    let stderr_text = stderr_handle
        .map(|handle| handle.join().unwrap_or_default())
        .unwrap_or_default();

    let exit_code = if cancelled {
        130
    } else {
        status.code().unwrap_or(1)
    };

    let final_message = fs::read_to_string(&out_file).unwrap_or_default();
    let _ = fs::remove_file(&out_file);

    let raw_output = if cancelled {
        "cancelled".to_string()
    } else if status.success() && !final_message.trim().is_empty() {
        final_message
    } else if !stdout_text.trim().is_empty() {
        stdout_text
    } else {
        stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: !cancelled && status.success(),
        exit_code,
    })
}

fn run_pi(
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    _progress_tx: Option<&Sender<String>>,
) -> Result<ProviderOutput> {
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

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.pi.bin))?;

    let mut cancelled = false;
    let status = loop {
        if let Some(status) = child.try_wait().context("failed waiting for pi command")? {
            break status;
        }
        if let Some(cancel_flag) = cancel_flag
            && cancel_flag.load(Ordering::SeqCst)
        {
            cancelled = true;
            let _ = child.kill();
            let status = child.wait().context("failed waiting after pi kill")?;
            break status;
        }
        thread::sleep(Duration::from_millis(200));
    };

    let mut stdout_buf = Vec::new();
    if let Some(mut stdout) = child.stdout.take() {
        let _ = stdout.read_to_end(&mut stdout_buf);
    }
    let mut stderr_buf = Vec::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_end(&mut stderr_buf);
    }

    let exit_code = if cancelled {
        130
    } else {
        status.code().unwrap_or(1)
    };

    let stdout_text = String::from_utf8_lossy(&stdout_buf).to_string();
    let stderr_text = String::from_utf8_lossy(&stderr_buf).to_string();

    let raw_output = if cancelled {
        "cancelled".to_string()
    } else if status.success() && config.pi.mode == "json" {
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
        success: !cancelled && status.success(),
        exit_code,
    })
}

fn parse_codex_progress_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;

    match event_type {
        "turn.started" => Some("Processing...".to_string()),
        "item.started" => parse_codex_item_progress(value.get("item")?, true),
        "item.completed" => parse_codex_item_progress(value.get("item")?, false),
        _ => None,
    }
}

fn parse_codex_item_progress(item: &Value, started: bool) -> Option<String> {
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
    match item_type {
        "command_execution" => {
            let command = item
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let command = shorten_status_text(command.trim(), 120);
            if started {
                if command.is_empty() {
                    Some("Running command...".to_string())
                } else {
                    Some(format!("Running: {command}"))
                }
            } else if command.is_empty() {
                Some("Command completed.".to_string())
            } else {
                let exit_code = item.get("exit_code").and_then(Value::as_i64).unwrap_or(0);
                if exit_code == 0 {
                    Some(format!("Completed: {command}"))
                } else {
                    Some(format!("Failed ({exit_code}): {command}"))
                }
            }
        }
        "reasoning" => {
            if started {
                Some("Reasoning...".to_string())
            } else {
                let text = item
                    .get("text")
                    .and_then(Value::as_str)
                    .map(clean_progress_text)
                    .unwrap_or_default();
                if text.is_empty() {
                    Some("Reasoning...".to_string())
                } else {
                    Some(format!("Reasoning: {text}"))
                }
            }
        }
        "file_change" if !started => {
            let file_path = item
                .get("file_path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            if file_path.is_empty() {
                Some("Edited files.".to_string())
            } else {
                Some(format!("Edited: {file_path}"))
            }
        }
        "agent_message" if !started => Some("Drafting response...".to_string()),
        _ => None,
    }
}

fn clean_progress_text(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    let trimmed = trimmed.trim_matches('*').trim_matches('`').trim();
    shorten_status_text(trimmed, 120)
}

fn shorten_status_text(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out: String = chars[..keep].iter().collect();
    out.push_str("...");
    out
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

        if let Some(text) = candidate
            && !text.trim().is_empty()
        {
            final_text = Some(text);
        }
    }

    final_text
}

fn join_text_blocks(node: &Value) -> Option<String> {
    let array = node.as_array()?;
    let mut chunks = Vec::new();
    for item in array {
        let item_type = item.get("type").and_then(Value::as_str);
        if item_type == Some("text")
            && let Some(text) = item.get("text").and_then(Value::as_str)
        {
            chunks.push(text.to_string());
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

#[cfg(test)]
mod tests {
    use super::parse_codex_progress_line;

    #[test]
    fn parses_codex_turn_started_progress() {
        let line = r#"{"type":"turn.started"}"#;
        assert_eq!(
            parse_codex_progress_line(line),
            Some("Processing...".to_string())
        );
    }

    #[test]
    fn parses_codex_command_started_progress() {
        let line =
            r#"{"type":"item.started","item":{"type":"command_execution","command":"git status"}}"#;
        assert_eq!(
            parse_codex_progress_line(line),
            Some("Running: git status".to_string())
        );
    }

    #[test]
    fn parses_codex_file_change_progress() {
        let line =
            r#"{"type":"item.completed","item":{"type":"file_change","file_path":"src/main.rs"}}"#;
        assert_eq!(
            parse_codex_progress_line(line),
            Some("Edited: src/main.rs".to_string())
        );
    }

    #[test]
    fn parses_codex_reasoning_completed_progress() {
        let line = r#"{"type":"item.completed","item":{"type":"reasoning","text":"**Executing shell command**"}}"#;
        assert_eq!(
            parse_codex_progress_line(line),
            Some("Reasoning: Executing shell command".to_string())
        );
    }

    #[test]
    fn parses_codex_command_completed_progress() {
        let line = r#"{"type":"item.completed","item":{"type":"command_execution","command":"\"C:\\WINDOWS\\System32\\WindowsPowerShell\\v1.0\\powershell.exe\" -Command pwd","exit_code":0}}"#;
        assert_eq!(
            parse_codex_progress_line(line),
            Some(
                r#"Completed: "C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe" -Command pwd"#
                    .to_string()
                )
        );
    }

    #[test]
    fn parses_codex_agent_message_progress() {
        let line = r#"{"type":"item.completed","item":{"type":"agent_message","text":"TELEGRAM_REPLY: done"}}"#;
        assert_eq!(
            parse_codex_progress_line(line),
            Some("Drafting response...".to_string())
        );
    }
}
