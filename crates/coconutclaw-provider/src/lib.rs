use anyhow::{Context, Result};
use coconutclaw_config::{AgentProvider, RuntimeConfig};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::Sender;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct ProviderOutput {
    pub raw_output: String,
    pub success: bool,
    pub exit_code: i32,
}

pub fn run_provider(
    attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    match config.provider {
        AgentProvider::Codex => run_codex(
            attachment_path,
            config,
            context,
            cancel_flag,
            progress_tx,
            timeout_secs,
        ),
        AgentProvider::Pi => run_pi(
            attachment_path,
            config,
            context,
            cancel_flag,
            progress_tx,
            timeout_secs,
        ),
        AgentProvider::Claude => run_claude(
            attachment_path,
            config,
            context,
            cancel_flag,
            progress_tx,
            timeout_secs,
        ),
        AgentProvider::OpenCode => run_opencode(
            attachment_path,
            config,
            context,
            cancel_flag,
            progress_tx,
            timeout_secs,
        ),
        AgentProvider::Gemini => run_gemini(
            attachment_path,
            config,
            context,
            cancel_flag,
            progress_tx,
            timeout_secs,
        ),
        AgentProvider::Factory => run_factory(
            attachment_path,
            config,
            context,
            cancel_flag,
            progress_tx,
            timeout_secs,
        ),
    }
}

fn run_codex(
    _attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let out_file = config
        .tmp_dir
        .join(format!("codex_last_{}.txt", now_nanos()));

    let run_once = |include_dangerous_flag: bool| -> Result<RunResult> {
        let mut cmd = Command::new(&config.codex.bin);
        cmd.arg("exec")
            .arg("--cd")
            .arg(&config.instance_dir)
            .arg("--skip-git-repo-check")
            .arg("--output-last-message")
            .arg(&out_file);

        if let Some(model) = &config.codex.model {
            cmd.arg("--model").arg(model);
        }
        if let Some(effort) = &config.codex.reasoning_effort {
            cmd.arg("--reasoning-effort").arg(effort);
        }
        if include_dangerous_flag {
            cmd.arg("--dangerously-bypass-approvals-and-sandbox");
        }
        if progress_tx.is_some() {
            cmd.arg("--json");
        }

        cmd.arg(context)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_child_command(&mut cmd);

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to start {}", config.codex.bin))?;
        run_child_process(
            child,
            cancel_flag,
            progress_tx.cloned(),
            Some(parse_codex_progress_line),
            "failed waiting for codex command".to_string(),
            "failed waiting after codex kill".to_string(),
            timeout_secs,
        )
    };

    let yolo_mode = config.exec_policy.eq_ignore_ascii_case("yolo");
    let mut run_result = run_once(yolo_mode)?;
    if yolo_mode
        && !run_result.status.success()
        && !run_result.cancelled
        && !run_result.timed_out
        && should_retry_without_dangerous_flag(&run_result.stdout_text, &run_result.stderr_text)
    {
        tracing::warn!(
            "codex CLI rejected dangerous permission flag; retrying without it for compatibility"
        );
        run_result = run_once(false)?;
    }

    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let raw_output = if run_result.status.success() {
        fs::read_to_string(&out_file).unwrap_or_else(|_| run_result.stdout_text.clone())
    } else if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else {
        run_result.stderr_text
    };

    let _ = fs::remove_file(out_file);

    Ok(ProviderOutput {
        raw_output,
        success: !run_result.cancelled && run_result.status.success(),
        exit_code,
    })
}

fn run_pi(
    attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    // Use JSON mode when progress updates are needed, otherwise text mode
    let pi_mode = if progress_tx.is_some() {
        "json"
    } else {
        "text"
    };

    let mut cmd = Command::new(&config.pi.bin);
    cmd.arg("-p").arg("--mode").arg(pi_mode);

    if let Some(model) = &config.pi.model {
        cmd.arg("--model").arg(model);
    }
    if let Some(effort) = &config.pi.reasoning_effort {
        cmd.arg("--reasoning-effort").arg(effort);
    }

    // Disable tools/extensions/skills for llama.cpp local compatibility.
    if config.pi.no_extensions {
        cmd.arg("--no-tools")
            .arg("--no-extensions")
            .arg("--no-skills");
    }

    if let Some(path) = attachment_path {
        cmd.arg(format!("@{}", path.display()));
    }

    cmd.arg(context)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.pi.bin))?;
    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx.cloned(),
        Some(parse_pi_progress_line),
        "failed waiting for pi command".to_string(),
        "failed waiting after pi kill".to_string(),
        timeout_secs,
    )?;

    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let raw_output = if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else if run_result.status.success() && pi_mode == "json" {
        extract_pi_json_final(&run_result.stdout_text).unwrap_or_else(|| {
            if !run_result.stdout_text.trim().is_empty() {
                run_result.stdout_text.clone()
            } else {
                run_result.stderr_text.clone()
            }
        })
    } else if !run_result.stdout_text.trim().is_empty() {
        run_result.stdout_text
    } else {
        run_result.stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: !run_result.cancelled && run_result.status.success(),
        exit_code,
    })
}

fn run_claude(
    _attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let run_once = |include_dangerous_flag: bool| -> Result<RunResult> {
        let (env_vars, bin_path) = parse_bin_with_env(&config.claude.bin);
        let mut cmd = Command::new(bin_path);
        for (key, value) in env_vars {
            cmd.env(&key, &value);
        }
        cmd.arg("-p");

        if include_dangerous_flag {
            cmd.arg("--dangerously-bypass-approvals-and-sandbox");
        }
        if let Some(model) = &config.claude.model {
            cmd.arg("--model").arg(model);
        }

        cmd.arg("--output-format")
            .arg("stream-json")
            .arg(context)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_child_command(&mut cmd);

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to start {}", config.claude.bin))?;
        run_child_process(
            child,
            cancel_flag,
            progress_tx.cloned(),
            Some(parse_claude_json_line),
            "failed waiting for claude command".to_string(),
            "failed waiting after claude kill".to_string(),
            timeout_secs,
        )
    };

    let yolo_mode = config.exec_policy.eq_ignore_ascii_case("yolo");
    let mut run_result = run_once(yolo_mode)?;
    if yolo_mode
        && !run_result.status.success()
        && !run_result.cancelled
        && !run_result.timed_out
        && should_retry_without_dangerous_flag(&run_result.stdout_text, &run_result.stderr_text)
    {
        tracing::warn!(
            "claude CLI rejected dangerous permission flag; retrying without it for compatibility"
        );
        run_result = run_once(false)?;
    }

    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let raw_output = if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else if run_result.status.success() {
        extract_claude_json_final(&run_result.stdout_text).unwrap_or_else(|| {
            if !run_result.stdout_text.trim().is_empty() {
                run_result.stdout_text.clone()
            } else {
                run_result.stderr_text.clone()
            }
        })
    } else if !run_result.stdout_text.trim().is_empty() {
        run_result.stdout_text
    } else {
        run_result.stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: !run_result.cancelled && run_result.status.success(),
        exit_code,
    })
}

fn run_opencode(
    _attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let (env_vars, bin_path) = parse_bin_with_env(&config.opencode.bin);
    let mut cmd = Command::new(bin_path);
    for (key, value) in env_vars {
        cmd.env(&key, &value);
    }

    if let Some(model) = &config.opencode.model {
        cmd.arg("--model").arg(model);
    }

    cmd.arg("--output-format")
        .arg("stream-json")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.opencode.bin))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(context.as_bytes())
            .context("failed to write context to opencode stdin")?;
    }

    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx.cloned(),
        Some(parse_opencode_json_line),
        "failed waiting for opencode command".to_string(),
        "failed waiting after opencode kill".to_string(),
        timeout_secs,
    )?;

    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let raw_output = if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else if run_result.status.success() {
        extract_opencode_json_final(&run_result.stdout_text).unwrap_or_else(|| {
            if !run_result.stdout_text.trim().is_empty() {
                run_result.stdout_text.clone()
            } else {
                run_result.stderr_text.clone()
            }
        })
    } else if !run_result.stdout_text.trim().is_empty() {
        run_result.stdout_text
    } else {
        run_result.stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: !run_result.cancelled && run_result.status.success(),
        exit_code,
    })
}

fn run_gemini(
    _attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let (env_vars, bin_path) = parse_bin_with_env(&config.gemini.bin);
    let mut cmd = Command::new(bin_path);
    for (key, value) in env_vars {
        cmd.env(&key, &value);
    }
    cmd.arg("-p");

    if let Some(model) = &config.gemini.model {
        cmd.arg("--model").arg(model);
    }

    cmd.arg(context)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.gemini.bin))?;
    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx.cloned(),
        None,
        "failed waiting for gemini command".to_string(),
        "failed waiting after gemini kill".to_string(),
        timeout_secs,
    )?;

    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let raw_output = if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else if !run_result.stdout_text.trim().is_empty() {
        run_result.stdout_text
    } else {
        run_result.stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: !run_result.cancelled && run_result.status.success(),
        exit_code,
    })
}

fn run_factory(
    _attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let (env_vars, bin_path) = parse_bin_with_env(&config.factory.bin);
    let mut cmd = Command::new(bin_path);
    for (key, value) in env_vars {
        cmd.env(&key, &value);
    }
    cmd.arg("-p");

    if let Some(model) = &config.factory.model {
        cmd.arg("--model").arg(model);
    }

    cmd.arg(context)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.factory.bin))?;
    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx.cloned(),
        None,
        "failed waiting for factory command".to_string(),
        "failed waiting after factory kill".to_string(),
        timeout_secs,
    )?;

    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let raw_output = if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else if !run_result.stdout_text.trim().is_empty() {
        run_result.stdout_text
    } else {
        run_result.stderr_text
    };

    Ok(ProviderOutput {
        raw_output,
        success: !run_result.cancelled && run_result.status.success(),
        exit_code,
    })
}

struct RunResult {
    status: ExitStatus,
    stdout_text: String,
    stderr_text: String,
    cancelled: bool,
    timed_out: bool,
}

fn run_child_process(
    mut child: Child,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<Sender<String>>,
    progress_parser: Option<fn(&str) -> Option<String>>,
    wait_error: String,
    kill_error: String,
    timeout_secs: Option<u64>,
) -> Result<RunResult> {
    let stdout = child.stdout.take().context("failed to take stdout")?;
    let stderr = child.stderr.take().context("failed to take stderr")?;

    let stdout_handle = thread::spawn(move || {
        let mut text = String::new();
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            let line = line.unwrap_or_default();
            if let Some(tx) = progress_tx.as_ref()
                && let Some(parser) = progress_parser
                && let Some(msg) = parser(&line)
            {
                let _ = tx.send(msg);
            }
            text.push_str(&line);
            text.push('\n');
        }
        text
    });

    let stderr_handle = thread::spawn(move || {
        let mut text = String::new();
        let mut reader = BufReader::new(stderr);
        let _ = reader.read_to_string(&mut text);
        text
    });

    let start = Instant::now();
    let mut cancelled = false;
    let mut timed_out = false;

    let status = loop {
        if let Some(status) = child.try_wait().context(wait_error.clone())? {
            break status;
        }

        if let Some(flag) = cancel_flag
            && flag.load(Ordering::SeqCst)
        {
            cancelled = true;
            break terminate_cancelled_child(&mut child, &wait_error, &kill_error)?;
        }

        if let Some(secs) = timeout_secs
            && start.elapsed() >= Duration::from_secs(secs)
        {
            timed_out = true;
            tracing::warn!("provider process timed out after {secs}s");
            break terminate_cancelled_child(&mut child, &wait_error, &kill_error)?;
        }

        thread::sleep(Duration::from_millis(50));
    };

    let stdout_text = stdout_handle.join().unwrap_or_default();
    let stderr_text = stderr_handle.join().unwrap_or_default();

    Ok(RunResult {
        status,
        stdout_text,
        stderr_text,
        cancelled,
        timed_out,
    })
}

#[cfg(unix)]
fn terminate_cancelled_child(
    child: &mut Child,
    wait_error: &str,
    kill_error: &str,
) -> Result<ExitStatus> {
    let pgid = child.id() as libc::pid_t;
    unsafe {
        libc::kill(-pgid, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if let Some(status) = child.try_wait().context(wait_error.to_string())? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
            return child.wait().context(kill_error.to_string());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

#[cfg(not(unix))]
fn terminate_cancelled_child(
    child: &mut Child,
    wait_error: &str,
    _kill_error: &str,
) -> Result<ExitStatus> {
    child.kill().context("failed to kill cancelled process")?;
    child.wait().context(wait_error.to_string())
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
                    Some(format!("▶ {command}"))
                }
            } else {
                let exit_code = item.get("exit_code").and_then(Value::as_i64).unwrap_or(0);
                if command.is_empty() {
                    Some("Command completed.".to_string())
                } else if exit_code == 0 {
                    Some(format!("✓ {command}"))
                } else {
                    Some(format!("✗ ({exit_code}): {command}"))
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
                    .map(|t| {
                        let collapsed = t.split_whitespace().collect::<Vec<_>>().join(" ");
                        let trimmed = collapsed.trim().trim_matches('*').trim_matches('`').trim();
                        shorten_status_text(trimmed, 120)
                    })
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

fn shorten_status_text(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out: String = chars[..keep].iter().collect();
    out.push_str("...");
    out
}

/// Extract a short human-readable summary from tool args for progress display.
/// Returns an empty string when no meaningful detail can be extracted.
fn tool_arg_summary(tool_name: &str, args: Option<&Value>) -> String {
    const MAX_LEN: usize = 60;
    let args = match args {
        Some(v) => v,
        None => return String::new(),
    };
    // Pick the most informative field per tool
    let raw = match tool_name {
        "bash" | "Bash" => args.get("cmd").or_else(|| args.get("command")),
        "read_file" | "Read" | "read" => args.get("path").or_else(|| args.get("file")),
        "edit_file" | "write_file" | "create_file" => args.get("path").or_else(|| args.get("file")),
        "glob" => args.get("filePattern").or_else(|| args.get("pattern")),
        "Grep" | "grep" => args.get("pattern"),
        "web_search" => args.get("objective").or_else(|| args.get("query")),
        "read_web_page" => args.get("url"),
        _ => {
            // Generic: try common keys
            args.get("path")
                .or_else(|| args.get("cmd"))
                .or_else(|| args.get("query"))
        }
    };
    let text = match raw.and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s,
        _ => return String::new(),
    };
    if text.len() <= MAX_LEN {
        text.to_string()
    } else {
        format!("{}…", &text[..MAX_LEN])
    }
}

fn parse_pi_progress_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let typ = value.get("type")?.as_str()?;
    // Legacy pi format
    if typ == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }
    // pi-rust --mode json tool execution events
    if typ == "tool_execution_start" {
        let name = value
            .get("toolName")
            .and_then(|v| v.as_str())
            .unwrap_or("tool");
        let detail = tool_arg_summary(name, value.get("args"));
        return if detail.is_empty() {
            Some(format!("▶ {name}"))
        } else {
            Some(format!("▶ {name}: {detail}"))
        };
    }
    if typ == "tool_execution_end" {
        let name = value
            .get("toolName")
            .and_then(|v| v.as_str())
            .unwrap_or("tool");
        let err = value
            .get("isError")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        return Some(format!("{} {name}", if err { "✗" } else { "✓" }));
    }
    if typ == "turn_start" {
        let idx = value.get("turnIndex").and_then(|v| v.as_u64()).unwrap_or(0);
        if idx > 0 {
            return Some(format!("turn {}", idx + 1));
        }
        return Some("Processing...".to_string());
    }
    // Streaming assistant message events
    if typ == "message_update" {
        let event = value.get("assistantMessageEvent")?;
        let event_type = event.get("type")?.as_str()?;
        match event_type {
            "thinking_start" => return Some("Reasoning...".to_string()),
            "toolcall_start" => return Some("Preparing tool call...".to_string()),
            "toolcall_end" => {
                let tc = event.get("toolCall")?;
                let name = tc.get("name").and_then(|v| v.as_str()).unwrap_or("tool");
                let args = tc.get("arguments").and_then(|v| v.as_str());
                let parsed_args: Option<Value> = args.and_then(|a| serde_json::from_str(a).ok());
                let detail = tool_arg_summary(name, parsed_args.as_ref());
                return if detail.is_empty() {
                    Some(format!("✓ {name}"))
                } else {
                    Some(format!("✓ {name}: {detail}"))
                };
            }
            _ => {}
        }
    }
    if typ == "auto_compaction_start" {
        return Some("Compacting context...".to_string());
    }
    if typ == "auto_compaction_end" {
        return Some("Context compacted".to_string());
    }
    if typ == "auto_retry_start" {
        let attempt = value.get("attempt").and_then(|v| v.as_u64()).unwrap_or(1);
        return Some(format!("Retrying (attempt {attempt})..."));
    }
    None
}

fn extract_pi_json_final(raw: &str) -> Option<String> {
    let mut final_text = String::new();
    // Try pi-rust format: scan from the end for terminal events
    for line in raw.lines().rev() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let typ = value.get("type").and_then(|v| v.as_str()).unwrap_or("");

        // agent_end carries the full messages array (plural)
        if typ == "agent_end"
            && let Some(messages) = value.get("messages").and_then(|v| v.as_array())
        {
            for msg in messages.iter().rev() {
                if msg.get("role").and_then(|v| v.as_str()) != Some("assistant") {
                    continue;
                }
                if let Some(text) = join_content_text_blocks(msg) {
                    return Some(text);
                }
            }
        }

        // turn_end carries a single message
        if typ == "turn_end"
            && let Some(msg) = value.get("message")
            && let Some(text) = join_content_text_blocks(msg)
        {
            return Some(text);
        }

        // message_end for assistant messages
        if typ == "message_end"
            && let Some(msg) = value.get("message")
            && msg.get("role").and_then(|v| v.as_str()) == Some("assistant")
            && let Some(text) = join_content_text_blocks(msg)
        {
            return Some(text);
        }
    }
    // Fallback: legacy pi format
    for line in raw.lines() {
        if let Ok(value) = serde_json::from_str::<Value>(line)
            && value.get("type").and_then(|v| v.as_str()) == Some("text")
            && let Some(c) = value.get("content").and_then(|v| v.as_str())
        {
            final_text.push_str(c);
        }
    }
    if final_text.is_empty() {
        None
    } else {
        Some(final_text)
    }
}

fn join_content_text_blocks(msg: &Value) -> Option<String> {
    let content = msg.get("content")?;
    if let Some(text) = content.as_str() {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        return Some(text.to_string());
    }
    let array = content.as_array()?;
    let mut chunks = Vec::new();
    for block in array {
        if block.get("type").and_then(|t| t.as_str()) == Some("text")
            && let Some(text) = block.get("text").and_then(|t| t.as_str())
        {
            let text = text.trim();
            if !text.is_empty() {
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

fn parse_claude_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    if value.get("type")?.as_str()? == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }
    None
}

fn extract_claude_json_final(raw: &str) -> Option<String> {
    let mut result_text: Option<String> = None;
    let mut assistant_text: Option<String> = None;
    let mut legacy_text = String::new();

    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        if event_type == "result" {
            if let Some(text) = value
                .get("result")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                result_text = Some(text.to_string());
            }
            continue;
        }

        if event_type == "assistant" {
            if let Some(text) = extract_claude_assistant_text(&value) {
                assistant_text = Some(text);
            }
            continue;
        }

        if event_type == "text"
            && let Some(text) = value.get("content").and_then(Value::as_str)
        {
            legacy_text.push_str(text);
        }
    }

    if let Some(text) = result_text {
        return Some(text);
    }
    if let Some(text) = assistant_text {
        return Some(text);
    }
    if legacy_text.is_empty() {
        return None;
    }
    Some(legacy_text)
}

fn extract_claude_assistant_text(event: &Value) -> Option<String> {
    let message = event.get("message")?;
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return None;
    }
    let content = message.get("content").and_then(Value::as_array)?;
    let mut text = String::new();
    for block in content {
        if block.get("type").and_then(Value::as_str) == Some("text")
            && let Some(chunk) = block.get("text").and_then(Value::as_str)
        {
            text.push_str(chunk);
        }
    }
    if text.trim().is_empty() {
        None
    } else {
        Some(text)
    }
}

fn parse_opencode_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    if value.get("type")?.as_str()? == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }
    None
}

fn extract_opencode_json_final(raw: &str) -> Option<String> {
    let mut final_text = String::new();
    for line in raw.lines() {
        if let Ok(value) = serde_json::from_str::<Value>(line)
            && value.get("type")?.as_str()? == "text"
        {
            final_text.push_str(value.get("content")?.as_str()?);
        }
    }
    if final_text.is_empty() {
        None
    } else {
        Some(final_text)
    }
}

fn parse_bin_with_env(raw: &str) -> (HashMap<String, String>, String) {
    let mut env_vars = HashMap::new();
    let parts: Vec<&str> = raw.split_whitespace().collect();
    let mut bin_index = 0;

    for (i, part) in parts.iter().enumerate() {
        if part.contains('=') {
            let env_parts: Vec<&str> = part.splitn(2, '=').collect();
            if env_parts.len() == 2 {
                env_vars.insert(env_parts[0].to_string(), env_parts[1].to_string());
                bin_index = i + 1;
            }
        } else {
            break;
        }
    }

    let bin_path = if bin_index < parts.len() {
        parts[bin_index..].join(" ")
    } else {
        raw.to_string()
    };

    (env_vars, bin_path)
}

fn should_retry_without_dangerous_flag(stdout: &str, stderr: &str) -> bool {
    let text = format!("{stdout}\n{stderr}").to_ascii_lowercase();
    let mentions_flag = text.contains("--dangerously-skip-permissions")
        || text.contains("--dangerously-bypass-approvals-and-sandbox");
    let argument_error = text.contains("unexpected argument")
        || text.contains("unrecognized option")
        || text.contains("unknown option");
    mentions_flag && argument_error
}

fn now_nanos() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    }
}

fn configure_child_command(_cmd: &mut Command) {
    #[cfg(unix)]
    {
        _cmd.process_group(0);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        extract_claude_json_final, extract_pi_json_final, parse_codex_progress_line,
        parse_pi_progress_line, tool_arg_summary,
    };
    use serde_json::Value;

    #[test]
    fn parse_codex_turn_started() {
        let line = r#"{"type":"turn.started"}"#;
        assert_eq!(
            parse_codex_progress_line(line).as_deref(),
            Some("Processing...")
        );
    }

    #[test]
    fn parse_codex_command_started() {
        let line =
            r#"{"type":"item.started","item":{"type":"command_execution","command":"git status"}}"#;
        assert_eq!(
            parse_codex_progress_line(line).as_deref(),
            Some("▶ git status")
        );
    }

    #[test]
    fn parse_codex_command_completed() {
        let line = r#"{"type":"item.completed","item":{"type":"command_execution","command":"cargo test","exit_code":0}}"#;
        assert_eq!(
            parse_codex_progress_line(line).as_deref(),
            Some("✓ cargo test")
        );
    }

    #[test]
    fn parse_codex_file_change() {
        let line =
            r#"{"type":"item.completed","item":{"type":"file_change","file_path":"src/main.rs"}}"#;
        assert_eq!(
            parse_codex_progress_line(line).as_deref(),
            Some("Edited: src/main.rs")
        );
    }

    #[test]
    fn parse_codex_agent_message() {
        let line = r#"{"type":"item.completed","item":{"type":"agent_message","text":"done"}}"#;
        assert_eq!(
            parse_codex_progress_line(line).as_deref(),
            Some("Drafting response...")
        );
    }

    #[test]
    fn parse_pi_progress_line_ignores_text_deltas() {
        let line = r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"hello"}}"#;
        assert!(parse_pi_progress_line(line).is_none());
    }

    #[test]
    fn parse_pi_progress_line_keeps_legacy_progress_events() {
        let line = r#"{"type":"progress","content":"running task"}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("running task")
        );
    }

    #[test]
    fn parse_pi_progress_line_tool_execution_start() {
        let line =
            r#"{"type":"tool_execution_start","toolCallId":"t1","toolName":"bash","args":{}}"#;
        assert_eq!(parse_pi_progress_line(line).as_deref(), Some("▶ bash"));
    }

    #[test]
    fn parse_pi_progress_line_tool_execution_start_with_args() {
        let line = r#"{"type":"tool_execution_start","toolCallId":"t1","toolName":"bash","args":{"cmd":"cargo test"}}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("▶ bash: cargo test")
        );
    }

    #[test]
    fn parse_pi_progress_line_tool_execution_start_read() {
        let line = r#"{"type":"tool_execution_start","toolCallId":"t2","toolName":"Read","args":{"path":"src/main.rs"}}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("▶ Read: src/main.rs")
        );
    }

    #[test]
    fn tool_arg_summary_truncates_long_args() {
        let args: Value = serde_json::json!({"cmd": "a]".repeat(50)});
        let result = tool_arg_summary("bash", Some(&args));
        assert!(result.len() <= 64); // 60 chars + "…"
        assert!(result.ends_with('…'));
    }

    #[test]
    fn parse_pi_progress_line_tool_execution_end() {
        let line = r#"{"type":"tool_execution_end","toolCallId":"t1","toolName":"bash","result":{},"isError":false}"#;
        assert_eq!(parse_pi_progress_line(line).as_deref(), Some("✓ bash"));
    }

    #[test]
    fn parse_pi_progress_line_tool_execution_end_error() {
        let line = r#"{"type":"tool_execution_end","toolCallId":"t1","toolName":"bash","result":{},"isError":true}"#;
        assert_eq!(parse_pi_progress_line(line).as_deref(), Some("✗ bash"));
    }

    #[test]
    fn parse_pi_progress_line_turn_start_first() {
        let line = r#"{"type":"turn_start","sessionId":"s1","turnIndex":0,"timestamp":0}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("Processing...")
        );
    }

    #[test]
    fn parse_pi_progress_line_turn_start_subsequent() {
        let line = r#"{"type":"turn_start","sessionId":"s1","turnIndex":2,"timestamp":0}"#;
        assert_eq!(parse_pi_progress_line(line).as_deref(), Some("turn 3"));
    }

    #[test]
    fn parse_pi_progress_line_thinking_start() {
        let line = r#"{"type":"message_update","message":{},"assistantMessageEvent":{"type":"thinking_start","contentIndex":0,"partial":{}}}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("Reasoning...")
        );
    }

    #[test]
    fn parse_pi_progress_line_toolcall_end() {
        let line = r#"{"type":"message_update","message":{},"assistantMessageEvent":{"type":"toolcall_end","contentIndex":0,"toolCall":{"id":"tc1","name":"bash","arguments":"{\"cmd\":\"cargo test\"}"},"partial":{}}}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("✓ bash: cargo test")
        );
    }

    #[test]
    fn parse_pi_progress_line_auto_compaction_start() {
        let line = r#"{"type":"auto_compaction_start","reason":"context too long"}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("Compacting context...")
        );
    }

    #[test]
    fn parse_pi_progress_line_auto_retry_start() {
        let line = r#"{"type":"auto_retry_start","attempt":2,"maxAttempts":3,"delayMs":1000,"errorMessage":"rate limit"}"#;
        assert_eq!(
            parse_pi_progress_line(line).as_deref(),
            Some("Retrying (attempt 2)...")
        );
    }

    #[test]
    fn extract_pi_json_final_agent_end_messages_plural() {
        let raw = r#"{"type":"agent_start","sessionId":"s1"}
{"type":"turn_start","sessionId":"s1","turnIndex":0,"timestamp":0}
{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: hello from agent_end"}]}}
{"type":"turn_end","sessionId":"s1","turnIndex":0,"message":{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: hello from turn_end"}]},"toolResults":[]}
{"type":"agent_end","sessionId":"s1","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: hello from agent_end"}]}]}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(
            text.as_deref(),
            Some("TELEGRAM_REPLY: hello from agent_end")
        );
    }

    #[test]
    fn extract_pi_json_final_turn_end_fallback() {
        let raw = r#"{"type":"turn_start","sessionId":"s1","turnIndex":0,"timestamp":0}
{"type":"turn_end","sessionId":"s1","turnIndex":0,"message":{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: from turn"}]},"toolResults":[]}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: from turn"));
    }

    #[test]
    fn extract_pi_json_final_message_end_fallback() {
        let raw = r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: from message_end"}]}}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: from message_end"));
    }

    #[test]
    fn extract_pi_json_final_string_content() {
        let raw = r#"{"type":"agent_end","sessionId":"s1","messages":[{"role":"assistant","content":"TELEGRAM_REPLY: plain string"}]}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: plain string"));
    }

    #[test]
    fn extract_claude_json_final_prefers_result_event() {
        let raw = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"not-final"}]}}
{"type":"result","subtype":"success","result":"TELEGRAM_REPLY: final reply"}
"#;
        let text = extract_claude_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: final reply"));
    }

    #[test]
    fn extract_claude_json_final_reads_assistant_text_blocks() {
        let raw = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"thinking","thinking":"t"},{"type":"text","text":"TELEGRAM_REPLY: hello"}]}}
"#;
        let text = extract_claude_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: hello"));
    }
}
