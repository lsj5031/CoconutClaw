use anyhow::{Context, Result, bail};
use coconutclaw_config::{AgentProvider, RuntimeConfig};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::Sender;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

/// Parses a bin string that may contain environment variables.
/// Format: `[KEY=value ...] /path/to/binary`
/// Returns (env_vars, binary_path) where env_vars is a Vec of (key, value) pairs.
fn parse_bin_with_env(bin: &str) -> (Vec<(String, String)>, &str) {
    let mut env_vars = Vec::new();
    let mut remaining = bin.trim();

    while let Some(space_pos) = remaining.find(' ') {
        let part = &remaining[..space_pos];
        // Check if this looks like an env var (contains = but doesn't start with / or .)
        if part.contains('=')
            && !part.starts_with('/')
            && !part.starts_with('.')
            && !part.starts_with('~')
        {
            if let Some(eq_pos) = part.find('=') {
                let key = &part[..eq_pos];
                let value = &part[eq_pos + 1..];
                env_vars.push((key.to_string(), value.to_string()));
            }
            remaining = remaining[space_pos + 1..].trim_start();
        } else {
            // Not an env var, the rest is the binary path
            break;
        }
    }

    (env_vars, remaining)
}

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
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    match config.provider {
        AgentProvider::Codex => run_codex(config, context, cancel_flag, progress_tx, timeout_secs),
        AgentProvider::Pi => run_pi(config, context, cancel_flag, progress_tx, timeout_secs),
        AgentProvider::Claude => {
            run_claude(config, context, cancel_flag, progress_tx, timeout_secs)
        }
        AgentProvider::OpenCode => {
            run_opencode(config, context, cancel_flag, progress_tx, timeout_secs)
        }
        AgentProvider::Gemini => {
            run_gemini(config, context, cancel_flag, progress_tx, timeout_secs)
        }
        AgentProvider::Factory => {
            run_factory(config, context, cancel_flag, progress_tx, timeout_secs)
        }
    }
}

fn run_codex(
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
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

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.codex.bin))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(context.as_bytes())
            .context("failed to write context to codex stdin")?;
    }
    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx,
        Some(parse_codex_progress_line),
        "failed waiting for codex command",
        "failed waiting after codex kill",
        timeout_secs,
    )?;

    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let final_message = fs::read_to_string(&out_file).unwrap_or_default();
    let _ = fs::remove_file(&out_file);

    let raw_output = if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else if run_result.status.success() && !final_message.trim().is_empty() {
        final_message
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

fn run_pi(
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let pi_mode = if progress_tx.is_some() {
        // JSON mode exposes structured streaming events we can forward as progress.
        "json"
    } else {
        config.pi.mode.as_str()
    };

    let mut cmd = Command::new(&config.pi.bin);
    cmd.arg("-p").arg("--mode").arg(pi_mode);

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
    configure_child_command(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.pi.bin))?;
    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx,
        Some(parse_pi_progress_line),
        "failed waiting for pi command",
        "failed waiting after pi kill",
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
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let (env_vars, bin_path) = parse_bin_with_env(&config.claude.bin);
    let mut cmd = Command::new(bin_path);
    for (key, value) in env_vars {
        cmd.env(&key, &value);
    }
    cmd.arg("-p"); // print mode

    match config.exec_policy.as_str() {
        "yolo" => {
            cmd.arg("--dangerously-skip-permissions");
        }
        "allowlist" | "strict" => {}
        other => bail!("invalid EXEC_POLICY: {other}"),
    }

    if let Some(model) = &config.claude.model {
        cmd.arg("--model").arg(model);
    }

    if let Some(effort) = &config.claude.reasoning_effort {
        cmd.env("CLAUDE_CODE_EFFORT_LEVEL", effort);
    }

    if progress_tx.is_some() {
        cmd.arg("--output-format").arg("stream-json");
    }

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", bin_path))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(context.as_bytes())
            .context("failed to write context to claude stdin")?;
    }

    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx,
        Some(parse_claude_progress_line),
        "failed waiting for claude command",
        "failed waiting after claude kill",
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
    } else if progress_tx.is_some() && run_result.status.success() {
        // In stream-json mode, extract the final assistant message from the JSON stream.
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
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let mut cmd = Command::new(&config.opencode.bin);
    cmd.arg("run");

    match config.exec_policy.as_str() {
        "yolo" => {
            cmd.arg("--yolo");
        }
        "allowlist" | "strict" => {}
        other => bail!("invalid EXEC_POLICY: {other}"),
    }

    if let Some(model) = &config.opencode.model {
        cmd.arg("--model").arg(model);
    }

    if let Some(effort) = &config.opencode.reasoning_effort {
        cmd.arg("--variant").arg(effort);
    }
    if progress_tx.is_some() {
        cmd.arg("--output-format").arg("stream-json");
    }

    cmd.stdin(Stdio::piped())
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
        progress_tx,
        Some(parse_generic_progress_line),
        "failed waiting for opencode command",
        "failed waiting after opencode kill",
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
    } else if progress_tx.is_some() && run_result.status.success() {
        // In stream-json mode, extract the final message from the JSON stream.
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
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let mut cmd = Command::new(&config.gemini.bin);
    cmd.arg("--prompt").arg(context);

    match config.exec_policy.as_str() {
        "yolo" => {
            cmd.arg("--yolo");
        }
        "allowlist" | "strict" => {}
        other => bail!("invalid EXEC_POLICY: {other}"),
    }

    if let Some(model) = &config.gemini.model {
        cmd.arg("--model").arg(model);
    }
    if progress_tx.is_some() {
        cmd.arg("-o").arg("stream-json");
    }

    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.gemini.bin))?;

    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx,
        Some(parse_gemini_progress_line),
        "failed waiting for gemini command",
        "failed waiting after gemini kill",
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
    } else if progress_tx.is_some() && run_result.status.success() {
        // In stream-json mode, extract the final assistant message from the JSON stream.
        extract_gemini_json_final(&run_result.stdout_text).unwrap_or_else(|| {
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

fn run_factory(
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let mut cmd = Command::new(&config.factory.bin);
    cmd.arg("exec");

    match config.exec_policy.as_str() {
        "yolo" => {
            cmd.arg("--skip-permissions-unsafe");
        }
        "allowlist" | "strict" => {}
        other => bail!("invalid EXEC_POLICY: {other}"),
    }

    if let Some(model) = &config.factory.model {
        cmd.arg("--model").arg(model);
    }

    if let Some(effort) = &config.factory.reasoning_effort {
        cmd.arg("--reasoning-effort").arg(effort);
    }
    if progress_tx.is_some() {
        cmd.arg("-o").arg("stream-json");
    }

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.factory.bin))?;

    // Droid exec supports piped stdin
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(context.as_bytes())
            .context("failed to write context to factory droid stdin")?;
    }

    let run_result = run_child_process(
        child,
        cancel_flag,
        progress_tx,
        Some(parse_factory_progress_line),
        "failed waiting for factory command",
        "failed waiting after factory kill",
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
    } else if progress_tx.is_some() && run_result.status.success() {
        // In stream-json mode, extract the finalText from completion event.
        extract_factory_json_final(&run_result.stdout_text).unwrap_or_else(|| {
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

struct ProcessRunResult {
    status: ExitStatus,
    cancelled: bool,
    timed_out: bool,
    stdout_text: String,
    stderr_text: String,
}

fn run_child_process(
    mut child: Child,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    parse_progress: Option<fn(&str) -> Option<String>>,
    wait_context: &'static str,
    kill_wait_context: &'static str,
    timeout_secs: Option<u64>,
) -> Result<ProcessRunResult> {
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
                if let Some(parser) = parse_progress
                    && let Some(status) = parser(&line)
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

    let start = Instant::now();
    let timeout_duration = timeout_secs.map(Duration::from_secs);
    let mut cancelled = false;
    let mut timed_out = false;
    let status = 'outer: loop {
        if let Some(status) = child.try_wait().context(wait_context)? {
            break status;
        }
        if let Some(cancel_flag) = cancel_flag
            && cancel_flag.load(Ordering::SeqCst)
        {
            cancelled = true;
            break 'outer terminate_cancelled_child(&mut child, wait_context, kill_wait_context)?;
        }
        if let Some(timeout) = timeout_duration
            && start.elapsed() >= timeout
        {
            timed_out = true;
            tracing::warn!("provider process timed out after {}s", timeout.as_secs());
            break 'outer terminate_cancelled_child(&mut child, wait_context, kill_wait_context)?;
        }
        thread::sleep(Duration::from_millis(200));
    };

    let stdout_text = stdout_handle
        .map(|handle| handle.join().unwrap_or_default())
        .unwrap_or_default();
    let stderr_text = stderr_handle
        .map(|handle| handle.join().unwrap_or_default())
        .unwrap_or_default();

    Ok(ProcessRunResult {
        status,
        cancelled,
        timed_out,
        stdout_text,
        stderr_text,
    })
}

#[cfg(unix)]
fn configure_child_command(cmd: &mut Command) {
    cmd.process_group(0);
}

#[cfg(not(unix))]
fn configure_child_command(_cmd: &mut Command) {}

#[cfg(unix)]
fn terminate_cancelled_child(
    child: &mut Child,
    wait_context: &'static str,
    kill_wait_context: &'static str,
) -> Result<ExitStatus> {
    let pgid = child.id() as libc::pid_t;
    unsafe { libc::kill(-pgid, libc::SIGTERM) };
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if let Some(status) = child.try_wait().context(wait_context)? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            unsafe { libc::kill(-pgid, libc::SIGKILL) };
            return child.wait().context(kill_wait_context);
        }
        thread::sleep(Duration::from_millis(200));
    }
}

#[cfg(not(unix))]
fn terminate_cancelled_child(
    child: &mut Child,
    wait_context: &'static str,
    _kill_wait_context: &'static str,
) -> Result<ExitStatus> {
    child.kill().context("failed to kill cancelled process")?;
    child.wait().context(wait_context)
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

fn parse_pi_progress_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;

    match event_type {
        "turn_start" => Some("Processing...".to_string()),
        "tool_execution_start" => {
            pi_tool_label_from_exec_event(&value).map(|label| format!("Running: {label}"))
        }
        "tool_execution_end" => {
            let label = pi_tool_label_from_exec_event(&value)
                .or_else(|| {
                    value
                        .get("toolName")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_else(|| "tool".to_string());
            let is_error = value
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if is_error {
                Some(format!("Failed: {label}"))
            } else {
                Some(format!("Completed: {label}"))
            }
        }
        "message_update" => {
            let update_type = value
                .get("assistantMessageEvent")
                .and_then(|node| node.get("type"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            match update_type {
                "thinking_start" => Some("Reasoning...".to_string()),
                "thinking_end" => value
                    .get("assistantMessageEvent")
                    .and_then(|node| node.get("content"))
                    .and_then(Value::as_str)
                    .map(clean_progress_text)
                    .filter(|text| !text.is_empty())
                    .map(|text| format!("Reasoning: {text}"))
                    .or_else(|| Some("Reasoning...".to_string())),
                "toolcall_end" => value
                    .get("assistantMessageEvent")
                    .and_then(|node| node.get("toolCall"))
                    .and_then(pi_tool_label_from_tool_call)
                    .map(|label| format!("Planned: {label}")),
                "text_start" => Some("Drafting response...".to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn parse_generic_progress_line(line: &str) -> Option<String> {
    // A simple heuristic for CLI providers that don't have strictly defined SSE JSON logs
    // but might output JSON logs
    if let Ok(value) = serde_json::from_str::<Value>(line)
        && let Some(event_type) = value.get("type").and_then(Value::as_str)
    {
        match event_type {
            "started" | "start" => return Some("Processing...".to_string()),
            "thinking" | "reasoning" | "thought" => return Some("Reasoning...".to_string()),
            "tool_call" | "action" => return Some("Running tool...".to_string()),
            "message" | "chunk" | "assistant_chunk" => {
                return Some("Drafting response...".to_string());
            }
            _ => {}
        }
    }
    None
}

fn parse_claude_progress_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;

    match event_type {
        "system" => Some("Processing...".to_string()),
        "assistant" => {
            // Check for tool use in the message
            if let Some(content) = value
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(Value::as_array)
            {
                for item in content {
                    if item.get("type").and_then(Value::as_str) == Some("tool_use") {
                        let tool_name = item.get("name").and_then(Value::as_str).unwrap_or("tool");
                        return Some(format!("Running: {tool_name}"));
                    }
                }
            }
            Some("Drafting response...".to_string())
        }
        "tool_use" => {
            let tool_name = value.get("name").and_then(Value::as_str).unwrap_or("tool");
            Some(format!("Running: {tool_name}"))
        }
        "result" => Some("Completed.".to_string()),
        _ => None,
    }
}

fn extract_claude_json_final(payload: &str) -> Option<String> {
    let mut final_text: Option<String> = None;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        if event_type == "assistant"
            && let Some(content) = value
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(Value::as_array)
        {
            for item in content {
                if item.get("type").and_then(Value::as_str) == Some("text")
                    && let Some(text) = item.get("text").and_then(Value::as_str)
                {
                    match &mut final_text {
                        Some(existing) => existing.push_str(text),
                        None => final_text = Some(text.to_string()),
                    }
                }
            }
        }
    }

    final_text
}

fn pi_tool_label_from_exec_event(value: &Value) -> Option<String> {
    let tool_name = value
        .get("toolName")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())?;
    let detail = value.get("args").and_then(pi_tool_args_detail);
    if let Some(detail) = detail {
        Some(format!("{tool_name} {detail}"))
    } else {
        Some(tool_name.to_string())
    }
}

fn pi_tool_label_from_tool_call(value: &Value) -> Option<String> {
    let tool_name = value
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())?;
    let detail = value.get("arguments").and_then(pi_tool_args_detail);
    if let Some(detail) = detail {
        Some(format!("{tool_name} {detail}"))
    } else {
        Some(tool_name.to_string())
    }
}

fn pi_tool_args_detail(args: &Value) -> Option<String> {
    if let Some(command) = args
        .get("command")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return Some(shorten_status_text(command, 100));
    }

    if let Some(path) = args
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return Some(shorten_status_text(path, 100));
    }

    None
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

fn parse_gemini_progress_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;

    match event_type {
        "init" => Some("Processing...".to_string()),
        "tool_use" => {
            let tool = value
                .get("tool_name")
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let detail = value
                .get("parameters")
                .and_then(|p| p.get("command"))
                .and_then(Value::as_str)
                .map(|c| shorten_status_text(c.trim(), 100));
            if let Some(detail) = detail {
                Some(format!("Running: {tool} {detail}"))
            } else {
                Some(format!("Running: {tool}"))
            }
        }
        "tool_result" => {
            let tool_name = value
                .get("tool_id")
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let is_error = value.get("status").and_then(Value::as_str) == Some("error");
            if is_error {
                Some(format!("Failed: {tool_name}"))
            } else {
                Some(format!("Completed: {tool_name}"))
            }
        }
        "message" => {
            let role = value.get("role").and_then(Value::as_str)?;
            if role == "assistant" {
                Some("Drafting response...".to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_gemini_json_final(payload: &str) -> Option<String> {
    let mut final_text: Option<String> = None;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        if event_type == "message"
            && let Some(role) = value.get("role").and_then(Value::as_str)
            && role == "assistant"
            && let Some(content) = value.get("content").and_then(Value::as_str)
            && !content.trim().is_empty()
        {
            match &mut final_text {
                Some(existing) => {
                    existing.push_str(content);
                }
                None => {
                    final_text = Some(content.to_string());
                }
            }
        }
    }

    final_text
}

/// Parses Factory/Droid stream-json progress lines.
/// Format: {"type":"system|message|tool_call|tool_result|completion",...}
fn parse_factory_progress_line(line: &str) -> Option<String> {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return None;
    };

    let event_type = value.get("type").and_then(Value::as_str)?;

    match event_type {
        "system" => {
            let model = value
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            Some(format!("[Factory] Initialized with {}", model))
        }
        "message" => {
            let role = value.get("role").and_then(Value::as_str).unwrap_or("");
            if role == "assistant" {
                let text = value.get("text").and_then(Value::as_str).unwrap_or("");
                if !text.is_empty() {
                    Some(format!("[Factory] {}", text))
                } else {
                    None
                }
            } else {
                None
            }
        }
        "tool_call" => {
            let tool_name = value
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let params = value.get("parameters");
            let detail = if let Some(p) = params {
                if let Some(cmd) = p.get("command").and_then(Value::as_str) {
                    format!(": {}", cmd)
                } else if let Some(path) = p.get("file_path").and_then(Value::as_str) {
                    format!(": {}", path)
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            Some(format!("[Factory] Tool: {}{}", tool_name, detail))
        }
        "tool_result" => {
            // Skip tool results, they can be verbose
            None
        }
        "completion" => {
            let duration = value.get("durationMs").and_then(Value::as_u64).unwrap_or(0);
            let turns = value.get("numTurns").and_then(Value::as_u64).unwrap_or(0);
            Some(format!(
                "[Factory] Completed in {}ms, {} turns",
                duration, turns
            ))
        }
        _ => None,
    }
}

/// Extracts final text from Factory/Droid stream-json completion event.
fn extract_factory_json_final(payload: &str) -> Option<String> {
    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        if event_type == "completion"
            && let Some(final_text) = value.get("finalText").and_then(Value::as_str)
            && !final_text.trim().is_empty()
        {
            return Some(final_text.to_string());
        }
    }
    None
}

/// Extracts final output from OpenCode stream-json format.
/// OpenCode uses similar format to Codex with result events.
fn extract_opencode_json_final(payload: &str) -> Option<String> {
    let mut final_text: Option<String> = None;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };

        // Check for result type (similar to Codex)
        if let Some(event_type) = value.get("type").and_then(Value::as_str) {
            if event_type == "result"
                && let Some(result) = value.get("result").and_then(Value::as_str)
                && !result.trim().is_empty()
            {
                return Some(result.to_string());
            }
            // Also check for assistant messages with content
            if event_type == "message"
                && value.get("role").and_then(Value::as_str) == Some("assistant")
                && let Some(content) = value.get("content").and_then(Value::as_str)
                && !content.trim().is_empty()
            {
                match &mut final_text {
                    Some(existing) => {
                        existing.push_str(content);
                    }
                    None => {
                        final_text = Some(content.to_string());
                    }
                }
            }
        }

        // Fallback: check for output field
        if let Some(output) = value.get("output").and_then(Value::as_str)
            && !output.trim().is_empty()
        {
            final_text = Some(output.to_string());
        }
    }

    final_text
}

#[cfg(test)]
mod tests {
    use super::{
        extract_factory_json_final, extract_gemini_json_final, extract_opencode_json_final,
        parse_codex_progress_line, parse_factory_progress_line, parse_gemini_progress_line,
        parse_pi_progress_line,
    };

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

    #[test]
    fn parses_pi_turn_started_progress() {
        let line = r#"{"type":"turn_start","turnIndex":0}"#;
        assert_eq!(
            parse_pi_progress_line(line),
            Some("Processing...".to_string())
        );
    }

    #[test]
    fn parses_pi_reasoning_started_progress() {
        let line = r#"{"type":"message_update","assistantMessageEvent":{"type":"thinking_start"}}"#;
        assert_eq!(
            parse_pi_progress_line(line),
            Some("Reasoning...".to_string())
        );
    }

    #[test]
    fn parses_pi_text_started_progress() {
        let line = r#"{"type":"message_update","assistantMessageEvent":{"type":"text_start"}}"#;
        assert_eq!(
            parse_pi_progress_line(line),
            Some("Drafting response...".to_string())
        );
    }

    #[test]
    fn parses_pi_toolcall_planned_progress() {
        let line = r#"{"type":"message_update","assistantMessageEvent":{"type":"toolcall_end","toolCall":{"name":"bash","arguments":{"command":"pwd"}}}}"#;
        assert_eq!(
            parse_pi_progress_line(line),
            Some("Planned: bash pwd".to_string())
        );
    }

    #[test]
    fn parses_pi_tool_execution_started_progress() {
        let line = r#"{"type":"tool_execution_start","toolName":"bash","args":{"command":"pwd"}}"#;
        assert_eq!(
            parse_pi_progress_line(line),
            Some("Running: bash pwd".to_string())
        );
    }

    #[test]
    fn parses_pi_tool_execution_completed_progress() {
        let line = r#"{"type":"tool_execution_end","toolName":"bash","args":{"command":"pwd"},"isError":false}"#;
        assert_eq!(
            parse_pi_progress_line(line),
            Some("Completed: bash pwd".to_string())
        );
    }

    #[test]
    fn parses_gemini_init_progress() {
        let line = r#"{"type":"init","timestamp":"2026-02-28T05:42:13.283Z","session_id":"abc","model":"auto-gemini-3"}"#;
        assert_eq!(
            parse_gemini_progress_line(line),
            Some("Processing...".to_string())
        );
    }

    #[test]
    fn parses_gemini_tool_use_progress() {
        let line = r#"{"type":"tool_use","tool_name":"read_file","tool_id":"read_file_123","parameters":{"path":"/tmp/foo"}}"#;
        assert_eq!(
            parse_gemini_progress_line(line),
            Some("Running: read_file".to_string())
        );
    }

    #[test]
    fn parses_gemini_tool_use_with_command_progress() {
        let line = r#"{"type":"tool_use","tool_name":"run_shell_command","tool_id":"cmd_123","parameters":{"command":"ls /tmp"}}"#;
        assert_eq!(
            parse_gemini_progress_line(line),
            Some("Running: run_shell_command ls /tmp".to_string())
        );
    }

    #[test]
    fn parses_gemini_tool_result_success_progress() {
        let line = r#"{"type":"tool_result","tool_id":"read_file_123","status":"success","output":"file contents"}"#;
        assert_eq!(
            parse_gemini_progress_line(line),
            Some("Completed: read_file_123".to_string())
        );
    }

    #[test]
    fn parses_gemini_tool_result_error_progress() {
        let line =
            r#"{"type":"tool_result","tool_id":"cmd_123","status":"error","output":"not found"}"#;
        assert_eq!(
            parse_gemini_progress_line(line),
            Some("Failed: cmd_123".to_string())
        );
    }

    #[test]
    fn parses_gemini_assistant_message_progress() {
        let line = r#"{"type":"message","role":"assistant","content":"Hello!","delta":true}"#;
        assert_eq!(
            parse_gemini_progress_line(line),
            Some("Drafting response...".to_string())
        );
    }

    #[test]
    fn ignores_gemini_user_message() {
        let line = r#"{"type":"message","role":"user","content":"hello"}"#;
        assert_eq!(parse_gemini_progress_line(line), None);
    }

    #[test]
    fn extracts_gemini_final_assistant_text() {
        let payload = r#"{"type":"init","session_id":"abc","model":"auto-gemini-3"}
{"type":"message","role":"user","content":"Say hello"}
{"type":"message","role":"assistant","content":"Hello! How can I help?","delta":true}
{"type":"result","status":"success","stats":{"total_tokens":100}}"#;
        let text = extract_gemini_json_final(payload);
        assert_eq!(text.as_deref(), Some("Hello! How can I help?"));
    }

    #[test]
    fn extracts_gemini_final_concatenates_deltas() {
        let payload = r#"{"type":"message","role":"assistant","content":"Hello ","delta":true}
{"type":"message","role":"assistant","content":"World!","delta":true}"#;
        let text = extract_gemini_json_final(payload);
        assert_eq!(text.as_deref(), Some("Hello World!"));
    }

    // Factory/Droid tests
    #[test]
    fn parses_factory_system_progress() {
        let line = r#"{"type":"system","subtype":"init","cwd":"/path","session_id":"abc","model":"claude-sonnet-4-5-20250929"}"#;
        assert_eq!(
            parse_factory_progress_line(line),
            Some("[Factory] Initialized with claude-sonnet-4-5-20250929".to_string())
        );
    }

    #[test]
    fn parses_factory_message_progress() {
        let line = r#"{"type":"message","role":"assistant","text":"I'll run the ls command"}"#;
        assert_eq!(
            parse_factory_progress_line(line),
            Some("[Factory] I'll run the ls command".to_string())
        );
    }

    #[test]
    fn parses_factory_tool_call_progress() {
        let line = r#"{"type":"tool_call","toolName":"Execute","parameters":{"command":"ls -la"}}"#;
        assert_eq!(
            parse_factory_progress_line(line),
            Some("[Factory] Tool: Execute: ls -la".to_string())
        );
    }

    #[test]
    fn parses_factory_tool_call_read_progress() {
        let line =
            r#"{"type":"tool_call","toolName":"Read","parameters":{"file_path":"src/main.rs"}}"#;
        assert_eq!(
            parse_factory_progress_line(line),
            Some("[Factory] Tool: Read: src/main.rs".to_string())
        );
    }

    #[test]
    fn parses_factory_completion_progress() {
        let line = r#"{"type":"completion","finalText":"Done","numTurns":2,"durationMs":3000}"#;
        assert_eq!(
            parse_factory_progress_line(line),
            Some("[Factory] Completed in 3000ms, 2 turns".to_string())
        );
    }

    #[test]
    fn extracts_factory_final_text() {
        let payload = r#"{"type":"system","subtype":"init","session_id":"abc"}
{"type":"message","role":"assistant","text":"Running command..."}
{"type":"tool_call","toolName":"Execute","parameters":{"command":"ls"}}
{"type":"completion","finalText":"The ls command completed successfully.","numTurns":1,"durationMs":1500}"#;
        let text = extract_factory_json_final(payload);
        assert_eq!(
            text.as_deref(),
            Some("The ls command completed successfully.")
        );
    }

    // OpenCode tests
    #[test]
    fn extracts_opencode_final_from_result() {
        let payload = r#"{"type":"result","result":"Task completed successfully"}"#;
        let text = extract_opencode_json_final(payload);
        assert_eq!(text.as_deref(), Some("Task completed successfully"));
    }

    #[test]
    fn extracts_opencode_final_from_assistant_message() {
        let payload = r#"{"type":"message","role":"assistant","content":"Here's the answer!"}"#;
        let text = extract_opencode_json_final(payload);
        assert_eq!(text.as_deref(), Some("Here's the answer!"));
    }
}
