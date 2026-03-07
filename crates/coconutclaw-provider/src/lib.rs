use anyhow::{Context, Result};
use coconutclaw_config::{AgentProvider, RuntimeConfig};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read};
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
    } else if run_result.status.success() {
        // Always try JSON extraction first — pi may emit JSON even in text mode
        // (e.g. when auto-retry events are present).
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

    let success = !run_result.cancelled
        && run_result.status.success()
        && !raw_output.starts_with("⚠️ Agent stopped:");

    Ok(ProviderOutput {
        raw_output,
        success,
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
            cmd.arg("--dangerously-skip-permissions");
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
    } else {
        extract_claude_json_final(&run_result.stdout_text).unwrap_or_else(|| {
            if !run_result.stderr_text.trim().is_empty() {
                run_result.stderr_text.clone()
            } else if !run_result.stdout_text.trim().is_empty() {
                run_result.stdout_text.clone()
            } else {
                String::new()
            }
        })
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

    cmd.arg("run");

    if let Some(model) = &config.opencode.model {
        cmd.arg("--model").arg(model);
    }
    if let Some(effort) = &config.opencode.reasoning_effort {
        cmd.arg("--variant").arg(effort);
    }

    cmd.arg("--format")
        .arg("json")
        .arg(context)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to start {}", config.opencode.bin))?;

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
    } else {
        extract_opencode_json_final(&run_result.stdout_text).unwrap_or_else(|| {
            if !run_result.stderr_text.trim().is_empty() {
                run_result.stderr_text.clone()
            } else if !run_result.stdout_text.trim().is_empty() {
                run_result.stdout_text.clone()
            } else {
                String::new()
            }
        })
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
    cmd.arg("-p").arg(context);

    if config.exec_policy.eq_ignore_ascii_case("yolo") {
        cmd.arg("--yolo");
    }
    if let Some(model) = &config.gemini.model {
        cmd.arg("--model").arg(model);
    }

    if progress_tx.is_some() {
        cmd.arg("--output-format").arg("stream-json");
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
        progress_tx.cloned(),
        Some(parse_gemini_json_line),
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
    } else {
        extract_gemini_json_final(&run_result.stdout_text).unwrap_or_else(|| {
            if !run_result.stderr_text.trim().is_empty() {
                run_result.stderr_text.clone()
            } else if !run_result.stdout_text.trim().is_empty() {
                run_result.stdout_text.clone()
            } else {
                String::new()
            }
        })
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
    let run_once = |include_dangerous_flag: bool| -> Result<RunResult> {
        let (env_vars, bin_path) = parse_bin_with_env(&config.factory.bin);
        let mut cmd = Command::new(bin_path);
        for (key, value) in env_vars {
            cmd.env(&key, &value);
        }
        cmd.arg("exec");

        if include_dangerous_flag {
            cmd.arg("--skip-permissions-unsafe");
        }
        if let Some(model) = &config.factory.model {
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
            .with_context(|| format!("failed to start {}", config.factory.bin))?;
        run_child_process(
            child,
            cancel_flag,
            progress_tx.cloned(),
            Some(parse_factory_json_line),
            "failed waiting for factory command".to_string(),
            "failed waiting after factory kill".to_string(),
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
            "factory CLI rejected dangerous permission flag; retrying without it for compatibility"
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
    } else {
        extract_factory_json_final(&run_result.stdout_text).unwrap_or_else(|| {
            if !run_result.stderr_text.trim().is_empty() {
                run_result.stderr_text.clone()
            } else if !run_result.stdout_text.trim().is_empty() {
                run_result.stdout_text.clone()
            } else {
                String::new()
            }
        })
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

    let tool_name = tool_name.trim().to_ascii_lowercase();
    let candidate = if matches!(
        tool_name.as_str(),
        "bash" | "shell" | "execute" | "run_shell_command" | "run_command"
    ) {
        first_string_value(args, &["cmd", "command", "script"])
    } else if matches!(
        tool_name.as_str(),
        "read"
            | "read_file"
            | "file_read"
            | "open"
            | "view"
            | "edit_file"
            | "write_file"
            | "create_file"
            | "replace"
            | "patch"
    ) {
        first_string_value(args, &["path", "file", "file_path"])
    } else if matches!(tool_name.as_str(), "glob" | "find") {
        first_string_value(args, &["filePattern", "pattern", "glob"])
    } else if matches!(tool_name.as_str(), "grep" | "search" | "search_files") {
        first_string_value(args, &["pattern", "query"])
    } else if matches!(tool_name.as_str(), "web_search" | "search_web") {
        first_string_value(args, &["objective", "query", "prompt"])
    } else if matches!(tool_name.as_str(), "read_web_page" | "fetch_url") {
        first_string_value(args, &["url"])
    } else {
        first_string_value(
            args,
            &[
                "path",
                "file",
                "file_path",
                "cmd",
                "command",
                "query",
                "pattern",
                "url",
                "directory_path",
                "prompt",
                "title",
            ],
        )
    };

    let text = match candidate {
        Some(s) if !s.is_empty() => s,
        _ => return String::new(),
    };
    truncate_status_detail(&text, MAX_LEN)
}

fn truncate_status_detail(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let mut out: String = chars[..keep].iter().collect();
    out.push('…');
    out
}

fn first_string_value(node: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = node.get(*key)
            && let Some(text) = value_to_status_text(value)
        {
            return Some(text);
        }
    }
    value_to_status_text(node)
}

fn value_to_status_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        let text = text.trim();
        if !text.is_empty() {
            return Some(text.to_string());
        }
        return None;
    }

    if let Some(array) = value.as_array() {
        let mut parts = Vec::new();
        for item in array {
            if let Some(text) = value_to_status_text(item) {
                parts.push(text);
            }
        }
        if !parts.is_empty() {
            return Some(parts.join(", "));
        }
        return None;
    }

    if let Some(object) = value.as_object() {
        for key in [
            "path",
            "file",
            "file_path",
            "cmd",
            "command",
            "query",
            "pattern",
            "url",
            "directory_path",
            "title",
        ] {
            if let Some(child) = object.get(key)
                && let Some(text) = value_to_status_text(child)
            {
                return Some(text);
            }
        }
        for child in object.values() {
            if let Some(text) = value_to_status_text(child) {
                return Some(text);
            }
        }
    }

    None
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
        if typ == "agent_end" {
            if let Some(messages) = value.get("messages").and_then(|v| v.as_array()) {
                for msg in messages.iter().rev() {
                    if msg.get("role").and_then(|v| v.as_str()) != Some("assistant") {
                        continue;
                    }
                    // Skip messages with non-terminal stopReason (e.g. "toolUse",
                    // "error") — their text is mid-execution thinking, not a final reply.
                    let stop = msg.get("stopReason").and_then(|v| v.as_str());
                    if matches!(stop, Some("toolUse") | Some("tool_use") | Some("error")) {
                        continue;
                    }
                    if let Some(text) = join_content_text_blocks(msg) {
                        return Some(text);
                    }
                }
            }
            // No assistant text found; surface agent_end error if present
            if let Some(err) = value.get("error").and_then(|v| v.as_str())
                && !err.trim().is_empty()
            {
                return Some(format!("⚠️ Agent stopped: {err}"));
            }
        }

        // turn_end carries a single message
        if typ == "turn_end"
            && let Some(msg) = value.get("message")
            && !matches!(
                msg.get("stopReason").and_then(|v| v.as_str()),
                Some("toolUse") | Some("tool_use") | Some("error")
            )
            && let Some(text) = join_content_text_blocks(msg)
        {
            return Some(text);
        }

        // message_end for assistant messages
        if typ == "message_end"
            && let Some(msg) = value.get("message")
            && msg.get("role").and_then(|v| v.as_str()) == Some("assistant")
            && !matches!(
                msg.get("stopReason").and_then(|v| v.as_str()),
                Some("toolUse") | Some("tool_use") | Some("error")
            )
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
    let event_type = value.get("type")?.as_str()?;

    // Legacy progress event
    if event_type == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }

    // system/init — session started
    if event_type == "system" {
        let subtype = value.get("subtype").and_then(Value::as_str).unwrap_or("");
        if subtype == "init" {
            return Some("Processing...".to_string());
        }
    }

    // assistant message with tool_use content blocks
    if event_type == "assistant" {
        let message = value.get("message")?;
        let content = message.get("content").and_then(Value::as_array)?;
        for block in content {
            let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
            if block_type == "tool_use" {
                let name = block.get("name").and_then(Value::as_str).unwrap_or("tool");
                let detail = tool_arg_summary(name, block.get("input"));
                return if detail.is_empty() {
                    Some(format!("▶ {name}"))
                } else {
                    Some(format!("▶ {name}: {detail}"))
                };
            }
            if block_type == "thinking" {
                return Some("Reasoning...".to_string());
            }
        }
        // Text-only assistant message means drafting response
        if content
            .iter()
            .any(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        {
            return Some("Drafting response...".to_string());
        }
    }

    // user message with tool_result content blocks
    if event_type == "user" {
        let message = value.get("message")?;
        let content = message.get("content").and_then(Value::as_array)?;
        for block in content {
            if block.get("type").and_then(Value::as_str) == Some("tool_result") {
                let is_error = block
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                // Try to find the tool name from parent_tool_use_id context;
                // fall back to generic marker
                return Some(if is_error {
                    "✗ tool completed".to_string()
                } else {
                    "✓ tool completed".to_string()
                });
            }
        }
    }

    None
}

fn extract_claude_json_final(raw: &str) -> Option<String> {
    let mut result_text: Option<String> = None;
    let mut assistant_text: Option<String> = None;

    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        // {"type":"result","result":"..."}
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

        // {"type":"assistant","message":{"role":"assistant","content":[...]}}
        if event_type == "assistant" {
            if let Some(text) = extract_claude_assistant_text(&value) {
                assistant_text = Some(text);
            }
            continue;
        }
    }

    if let Some(text) = result_text {
        return Some(text);
    }
    if let Some(text) = assistant_text {
        return Some(text);
    }
    None
}

/// Extract final response from Factory/Droid `--output-format stream-json` output.
///
/// Droid format:
///   {"type":"completion","finalText":"..."}
///   {"type":"message","role":"assistant","text":"..."}
fn extract_factory_json_final(raw: &str) -> Option<String> {
    let mut completion_text: Option<String> = None;
    let mut assistant_text: Option<String> = None;

    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        // {"type":"completion","finalText":"..."}
        if event_type == "completion" {
            if let Some(text) = value
                .get("finalText")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                completion_text = Some(text.to_string());
            }
            continue;
        }

        // {"type":"message","role":"assistant","text":"..."}
        if event_type == "message" && value.get("role").and_then(Value::as_str) == Some("assistant")
        {
            if let Some(text) = value
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                assistant_text = Some(text.to_string());
            }
            continue;
        }
    }

    completion_text.or(assistant_text)
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

/// Parse a Factory/Droid `--output-format stream-json` JSONL line for progress.
///
/// Droid event types: system/init, tool_call, tool_result, message, completion.
fn parse_factory_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type")?.as_str()?;

    // system/init — session started
    if event_type == "system" {
        let subtype = value.get("subtype").and_then(Value::as_str).unwrap_or("");
        if subtype == "init" {
            return Some("Processing...".to_string());
        }
    }

    // tool_call — tool invocation starting
    if event_type == "tool_call" {
        let name = value
            .get("toolName")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let detail = tool_arg_summary(name, value.get("parameters"));
        return if detail.is_empty() {
            Some(format!("▶ {name}"))
        } else {
            Some(format!("▶ {name}: {detail}"))
        };
    }

    // tool_result — tool finished
    if event_type == "tool_result" {
        let name = value
            .get("toolId")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let is_error = value
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        return Some(format!("{} {name}", if is_error { "✗" } else { "✓" }));
    }

    // assistant message — drafting response
    if event_type == "message" && value.get("role").and_then(Value::as_str) == Some("assistant") {
        return Some("Drafting response...".to_string());
    }

    None
}

fn parse_opencode_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type")?.as_str()?;

    // Legacy progress event
    if event_type == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }

    // step_start — beginning of a processing step
    if event_type == "step_start" {
        return Some("Processing...".to_string());
    }

    // tool_use — tool invocation (often emitted with final state)
    if event_type == "tool_use" {
        let part = value.get("part")?;
        let tool = part.get("tool").and_then(Value::as_str).unwrap_or("tool");
        let state = part.get("state");
        let status = state
            .and_then(|s| s.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_started = matches!(status, "running" | "in_progress" | "started" | "pending");
        let is_error = state
            .and_then(|s| s.get("metadata"))
            .and_then(|m| m.get("exit"))
            .and_then(Value::as_i64)
            .map(|c| c != 0)
            .unwrap_or(false);
        let detail = state
            .and_then(|s| s.get("title"))
            .and_then(Value::as_str)
            .map(|t| shorten_status_text(t, 60))
            .filter(|t| !t.is_empty())
            .or_else(|| {
                state
                    .and_then(|s| s.get("input"))
                    .map(|input| tool_arg_summary(tool, Some(input)))
                    .filter(|text| !text.is_empty())
            })
            .unwrap_or_default();
        let marker = if is_started {
            "▶"
        } else if is_error {
            "✗"
        } else {
            "✓"
        };
        return if detail.is_empty() {
            Some(format!("{marker} {tool}"))
        } else {
            Some(format!("{marker} {tool}: {detail}"))
        };
    }

    // step_finish — end of a processing step
    if event_type == "step_finish" {
        let part = value.get("part")?;
        let reason = part.get("reason").and_then(Value::as_str).unwrap_or("");
        if reason == "tool-calls" {
            return Some("Continuing...".to_string());
        }
    }

    // error event
    if event_type == "error" {
        let msg = value
            .get("error")
            .and_then(|e| e.get("data"))
            .and_then(|d| d.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("error");
        return Some(format!("⚠ {msg}"));
    }

    None
}

fn extract_opencode_json_final(raw: &str) -> Option<String> {
    let mut final_text = String::new();
    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        if event_type == "text" {
            // Actual format: {"type":"text","part":{"type":"text","text":"..."}}
            if let Some(text) = value
                .get("part")
                .and_then(|p| p.get("text"))
                .and_then(Value::as_str)
            {
                final_text.push_str(text);
            }
            // Legacy format: {"type":"text","content":"..."}
            else if let Some(text) = value.get("content").and_then(Value::as_str) {
                final_text.push_str(text);
            }
        }
    }
    if final_text.is_empty() {
        None
    } else {
        Some(final_text)
    }
}

/// Parse a Gemini CLI `--output-format stream-json` JSONL line.
///
/// Event types: init, message, tool_use, tool_result, error, result.
fn parse_gemini_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type")?.as_str()?;

    // init — session started
    if event_type == "init" {
        return Some("Processing...".to_string());
    }

    // tool_use — tool call request with arguments
    if event_type == "tool_use" {
        let name = value.get("name").and_then(Value::as_str).unwrap_or("tool");
        let detail = tool_arg_summary(name, value.get("input"));
        return if detail.is_empty() {
            Some(format!("▶ {name}"))
        } else {
            Some(format!("▶ {name}: {detail}"))
        };
    }

    // tool_result — tool finished executing
    if event_type == "tool_result" {
        let name = value
            .get("name")
            .or_else(|| value.get("tool"))
            .or_else(|| value.get("tool_name"))
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let is_error = value
            .get("is_error")
            .or_else(|| value.get("isError"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let detail = value
            .get("input")
            .or_else(|| value.get("args"))
            .map(|input| tool_arg_summary(name, Some(input)))
            .filter(|text| !text.is_empty())
            .or_else(|| {
                value
                    .get("output")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(|text| shorten_status_text(text, 60))
            })
            .unwrap_or_default();
        let marker = if is_error { "✗" } else { "✓" };
        return if detail.is_empty() {
            Some(format!("{marker} {name}"))
        } else {
            Some(format!("{marker} {name}: {detail}"))
        };
    }

    // message — assistant message chunk (drafting response)
    if event_type == "message" {
        let role = value.get("role").and_then(Value::as_str).unwrap_or("");
        if role == "assistant" {
            return Some("Drafting response...".to_string());
        }
    }

    // error — non-fatal warning or system error
    if event_type == "error" {
        let msg = value
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| value.get("error").and_then(Value::as_str))
            .unwrap_or("error");
        return Some(format!("⚠ {}", shorten_status_text(msg, 80)));
    }

    None
}

/// Extract the final response from Gemini CLI `--output-format stream-json` output.
///
/// Prefers the `result` event's `response` field, falls back to the last `message`
/// event with role=assistant.
fn extract_gemini_json_final(raw: &str) -> Option<String> {
    let mut result_text: Option<String> = None;
    let mut assistant_text = String::new();

    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        // result event may carry the final response
        if event_type == "result" {
            if let Some(text) = value
                .get("response")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|t| !t.is_empty())
            {
                result_text = Some(text.to_string());
            }
            continue;
        }

        // Accumulate assistant message content (may be delta-streamed)
        if event_type == "message"
            && let Some(role) = value.get("role").and_then(Value::as_str)
            && role == "assistant"
        {
            let is_delta = value.get("delta").and_then(Value::as_bool).unwrap_or(false);

            if let Some(content) = value.get("content") {
                if let Some(text) = content.as_str() {
                    if is_delta {
                        assistant_text.push_str(text);
                    } else {
                        // Non-delta replaces accumulated text
                        assistant_text = text.to_string();
                    }
                } else if let Some(arr) = content.as_array() {
                    let mut chunks = Vec::new();
                    for block in arr {
                        if block.get("type").and_then(Value::as_str) == Some("text")
                            && let Some(t) = block.get("text").and_then(Value::as_str)
                        {
                            chunks.push(t);
                        }
                    }
                    let joined = chunks.join("");
                    if is_delta {
                        assistant_text.push_str(&joined);
                    } else {
                        assistant_text = joined;
                    }
                }
            }
        }
    }

    if let Some(text) = result_text {
        return Some(text);
    }
    let trimmed = assistant_text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
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
        || text.contains("--dangerously-bypass-approvals-and-sandbox")
        || text.contains("--skip-permissions-unsafe");
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

/// Environment variables that must be stripped from child provider processes.
/// These are injected by Amp / Claude Code sessions and would poison nested
/// CLI invocations (e.g. `CLAUDECODE=1` makes `claude` refuse to start,
/// `ANTHROPIC_BASE_URL` redirects API calls to a local proxy).
const POISONED_ENV_VARS: &[&str] = &["CLAUDECODE"];

fn configure_child_command(_cmd: &mut Command) {
    for key in POISONED_ENV_VARS {
        _cmd.env_remove(key);
    }
    #[cfg(unix)]
    {
        _cmd.process_group(0);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        extract_claude_json_final, extract_factory_json_final, extract_gemini_json_final,
        extract_opencode_json_final, extract_pi_json_final, parse_claude_json_line,
        parse_codex_progress_line, parse_factory_json_line, parse_gemini_json_line,
        parse_opencode_json_line, parse_pi_progress_line, tool_arg_summary,
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
    fn tool_arg_summary_handles_generic_command_key() {
        let args: Value = serde_json::json!({"command": "git status"});
        assert_eq!(
            tool_arg_summary("run_shell_command", Some(&args)),
            "git status"
        );
    }

    #[test]
    fn tool_arg_summary_reads_nested_object_values() {
        let args: Value = serde_json::json!({"target": {"path": "src/main.rs"}});
        assert_eq!(tool_arg_summary("unknown_tool", Some(&args)), "src/main.rs");
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
    fn extract_pi_json_final_agent_end_error_no_text() {
        let raw = r#"{"type":"session","version":3,"id":"s1"}
{"type":"agent_start","sessionId":"s1"}
{"type":"agent_end","sessionId":"s1","messages":[{"role":"assistant","content":[{"type":"toolCall","id":"tc1","name":"bash","arguments":{"cmd":"ls"}}]}],"error":"Maximum tool iterations (50) exceeded"}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(
            text.as_deref(),
            Some("⚠️ Agent stopped: Maximum tool iterations (50) exceeded")
        );
    }

    #[test]
    fn extract_pi_json_final_skips_tool_use_stop_reason() {
        let raw = r#"{"type":"session","version":3,"id":"s1"}
{"type":"agent_start","sessionId":"s1"}
{"type":"agent_end","sessionId":"s1","messages":[{"role":"user","content":"hello"},{"role":"assistant","content":[{"type":"text","text":"Now I need to find where the response is processed:"}],"stopReason":"toolUse"}],"error":"API error: service overloaded"}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(
            text.as_deref(),
            Some("⚠️ Agent stopped: API error: service overloaded")
        );
    }

    #[test]
    fn extract_pi_json_final_skips_error_stop_reason_with_text() {
        let raw = r#"{"type":"agent_end","sessionId":"s1","messages":[{"role":"assistant","content":[{"type":"text","text":"Let me check the logs:"}],"stopReason":"error","errorMessage":"connection reset"}],"error":"connection reset"}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(text.as_deref(), Some("⚠️ Agent stopped: connection reset"));
    }

    #[test]
    fn extract_pi_json_final_accepts_end_turn_stop_reason() {
        let raw = r#"{"type":"agent_end","sessionId":"s1","messages":[{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: done"}],"stopReason":"end_turn"}]}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: done"));
    }

    #[test]
    fn extract_pi_json_final_accepts_stop_stop_reason() {
        let raw = r#"{"type":"agent_end","sessionId":"s1","messages":[{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: finished"}],"stopReason":"stop"}]}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: finished"));
    }

    #[test]
    fn extract_pi_json_final_accepts_no_stop_reason() {
        let raw = r#"{"type":"agent_end","sessionId":"s1","messages":[{"role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: hello"}]}]}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: hello"));
    }

    #[test]
    fn extract_pi_json_final_rate_limit_empty_content() {
        let raw = r#"{"type":"session","version":3,"id":"s1"}
{"type":"agent_start","sessionId":"s1"}
{"type":"agent_end","sessionId":"s1","messages":[{"role":"user","content":"hello"},{"role":"assistant","content":[],"stopReason":"error","errorMessage":"API error: Rate limit reached"}],"error":"API error: Rate limit reached"}
{"type":"auto_retry_end","success":false,"attempt":3,"finalError":"API error: Rate limit reached"}
"#;
        let text = extract_pi_json_final(raw);
        assert_eq!(
            text.as_deref(),
            Some("⚠️ Agent stopped: API error: Rate limit reached")
        );
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

    // --- Factory/Droid extraction tests ---

    #[test]
    fn extract_factory_json_final_prefers_completion() {
        let raw = r#"{"type":"system","subtype":"init","cwd":"/tmp","session_id":"s1","tools":[],"model":"m"}
{"type":"message","role":"user","id":"u1","text":"hi","timestamp":1}
{"type":"message","role":"assistant","id":"a1","text":"fallback text","timestamp":2}
{"type":"completion","finalText":"TELEGRAM_REPLY: final","numTurns":1,"durationMs":100}
"#;
        let text = extract_factory_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: final"));
    }

    #[test]
    fn extract_factory_json_final_falls_back_to_assistant_message() {
        let raw = r#"{"type":"system","subtype":"init","cwd":"/tmp","session_id":"s1","tools":[],"model":"m"}
{"type":"message","role":"assistant","id":"a1","text":"TELEGRAM_REPLY: hello from droid","timestamp":2}
"#;
        let text = extract_factory_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: hello from droid"));
    }

    #[test]
    fn extract_factory_json_final_ignores_user_messages() {
        let raw = r#"{"type":"message","role":"user","id":"u1","text":"user input","timestamp":1}
"#;
        assert!(extract_factory_json_final(raw).is_none());
    }

    // --- Factory/Droid progress parser tests ---

    #[test]
    fn parse_factory_system_init() {
        let line = r#"{"type":"system","subtype":"init","cwd":"/tmp","session_id":"s1","tools":["Read"],"model":"claude-opus-4-6"}"#;
        assert_eq!(
            parse_factory_json_line(line).as_deref(),
            Some("Processing...")
        );
    }

    #[test]
    fn parse_factory_tool_call() {
        let line = r#"{"type":"tool_call","id":"call_1","toolName":"LS","parameters":{"directory_path":"/home"},"timestamp":1}"#;
        assert_eq!(
            parse_factory_json_line(line).as_deref(),
            Some("▶ LS: /home")
        );
    }

    #[test]
    fn parse_factory_tool_result() {
        let line = r#"{"type":"tool_result","id":"call_1","toolId":"LS","isError":false,"value":"file1\nfile2"}"#;
        assert_eq!(parse_factory_json_line(line).as_deref(), Some("✓ LS"));
    }

    #[test]
    fn parse_factory_tool_result_error() {
        let line = r#"{"type":"tool_result","id":"call_1","toolId":"Execute","isError":true,"value":"command failed"}"#;
        assert_eq!(parse_factory_json_line(line).as_deref(), Some("✗ Execute"));
    }

    #[test]
    fn parse_factory_assistant_message() {
        let line =
            r#"{"type":"message","role":"assistant","id":"a1","text":"hello","timestamp":1}"#;
        assert_eq!(
            parse_factory_json_line(line).as_deref(),
            Some("Drafting response...")
        );
    }

    #[test]
    fn parse_factory_ignores_user_message() {
        let line = r#"{"type":"message","role":"user","id":"u1","text":"hello","timestamp":1}"#;
        assert!(parse_factory_json_line(line).is_none());
    }

    // --- OpenCode extraction tests ---

    #[test]
    fn extract_opencode_json_final_reads_part_text() {
        let raw = r#"{"type":"step_start","timestamp":1,"sessionID":"s1","part":{"type":"step-start"}}
{"type":"text","timestamp":2,"sessionID":"s1","part":{"type":"text","text":"TELEGRAM_REPLY: hello from opencode"}}
{"type":"step_finish","timestamp":3,"sessionID":"s1","part":{"type":"step-finish","reason":"stop"}}
"#;
        let text = extract_opencode_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: hello from opencode"));
    }

    // --- Claude progress parser tests ---

    #[test]
    fn parse_claude_system_init() {
        let line =
            r#"{"type":"system","subtype":"init","session_id":"s1","tools":["Bash","Read"]}"#;
        assert_eq!(
            parse_claude_json_line(line).as_deref(),
            Some("Processing...")
        );
    }

    #[test]
    fn parse_claude_assistant_tool_use() {
        let line = r#"{"type":"assistant","session_id":"s1","message":{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"cargo test"}}]}}"#;
        assert_eq!(
            parse_claude_json_line(line).as_deref(),
            Some("▶ Bash: cargo test")
        );
    }

    #[test]
    fn parse_claude_assistant_thinking() {
        let line = r#"{"type":"assistant","session_id":"s1","message":{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"thinking","thinking":"hmm"}]}}"#;
        assert_eq!(
            parse_claude_json_line(line).as_deref(),
            Some("Reasoning...")
        );
    }

    #[test]
    fn parse_claude_assistant_text_only() {
        let line = r#"{"type":"assistant","session_id":"s1","message":{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"hello"}]}}"#;
        assert_eq!(
            parse_claude_json_line(line).as_deref(),
            Some("Drafting response...")
        );
    }

    #[test]
    fn parse_claude_user_tool_result() {
        let line = r#"{"type":"user","session_id":"s1","message":{"id":"msg_2","type":"message","role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"}]}}"#;
        assert_eq!(
            parse_claude_json_line(line).as_deref(),
            Some("✓ tool completed")
        );
    }

    #[test]
    fn parse_claude_user_tool_result_error() {
        let line = r#"{"type":"user","session_id":"s1","message":{"id":"msg_2","type":"message","role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"fail","is_error":true}]}}"#;
        assert_eq!(
            parse_claude_json_line(line).as_deref(),
            Some("✗ tool completed")
        );
    }

    // --- OpenCode progress parser tests ---

    #[test]
    fn parse_opencode_step_start() {
        let line = r#"{"type":"step_start","timestamp":1767036059338,"sessionID":"ses_1","part":{"type":"step-start"}}"#;
        assert_eq!(
            parse_opencode_json_line(line).as_deref(),
            Some("Processing...")
        );
    }

    #[test]
    fn parse_opencode_tool_use_success() {
        let line = r#"{"type":"tool_use","timestamp":1767036061199,"sessionID":"ses_1","part":{"tool":"bash","state":{"status":"completed","input":{"command":"echo hello"},"title":"Print hello","metadata":{"exit":0}}}}"#;
        assert_eq!(
            parse_opencode_json_line(line).as_deref(),
            Some("✓ bash: Print hello")
        );
    }

    #[test]
    fn parse_opencode_tool_use_falls_back_to_input_detail() {
        let line = r#"{"type":"tool_use","timestamp":1767036061199,"sessionID":"ses_1","part":{"tool":"bash","state":{"status":"completed","input":{"command":"echo hello"},"metadata":{"exit":0}}}}"#;
        assert_eq!(
            parse_opencode_json_line(line).as_deref(),
            Some("✓ bash: echo hello")
        );
    }

    #[test]
    fn parse_opencode_tool_use_started() {
        let line = r#"{"type":"tool_use","timestamp":1767036061199,"sessionID":"ses_1","part":{"tool":"bash","state":{"status":"running","input":{"command":"cargo test"},"metadata":{}}}}"#;
        assert_eq!(
            parse_opencode_json_line(line).as_deref(),
            Some("▶ bash: cargo test")
        );
    }

    #[test]
    fn parse_opencode_tool_use_error() {
        let line = r#"{"type":"tool_use","timestamp":1767036061199,"sessionID":"ses_1","part":{"tool":"bash","state":{"status":"completed","input":{"command":"false"},"title":"Run false","metadata":{"exit":1}}}}"#;
        assert_eq!(
            parse_opencode_json_line(line).as_deref(),
            Some("✗ bash: Run false")
        );
    }

    #[test]
    fn parse_opencode_step_finish_tool_calls() {
        let line = r#"{"type":"step_finish","timestamp":1767036061205,"sessionID":"ses_1","part":{"type":"step-finish","reason":"tool-calls"}}"#;
        assert_eq!(
            parse_opencode_json_line(line).as_deref(),
            Some("Continuing...")
        );
    }

    #[test]
    fn parse_opencode_error() {
        let line = r#"{"type":"error","timestamp":1767036065000,"sessionID":"ses_1","error":{"name":"APIError","data":{"message":"Rate limit exceeded"}}}"#;
        assert_eq!(
            parse_opencode_json_line(line).as_deref(),
            Some("⚠ Rate limit exceeded")
        );
    }

    // --- Gemini progress parser tests ---

    #[test]
    fn parse_gemini_init() {
        let line = r#"{"type":"init","session_id":"abc","model":"gemini-2.5-pro"}"#;
        assert_eq!(
            parse_gemini_json_line(line).as_deref(),
            Some("Processing...")
        );
    }

    #[test]
    fn parse_gemini_tool_use() {
        let line = r#"{"type":"tool_use","name":"Bash","input":{"command":"ls -la"}}"#;
        assert_eq!(
            parse_gemini_json_line(line).as_deref(),
            Some("▶ Bash: ls -la")
        );
    }

    #[test]
    fn parse_gemini_tool_result() {
        let line = r#"{"type":"tool_result","output":"hello\n"}"#;
        assert_eq!(
            parse_gemini_json_line(line).as_deref(),
            Some("✓ tool: hello")
        );
    }

    #[test]
    fn parse_gemini_tool_result_with_name_and_input() {
        let line = r#"{"type":"tool_result","name":"run_shell_command","input":{"command":"ls -la"},"output":"done"}"#;
        assert_eq!(
            parse_gemini_json_line(line).as_deref(),
            Some("✓ run_shell_command: ls -la")
        );
    }

    #[test]
    fn parse_gemini_assistant_message() {
        let line =
            r#"{"type":"message","role":"assistant","content":[{"type":"text","text":"hello"}]}"#;
        assert_eq!(
            parse_gemini_json_line(line).as_deref(),
            Some("Drafting response...")
        );
    }

    #[test]
    fn parse_gemini_error() {
        let line = r#"{"type":"error","message":"API rate limit"}"#;
        assert_eq!(
            parse_gemini_json_line(line).as_deref(),
            Some("⚠ API rate limit")
        );
    }

    #[test]
    fn extract_gemini_json_final_prefers_result() {
        let raw = r#"{"type":"init","session_id":"s1"}
{"type":"message","role":"assistant","content":[{"type":"text","text":"not-final"}]}
{"type":"result","response":"TELEGRAM_REPLY: final answer","stats":{}}
"#;
        let text = extract_gemini_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: final answer"));
    }

    #[test]
    fn extract_gemini_json_final_falls_back_to_message() {
        let raw = r#"{"type":"init","session_id":"s1"}
{"type":"message","role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: from message"}]}
"#;
        let text = extract_gemini_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: from message"));
    }

    #[test]
    fn extract_gemini_json_final_string_content() {
        let raw = r#"{"type":"message","role":"assistant","content":"TELEGRAM_REPLY: plain string"}
"#;
        let text = extract_gemini_json_final(raw);
        assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: plain string"));
    }

    #[test]
    fn extract_gemini_json_final_accumulates_deltas() {
        let raw = r#"{"type":"init","session_id":"s1","model":"gemini-3.1-pro-preview"}
{"type":"message","role":"user","content":"say hello"}
{"type":"message","role":"assistant","content":"TELEGRAM_REPLY: This is a test reply with","delta":true}
{"type":"message","role":"assistant","content":" multiple sentences. The quick brown fox.","delta":true}
{"type":"result","status":"success","stats":{"total_tokens":100}}
"#;
        let text = extract_gemini_json_final(raw);
        assert_eq!(
            text.as_deref(),
            Some(
                "TELEGRAM_REPLY: This is a test reply with multiple sentences. The quick brown fox."
            )
        );
    }
}
