use anyhow::{Context, Result};
use coconutclaw_config::{AgentProvider, RuntimeConfig};
use serde_json::Value;
use std::cell::RefCell;
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

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub struct ProviderOutput {
    pub raw_output: String,
    pub success: bool,
    pub exit_code: i32,
}

/// Context passed to every provider when constructing a command and extracting
/// the final output.
pub struct ProviderCtx<'a> {
    pub config: &'a RuntimeConfig,
    pub attachment_path: Option<&'a PathBuf>,
    pub context: &'a str,
    pub cancel_flag: Option<&'a Arc<AtomicBool>>,
    pub progress_tx: Option<&'a Sender<String>>,
    pub timeout_secs: Option<u64>,
}

// ---------------------------------------------------------------------------
// ProviderRunner trait — one impl per provider, one shared driver
// ---------------------------------------------------------------------------

trait ProviderRunner {
    /// Human-readable name used in error messages (e.g. "codex").
    fn bin_name(&self) -> &str;

    /// Build the CLI command. The caller decides whether to include the
    /// dangerous/skip-permissions flag based on `has_dangerous_flag()`.
    fn build_cmd(&self, ctx: &ProviderCtx, include_dangerous: bool) -> std::io::Result<Command>;

    /// Optional line-by-line JSON progress parser.
    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> {
        None
    }

    /// Extract the final reply text from a successful run result.
    fn extract_final(&self, _run_result: &RunResult, _ctx: &ProviderCtx) -> Option<String> {
        None
    }

    /// When true the final output prefers stdout text; otherwise stderr.
    fn prefer_stdout(&self) -> bool {
        false
    }

    /// Whether this provider supports a dangerous/skip-permissions flag and
    /// therefore should participate in the YOLO → retry-without-flag dance.
    fn has_dangerous_flag(&self) -> bool {
        false
    }

    /// Final hook called after `finalize_output`.  Providers that need
    /// cleanup (e.g. deleting a temp file) can override this.
    fn post_process(
        &self,
        output: ProviderOutput,
        _run_result: &RunResult,
        _ctx: &ProviderCtx,
    ) -> ProviderOutput {
        output
    }
}

// ---------------------------------------------------------------------------
// Concrete provider structs
// ---------------------------------------------------------------------------

struct CodexRunner {
    /// Temp file path written by `--output-last-message`, read in
    /// `extract_final`, and deleted in `post_process`.
    out_file: RefCell<Option<PathBuf>>,
}

struct PiRunner;

struct ClaudeRunner;

struct OpenCodeRunner;

struct AntigravityRunner;

struct FactoryRunner;

// ---------------------------------------------------------------------------
// Codex impl
// ---------------------------------------------------------------------------

impl ProviderRunner for CodexRunner {
    fn bin_name(&self) -> &str {
        "codex"
    }

    fn build_cmd(&self, ctx: &ProviderCtx, include_dangerous: bool) -> std::io::Result<Command> {
        let out_file = ctx
            .config
            .tmp_dir
            .join(format!("codex_last_{}.txt", now_nanos()));
        *self.out_file.borrow_mut() = Some(out_file.clone());

        let mut cmd = new_provider_command(&ctx.config.codex.bin);
        cmd.arg("exec")
            .arg("--cd")
            .arg(&ctx.config.instance_dir)
            .arg("--skip-git-repo-check")
            .arg("--output-last-message")
            .arg(&out_file);

        if let Some(model) = &ctx.config.codex.model {
            cmd.arg("--model").arg(model);
        }
        if include_dangerous {
            cmd.arg("--dangerously-bypass-approvals-and-sandbox");
        }
        if ctx.progress_tx.is_some() {
            cmd.arg("--json");
        }
        cmd.arg(ctx.context);
        Ok(cmd)
    }

    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> {
        Some(parse_codex_progress_line)
    }

    fn extract_final(&self, run_result: &RunResult, _ctx: &ProviderCtx) -> Option<String> {
        let out_file = self.out_file.borrow();
        let path = out_file.as_ref()?;
        Some(fs::read_to_string(path).unwrap_or_else(|_| run_result.stdout_text.clone()))
    }

    fn has_dangerous_flag(&self) -> bool {
        true
    }

    fn post_process(
        &self,
        output: ProviderOutput,
        _run_result: &RunResult,
        _ctx: &ProviderCtx,
    ) -> ProviderOutput {
        if let Some(path) = self.out_file.borrow().as_ref() {
            let _ = fs::remove_file(path);
        }
        output
    }
}

// ---------------------------------------------------------------------------
// Pi impl
// ---------------------------------------------------------------------------

impl ProviderRunner for PiRunner {
    fn bin_name(&self) -> &str {
        "pi"
    }

    fn build_cmd(&self, ctx: &ProviderCtx, _include_dangerous: bool) -> std::io::Result<Command> {
        let pi_mode = if ctx.progress_tx.is_some() {
            "json"
        } else {
            "text"
        };

        let mut cmd = new_provider_command(&ctx.config.pi.bin);
        cmd.arg("-p").arg("--mode").arg(pi_mode);

        if let Some(model) = &ctx.config.pi.model {
            cmd.arg("--model").arg(model);
        }
        if let Some(effort) = &ctx.config.pi.reasoning_effort {
            cmd.arg("--reasoning-effort").arg(effort);
        }
        if ctx.config.pi.no_extensions {
            cmd.arg("--no-tools")
                .arg("--no-extensions")
                .arg("--no-skills");
        }
        if let Some(path) = ctx.attachment_path {
            cmd.arg(format!("@{}", path.display()));
        }
        cmd.arg(ctx.context);
        Ok(cmd)
    }

    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> {
        Some(parse_pi_progress_line)
    }

    fn extract_final(&self, run_result: &RunResult, _ctx: &ProviderCtx) -> Option<String> {
        extract_json_or_fallback(run_result, extract_pi_json_final, true)
    }

    fn prefer_stdout(&self) -> bool {
        true
    }

    fn post_process(
        &self,
        output: ProviderOutput,
        _run_result: &RunResult,
        _ctx: &ProviderCtx,
    ) -> ProviderOutput {
        let success = output.success && !output.raw_output.starts_with("⚠️ Agent stopped:");
        ProviderOutput { success, ..output }
    }
}

// ---------------------------------------------------------------------------
// Claude impl
// ---------------------------------------------------------------------------

impl ProviderRunner for ClaudeRunner {
    fn bin_name(&self) -> &str {
        "claude"
    }

    fn build_cmd(&self, ctx: &ProviderCtx, include_dangerous: bool) -> std::io::Result<Command> {
        let mut cmd = new_provider_command(&ctx.config.claude.bin);
        cmd.arg("-p");

        if include_dangerous {
            cmd.arg("--dangerously-skip-permissions");
        }
        if let Some(model) = &ctx.config.claude.model {
            cmd.arg("--model").arg(model);
        }

        cmd.arg("--output-format")
            .arg("stream-json")
            .arg(ctx.context);
        Ok(cmd)
    }

    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> {
        Some(parse_claude_json_line)
    }

    fn extract_final(&self, run_result: &RunResult, _ctx: &ProviderCtx) -> Option<String> {
        extract_json_or_fallback(run_result, extract_claude_json_final, false)
    }

    fn has_dangerous_flag(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// OpenCode impl
// ---------------------------------------------------------------------------

impl ProviderRunner for OpenCodeRunner {
    fn bin_name(&self) -> &str {
        "opencode"
    }

    fn build_cmd(&self, ctx: &ProviderCtx, include_dangerous: bool) -> std::io::Result<Command> {
        let yolo_mode = ctx
            .config
            .opencode
            .skip_permissions
            .unwrap_or_else(|| ctx.config.exec_policy.eq_ignore_ascii_case("yolo"));

        let mut cmd = new_provider_command(&ctx.config.opencode.bin);
        cmd.arg("run");

        if let Some(model) = &ctx.config.opencode.model {
            cmd.arg("--model").arg(model);
        }
        if let Some(effort) = &ctx.config.opencode.reasoning_effort {
            cmd.arg("--variant").arg(effort);
        }
        if ctx.progress_tx.is_some() {
            cmd.arg("--thinking");
        }
        if yolo_mode {
            cmd.env("OPENCODE_PERMISSION", r#"{"*":"allow"}"#);
        }
        let opencode_db_path = ctx.config.instance_dir.join("opencode.db");
        cmd.env("OPENCODE_DB", &opencode_db_path);
        if include_dangerous {
            cmd.arg("--dangerously-skip-permissions");
        }
        cmd.arg("--format").arg("json").arg(ctx.context);
        Ok(cmd)
    }

    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> {
        Some(parse_opencode_json_line)
    }

    fn extract_final(&self, run_result: &RunResult, _ctx: &ProviderCtx) -> Option<String> {
        extract_json_or_fallback(run_result, extract_opencode_json_final, false)
    }

    fn has_dangerous_flag(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Antigravity impl
// ---------------------------------------------------------------------------

impl ProviderRunner for AntigravityRunner {
    fn bin_name(&self) -> &str {
        "antigravity"
    }

    fn build_cmd(&self, ctx: &ProviderCtx, _include_dangerous: bool) -> std::io::Result<Command> {
        let mut cmd = new_provider_command(&ctx.config.antigravity.bin);
        cmd.arg("-p");

        if ctx.config.exec_policy.eq_ignore_ascii_case("yolo") {
            cmd.arg("--yolo");
        }
        if let Some(model) = &ctx.config.antigravity.model {
            cmd.arg("--model").arg(model);
        }
        if ctx.progress_tx.is_some() {
            cmd.arg("--output-format").arg("stream-json");
        }
        cmd.arg(ctx.context);
        Ok(cmd)
    }

    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> {
        Some(parse_antigravity_json_line)
    }

    fn extract_final(&self, run_result: &RunResult, _ctx: &ProviderCtx) -> Option<String> {
        extract_json_or_fallback(run_result, extract_antigravity_json_final, true)
    }

    fn prefer_stdout(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Factory impl
// ---------------------------------------------------------------------------

impl ProviderRunner for FactoryRunner {
    fn bin_name(&self) -> &str {
        "factory"
    }

    fn build_cmd(&self, ctx: &ProviderCtx, include_dangerous: bool) -> std::io::Result<Command> {
        let mut cmd = new_provider_command(&ctx.config.factory.bin);
        cmd.arg("exec");

        if include_dangerous {
            cmd.arg("--skip-permissions-unsafe");
        }
        if let Some(model) = &ctx.config.factory.model {
            cmd.arg("--model").arg(model);
        }

        cmd.arg("--output-format")
            .arg("stream-json")
            .arg(ctx.context);
        Ok(cmd)
    }

    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> {
        Some(parse_factory_json_line)
    }

    fn extract_final(&self, run_result: &RunResult, _ctx: &ProviderCtx) -> Option<String> {
        extract_json_or_fallback(run_result, extract_factory_json_final, false)
    }

    fn has_dangerous_flag(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Public entry point — unchanged signature
// ---------------------------------------------------------------------------

pub fn run_provider(
    attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    let ctx = ProviderCtx {
        config,
        attachment_path,
        context,
        cancel_flag,
        progress_tx,
        timeout_secs,
    };

    match config.provider {
        AgentProvider::Codex => {
            let runner = CodexRunner {
                out_file: RefCell::new(None),
            };
            run_provider_impl(&runner, &ctx)
        }
        AgentProvider::Pi => run_provider_impl(&PiRunner, &ctx),
        AgentProvider::Claude => run_provider_impl(&ClaudeRunner, &ctx),
        AgentProvider::OpenCode => run_provider_impl(&OpenCodeRunner, &ctx),
        AgentProvider::Antigravity => run_provider_impl(&AntigravityRunner, &ctx),
        AgentProvider::Factory => run_provider_impl(&FactoryRunner, &ctx),
    }
}

// ---------------------------------------------------------------------------
// Shared driver — the single implementation that every provider uses
// ---------------------------------------------------------------------------

fn run_provider_impl<P: ProviderRunner>(provider: &P, ctx: &ProviderCtx) -> Result<ProviderOutput> {
    let progress_parser = provider.progress_parser();

    // Helper that runs the process once, optionally including the dangerous flag.
    let run_once = |include_dangerous: bool| -> Result<RunResult> {
        let cmd = provider
            .build_cmd(ctx, include_dangerous)
            .context("failed to build provider command")?;
        run_provider_process(
            cmd,
            ctx.cancel_flag,
            ctx.progress_tx,
            progress_parser,
            provider.bin_name(),
            ctx.timeout_secs,
        )
    };

    let yolo_mode = ctx.config.exec_policy.eq_ignore_ascii_case("yolo");
    let use_dangerous = yolo_mode && provider.has_dangerous_flag();

    let mut run_result = run_once(use_dangerous)?;

    // YOLO retry: if the provider rejected the dangerous flag, retry without it.
    if use_dangerous
        && !run_result.status.success()
        && !run_result.cancelled
        && !run_result.timed_out
        && should_retry_without_dangerous_flag(&run_result.stdout_text, &run_result.stderr_text)
    {
        tracing::warn!(
            "{} CLI rejected dangerous permission flag; retrying without it for compatibility",
            provider.bin_name()
        );
        run_result = run_once(false)?;
    }

    let raw_output_override = if run_result.status.success() {
        provider.extract_final(&run_result, ctx)
    } else {
        None
    };

    let output = finalize_output(
        &run_result,
        None,
        raw_output_override,
        provider.prefer_stdout(),
    );

    Ok(provider.post_process(output, &run_result, ctx))
}

// ---------------------------------------------------------------------------
// Shared helper functions (unchanged from original)
// ---------------------------------------------------------------------------

fn new_provider_command(bin_raw: &str) -> Command {
    let (env_vars, bin_path, initial_args) = parse_bin_with_env(bin_raw);
    let mut cmd = Command::new(bin_path);
    for (key, value) in env_vars {
        cmd.env(&key, &value);
    }
    cmd.args(initial_args);
    cmd
}

fn run_provider_process(
    mut cmd: Command,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    progress_parser: Option<fn(&str) -> Option<String>>,
    bin_name: &str,
    timeout_secs: Option<u64>,
) -> Result<RunResult> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_command(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to start {bin_name}"))?;
    run_child_process(
        child,
        cancel_flag,
        progress_tx.cloned(),
        progress_parser,
        format!("failed waiting for {bin_name} command"),
        format!("failed waiting after {bin_name} kill"),
        timeout_secs,
    )
}

fn fallback_text(run_result: &RunResult, prefer_stdout: bool) -> String {
    if prefer_stdout {
        if !run_result.stdout_text.trim().is_empty() {
            run_result.stdout_text.clone()
        } else {
            run_result.stderr_text.clone()
        }
    } else if !run_result.stderr_text.trim().is_empty() {
        run_result.stderr_text.clone()
    } else {
        run_result.stdout_text.clone()
    }
}

fn finalize_output(
    run_result: &RunResult,
    success_override: Option<bool>,
    raw_output_override: Option<String>,
    prefer_stdout: bool,
) -> ProviderOutput {
    let exit_code = if run_result.cancelled || run_result.timed_out {
        if run_result.timed_out { 124 } else { 130 }
    } else {
        run_result.status.code().unwrap_or(1)
    };

    let raw_output = if let Some(raw) = raw_output_override {
        raw
    } else if run_result.timed_out {
        "provider execution timed out".to_string()
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else {
        fallback_text(run_result, prefer_stdout)
    };

    let success = if let Some(s) = success_override {
        s
    } else {
        !run_result.cancelled && run_result.status.success()
    };

    ProviderOutput {
        raw_output,
        success,
        exit_code,
    }
}

fn extract_json_or_fallback<F>(
    run_result: &RunResult,
    extractor: F,
    prefer_stdout: bool,
) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    if run_result.cancelled || run_result.timed_out {
        return None;
    }
    Some(
        extractor(&run_result.stdout_text)
            .unwrap_or_else(|| fallback_text(run_result, prefer_stdout)),
    )
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
    let ret = unsafe { libc::kill(-pgid, libc::SIGTERM) };
    if ret != 0 {
        tracing::debug!("kill(-{pgid}, SIGTERM) returned {ret}");
    }
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if let Some(status) = child.try_wait().context(wait_error.to_string())? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let ret = unsafe { libc::kill(-pgid, libc::SIGKILL) };
            if ret != 0 {
                tracing::debug!("kill(-{pgid}, SIGKILL) returned {ret}");
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

// ---------------------------------------------------------------------------
// Progress parsers (unchanged from original)
// ---------------------------------------------------------------------------

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
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out = String::with_capacity(keep + 3);
    out.extend(text.chars().take(keep));
    out.push_str("...");
    out
}

/// Extract a short human-readable summary from tool args for progress display.
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
    if text.len() <= max_chars {
        return text.to_string();
    }
    if text.chars().take(max_chars + 1).count() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let mut out = String::with_capacity(keep + 3);
    out.extend(text.chars().take(keep));
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
    if typ == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }
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
    for line in raw.lines().rev() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let typ = value.get("type").and_then(|v| v.as_str()).unwrap_or("");

        if typ == "agent_end" {
            if let Some(messages) = value.get("messages").and_then(|v| v.as_array()) {
                for msg in messages.iter().rev() {
                    if msg.get("role").and_then(|v| v.as_str()) != Some("assistant") {
                        continue;
                    }
                    let stop = msg.get("stopReason").and_then(|v| v.as_str());
                    if matches!(stop, Some("toolUse") | Some("tool_use") | Some("error")) {
                        continue;
                    }
                    if let Some(text) = join_content_text_blocks(msg) {
                        return Some(text);
                    }
                }
            }
            if let Some(err) = value.get("error").and_then(|v| v.as_str())
                && !err.trim().is_empty()
            {
                return Some(format!("⚠️ Agent stopped: {err}"));
            }
        }

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

    if event_type == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }

    if event_type == "system" {
        let subtype = value.get("subtype").and_then(Value::as_str).unwrap_or("");
        if subtype == "init" {
            return Some("Processing...".to_string());
        }
    }

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
        if content
            .iter()
            .any(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        {
            return Some("Drafting response...".to_string());
        }
    }

    if event_type == "user" {
        let message = value.get("message")?;
        let content = message.get("content").and_then(Value::as_array)?;
        for block in content {
            if block.get("type").and_then(Value::as_str) == Some("tool_result") {
                let is_error = block
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
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
    }

    if let Some(text) = result_text {
        return Some(text);
    }
    if let Some(text) = assistant_text {
        return Some(text);
    }
    None
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

        if event_type == "completion" {
            if let Some(text) = value
                .get("finalText")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                completion_text = Some(trim_factory_final_text(text));
            }
            continue;
        }

        if event_type == "message" && value.get("role").and_then(Value::as_str) == Some("assistant")
        {
            if let Some(text) = value
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                assistant_text = Some(trim_factory_final_text(text));
            }
            continue;
        }
    }

    completion_text.or(assistant_text)
}

fn trim_factory_final_text(text: &str) -> String {
    let trimmed = text.trim();
    let start = [
        "TELEGRAM_REPLY:",
        "VOICE_REPLY:",
        "SEND_PHOTO:",
        "SEND_DOCUMENT:",
        "SEND_VIDEO:",
        "MEMORY_APPEND:",
        "TASK_APPEND:",
    ]
    .iter()
    .filter_map(|prefix| trimmed.find(prefix))
    .min();

    match start {
        Some(index) if index > 0 => trimmed[index..].trim().to_string(),
        _ => trimmed.to_string(),
    }
}

fn parse_factory_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type")?.as_str()?;

    if event_type == "system" {
        let subtype = value.get("subtype").and_then(Value::as_str).unwrap_or("");
        if subtype == "init" {
            return Some("Processing...".to_string());
        }
    }

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

    if event_type == "message" && value.get("role").and_then(Value::as_str) == Some("assistant") {
        return Some("Drafting response...".to_string());
    }

    None
}

fn parse_opencode_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type")?.as_str()?;

    if event_type == "progress" {
        return Some(value.get("content")?.as_str()?.to_string());
    }

    if event_type == "step_start" {
        return Some("Processing...".to_string());
    }

    if event_type == "text" {
        let part = value.get("part")?;
        let part_type = part.get("type").and_then(Value::as_str).unwrap_or("text");
        if part_type == "text" {
            return Some("Drafting response...".to_string());
        }
    }

    if matches!(event_type, "reasoning" | "thinking") {
        let part = value.get("part")?;
        let part_type = part
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(event_type);
        if matches!(part_type, "reasoning" | "thinking") {
            let text = part
                .get("text")
                .and_then(Value::as_str)
                .map(|text| {
                    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
                    let trimmed = collapsed.trim().trim_matches('*').trim_matches('`').trim();
                    shorten_status_text(trimmed, 120)
                })
                .unwrap_or_default();
            return if text.is_empty() {
                Some("Reasoning...".to_string())
            } else {
                Some(format!("Reasoning: {text}"))
            };
        }
    }

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
            .and_then(|s| s.get("status"))
            .and_then(Value::as_str)
            .map(|status| matches!(status, "error" | "failed"))
            .unwrap_or(false)
            || state
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
            .or_else(|| {
                state
                    .and_then(|s| s.get("error"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(|text| shorten_status_text(text, 60))
            })
            .or_else(|| {
                state
                    .and_then(|s| s.get("output"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(|text| shorten_status_text(text, 60))
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

    if event_type == "step_finish" {
        let part = value.get("part")?;
        let reason = part.get("reason").and_then(Value::as_str).unwrap_or("");
        if matches!(reason, "tool_calls" | "tool-calls") {
            return Some("Continuing...".to_string());
        }
    }

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
    let mut marker_text: Option<String> = None;
    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        if event_type == "text" {
            let text = value
                .get("part")
                .and_then(|p| p.get("text"))
                .and_then(Value::as_str)
                .or_else(|| value.get("content").and_then(Value::as_str));

            if let Some(text) = text {
                if !final_text.is_empty() && !final_text.ends_with('\n') {
                    final_text.push('\n');
                }
                final_text.push_str(text);
            }
        } else if matches!(event_type, "reasoning" | "thinking")
            && let Some(text) = value
                .get("part")
                .and_then(|p| p.get("text"))
                .and_then(Value::as_str)
                .and_then(trim_to_first_marker)
        {
            marker_text = Some(text);
        }
    }
    if final_text.is_empty() {
        marker_text
    } else if let Some(markers) = trim_to_first_marker(&final_text) {
        Some(markers)
    } else {
        marker_text.or(Some(final_text))
    }
}

fn trim_to_first_marker(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let start = [
        "TELEGRAM_REPLY:",
        "VOICE_REPLY:",
        "SEND_PHOTO:",
        "SEND_DOCUMENT:",
        "SEND_VIDEO:",
        "MEMORY_APPEND:",
        "TASK_APPEND:",
        "SCHEDULE_PROMPT:",
    ]
    .iter()
    .filter_map(|prefix| trimmed.find(prefix))
    .min()?;

    Some(trimmed[start..].trim().to_string())
}

fn parse_antigravity_json_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    let event_type = value.get("type")?.as_str()?;

    if event_type == "init" {
        return Some("Processing...".to_string());
    }

    if event_type == "tool_use" {
        let name = value.get("name").and_then(Value::as_str).unwrap_or("tool");
        let detail = tool_arg_summary(name, value.get("input"));
        return if detail.is_empty() {
            Some(format!("▶ {name}"))
        } else {
            Some(format!("▶ {name}: {detail}"))
        };
    }

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

    if event_type == "message" {
        let role = value.get("role").and_then(Value::as_str).unwrap_or("");
        if role == "assistant" {
            return Some("Drafting response...".to_string());
        }
    }

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

fn extract_antigravity_json_final(raw: &str) -> Option<String> {
    let mut result_text: Option<String> = None;
    let mut assistant_text = String::new();

    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

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

fn parse_bin_with_env(raw: &str) -> (HashMap<String, String>, String, Vec<String>) {
    let parts = split_command_spec(raw);
    if parts.is_empty() {
        return (HashMap::new(), raw.to_string(), Vec::new());
    }

    let mut env_vars = HashMap::new();
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
        parts[bin_index].clone()
    } else {
        raw.to_string()
    };
    let initial_args = if bin_index < parts.len() {
        parts[bin_index + 1..].to_vec()
    } else {
        Vec::new()
    };

    (env_vars, bin_path, initial_args)
}

fn split_command_spec(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;

    for ch in raw.chars() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => current.push(ch),
            None if ch == '"' || ch == '\'' => quote = Some(ch),
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            None => current.push(ch),
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
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

// ---------------------------------------------------------------------------
// Tests (unchanged from original)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
