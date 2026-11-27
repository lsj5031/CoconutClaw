use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
#[cfg(not(windows))]
use std::process::{Child, Stdio};
use std::process::{Command, ExitStatus};
use std::sync::mpsc::Sender;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(windows)]
use windows::Child;

mod activity;
#[cfg(test)]
mod activity_tests;
#[cfg(windows)]
mod windows;
use activity::{Activity, TimeoutPolicy};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub struct ProviderOutput {
    pub raw_output: String,
    pub success: bool,
    pub exit_code: i32,
    pub session_id: Option<String>,
}

/// Context passed to every provider when constructing a command and extracting
/// the final output.
pub struct ProviderCtx<'a> {
    pub config: &'a RuntimeConfig,
    pub attachment_path: Option<&'a PathBuf>,
    pub context: &'a str,
    pub cancel_flag: Option<&'a Arc<AtomicBool>>,
    pub progress_tx: Option<&'a Sender<String>>,
    /// None inherits the configured total deadline; Some(0) disables it.
    /// Activity-based idle limits apply independently.
    pub timeout_secs: Option<u64>,
    pub session_id: Option<&'a str>,
    /// Remote text runs never inherit a dangerous host sandbox setting.
    pub workspace_only: bool,
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

    /// Whether the provider prompt should be supplied over stdin instead of as
    /// a command-line argument. This avoids OS argv limits for CLIs that
    /// support stdin prompts.
    fn context_via_stdin(&self) -> bool {
        false
    }

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
        cmd.current_dir(&ctx.config.instance_dir).arg("exec");
        if let Some(id) = ctx.session_id {
            cmd.arg("resume").arg(id);
        } else {
            cmd.arg("--cd").arg(&ctx.config.instance_dir);
        }
        cmd.arg("--skip-git-repo-check")
            .arg("--output-last-message")
            .arg(&out_file);
        if ctx.workspace_only {
            cmd.args(["--sandbox", "workspace-write"])
                .args(["-c", "approval_policy=\"never\""])
                .args(["-c", "shell_environment_policy.inherit=\"none\""]);
        }

        if let Some(model) = &ctx.config.codex.model {
            cmd.arg("--model").arg(model);
        }
        if include_dangerous {
            cmd.arg("--dangerously-bypass-approvals-and-sandbox");
        }
        cmd.arg("--json");
        if let Some(path) = ctx.attachment_path
            && path.extension().and_then(|e| e.to_str()).is_some_and(|e| {
                matches!(
                    e.to_ascii_lowercase().as_str(),
                    "png" | "jpg" | "jpeg" | "webp" | "gif"
                )
            })
        {
            cmd.arg("--image").arg(path);
        }
        cmd.arg("-");
        Ok(cmd)
    }

    fn context_via_stdin(&self) -> bool {
        true
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

pub fn run_provider(
    attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
) -> Result<ProviderOutput> {
    run_provider_session(
        attachment_path,
        config,
        context,
        cancel_flag,
        progress_tx,
        timeout_secs,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_provider_session(
    attachment_path: Option<&PathBuf>,
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    timeout_secs: Option<u64>,
    session_id: Option<&str>,
) -> Result<ProviderOutput> {
    if cancel_flag.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
        return Ok(ProviderOutput {
            raw_output: "cancelled".into(),
            success: false,
            exit_code: 130,
            session_id: None,
        });
    }
    let ctx = ProviderCtx {
        config,
        attachment_path,
        context,
        cancel_flag,
        progress_tx,
        timeout_secs,
        session_id,
        workspace_only: false,
    };

    let runner = CodexRunner {
        out_file: RefCell::new(None),
    };
    run_provider_impl(&runner, &ctx)
}

/// A fresh, non-retrying workspace-scoped invocation for an explicitly paired
/// runner. Authentication and provider routing remain entirely host-local.
pub fn run_workspace(
    config: &RuntimeConfig,
    context: &str,
    cancel_flag: &Arc<AtomicBool>,
) -> Result<ProviderOutput> {
    let mut config = config.clone();
    config.exec_policy = "safe".into();
    let ctx = ProviderCtx {
        config: &config,
        attachment_path: None,
        context,
        cancel_flag: Some(cancel_flag),
        progress_tx: None,
        timeout_secs: None,
        session_id: None,
        workspace_only: true,
    };
    if cancel_flag.load(Ordering::SeqCst) {
        return Ok(ProviderOutput {
            raw_output: "cancelled".into(),
            success: false,
            exit_code: 130,
            session_id: None,
        });
    }
    run_provider_impl(
        &CodexRunner {
            out_file: RefCell::new(None),
        },
        &ctx,
    )
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
            if provider.context_via_stdin() {
                Some(ctx.context)
            } else {
                None
            },
            ctx.cancel_flag,
            ctx.progress_tx,
            progress_parser,
            provider.bin_name(),
            TimeoutPolicy::new(
                ctx.timeout_secs.or(Some(ctx.config.provider_timeout_secs)),
                ctx.config.provider_idle_timeout_secs,
                ctx.config.provider_tool_idle_timeout_secs,
            )
            .into(),
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

    let raw_output_override =
        if run_result.status.success() && !run_result.cancelled && !run_result.timed_out {
            provider.extract_final(&run_result, ctx)
        } else {
            None
        };

    let mut output = finalize_output(
        &run_result,
        None,
        raw_output_override,
        provider.prefer_stdout(),
    );
    if output.success {
        output.session_id = run_result
            .stdout_text
            .lines()
            .filter_map(|line| {
                let event: Value = serde_json::from_str(line).ok()?;
                (event.get("type")?.as_str()? == "thread.started")
                    .then(|| event.get("thread_id")?.as_str().map(ToOwned::to_owned))
                    .flatten()
            })
            .next();
    }

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

pub struct ChildBoundary<'a> {
    pub before_spawn: &'a dyn Fn() -> Result<()>,
    pub after_spawn: &'a dyn Fn(),
}

pub enum ChildExit {
    Success(String),
    Cancelled,
    Failed,
}

pub fn run_single_child(
    mut command: Command,
    stdin: &str,
    cancel: &Arc<AtomicBool>,
    timeout_secs: u64,
    boundary: ChildBoundary<'_>,
) -> Result<ChildExit> {
    let environment: Vec<_> = command
        .get_envs()
        .filter_map(|(name, value)| Some((name.to_owned(), value?.to_owned())))
        .collect();
    command.env_clear().envs(environment);
    let result = run_provider_process(
        command,
        Some(stdin),
        Some(cancel),
        None,
        None,
        "local child",
        ProcessOptions {
            timeout: TimeoutPolicy::new(Some(timeout_secs), timeout_secs, timeout_secs),
            boundary: Some(boundary),
            #[cfg(windows)]
            inherit_environment: false,
        },
    )?;
    Ok(if result.cancelled {
        ChildExit::Cancelled
    } else if !result.timed_out && result.status.success() {
        ChildExit::Success(result.stdout_text)
    } else {
        ChildExit::Failed
    })
}

struct ProcessOptions<'a> {
    timeout: TimeoutPolicy,
    boundary: Option<ChildBoundary<'a>>,
    #[cfg(windows)]
    inherit_environment: bool,
}

impl From<TimeoutPolicy> for ProcessOptions<'_> {
    fn from(timeout: TimeoutPolicy) -> Self {
        Self {
            timeout,
            boundary: None,
            #[cfg(windows)]
            inherit_environment: true,
        }
    }
}

fn run_provider_process(
    mut cmd: Command,
    stdin_text: Option<&str>,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<&Sender<String>>,
    progress_parser: Option<fn(&str) -> Option<String>>,
    bin_name: &str,
    options: ProcessOptions<'_>,
) -> Result<RunResult> {
    configure_child_command(&mut cmd);

    #[cfg(not(windows))]
    if let Some(boundary) = &options.boundary {
        (boundary.before_spawn)()?;
    }
    #[cfg(windows)]
    let (mut child, job) = windows::spawn(
        &cmd,
        stdin_text.is_some(),
        options.inherit_environment,
        options
            .boundary
            .as_ref()
            .map(|boundary| boundary.before_spawn),
    )
    .with_context(|| format!("failed to contain/start {bin_name}"))?;
    #[cfg(not(windows))]
    let mut child = cmd
        .stdin(if stdin_text.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to start {bin_name}"))?;
    if let Some(boundary) = &options.boundary {
        (boundary.after_spawn)();
    }
    let _group = ActiveGroup::register(child.id());
    let io_stop = Arc::new(AtomicBool::new(false));
    let stdin_handle = if let Some(text) = stdin_text {
        let mut stdin = child.stdin.take().context("failed to take stdin")?;
        #[cfg(unix)]
        set_nonblocking(&stdin)?;
        let text = text.to_string();
        let stop = io_stop.clone();
        Some(thread::spawn(move || {
            let mut bytes = text.as_bytes();
            while !bytes.is_empty() && !stop.load(Ordering::SeqCst) {
                match stdin.write(bytes) {
                    Ok(0) => break,
                    Ok(n) => bytes = &bytes[n..],
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10))
                    }
                    Err(_) => break,
                }
            }
        }))
    } else {
        None
    };

    let result = run_child_process(
        child,
        cancel_flag,
        progress_tx.cloned(),
        progress_parser,
        format!("failed waiting for {bin_name} command"),
        format!("failed waiting after {bin_name} kill"),
        options.timeout,
        stdin_handle.as_ref(),
        io_stop.clone(),
        #[cfg(windows)]
        &job,
    );
    io_stop.store(true, Ordering::SeqCst);
    if let Some(handle) = stdin_handle {
        let until = Instant::now() + Duration::from_secs(1);
        while !handle.is_finished() && Instant::now() < until {
            thread::sleep(Duration::from_millis(10));
        }
        if handle.is_finished() {
            let _ = handle.join();
        }
    }
    result
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
        run_result
            .timeout_reason
            .clone()
            .unwrap_or_else(|| "provider execution timed out".into())
    } else if run_result.cancelled {
        "cancelled".to_string()
    } else {
        fallback_text(run_result, prefer_stdout)
    };

    let success = if let Some(s) = success_override {
        s
    } else {
        !run_result.cancelled && !run_result.timed_out && run_result.status.success()
    };

    ProviderOutput {
        raw_output,
        success,
        exit_code,
        session_id: None,
    }
}

struct RunResult {
    status: ExitStatus,
    stdout_text: String,
    stderr_text: String,
    cancelled: bool,
    timed_out: bool,
    timeout_reason: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn run_child_process(
    mut child: Child,
    cancel_flag: Option<&Arc<AtomicBool>>,
    progress_tx: Option<Sender<String>>,
    progress_parser: Option<fn(&str) -> Option<String>>,
    wait_error: String,
    kill_error: String,
    timeout: TimeoutPolicy,
    stdin_handle: Option<&thread::JoinHandle<()>>,
    io_stop: Arc<AtomicBool>,
    #[cfg(windows)] job: &windows::Job,
) -> Result<RunResult> {
    let stdout = child.stdout.take().context("failed to take stdout")?;
    let stderr = child.stderr.take().context("failed to take stderr")?;

    #[cfg(unix)]
    {
        set_nonblocking(&stdout)?;
        set_nonblocking(&stderr)?;
    }
    let start = Instant::now();
    let activity = Arc::new(Mutex::new(Activity::new(start)));
    let stdout_activity = activity.clone();
    let stdout_stop = io_stop.clone();
    let stdout_handle = thread::spawn(move || {
        read_pipe(
            stdout,
            stdout_stop,
            progress_tx,
            progress_parser,
            Some(stdout_activity),
        )
    });
    let stderr_stop = io_stop.clone();
    let stderr_handle = thread::spawn(move || read_pipe(stderr, stderr_stop, None, None, None));

    let mut cancelled = false;
    let mut timed_out = false;
    let mut timeout_reason = None;

    let status = loop {
        if let Some(flag) = cancel_flag
            && flag.load(Ordering::SeqCst)
        {
            cancelled = true;
            #[cfg(windows)]
            job.terminate()?;
            break terminate_cancelled_child(&mut child, &wait_error, &kill_error, false)?;
        }

        let reason =
            activity
                .lock()
                .expect("provider activity")
                .timeout(timeout, start, Instant::now());
        if let Some(reason) = reason {
            timed_out = true;
            tracing::warn!("{reason}");
            timeout_reason = Some(reason);
            #[cfg(windows)]
            job.terminate()?;
            break terminate_cancelled_child(&mut child, &wait_error, &kill_error, true)?;
        }

        let leader_status = child.try_wait().context(wait_error.clone())?;
        // A successful leader must not leave detached tools running. Job
        // membership includes descendants even after the leader exits.
        #[cfg(windows)]
        if leader_status.is_some() {
            job.terminate()?;
        }
        if let Some(status) = leader_status
            && stdout_handle.is_finished()
            && stderr_handle.is_finished()
            && stdin_handle.is_none_or(thread::JoinHandle::is_finished)
        {
            break status;
        }
        thread::sleep(Duration::from_millis(50));
    };

    io_stop.store(true, Ordering::SeqCst);
    let until = Instant::now() + Duration::from_secs(1);
    while !(stdout_handle.is_finished() && stderr_handle.is_finished()) && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    let stdout_text = if stdout_handle.is_finished() {
        stdout_handle.join().unwrap_or_default()
    } else {
        String::new()
    };
    let stderr_text = if stderr_handle.is_finished() {
        stderr_handle.join().unwrap_or_default()
    } else {
        String::new()
    };

    Ok(RunResult {
        status,
        stdout_text,
        stderr_text,
        cancelled,
        timed_out,
        timeout_reason,
    })
}

#[cfg(unix)]
fn set_nonblocking(pipe: &impl std::os::fd::AsRawFd) -> Result<()> {
    let fd = pipe.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error()).context("failed to configure owned pipe");
    }
    Ok(())
}

fn read_pipe(
    mut pipe: impl Read,
    stop: Arc<AtomicBool>,
    progress: Option<Sender<String>>,
    parser: Option<fn(&str) -> Option<String>>,
    activity: Option<Arc<Mutex<Activity>>>,
) -> String {
    let mut bytes = Vec::new();
    let mut line_start = 0;
    let mut buffer = [0; 8192];
    while !stop.load(Ordering::SeqCst) {
        match pipe.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                bytes.extend_from_slice(&buffer[..n]);
                if activity.is_some() || (progress.is_some() && parser.is_some()) {
                    while let Some(end) = bytes[line_start..].iter().position(|byte| *byte == b'\n')
                    {
                        let end = line_start + end;
                        let line = String::from_utf8_lossy(&bytes[line_start..end]);
                        if let Some(activity) = &activity {
                            activity
                                .lock()
                                .expect("provider activity")
                                .observe(&line, Instant::now());
                        }
                        if let (Some(tx), Some(parser)) = (&progress, parser)
                            && let Some(message) = parser(&line)
                        {
                            let _ = tx.send(message);
                        }
                        line_start = end + 1;
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    if line_start < bytes.len() {
        let line = String::from_utf8_lossy(&bytes[line_start..]);
        if let Some(activity) = activity {
            activity
                .lock()
                .expect("provider activity")
                .observe(&line, Instant::now());
        }
        if let (Some(tx), Some(parser)) = (progress, parser)
            && let Some(message) = parser(&line)
        {
            let _ = tx.send(message);
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

// Only owned, still-invoked process groups are registered. Shutdown can kill
// them even if a worker is waiting on a network presentation operation.
static ACTIVE_GROUPS: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());
struct ActiveGroup(u32);
impl ActiveGroup {
    fn register(id: u32) -> Self {
        ACTIVE_GROUPS.lock().expect("provider groups").push(id);
        Self(id)
    }
}
impl Drop for ActiveGroup {
    fn drop(&mut self) {
        ACTIVE_GROUPS
            .lock()
            .expect("provider groups")
            .retain(|id| *id != self.0);
    }
}
pub fn kill_active_process_groups() {
    #[cfg(windows)]
    windows::kill_active_jobs();
    #[cfg(unix)]
    for id in ACTIVE_GROUPS.lock().expect("provider groups").iter() {
        unsafe {
            libc::kill(-(*id as libc::pid_t), libc::SIGKILL);
        }
    }
}

#[cfg(unix)]
fn terminate_cancelled_child(
    child: &mut Child,
    wait_error: &str,
    kill_error: &str,
    force: bool,
) -> Result<ExitStatus> {
    let pgid = child.id() as libc::pid_t;
    if force {
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
        return child.wait().context(kill_error.to_string());
    }
    let ret = unsafe { libc::kill(-pgid, libc::SIGTERM) };
    if ret != 0 {
        tracing::debug!("kill(-{pgid}, SIGTERM) returned {ret}");
    }
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if let Some(status) = child.try_wait().context(wait_error.to_string())? {
            // Descendants can outlive their group leader and keep our pipes open.
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
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

#[cfg(windows)]
fn terminate_cancelled_child(
    child: &mut Child,
    wait_error: &str,
    _kill_error: &str,
    _force: bool,
) -> Result<ExitStatus> {
    // The caller already terminated the whole owned job, including when the
    // leader has exited. Never rediscover descendants from stale PIDs.
    child.wait().context(wait_error.to_string())
}

#[cfg(not(any(unix, windows)))]
fn terminate_cancelled_child(
    child: &mut Child,
    wait_error: &str,
    _kill_error: &str,
    _force: bool,
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
