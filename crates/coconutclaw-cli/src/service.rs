use crate::{ServiceAction, ServiceArgs};
use anyhow::{Context, Result, bail};
use coconutclaw_config::{CliOverrides, RuntimeConfig};
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub fn run_service(cfg: &RuntimeConfig, overrides: &CliOverrides, args: ServiceArgs) -> Result<()> {
    let names = service_names(cfg, overrides)?;
    match detect_platform() {
        Platform::Linux => run_linux(cfg, &names, args.action),
        Platform::MacOs => run_macos(cfg, &names, args.action),
        Platform::Windows => bail!(
            "unsupported OS: windows\nservice management is currently available on Linux (systemd) and macOS (launchd)"
        ),
        Platform::Other(os) => bail!("unsupported OS: {os}"),
    }
}

#[derive(Debug, Clone, Copy)]
enum Platform {
    Linux,
    MacOs,
    Windows,
    Other(&'static str),
}

fn detect_platform() -> Platform {
    if cfg!(target_os = "linux") {
        Platform::Linux
    } else if cfg!(target_os = "macos") {
        Platform::MacOs
    } else if cfg!(target_os = "windows") {
        Platform::Windows
    } else {
        Platform::Other(env::consts::OS)
    }
}

#[derive(Debug, Clone)]
struct ServiceNames {
    run_task: String,
    heartbeat_task: String,
    heartbeat_timer: String,
    reflection_task: String,
    reflection_timer: String,
    run_label: String,
    heartbeat_label: String,
    reflection_label: String,
    instance_key: String,
}

fn service_names(cfg: &RuntimeConfig, overrides: &CliOverrides) -> Result<ServiceNames> {
    let mut key = String::new();
    if let Some(instance) = overrides.instance.as_deref() {
        let normalized = sanitize_identifier(instance);
        if !normalized.is_empty() && normalized != "default" {
            key = normalized;
        }
    } else if overrides.instance_dir.is_some() && !paths_equal(&cfg.instance_dir, &cfg.root_dir) {
        let base = cfg
            .instance_dir
            .file_name()
            .and_then(|value| value.to_str())
            .map(sanitize_identifier)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "instance".to_string());
        let hash = cksum_identifier(cfg.instance_dir.to_string_lossy().as_ref());
        key = format!("dir-{base}-{hash}");
    }

    if key.is_empty() {
        return Ok(ServiceNames {
            run_task: "coconutclaw.service".to_string(),
            heartbeat_task: "coconutclaw-heartbeat.service".to_string(),
            heartbeat_timer: "coconutclaw-heartbeat.timer".to_string(),
            reflection_task: "coconutclaw-nightly-reflection.service".to_string(),
            reflection_timer: "coconutclaw-nightly-reflection.timer".to_string(),
            run_label: "io.coconutclaw.run".to_string(),
            heartbeat_label: "io.coconutclaw.heartbeat".to_string(),
            reflection_label: "io.coconutclaw.nightly_reflection".to_string(),
            instance_key: "default".to_string(),
        });
    }

    let label_key = key.replace('_', "-");
    Ok(ServiceNames {
        run_task: format!("coconutclaw-{key}.service"),
        heartbeat_task: format!("coconutclaw-heartbeat-{key}.service"),
        heartbeat_timer: format!("coconutclaw-heartbeat-{key}.timer"),
        reflection_task: format!("coconutclaw-nightly-reflection-{key}.service"),
        reflection_timer: format!("coconutclaw-nightly-reflection-{key}.timer"),
        run_label: format!("io.coconutclaw.run.{label_key}"),
        heartbeat_label: format!("io.coconutclaw.heartbeat.{label_key}"),
        reflection_label: format!("io.coconutclaw.nightly_reflection.{label_key}"),
        instance_key: key,
    })
}

#[derive(Debug, Clone, Copy)]
struct TimeOfDay {
    hour: u8,
    minute: u8,
}

impl TimeOfDay {
    fn parse(raw: &str, field_name: &str) -> Result<Self> {
        let Some((hour_raw, minute_raw)) = raw.split_once(':') else {
            bail!("invalid {field_name} time: {raw} (expected HH:MM)");
        };
        let hour = hour_raw.parse::<u8>().ok().filter(|value| *value <= 23);
        let minute = minute_raw.parse::<u8>().ok().filter(|value| *value <= 59);
        let Some(hour) = hour else {
            bail!("invalid {field_name} time: {raw} (expected HH:MM)");
        };
        let Some(minute) = minute else {
            bail!("invalid {field_name} time: {raw} (expected HH:MM)");
        };
        Ok(Self { hour, minute })
    }

    fn hhmm(self) -> String {
        format!("{:02}:{:02}", self.hour, self.minute)
    }
}

fn run_linux(cfg: &RuntimeConfig, names: &ServiceNames, action: ServiceAction) -> Result<()> {
    require_cmd("systemctl")?;
    match action {
        ServiceAction::Install {
            heartbeat,
            reflection,
        } => {
            let heartbeat = TimeOfDay::parse(&heartbeat, "heartbeat")?;
            let reflection = TimeOfDay::parse(&reflection, "reflection")?;
            linux_install(cfg, names, heartbeat, reflection)
        }
        ServiceAction::Start => linux_start(names),
        ServiceAction::Stop => linux_stop(names),
        ServiceAction::Status => linux_status(names),
        ServiceAction::Uninstall => linux_uninstall(names),
    }
}

fn linux_install(
    cfg: &RuntimeConfig,
    names: &ServiceNames,
    heartbeat: TimeOfDay,
    reflection: TimeOfDay,
) -> Result<()> {
    let dir = linux_systemd_dir()?;
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;

    let binary = resolve_binary_path()?;
    let working_dir = cfg.root_dir.display().to_string();
    let run_exec = systemd_exec_line(&binary, &service_command_args(cfg, "run"));
    let heartbeat_exec = systemd_exec_line(&binary, &service_command_args(cfg, "heartbeat"));
    let reflection_exec =
        systemd_exec_line(&binary, &service_command_args(cfg, "nightly-reflection"));

    let run_unit = format!(
        "[Unit]\nDescription=CoconutClaw Telegram Agent\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nWorkingDirectory={working_dir}\nExecStart={run_exec}\nRestart=always\nRestartSec=3\n\n[Install]\nWantedBy=default.target\n"
    );
    let heartbeat_service = format!(
        "[Unit]\nDescription=CoconutClaw Daily Heartbeat\n\n[Service]\nType=oneshot\nWorkingDirectory={working_dir}\nExecStart={heartbeat_exec}\n"
    );
    let heartbeat_timer = format!(
        "[Unit]\nDescription=Run CoconutClaw heartbeat daily\n\n[Timer]\nOnCalendar=*-*-* {}:00\nPersistent=true\nUnit={}\n\n[Install]\nWantedBy=timers.target\n",
        heartbeat.hhmm(),
        names.heartbeat_task
    );
    let reflection_service = format!(
        "[Unit]\nDescription=CoconutClaw Nightly Reflection\n\n[Service]\nType=oneshot\nWorkingDirectory={working_dir}\nExecStart={reflection_exec}\n"
    );
    let reflection_timer = format!(
        "[Unit]\nDescription=Run CoconutClaw nightly reflection daily\n\n[Timer]\nOnCalendar=*-*-* {}:00\nPersistent=true\nUnit={}\n\n[Install]\nWantedBy=timers.target\n",
        reflection.hhmm(),
        names.reflection_task
    );

    fs::write(dir.join(&names.run_task), run_unit)
        .with_context(|| format!("failed to write {}", names.run_task))?;
    fs::write(dir.join(&names.heartbeat_task), heartbeat_service)
        .with_context(|| format!("failed to write {}", names.heartbeat_task))?;
    fs::write(dir.join(&names.heartbeat_timer), heartbeat_timer)
        .with_context(|| format!("failed to write {}", names.heartbeat_timer))?;
    fs::write(dir.join(&names.reflection_task), reflection_service)
        .with_context(|| format!("failed to write {}", names.reflection_task))?;
    fs::write(dir.join(&names.reflection_timer), reflection_timer)
        .with_context(|| format!("failed to write {}", names.reflection_timer))?;

    run_checked(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "systemctl --user daemon-reload",
    )?;
    run_checked(
        Command::new("systemctl").args([
            "--user",
            "enable",
            &names.run_task,
            &names.heartbeat_timer,
            &names.reflection_timer,
        ]),
        "systemctl --user enable",
    )?;

    println!("installed user systemd units in {}", dir.display());
    println!("instance key: {}", names.instance_key);
    Ok(())
}

fn linux_start(names: &ServiceNames) -> Result<()> {
    run_checked(
        Command::new("systemctl").args([
            "--user",
            "start",
            &names.run_task,
            &names.heartbeat_timer,
            &names.reflection_timer,
        ]),
        "systemctl --user start",
    )
}

fn linux_stop(names: &ServiceNames) -> Result<()> {
    run_best_effort(
        Command::new("systemctl").args([
            "--user",
            "stop",
            &names.run_task,
            &names.heartbeat_timer,
            &names.reflection_timer,
        ]),
        "systemctl --user stop",
    )
}

fn linux_status(names: &ServiceNames) -> Result<()> {
    run_best_effort(
        Command::new("systemctl").args([
            "--user",
            "status",
            &names.run_task,
            &names.heartbeat_task,
            &names.heartbeat_timer,
            &names.reflection_task,
            &names.reflection_timer,
            "--no-pager",
        ]),
        "systemctl --user status",
    )
}

fn linux_uninstall(names: &ServiceNames) -> Result<()> {
    linux_stop(names)?;
    run_best_effort(
        Command::new("systemctl").args([
            "--user",
            "disable",
            &names.run_task,
            &names.heartbeat_timer,
            &names.reflection_timer,
        ]),
        "systemctl --user disable",
    )?;

    let dir = linux_systemd_dir()?;
    let files = [
        dir.join(&names.run_task),
        dir.join(&names.heartbeat_task),
        dir.join(&names.heartbeat_timer),
        dir.join(&names.reflection_task),
        dir.join(&names.reflection_timer),
    ];
    for path in files {
        if let Err(err) = fs::remove_file(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err).with_context(|| format!("failed to remove {}", path.display()));
        }
    }

    run_checked(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "systemctl --user daemon-reload",
    )?;
    println!("removed user systemd units from {}", dir.display());
    Ok(())
}

fn run_macos(cfg: &RuntimeConfig, names: &ServiceNames, action: ServiceAction) -> Result<()> {
    require_cmd("launchctl")?;
    match action {
        ServiceAction::Install {
            heartbeat,
            reflection,
        } => {
            let heartbeat = TimeOfDay::parse(&heartbeat, "heartbeat")?;
            let reflection = TimeOfDay::parse(&reflection, "reflection")?;
            mac_install(cfg, names, heartbeat, reflection)
        }
        ServiceAction::Start => mac_start(names),
        ServiceAction::Stop => mac_stop(names),
        ServiceAction::Status => mac_status(names),
        ServiceAction::Uninstall => mac_uninstall(names),
    }
}

fn mac_install(
    cfg: &RuntimeConfig,
    names: &ServiceNames,
    heartbeat: TimeOfDay,
    reflection: TimeOfDay,
) -> Result<()> {
    let dir = mac_launchagents_dir()?;
    let log_dir = cfg.root_dir.join("LOGS");
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    fs::create_dir_all(&log_dir)
        .with_context(|| format!("failed to create {}", log_dir.display()))?;

    let binary = resolve_binary_path()?;
    let run_args = prefixed_program_arguments(&binary, &service_command_args(cfg, "run"));
    let heartbeat_args =
        prefixed_program_arguments(&binary, &service_command_args(cfg, "heartbeat"));
    let reflection_args =
        prefixed_program_arguments(&binary, &service_command_args(cfg, "nightly-reflection"));

    let run_plist = dir.join(format!("{}.plist", names.run_label));
    let heartbeat_plist = dir.join(format!("{}.plist", names.heartbeat_label));
    let reflection_plist = dir.join(format!("{}.plist", names.reflection_label));

    fs::write(
        &run_plist,
        render_run_plist(
            &names.run_label,
            &run_args,
            &cfg.root_dir,
            &log_dir.join(format!("{}.log", names.run_label)),
            &log_dir.join(format!("{}.err.log", names.run_label)),
        ),
    )
    .with_context(|| format!("failed to write {}", run_plist.display()))?;

    fs::write(
        &heartbeat_plist,
        render_timer_plist(
            &names.heartbeat_label,
            &heartbeat_args,
            &cfg.root_dir,
            heartbeat,
            &log_dir.join(format!("{}.log", names.heartbeat_label)),
            &log_dir.join(format!("{}.err.log", names.heartbeat_label)),
        ),
    )
    .with_context(|| format!("failed to write {}", heartbeat_plist.display()))?;

    fs::write(
        &reflection_plist,
        render_timer_plist(
            &names.reflection_label,
            &reflection_args,
            &cfg.root_dir,
            reflection,
            &log_dir.join(format!("{}.log", names.reflection_label)),
            &log_dir.join(format!("{}.err.log", names.reflection_label)),
        ),
    )
    .with_context(|| format!("failed to write {}", reflection_plist.display()))?;

    let domain = launchctl_domain()?;
    mac_bootstrap_plist(&domain, &run_plist)?;
    mac_bootstrap_plist(&domain, &heartbeat_plist)?;
    mac_bootstrap_plist(&domain, &reflection_plist)?;

    println!("installed launchd agents in {}", dir.display());
    println!("instance key: {}", names.instance_key);
    Ok(())
}

fn mac_start(names: &ServiceNames) -> Result<()> {
    let domain = launchctl_domain()?;
    run_checked(
        Command::new("launchctl").args([
            "kickstart",
            "-k",
            &format!("{domain}/{}", names.run_label),
        ]),
        "launchctl kickstart run",
    )?;
    run_best_effort(
        Command::new("launchctl").args([
            "kickstart",
            "-k",
            &format!("{domain}/{}", names.heartbeat_label),
        ]),
        "launchctl kickstart heartbeat",
    )?;
    run_best_effort(
        Command::new("launchctl").args([
            "kickstart",
            "-k",
            &format!("{domain}/{}", names.reflection_label),
        ]),
        "launchctl kickstart reflection",
    )
}

fn mac_stop(names: &ServiceNames) -> Result<()> {
    let domain = launchctl_domain()?;
    run_best_effort(
        Command::new("launchctl").args(["bootout", &format!("{domain}/{}", names.run_label)]),
        "launchctl bootout run",
    )?;
    run_best_effort(
        Command::new("launchctl").args(["bootout", &format!("{domain}/{}", names.heartbeat_label)]),
        "launchctl bootout heartbeat",
    )?;
    run_best_effort(
        Command::new("launchctl")
            .args(["bootout", &format!("{domain}/{}", names.reflection_label)]),
        "launchctl bootout reflection",
    )
}

fn mac_status(names: &ServiceNames) -> Result<()> {
    let domain = launchctl_domain()?;
    run_best_effort(
        Command::new("launchctl").args(["print", &format!("{domain}/{}", names.run_label)]),
        "launchctl print run",
    )?;
    run_best_effort(
        Command::new("launchctl").args(["print", &format!("{domain}/{}", names.heartbeat_label)]),
        "launchctl print heartbeat",
    )?;
    run_best_effort(
        Command::new("launchctl").args(["print", &format!("{domain}/{}", names.reflection_label)]),
        "launchctl print reflection",
    )
}

fn mac_uninstall(names: &ServiceNames) -> Result<()> {
    mac_stop(names)?;
    let dir = mac_launchagents_dir()?;
    for path in [
        dir.join(format!("{}.plist", names.run_label)),
        dir.join(format!("{}.plist", names.heartbeat_label)),
        dir.join(format!("{}.plist", names.reflection_label)),
    ] {
        if let Err(err) = fs::remove_file(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err).with_context(|| format!("failed to remove {}", path.display()));
        }
    }
    println!("removed launchd agents from {}", dir.display());
    Ok(())
}

fn mac_bootstrap_plist(domain: &str, plist: &Path) -> Result<()> {
    run_best_effort(
        Command::new("launchctl").args(["bootout", domain, &plist.display().to_string()]),
        "launchctl bootout plist",
    )?;
    run_checked(
        Command::new("launchctl").args(["bootstrap", domain, &plist.display().to_string()]),
        "launchctl bootstrap plist",
    )
}

fn prefixed_program_arguments(binary: &Path, command_args: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(command_args.len() + 1);
    out.push(binary.display().to_string());
    out.extend(command_args.iter().cloned());
    out
}

fn render_run_plist(
    label: &str,
    args: &[String],
    working_dir: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{}</string>\n  <key>ProgramArguments</key>\n  <array>\n{}\n  </array>\n  <key>WorkingDirectory</key>\n  <string>{}</string>\n  <key>RunAtLoad</key>\n  <true/>\n  <key>KeepAlive</key>\n  <true/>\n  <key>StandardOutPath</key>\n  <string>{}</string>\n  <key>StandardErrorPath</key>\n  <string>{}</string>\n</dict>\n</plist>\n",
        xml_escape(label),
        plist_program_arguments(args),
        xml_escape(&working_dir.display().to_string()),
        xml_escape(&stdout_path.display().to_string()),
        xml_escape(&stderr_path.display().to_string())
    )
}

fn render_timer_plist(
    label: &str,
    args: &[String],
    working_dir: &Path,
    time: TimeOfDay,
    stdout_path: &Path,
    stderr_path: &Path,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{}</string>\n  <key>ProgramArguments</key>\n  <array>\n{}\n  </array>\n  <key>WorkingDirectory</key>\n  <string>{}</string>\n  <key>StartCalendarInterval</key>\n  <dict>\n    <key>Hour</key>\n    <integer>{}</integer>\n    <key>Minute</key>\n    <integer>{}</integer>\n  </dict>\n  <key>StandardOutPath</key>\n  <string>{}</string>\n  <key>StandardErrorPath</key>\n  <string>{}</string>\n</dict>\n</plist>\n",
        xml_escape(label),
        plist_program_arguments(args),
        xml_escape(&working_dir.display().to_string()),
        time.hour,
        time.minute,
        xml_escape(&stdout_path.display().to_string()),
        xml_escape(&stderr_path.display().to_string())
    )
}

fn plist_program_arguments(args: &[String]) -> String {
    let mut out = String::new();
    for value in args {
        out.push_str("    <string>");
        out.push_str(&xml_escape(value));
        out.push_str("</string>\n");
    }
    out
}

fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn launchctl_domain() -> Result<String> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .context("failed to run id -u")?;
    if !output.status.success() {
        bail!("id -u exited with status {:?}", output.status);
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if uid.is_empty() {
        bail!("id -u returned an empty uid");
    }
    Ok(format!("gui/{uid}"))
}

fn linux_systemd_dir() -> Result<PathBuf> {
    if let Some(value) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(value).join("systemd/user"));
    }
    Ok(home_dir()?.join(".config/systemd/user"))
}

fn mac_launchagents_dir() -> Result<PathBuf> {
    Ok(home_dir()?.join("Library/LaunchAgents"))
}

fn home_dir() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("HOME is not set"))
}

fn require_cmd(bin: &str) -> Result<()> {
    if find_on_path(bin).is_some() {
        Ok(())
    } else {
        bail!("missing dependency: {bin}")
    }
}

fn resolve_binary_path() -> Result<PathBuf> {
    if let Ok(path) = env::current_exe()
        && path.is_file()
    {
        return Ok(path);
    }
    if let Some(path) = find_on_path("coconutclaw") {
        return Ok(path);
    }
    bail!("failed to resolve coconutclaw binary path (tried current executable and PATH lookup)")
}

fn find_on_path(bin: &str) -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;
    for dir in env::split_paths(&paths) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Some(candidate);
        }
        if cfg!(windows) {
            let candidate = dir.join(format!("{bin}.exe"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn run_checked(command: &mut Command, summary: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("failed to run {summary}"))?;
    if !status.success() {
        bail!("{summary} exited with status {status:?}");
    }
    Ok(())
}

fn run_best_effort(command: &mut Command, summary: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("failed to run {summary}"))?;
    if !status.success() {
        eprintln!("{summary} exited with status {status:?}");
    }
    Ok(())
}

fn service_command_args(cfg: &RuntimeConfig, mode: &str) -> Vec<String> {
    vec![
        "--instance-dir".to_string(),
        cfg.instance_dir.display().to_string(),
        mode.to_string(),
    ]
}

fn systemd_exec_line(binary: &Path, args: &[String]) -> String {
    let mut parts = Vec::with_capacity(args.len() + 1);
    parts.push(binary.display().to_string());
    parts.extend(args.iter().cloned());
    parts
        .iter()
        .map(|value| systemd_quote(value))
        .collect::<Vec<_>>()
        .join(" ")
}

fn systemd_quote(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn sanitize_identifier(input: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for mut ch in input.chars() {
        ch.make_ascii_lowercase();
        let allowed = ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '-';
        let mapped = if allowed { ch } else { '-' };
        if mapped == '-' {
            if out.is_empty() || last_dash {
                continue;
            }
            out.push('-');
            last_dash = true;
        } else {
            out.push(mapped);
            last_dash = false;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out
}

fn paths_equal(lhs: &Path, rhs: &Path) -> bool {
    let lhs = fs::canonicalize(lhs).unwrap_or_else(|_| lhs.to_path_buf());
    let rhs = fs::canonicalize(rhs).unwrap_or_else(|_| rhs.to_path_buf());
    lhs == rhs
}

fn cksum_identifier(input: &str) -> String {
    let mut command = Command::new("cksum");
    command.stdin(Stdio::piped()).stdout(Stdio::piped());
    if let Ok(mut child) = command.spawn() {
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(input.as_bytes());
        }
        if let Ok(output) = child.wait_with_output()
            && output.status.success()
        {
            let parsed = String::from_utf8_lossy(&output.stdout);
            if let Some(value) = parsed.split_whitespace().next()
                && !value.is_empty()
            {
                return value.to_string();
            }
        }
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_identifier_matches_shell_behavior_for_basic_cases() {
        assert_eq!(sanitize_identifier("Work_Prod"), "work_prod");
        assert_eq!(sanitize_identifier("x y z"), "x-y-z");
        assert_eq!(sanitize_identifier("##"), "");
        assert_eq!(sanitize_identifier("___"), "___");
    }
}
