use crate::{ServiceAction, ServiceArgs};
use anyhow::{Context, Result, bail};
use coconutclaw_config::{CliOverrides, RuntimeConfig};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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
    run_label: String,
}

fn service_names(cfg: &RuntimeConfig, overrides: &CliOverrides) -> Result<ServiceNames> {
    let _ = (cfg, overrides);
    Ok(ServiceNames {
        run_task: "coconutclaw.service".to_string(),
        run_label: "io.coconutclaw.run".to_string(),
    })
}

fn run_linux(cfg: &RuntimeConfig, names: &ServiceNames, action: ServiceAction) -> Result<()> {
    require_cmd("systemctl")?;
    match action {
        ServiceAction::Install => linux_install(cfg, names),
        ServiceAction::Start => linux_start(names),
        ServiceAction::Stop => linux_stop(names),
        ServiceAction::Status => linux_status(names),
        ServiceAction::Uninstall => linux_uninstall(names),
    }
}

fn linux_install(cfg: &RuntimeConfig, names: &ServiceNames) -> Result<()> {
    let dir = linux_systemd_dir()?;
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;

    let binary = resolve_binary_path()?;
    let working_dir = cfg.root_dir.display().to_string();
    let run_exec = systemd_exec_line(&binary, &service_command_args(cfg, "run"));
    let run_unit = format!(
        "[Unit]\nDescription=CoconutClaw Telegram Agent\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nWorkingDirectory={working_dir}\nExecStart={run_exec}\nRestart=always\nRestartSec=3\n\n[Install]\nWantedBy=default.target\n"
    );
    fs::write(dir.join(&names.run_task), run_unit)
        .with_context(|| format!("failed to write {}", names.run_task))?;
    run_checked(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "systemctl --user daemon-reload",
    )?;
    run_checked(
        Command::new("systemctl").args(["--user", "enable", &names.run_task]),
        "systemctl --user enable",
    )?;

    println!("installed user systemd units in {}", dir.display());
    Ok(())
}

fn linux_start(names: &ServiceNames) -> Result<()> {
    run_checked(
        Command::new("systemctl").args(["--user", "start", &names.run_task]),
        "systemctl --user start",
    )
}

fn linux_stop(names: &ServiceNames) -> Result<()> {
    run_best_effort(
        Command::new("systemctl").args(["--user", "stop", &names.run_task]),
        "systemctl --user stop",
    )
}

fn linux_status(names: &ServiceNames) -> Result<()> {
    run_best_effort(
        Command::new("systemctl").args(["--user", "status", &names.run_task, "--no-pager"]),
        "systemctl --user status",
    )
}

fn linux_uninstall(names: &ServiceNames) -> Result<()> {
    linux_stop(names)?;
    run_best_effort(
        Command::new("systemctl").args(["--user", "disable", &names.run_task]),
        "systemctl --user disable",
    )?;

    let dir = linux_systemd_dir()?;
    let files = [dir.join(&names.run_task)];
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
        ServiceAction::Install => mac_install(cfg, names),
        ServiceAction::Start => mac_start(names),
        ServiceAction::Stop => mac_stop(names),
        ServiceAction::Status => mac_status(names),
        ServiceAction::Uninstall => mac_uninstall(names),
    }
}

fn mac_install(cfg: &RuntimeConfig, names: &ServiceNames) -> Result<()> {
    let dir = mac_launchagents_dir()?;
    let log_dir = &cfg.log_dir;
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    fs::create_dir_all(log_dir)
        .with_context(|| format!("failed to create {}", log_dir.display()))?;

    let binary = resolve_binary_path()?;
    let env_path = launchd_path();
    let run_plist = dir.join(format!("{}.plist", names.run_label));

    fs::write(
        &run_plist,
        configured_run_plist(cfg, names, &binary, &env_path),
    )
    .with_context(|| format!("failed to write {}", run_plist.display()))?;

    let domain = launchctl_domain()?;
    mac_bootstrap_plist(&domain, &run_plist)?;

    println!("installed launchd agents in {}", dir.display());
    Ok(())
}

fn configured_run_plist(
    cfg: &RuntimeConfig,
    names: &ServiceNames,
    binary: &Path,
    env_path: &str,
) -> String {
    render_run_plist(
        &names.run_label,
        &prefixed_program_arguments(binary, &service_command_args(cfg, "run")),
        &cfg.root_dir,
        env_path,
        &cfg.log_dir.join(format!("{}.log", names.run_label)),
        &cfg.log_dir.join(format!("{}.err.log", names.run_label)),
    )
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
    )
}

fn mac_stop(names: &ServiceNames) -> Result<()> {
    let domain = launchctl_domain()?;
    run_best_effort(
        Command::new("launchctl").args(["bootout", &format!("{domain}/{}", names.run_label)]),
        "launchctl bootout run",
    )
}

fn mac_status(names: &ServiceNames) -> Result<()> {
    let domain = launchctl_domain()?;
    run_best_effort(
        Command::new("launchctl").args(["print", &format!("{domain}/{}", names.run_label)]),
        "launchctl print run",
    )
}

fn mac_uninstall(names: &ServiceNames) -> Result<()> {
    mac_stop(names)?;
    let dir = mac_launchagents_dir()?;
    let path = dir.join(format!("{}.plist", names.run_label));
    if let Err(err) = fs::remove_file(&path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        return Err(err).with_context(|| format!("failed to remove {}", path.display()));
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
    env_path: &str,
    stdout_path: &Path,
    stderr_path: &Path,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{}</string>\n  <key>ProgramArguments</key>\n  <array>\n{}\n  </array>\n  <key>WorkingDirectory</key>\n  <string>{}</string>\n  <key>EnvironmentVariables</key>\n  <dict>\n    <key>PATH</key>\n    <string>{}</string>\n  </dict>\n  <key>RunAtLoad</key>\n  <true/>\n  <key>KeepAlive</key>\n  <true/>\n  <key>StandardOutPath</key>\n  <string>{}</string>\n  <key>StandardErrorPath</key>\n  <string>{}</string>\n</dict>\n</plist>\n",
        xml_escape(label),
        plist_program_arguments(args),
        xml_escape(&working_dir.display().to_string()),
        xml_escape(env_path),
        xml_escape(&stdout_path.display().to_string()),
        xml_escape(&stderr_path.display().to_string())
    )
}

fn launchd_path() -> String {
    env::var("PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin".to_string()
        })
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

pub(crate) fn find_on_path(bin: &str) -> Option<PathBuf> {
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
        "--data-dir".to_string(),
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

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
