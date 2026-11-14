#![cfg(unix)]
use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn command(dir: &std::path::Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_coconutclaw"));
    cmd.arg("--data-dir")
        .arg(dir)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", dir)
        .env("RUST_LOG", "error");
    cmd
}

#[test]
fn polling_codex_result_survives_restart_without_reexecution_or_redelivery() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = dir.join("codex-mock");
    fs::write(
        &provider,
        r#"#!/bin/sh
printf '%s\n' "$@" >> "${0%/*}/args"
/bin/cat > "${0%/*}/prompt"
printf x >> "${0%/*}/calls"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
  shift
done
printf 'TELEGRAM_REPLY: retained polling reply\nMEMORY_APPEND: retained memory\n' > "$out"
printf '%s\n' '{"type":"thread.started","thread_id":"runtime-thread"}'
printf '%s\n' '{"type":"item.started","item":{"type":"command_execution","command":"persisted-without-placeholder"}}'
"#,
    )
    .unwrap();
    fs::set_permissions(&provider, fs::Permissions::from_mode(0o755)).unwrap();
    let generated = command(dir).args(["doctor", "--json"]).output().unwrap();
    assert!(generated.status.success());
    let generated_config = fs::read_to_string(dir.join("config.toml")).unwrap();
    assert!(generated_config.contains("CODEX_BIN = \"codex\""));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen = requests.clone();
    let server = thread::spawn(move || {
        while !stopped.load(Ordering::SeqCst) {
            let Ok((mut stream, _)) = listener.accept() else {
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            stream
                .set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            let mut data = Vec::new();
            let mut buf = [0; 8192];
            loop {
                let n = stream.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                data.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&data);
                if let Some((headers, body)) = text.split_once("\r\n\r\n") {
                    let len = headers
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|s| s.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if body.len() >= len {
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&data).to_string();
            let result = if text.starts_with("GET ") {
                if text.lines().next().unwrap().contains("offset=2") {
                    "[]"
                } else {
                    r#"[{"update_id":1,"message":{"chat":{"id":321},"text":"hello"}}]"#
                }
            } else if text.contains("/setMyCommands ") {
                "true"
            } else {
                r#"{"message_id":42}"#
            };
            seen.lock().unwrap().push(text.clone());
            let placeholder_failed = text.contains("/sendMessage ") && text.contains("Thinking");
            let body = if placeholder_failed {
                r#"{"ok":false,"description":"placeholder rejected"}"#.into()
            } else {
                format!(r#"{{"ok":true,"result":{result}}}"#)
            };
            let status = if placeholder_failed { 400 } else { 200 };
            let response = format!(
                "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    let spawn = || {
        Process(
            command(dir)
                .arg("run")
                .env("PATH", dir)
                .env("TELEGRAM_BOT_TOKEN", "fake")
                .env("TELEGRAM_CHAT_ID", "321")
                .env("TELEGRAM_API_BASE", &base)
                .env("CODEX_BIN", format!("/bin/sh \"{}\"", provider.display()))
                .env("POLL_INTERVAL_SECONDS", "1")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    };
    let mut process = spawn();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let delivered = requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("retained+polling+reply"));
        if delivered {
            break;
        }
        assert!(process.0.try_wait().unwrap().is_none(), "runtime exited");
        assert!(Instant::now() < deadline, "no final reply");
        thread::sleep(Duration::from_millis(50));
    }
    thread::sleep(Duration::from_millis(200));
    drop(process);
    let once = command(dir)
        .env("PATH", dir)
        .env("CODEX_BIN", format!("/bin/sh \"{}\"", provider.display()))
        .args(["once", "--chat-id", "321", "--inject-text", "next"])
        .output()
        .unwrap();
    assert!(
        once.status.success(),
        "{}",
        String::from_utf8_lossy(&once.stderr)
    );
    assert!(
        fs::read_to_string(dir.join("prompt"))
            .unwrap()
            .contains("in=hello")
    );
    assert!(
        fs::read_to_string(dir.join("args"))
            .unwrap()
            .contains("resume\nruntime-thread\n")
    );
    let conn = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    let ids: Vec<String> = conn
        .prepare("SELECT DISTINCT chat_id FROM turns WHERE status!='boundary'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ids, ["telegram:321"]);
    let progress: String = conn
        .query_row("SELECT last_progress FROM task_runs LIMIT 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(progress.contains("persisted-without-placeholder"));
    assert!(
        fs::read_to_string(dir.join("MEMORY.md"))
            .unwrap()
            .contains("retained memory")
    );
    let before = requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.contains("retained+polling+reply"))
        .count();
    let process = spawn();
    thread::sleep(Duration::from_secs(2));
    drop(process);
    assert_eq!(fs::read_to_string(dir.join("calls")).unwrap(), "xx");
    assert_eq!(
        fs::read_to_string(dir.join("config.toml")).unwrap(),
        generated_config
    );
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.contains("retained+polling+reply"))
            .count(),
        before
    );
    stop.store(true, Ordering::SeqCst);
    server.join().unwrap();
}

#[test]
fn removed_commands_and_named_instance_flags_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for arg in [
        "heartbeat",
        "nightly-reflection",
        "--instance",
        "--instance-dir",
    ] {
        let output = command(dir.path()).arg(arg).output().unwrap();
        assert!(!output.status.success(), "{arg} must not be accepted");
    }
    let output = command(dir.path()).arg("--help").output().unwrap();
    let help = String::from_utf8(output.stdout).unwrap();
    for retained in ["run", "once", "doctor", "service"] {
        assert!(help.contains(retained));
    }
}

#[test]
fn actual_cli_config_errors_redact_complete_diagnostics_and_create_no_runtime_state() {
    for body in [
        "TELEGRAM_BOT_TOKEN = \"SYNTHETIC_SECRET_SENTINEL\" trailing",
        "TELEGRAM_PARSE_MODE = \"SYNTHETIC_SECRET_SENTINEL\"",
        "TELEGRAM_PARSE_FALLBACK = \"SYNTHETIC_SECRET_SENTINEL\"",
        "PROVIDER_TIMEOUT_SECS = false",
        "SQLITE_DB_PATH = false",
        "LOG_DIR = \"\\nSYNTHETIC_SECRET_SENTINEL\"",
        "[CODEX_BIN]\nvalue = 'SYNTHETIC_SECRET_SENTINEL'",
    ] {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("config.toml"), body).unwrap();
        let output = command(dir.path())
            .args(["doctor", "--json"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            !String::from_utf8_lossy(&output.stderr)
                .to_uppercase()
                .contains("SYNTHETIC_SECRET_SENTINEL")
        );
        assert!(!dir.path().join("runtime").exists());
        assert!(!dir.path().join("false").exists());
    }
    let dir = tempfile::tempdir().unwrap();
    let output = command(dir.path())
        .env("PROVIDER_TIMEOUT_SECS", "18446744073709551616")
        .args(["doctor", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("18446744073709551616"));
    assert!(!dir.path().join("config.toml").exists());
}

struct ScratchProviderGroup(std::path::PathBuf);
impl Drop for ScratchProviderGroup {
    fn drop(&mut self) {
        if let Ok(pid) = fs::read_to_string(&self.0)
            && let Ok(pid) = pid.trim().parse::<u32>()
        {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", "--", &format!("-{pid}")])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

#[test]
fn once_timeout_bounds_early_leader_exit_and_descendant_output_drain() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = dir.join("early-leader.sh");
    let leader = dir.join("leader");
    let descendant = dir.join("descendant");
    let _cleanup = ScratchProviderGroup(leader.clone());
    fs::write(
        &provider,
        format!(
            r#"/bin/cat >/dev/null
printf '%s' "$$" > '{}'
/bin/sleep 30 &
printf '%s' "$!" > '{}'
exit 0
"#,
            leader.display(),
            descendant.display()
        ),
    )
    .unwrap();
    let stdout = fs::File::create(dir.join("stdout")).unwrap();
    let start = Instant::now();
    let mut process = Process(
        command(dir)
            .env("PATH", dir)
            .env("CODEX_BIN", format!("/bin/sh \"{}\"", provider.display()))
            .env("PROVIDER_TIMEOUT_SECS", "1")
            .args(["once", "--chat-id", "321", "--inject-text", "test"])
            .stdout(stdout)
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "once exceeded the configured deadline/drain allowance"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        fs::read_to_string(dir.join("stdout"))
            .unwrap()
            .contains("timed out")
    );
    eprintln!(
        "once early-leader deadline: {} ms",
        start.elapsed().as_millis()
    );
    let pid = fs::read_to_string(&descendant).unwrap();
    #[cfg(target_os = "linux")]
    if let Ok(stat) = fs::read_to_string(format!("/proc/{}/stat", pid.trim())) {
        assert_eq!(stat.split(") ").nth(1).unwrap().chars().next(), Some('Z'));
    }
}

#[test]
fn graceful_int_and_term_stop_provider_groups_and_preserve_queued_work_during_blocked_poll() {
    for signal in ["INT", "TERM"] {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        let provider = dir.join("long-provider.sh");
        let leader = dir.join("leader");
        let descendant = dir.join("descendant");
        let _cleanup = ScratchProviderGroup(leader.clone());
        let stubborn = if signal == "INT" {
            "trap '' TERM\n"
        } else {
            ""
        };
        fs::write(
            &provider,
            format!(
                r#"/bin/cat >/dev/null
{stubborn}
printf '%s' "$$" > '{}'
/bin/sleep 30 &
printf '%s' "$!" > '{}'
wait
"#,
                leader.display(),
                descendant.display()
            ),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = stop.clone();
        let server = thread::spawn(move || {
            let mut got_update = false;
            while !server_stop.load(Ordering::SeqCst) {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                stream
                    .set_read_timeout(Some(Duration::from_millis(100)))
                    .unwrap();
                let mut buf = [0; 8192];
                let mut data = Vec::new();
                loop {
                    let n = stream.read(&mut buf).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    data.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&data);
                    if let Some((header, body)) = text.split_once("\r\n\r\n") {
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|s| s.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if body.len() >= len {
                            break;
                        }
                    }
                }
                let request = String::from_utf8_lossy(&data);
                let result = if request.starts_with("GET ") {
                    if got_update {
                        // Deliberately hold this HTTP operation until the runtime exits.
                        while !server_stop.load(Ordering::SeqCst) {
                            thread::sleep(Duration::from_millis(10));
                        }
                        break;
                    }
                    got_update = true;
                    r#"[{"update_id":1,"message":{"chat":{"id":321},"text":"active"}},{"update_id":2,"message":{"chat":{"id":321},"text":"queued"}}]"#
                } else if request.contains("setMyCommands") {
                    "true"
                } else {
                    r#"{"message_id":42}"#
                };
                let body = format!(r#"{{"ok":true,"result":{result}}}"#);
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        let mut process = Process(
            command(dir)
                .arg("run")
                .env("PATH", dir)
                .env("TELEGRAM_BOT_TOKEN", "fake")
                .env("TELEGRAM_CHAT_ID", "321")
                .env("TELEGRAM_API_BASE", base)
                .env("TELEGRAM_API_TIMEOUT_SECS", "30")
                .env("TELEGRAM_API_RETRY_ATTEMPTS", "0")
                .env("CODEX_BIN", format!("/bin/sh \"{}\"", provider.display()))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let queued = rusqlite::Connection::open(dir.join("state.db"))
                .ok()
                .and_then(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
                        .ok()
                })
                .unwrap_or(0);
            if descendant.exists() && queued == 2 {
                break;
            }
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "runtime exited before accepting work"
            );
            assert!(Instant::now() < deadline, "mock provider did not start");
            thread::sleep(Duration::from_millis(20));
        }
        let started = Instant::now();
        assert!(
            Command::new("/bin/kill")
                .args(["-s", signal, &process.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                started.elapsed() < Duration::from_secs(8),
                "unbounded graceful shutdown"
            );
            thread::sleep(Duration::from_millis(20));
        };
        stop.store(true, Ordering::SeqCst);
        server.join().unwrap();
        assert!(status.success());
        eprintln!(
            "graceful {signal}, blocked poll, stubborn={}: {} ms",
            signal == "INT",
            started.elapsed().as_millis()
        );
        for file in [&leader, &descendant] {
            let pid = fs::read_to_string(file).unwrap();
            #[cfg(target_os = "linux")]
            if let Ok(stat) = fs::read_to_string(format!("/proc/{}/stat", pid.trim())) {
                assert_eq!(
                    stat.split(") ").nth(1).unwrap().chars().next(),
                    Some('Z'),
                    "provider process survived shutdown"
                );
            }
        }
        let conn = rusqlite::Connection::open(dir.join("state.db")).unwrap();
        let statuses: Vec<(String, Option<String>)> = conn
            .prepare("SELECT status,started_at FROM task_runs ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(statuses[0].0, "cancelled");
        assert_eq!(statuses[1], ("queued".into(), None));
    }
}

#[test]
fn once_resumes_explicit_session_and_doctor_never_executes_codex() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = dir.join("codex-once-mock");
    fs::write(
        &provider,
        r#"#!/bin/sh
printf '%s\n' "$@" >> "$(dirname "$0")/args"
cat >/dev/null
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf 'TELEGRAM_REPLY: safe mocked response\n' > "$out"
printf '%s\n' '{"type":"thread.started","thread_id":"one-chat-thread"}'
"#,
    )
    .unwrap();
    fs::set_permissions(&provider, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(
        dir.join("config.toml"),
        format!("CODEX_BIN = \"{}\"\n", provider.display()),
    )
    .unwrap();
    let doctor = command(dir).args(["doctor", "--json"]).output().unwrap();
    assert!(
        doctor.status.success(),
        "{}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(report["runtime"], "codex");
    assert_eq!(report["transport"], "telegram-polling");
    assert!(!dir.join("args").exists());
    for prompt in ["first", "second"] {
        let output = command(dir)
            .args(["once", "--chat-id", "321", "--inject-text", prompt])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("safe mocked response"));
    }
    let args = fs::read_to_string(dir.join("args")).unwrap();
    assert_eq!(args.lines().filter(|s| *s == "resume").count(), 1);
    assert!(args.contains("resume\none-chat-thread\n"));
    assert!(!args.contains("--last"));
    let service = command(dir)
        .args(["service", "install", "--help"])
        .output()
        .unwrap();
    assert!(service.status.success());
    let help = String::from_utf8_lossy(&service.stdout);
    assert!(!help.contains("heartbeat"));
    assert!(!help.contains("reflection"));
}
