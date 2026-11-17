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
                    r#"[{"update_id":1,"message":{"from":{"id":321},"chat":{"id":321},"text":"hello"}}]"#
                }
            } else if text.contains("/setMyCommands ") {
                "true"
            } else if text.contains("/getMe ") {
                r#"{"is_bot":true,"username":"ActualBotName"}"#
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
    let candidate: String = conn
        .query_row(
            "SELECT content FROM memory_records WHERE scope='telegram:321'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(candidate, "retained memory");
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM memory_active", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
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

fn memory_once(
    dir: &std::path::Path,
    provider: &std::path::Path,
    chat: &str,
    text: &str,
) -> String {
    let result = command(dir)
        .env("CODEX_BIN", format!("/bin/sh \"{}\"", provider.display()))
        .args(["once", "--chat-id", chat, "--inject-text", text])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap()
}

fn memory_provider(dir: &std::path::Path) -> std::path::PathBuf {
    let provider = dir.join("memory-provider.sh");
    fs::write(
        &provider,
        r#"/bin/cat > "${0%/*}/memory-prompt"
printf '%s\n' "$@" > "${0%/*}/memory-args"
printf x >> "${0%/*}/memory-calls"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
  shift
done
printf 'TELEGRAM_REPLY: mock memory reply\nMEMORY_APPEND: orchard cultivar green\n' > "$out"
printf '%s\n' '{"type":"thread.started","thread_id":"memory-mock-session"}'
"#,
    )
    .unwrap();
    provider
}

#[test]
fn actual_once_memory_proposal_promote_search_get_correct_undo_and_direct_remember() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = memory_provider(dir);
    assert!(
        memory_once(dir, &provider, "321", "propose orchard note").contains("mock memory reply")
    );
    assert!(memory_once(dir, &provider, "321", "/memory list").contains("candidate"));
    assert!(memory_once(dir, &provider, "321", "/memory get 1").contains("orchard cultivar green"));
    assert!(!memory_once(dir, &provider, "321", "/memory search orchard").contains("cultivar"));
    assert!(memory_once(dir, &provider, "654", "/memory promote 1").contains("not found"));
    assert!(memory_once(dir, &provider, "321", "/memory promote 1").contains("applied"));
    assert!(
        memory_once(dir, &provider, "321", "/memory search orchard").contains("cultivar green")
    );
    assert!(!memory_once(dir, &provider, "654", "/memory get 1").contains("cultivar"));
    assert!(
        memory_once(
            dir,
            &provider,
            "321",
            "/memory correct 1 orchard cultivar crimson"
        )
        .contains("applied")
    );
    for addressed in [
        "/memory@ProbeBot remember synthetic addressed fact",
        "/memory@OtherBot remember unauthorized fact",
    ] {
        assert!(memory_once(dir, &provider, "321", addressed).contains("not verified"));
    }
    assert!(!memory_once(dir, &provider, "321", "/memory search green").contains("cultivar green"));
    assert!(memory_once(dir, &provider, "321", "/memory get 1").contains("not found"));
    assert!(memory_once(dir, &provider, "321", "/memory get 2").contains("crimson"));
    assert!(memory_once(dir, &provider, "321", "/memory undo").contains("applied"));
    assert!(memory_once(dir, &provider, "321", "/memory search green").contains("cultivar green"));
    assert!(
        !memory_once(dir, &provider, "321", "/memory search crimson").contains("cultivar crimson")
    );
    assert!(
        memory_once(
            dir,
            &provider,
            "321",
            "/memory remember prefer concise responses"
        )
        .contains("applied")
    );
    assert_eq!(
        fs::read_to_string(dir.join("memory-calls")).unwrap(),
        "x",
        "commands must not invoke Codex"
    );
    memory_once(dir, &provider, "321", "orchard");
    let prompt = fs::read_to_string(dir.join("memory-prompt")).unwrap();
    assert!(prompt.contains("prefer concise responses"));
    assert!(prompt.contains("orchard cultivar green"));
    assert!(
        !prompt.contains("orchard cultivar crimson"),
        "superseded command sources must not reenter prompt history"
    );
    assert!(
        !fs::read_to_string(dir.join("memory-args"))
            .unwrap()
            .lines()
            .any(|l| l == "resume"),
        "memory revision starts a fresh native session"
    );
    memory_once(dir, &provider, "321", "orchard");
    assert!(
        fs::read_to_string(dir.join("memory-args"))
            .unwrap()
            .lines()
            .any(|l| l == "resume"),
        "unchanged revision resumes its own session"
    );
    memory_once(
        dir,
        &provider,
        "321",
        "/memory correct 3 prefer detailed responses",
    );
    memory_once(dir, &provider, "321", "orchard");
    assert!(
        !fs::read_to_string(dir.join("memory-args"))
            .unwrap()
            .lines()
            .any(|l| l == "resume"),
        "correction must not resume a session with stale memory"
    );
    let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM memory_active", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM memory_history", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        5
    );
    assert_eq!(
        db.query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn actual_once_large_corpus_has_fixed_memory_budget_and_scoped_bounded_fetch() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = memory_provider(dir);
    assert!(
        command(dir)
            .args(["doctor", "--json"])
            .output()
            .unwrap()
            .status
            .success()
    );
    // Seed a heavy corpus with the same SQLite/index triggers the runtime uses.
    let mut db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    let tx = db.transaction().unwrap();
    tx.execute("INSERT INTO turns(ts,chat_id,input_type,provider_raw,status,channel) VALUES('now','telegram:321','text','','ok','telegram')", []).unwrap();
    let source = tx.last_insert_rowid();
    tx.execute("INSERT INTO turns(ts,chat_id,input_type,provider_raw,status,channel) VALUES('now','telegram:654','text','','ok','telegram')", []).unwrap();
    let other_source = tx.last_insert_rowid();
    let bulk = format!(
        "orchard {} full-source-tail",
        "b".repeat(2048 - "orchard  full-source-tail".len())
    );
    assert_eq!(bulk.len(), 2048);
    for index in 0..5000 {
        let scope = if index % 2 == 0 {
            "telegram:321"
        } else {
            "telegram:654"
        };
        tx.execute(
            "INSERT INTO memory_records(scope,source_turn,content) VALUES(?1,?2,?3)",
            rusqlite::params![
                scope,
                if index % 2 == 0 { source } else { other_source },
                bulk
            ],
        )
        .unwrap();
        let id = tx.last_insert_rowid();
        tx.execute("INSERT INTO memory_active(record_id) VALUES(?1)", [id])
            .unwrap();
    }
    for _ in 0..8 {
        tx.execute("INSERT INTO memory_records(scope,source_turn,content,curated) VALUES('telegram:321',?1,?2,1)", rusqlite::params![source, format!("profile {}", "\"".repeat(504))]).unwrap();
        let id = tx.last_insert_rowid();
        tx.execute("INSERT INTO memory_active(record_id) VALUES(?1)", [id])
            .unwrap();
    }
    tx.execute(
        "INSERT INTO memory_records(scope,source_turn,content) VALUES('telegram:321',?1,?2)",
        rusqlite::params![source, "\"".repeat(2048)],
    )
    .unwrap();
    let escaped_id = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO memory_active(record_id) VALUES(?1)",
        [escaped_id],
    )
    .unwrap();
    tx.commit().unwrap();
    // Old unscoped files are not read or rewritten, regardless of their size.
    let manual = "UNSCOPED_MANUAL_SENTINEL".repeat(100_000);
    fs::write(dir.join("MEMORY.md"), &manual).unwrap();
    fs::write(dir.join("USER.md"), &manual).unwrap();
    let start = Instant::now();
    memory_once(dir, &provider, "321", "orchard");
    let prompt = fs::read_to_string(dir.join("memory-prompt")).unwrap();
    let memory = prompt
        .split("## Curated memory data")
        .nth(1)
        .unwrap()
        .split("## Session pending tasks")
        .next()
        .unwrap();
    assert!(
        memory.len() <= 4700,
        "fixed payload budgets plus fixed headings: {}",
        memory.len()
    );
    assert!(
        !prompt.contains("full-source-tail"),
        "search injects snippets, never whole records"
    );
    assert!(!prompt.contains("UNSCOPED_MANUAL_SENTINEL"));
    assert_eq!(fs::read_to_string(dir.join("MEMORY.md")).unwrap(), manual);
    let search = memory_once(dir, &provider, "321", "/memory search orchard");
    assert!(search.len() <= 2100);
    assert!(search.lines().filter(|line| line.starts_with('#')).count() <= 8);
    assert!(!search.contains("full-source-tail"));
    let boundary_query = format!("orchard {}", "z".repeat(504));
    assert_eq!(boundary_query.len(), 512);
    assert!(
        memory_once(
            dir,
            &provider,
            "321",
            &format!("/memory search {boundary_query}")
        )
        .contains("orchard")
    );
    let get = memory_once(dir, &provider, "321", "/memory get 1");
    assert!(get.contains("full-source-tail"));
    assert!(get.len() <= 8192);
    let escaped_get = memory_once(dir, &provider, "321", &format!("/memory get {escaped_id}"));
    assert!(escaped_get.len() > 4096 && escaped_get.len() <= 8192);
    assert!(!memory_once(dir, &provider, "654", "/memory get 1").contains("full-source-tail"));
    let listed = memory_once(dir, &provider, "321", "/memory list 5");
    assert!(listed.contains("#3"));
    assert!(!listed.contains("#5"));
    assert!(
        memory_once(
            dir,
            &provider,
            "321",
            &format!("/memory search {}", "x".repeat(513))
        )
        .contains("exceeds")
    );
    assert!(memory_once(dir, &provider, "321", "/memory search \" : * NEAR(").len() <= 2100);
    eprintln!(
        "memory heavy fresh-process path: {} ms; memory context {} bytes; search {} bytes; get {} bytes; escaped get {} bytes; 5000 max-size bulk + 8 max-size curated + 1 max-escaped record",
        start.elapsed().as_millis(),
        memory.len(),
        search.len(),
        get.len(),
        escaped_get.len()
    );
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
#[ignore = "real wall-clock regression takes over five minutes; run explicitly for timeout acceptance"]
fn activity_progress_survives_former_five_minute_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = dir.join("progress.sh");
    fs::write(
        &provider,
        r#"/bin/cat >/dev/null
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
  shift
done
for id in 1 2 3 4 5 6; do
  printf '{"type":"item.completed","item":{"id":"r%s","type":"reasoning","text":"working"}}\n' "$id"
  /bin/sleep 51
done
printf 'TELEGRAM_REPLY: survived former five-minute cutoff\n' > "$out"
"#,
    )
    .unwrap();
    let stdout = fs::File::create(dir.join("stdout")).unwrap();
    let start = Instant::now();
    let mut process = Process(
        command(dir)
            .env("CODEX_BIN", format!("/bin/sh \"{}\"", provider.display()))
            // No timeout overrides: exercise generated defaults through real CLI.
            .args([
                "once",
                "--chat-id",
                "321",
                "--inject-text",
                "synthetic timeout acceptance",
            ])
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
        assert!(start.elapsed() < Duration::from_secs(330));
        thread::sleep(Duration::from_millis(100));
    }
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_secs(306));
    assert!(
        fs::read_to_string(dir.join("stdout"))
            .unwrap()
            .contains("survived former five-minute cutoff")
    );
    let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    let (status, duration): (String, i64) = db
        .query_row(
            "SELECT status, duration_ms FROM turns ORDER BY id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "ok");
    assert!(duration > 300_000);
    eprintln!(
        "actual CLI progress acceptance: {} ms; persisted turn duration: {duration} ms",
        elapsed.as_millis()
    );
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
fn once_older_unfinished_replays_persist_inactivity_not_success() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = dir.join("replay.sh");
    fs::write(&provider, r#"/bin/cat >/dev/null
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
  shift
done
for iteration in 1 2 3 4 5 6 7 8; do
  printf '%s\n' '{"type":"item.updated","item":{"id":"c","type":"command_execution","command":"build","status":"in_progress","aggregated_output":"A"}}'
  /bin/sleep 0.2
  printf '%s\n' '{"type":"item.updated","item":{"id":"c","type":"command_execution","command":"build","status":"in_progress","aggregated_output":"AB"}}'
  /bin/sleep 0.2
done
printf 'TELEGRAM_REPLY: replay-only success\n' > "$out"
"#).unwrap();
    let stdout = fs::File::create(dir.join("stdout")).unwrap();
    let start = Instant::now();
    let mut process = Process(
        command(dir)
            .env("CODEX_BIN", format!("/bin/sh \"{}\"", provider.display()))
            .env("PROVIDER_TIMEOUT_SECS", "0")
            .env("PROVIDER_IDLE_TIMEOUT_SECS", "1")
            .env("PROVIDER_TOOL_IDLE_TIMEOUT_SECS", "2")
            .args([
                "once",
                "--chat-id",
                "321",
                "--inject-text",
                "synthetic replay",
            ])
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
        assert!(start.elapsed() < Duration::from_secs(6));
        thread::sleep(Duration::from_millis(20));
    }
    let output = fs::read_to_string(dir.join("stdout")).unwrap();
    assert!(output.contains("inactivity"), "{output}");
    assert!(!output.contains("replay-only success"));
    let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    let (status, duration): (String, i64) = db
        .query_row(
            "SELECT status, duration_ms FROM turns ORDER BY id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_ne!(status, "ok");
    assert!((2000..4000).contains(&duration), "{duration}");
    eprintln!("CLI replay: status={status}, persisted duration={duration}ms");
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
                    r#"[{"update_id":1,"message":{"from":{"id":321},"chat":{"id":321},"text":"active"}},{"update_id":2,"message":{"from":{"id":321},"chat":{"id":321},"text":"queued"}}]"#
                } else if request.contains("setMyCommands") {
                    "true"
                } else if request.contains("/getMe ") {
                    r#"{"is_bot":true,"username":"ActualBotName"}"#
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
