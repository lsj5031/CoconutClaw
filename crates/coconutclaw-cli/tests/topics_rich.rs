#![cfg(unix)]
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

struct Runtime(Child, PathBuf);

fn process_alive(pid: &str) -> bool {
    let pid: i32 = pid.trim().parse().unwrap();
    assert!(pid > 1);
    if unsafe { libc::kill(pid, 0) } == 0 {
        true
    } else {
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        false
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        if thread::panicking() {
            if let Ok(log) = fs::read_to_string(&self.1) {
                eprintln!("synthetic runtime diagnostics:\n{log}");
            }
            if let Some(dir) = self.1.parent()
                && let Ok(counter) = fs::read_to_string(dir.join("counter.json"))
            {
                eprintln!("synthetic provider counter: {counter}");
            }
        }
    }
}
#[track_caller]
fn wait(mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !check() {
        assert!(Instant::now() < deadline, "mock acceptance timed out");
        thread::sleep(Duration::from_millis(20));
    }
}
fn reply_text(entry: &Value) -> String {
    entry["params"]["text"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| {
            entry["params"]["rich_message"]
                .as_str()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .and_then(|rich| rich["markdown"].as_str().map(str::to_owned))
                .unwrap_or_default()
        })
}
fn stop(runtime: &mut Runtime) {
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &runtime.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    wait(|| runtime.0.try_wait().unwrap().is_some());
    assert!(runtime.0.wait().unwrap().success());
}

struct Telegram {
    base: String,
    updates: Arc<Mutex<Vec<Value>>>,
    transcript: Arc<Mutex<Vec<Value>>>,
    topics: Arc<AtomicBool>,
    stopping: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

fn configure_mock_connection(stream: &TcpStream) {
    // Darwin inherits O_NONBLOCK from the listener; Linux does not. The
    // request reader needs blocking reads to wait for headers and body.
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
}

#[test]
fn mock_connection_waits_for_request_after_nonblocking_accept() {
    use std::os::fd::AsRawFd;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut connection, _) = listener.accept().unwrap();
    // Reproduce Darwin's accepted descriptor state on every Unix platform.
    connection.set_nonblocking(true).unwrap();
    let flags = unsafe { libc::fcntl(connection.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_ne!(flags & libc::O_NONBLOCK, 0);
    assert_eq!(
        connection.read(&mut [0; 1]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    configure_mock_connection(&connection);
    let flags = unsafe { libc::fcntl(connection.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(flags & libc::O_NONBLOCK, 0);
    let sender = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        client.write_all(b"GET /").unwrap();
    });
    let mut request = [0; 5];
    connection.read_exact(&mut request).unwrap();
    assert_eq!(&request, b"GET /");
    sender.join().unwrap();
}

impl Drop for Telegram {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.worker.take().unwrap().join().unwrap();
    }
}
impl Telegram {
    fn start() -> Self {
        Self::start_with_retry_after(None)
    }

    fn start_with_retry_after(rate_method: Option<&'static str>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let updates = Arc::new(Mutex::new(Vec::<Value>::new()));
        let transcript = Arc::new(Mutex::new(Vec::<Value>::new()));
        let topics = Arc::new(AtomicBool::new(true));
        let stopping = Arc::new(AtomicBool::new(false));
        let (input, seen, mode, quit) = (
            updates.clone(),
            transcript.clone(),
            topics.clone(),
            stopping.clone(),
        );
        let worker = thread::spawn(move || {
            let clock = Instant::now();
            let mut rate_attempts = 0;
            let mut intake_id = 1u64;
            while !quit.load(Ordering::SeqCst) {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                configure_mock_connection(&stream);
                let mut data = Vec::new();
                let mut buffer = [0; 8192];
                loop {
                    let n = stream.read(&mut buffer).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    data.extend_from_slice(&buffer[..n]);
                    let text = String::from_utf8_lossy(&data);
                    if let Some((header, body)) = text.split_once("\r\n\r\n") {
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if body.len() >= length {
                            break;
                        }
                    }
                }
                let request = String::from_utf8_lossy(&data);
                let request_at = clock.elapsed().as_secs_f64();
                let request_wall = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();
                let Some(path) = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                else {
                    continue;
                };
                let method = path
                    .split('/')
                    .next_back()
                    .unwrap()
                    .split('?')
                    .next()
                    .unwrap();
                let body = request
                    .split_once("\r\n\r\n")
                    .map(|(_, body)| body)
                    .unwrap_or("");
                let params: std::collections::HashMap<String, String> =
                    reqwest::Url::parse(&format!("http://mock/?{body}"))
                        .unwrap()
                        .query_pairs()
                        .into_owned()
                        .collect();
                let result = if method == "getUpdates" {
                    let url = reqwest::Url::parse(&format!("http://mock{path}")).unwrap();
                    let offset = url
                        .query_pairs()
                        .find(|(key, _)| key == "offset")
                        .and_then(|(_, value)| value.parse::<u64>().ok())
                        .unwrap_or(0);
                    if rate_method.is_some() {
                        // Exercise ordinary retry entry calls under continuous
                        // authorized intake, not the idle one-second sleep.
                        thread::sleep(Duration::from_millis(10));
                        intake_id = intake_id.max(offset);
                        let update = json!({"update_id":intake_id,"message":{"chat":{"id":321951227,"type":"private"},"message_thread_id":8,"text":"/tasks"}});
                        intake_id += 1;
                        json!([update])
                    } else {
                        Value::Array(
                            input
                                .lock()
                                .unwrap()
                                .iter()
                                .filter(|update| update["update_id"].as_u64().unwrap() >= offset)
                                .cloned()
                                .collect(),
                        )
                    }
                } else if method == "getMe" {
                    json!({"is_bot":true,"username":"TopicsMockBot","has_topics_enabled":mode.load(Ordering::SeqCst),"allows_users_to_create_topics":true})
                } else if method == "setMyCommands" {
                    json!(true)
                } else {
                    assert_eq!(params.get("chat_id").map(String::as_str), Some("321951227"));
                    json!({"message_id":seen.lock().unwrap().len()+1})
                };
                // Busy intake has its own durable command acknowledgments on8.
                // Rate-limit only the original target operation on7.
                let limited = rate_method == Some(method)
                    && params.get("message_thread_id").map(String::as_str) == Some("7");
                if limited && rate_attempts == 0 {
                    // Force the reported boundary condition at response time.
                    loop {
                        let fraction = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .subsec_millis();
                        if (800..850).contains(&fraction) {
                            break;
                        }
                        thread::sleep(Duration::from_millis(1));
                    }
                }
                if limited {
                    rate_attempts += 1;
                }
                let response_at = clock.elapsed().as_secs_f64();
                let response_wall = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();
                if method != "getUpdates" || rate_method.is_some() {
                    seen.lock().unwrap().push(
                        json!({"method":method,"params":params,"status":if limited {429} else {200},
                            "request_monotonic":request_at,"request_wall":request_wall,
                            "response_monotonic":response_at,"response_wall":response_wall}),
                    );
                }
                let (status, body) = if limited {
                    (
                        429,
                        json!({"ok":false,"error_code":429,"parameters":{"retry_after":1}})
                            .to_string(),
                    )
                } else {
                    (200, json!({"ok":true,"result":result}).to_string())
                };
                let response = format!(
                    "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Self {
            base,
            updates,
            transcript,
            topics,
            stopping,
            worker: Some(worker),
        }
    }
    fn message(&self, id: u64, topic: Option<u64>, text: &str) {
        let mut message = json!({"chat":{"id":321951227,"type":"private"},"text":text});
        if let Some(topic) = topic {
            message["message_thread_id"] = topic.into();
        }
        self.updates
            .lock()
            .unwrap()
            .push(json!({"update_id":id,"message":message}));
    }
}

fn spawn(dir: &Path, telegram: &Telegram, provider: &Path) -> Runtime {
    let binary = std::env::var_os("COCONUTCLAW_TEST_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_coconutclaw")));
    let log = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS")
        .map(|artifacts| {
            let artifacts = PathBuf::from(artifacts);
            fs::create_dir_all(&artifacts).unwrap();
            artifacts.join(format!(
                "runtime-{}.log",
                dir.file_name().unwrap().to_string_lossy()
            ))
        })
        .unwrap_or_else(|| dir.join("runtime.log"));
    Runtime(
        Command::new(binary)
            .arg("--data-dir")
            .arg(dir)
            .arg("run")
            .env_clear()
            .env("HOME", dir)
            .env("PATH", "/usr/bin:/bin")
            .env("FACTORY_AIRGAP_ENABLED", "1")
            .env("TELEGRAM_BOT_TOKEN", "synthetic-mock-token")
            .env("TELEGRAM_CHAT_ID", "321951227")
            .env("TELEGRAM_API_BASE", &telegram.base)
            .env("TELEGRAM_API_RETRY_ATTEMPTS", "0")
            .env(
                "CODEX_BIN",
                format!("/usr/bin/python3 {}", provider.display()),
            )
            .env("POLL_INTERVAL_SECONDS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log)
                    .unwrap(),
            ))
            .spawn()
            .unwrap(),
        log,
    )
}

#[test]
fn polling_topics_rich_five_actual_providers_and_mode_transitions() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = dir.join("provider.py");
    fs::write(&provider, r#"import os,sys,re,time,pathlib,json,fcntl
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
out=args[args.index("--output-last-message")+1]
prompt=sys.stdin.read()
n=re.findall(r"gate-(\d+)",prompt)[-1]
def alive(pid):
 try: os.kill(pid,0)
 except ProcessLookupError: return False
 return True
(root/("prompt-"+n)).write_text(prompt)
(root/("args-"+n)).write_text("\n".join(args))
with (root/"counter.lock").open("w") as lock:
 fcntl.flock(lock,fcntl.LOCK_EX)
 file=root/"counter.json"
 state=json.loads(file.read_text()) if file.exists() else {"pids":[],"peak":0,"starts":[]}
 state["pids"]=[pid for pid in state["pids"] if alive(pid)]
 state["pids"].append(os.getpid())
 state["peak"]=max(state["peak"],len(state["pids"]))
 state["starts"].append(int(n))
 file.write_text(json.dumps(state))
(root/("started-"+n)).write_text(str(os.getpid()))
while not (root/("release-"+n)).exists(): time.sleep(.025)
pathlib.Path(out).write_text("TELEGRAM_REPLY: # Result "+n+"\n\n|A|B|\n|---|---|\n|🥥|2|\n\n$$x^2$$\nMEMORY_APPEND: topic-fact-"+n)
print(json.dumps({"type":"thread.started","thread_id":"native-"+n}))
"#).unwrap();
    let telegram = Telegram::start();
    for n in 1..=6 {
        telegram.message(n, Some(n), &format!("gate-{n}"));
    }
    telegram.message(7, Some(1), "gate-7");
    let mut runtime = spawn(dir, &telegram, &provider);
    wait(|| (1..=5).all(|n| dir.join(format!("started-{n}")).exists()));
    let alive = |n| {
        fs::read_to_string(dir.join(format!("started-{n}")))
            .ok()
            .is_some_and(|pid| process_alive(&pid))
    };
    assert!((1..=5).all(alive), "all five actual providers overlap");
    assert!(!dir.join("started-6").exists());
    assert!(!dir.join("started-7").exists());
    let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    wait(|| {
        db.query_row(
            "SELECT COUNT(*) FROM task_runs WHERE status='queued'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            == 2
    });
    let first: i64 = db
        .query_row("SELECT id FROM task_runs WHERE update_id='1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    telegram.message(8, Some(3), &format!("/cancel {first}"));
    wait(|| {
        telegram.transcript.lock().unwrap().iter().any(|entry| {
            entry["params"]["message_thread_id"] == "3"
                && reply_text(entry).contains("not active for this chat")
        })
    });
    assert!((1..=5).all(alive));
    telegram.updates.lock().unwrap().push(json!({"update_id":9,"callback_query":{"data":"cancel","message":{"chat":{"id":321951227,"type":"private"},"message_thread_id":2}}}));
    wait(|| dir.join("started-6").exists());
    assert!(!alive(2));
    assert!([1, 3, 4, 5, 6].into_iter().all(alive));
    telegram.message(10, Some(1), "/cancel");
    wait(|| dir.join("started-7").exists());
    assert!(!alive(1));
    assert!([3, 4, 5, 6, 7].into_iter().all(alive));
    assert!(
        !fs::read_to_string(dir.join("prompt-7"))
            .unwrap()
            .contains("gate-2")
    );
    telegram.message(11, Some(8), "gate-8");
    wait(|| {
        db.query_row(
            "SELECT COUNT(*) FROM task_runs WHERE update_id='11' AND status='queued'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            == 1
    });
    stop(&mut runtime);
    assert!(!(1..=7).any(alive));
    assert!(!dir.join("started-8").exists());
    telegram.topics.store(false, Ordering::SeqCst);
    telegram.message(12, None, "gate-9");
    telegram.message(13, None, "gate-10");
    telegram.message(14, Some(9), "gate-99");
    let mut ordinary = spawn(dir, &telegram, &provider);
    wait(|| dir.join("started-9").exists());
    assert!(!dir.join("started-8").exists());
    assert!(!dir.join("started-10").exists());
    assert!(!dir.join("started-99").exists());
    fs::write(dir.join("release-9"), "").unwrap();
    wait(|| dir.join("started-10").exists());
    assert!(!alive(9));
    fs::write(dir.join("release-10"), "").unwrap();
    wait(|| {
        db.query_row("SELECT COUNT(*) FROM task_payloads p JOIN task_runs r ON r.id=p.task_run_id WHERE r.update_id='13' AND p.delivered=1", [], |r| r.get::<_,i64>(0)).unwrap() == 1
    });
    let root_prompt = fs::read_to_string(dir.join("prompt-10")).unwrap();
    assert!(root_prompt.contains("gate-9"));
    assert!(!root_prompt.contains("gate-7"));
    assert!(
        fs::read_to_string(dir.join("args-10"))
            .unwrap()
            .contains("native-9")
    );
    stop(&mut ordinary);
    telegram.topics.store(true, Ordering::SeqCst);
    let mut resumed = spawn(dir, &telegram, &provider);
    wait(|| dir.join("started-8").exists());
    fs::write(dir.join("release-8"), "").unwrap();
    wait(|| {
        db.query_row("SELECT COUNT(*) FROM task_payloads p JOIN task_runs r ON r.id=p.task_run_id WHERE r.update_id='11' AND p.delivered=1", [], |r| r.get::<_,i64>(0)).unwrap() == 1
    });
    telegram.message(15, Some(8), "gate-11");
    wait(|| dir.join("started-11").exists());
    assert!(
        fs::read_to_string(dir.join("args-11"))
            .unwrap()
            .contains("native-8")
    );
    let topic_prompt = fs::read_to_string(dir.join("prompt-11")).unwrap();
    assert!(topic_prompt.contains("gate-8"));
    assert!(!topic_prompt.contains("gate-10"));
    fs::write(dir.join("release-11"), "").unwrap();
    wait(|| {
        db.query_row("SELECT COUNT(*) FROM task_payloads p JOIN task_runs r ON r.id=p.task_run_id WHERE r.update_id='15' AND p.delivered=1", [], |r| r.get::<_,i64>(0)).unwrap() == 1
    });
    stop(&mut resumed);
    assert!(!(1..=11).any(alive));
    let counters: Value =
        serde_json::from_str(&fs::read_to_string(dir.join("counter.json")).unwrap()).unwrap();
    assert_eq!(counters["peak"], 5);
    let starts = counters["starts"].as_array().unwrap();
    let mut first_five: Vec<_> = starts[..5].iter().map(|n| n.as_u64().unwrap()).collect();
    first_five.sort();
    assert_eq!(first_five, [1, 2, 3, 4, 5]);
    assert_eq!(
        &starts[5..],
        &[json!(6), json!(7), json!(9), json!(10), json!(8), json!(11)]
    );
    let scope: String = db
        .query_row(
            "SELECT scope FROM memory_records WHERE content='topic-fact-8'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(scope, "telegram:321951227#8");
    let transport: (String, String) = db
        .query_row(
            "SELECT telegram_chat_id,telegram_message_thread_id FROM turns WHERE update_id='11'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(transport, ("321951227".into(), "8".into()));
    let transport: (String, String) = db
        .query_row(
            "SELECT source_chat_id,source_thread_id FROM task_runs WHERE update_id='11'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(transport, ("321951227".into(), "8".into()));
    let transcript = telegram.transcript.lock().unwrap();
    let finals: Vec<_> = transcript
        .iter()
        .filter(|entry| entry["method"] == "sendRichMessage")
        .collect();
    assert!(
        finals
            .iter()
            .any(|entry| entry["params"]["message_thread_id"] == "8")
    );
    assert!(
        finals
            .iter()
            .any(|entry| entry["params"].get("message_thread_id").is_none())
    );
    assert!(
        finals
            .iter()
            .all(|entry| entry["params"].get("parse_mode").is_none())
    );
    if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
        let artifacts = PathBuf::from(artifacts);
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(
            artifacts.join("binary-transcript.json"),
            serde_json::to_string_pretty(&*transcript).unwrap(),
        )
        .unwrap();
        fs::write(
            artifacts.join("provider-counters.json"),
            serde_json::to_string_pretty(&counters).unwrap(),
        )
        .unwrap();
    }
    eprintln!(
        "binary topics/rich acceptance: peak 5 actual provider processes; sixth durable; topic/root FIFO; isolated cancel; ON/OFF/ON recovery; native rich and scoped memory passed"
    );
}

#[test]
fn polling_session_task_context_isolation_survives_restart_and_topic_mode_transitions() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    fs::create_dir(dir.join("TASKS")).unwrap();
    let prefix = "# Operator checklist\r\nmanual-private-sentinel  \r\n";
    let suffix = "\r\nmanual-footer-sentinel\t\r\n";
    let start = "<!-- COCONUTCLAW:MANAGED:TASKS:START -->";
    let end = "<!-- COCONUTCLAW:MANAGED:TASKS:END -->";
    fs::write(
        dir.join("TASKS/pending.md"),
        format!("{prefix}{start}\n{end}{suffix}"),
    )
    .unwrap();
    let provider = dir.join("capture-provider.py");
    fs::write(&provider, r#"import sys,pathlib,re
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
out=args[args.index("--output-last-message")+1]
prompt=sys.stdin.read()
n=re.findall(r"capture-(\d+)",prompt)[-1]
(root/("captured-"+n)).write_text(prompt)
(root/("capture-args-"+n)).write_text("\n".join(args))
pathlib.Path(out).write_text("TELEGRAM_REPLY: captured" + ("\nTASK_APPEND: topic7-private-task-sentinel" if n=="1" else ""))
print('{"type":"thread.started","thread_id":"scoped-native-'+n+'"}')
"#).unwrap();
    let telegram = Telegram::start();
    let mut runtime = spawn(dir, &telegram, &provider);
    let db_path = dir.join("state.db");
    wait(|| db_path.exists());
    wait(|| {
        telegram
            .transcript
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry["method"] == "setMyCommands")
    });
    let db = rusqlite::Connection::open(&db_path).unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    // Old native transcripts may already contain the shared projection. Keep
    // their IDs intact, but never resume them into the scoped context contract.
    for key in [
        "codex_session:telegram:321951227#8:0",
        "codex_session:telegram:321951227:0",
    ] {
        db.execute(
            "INSERT INTO kv(key,value) VALUES(?1,'pre-isolation-native-sentinel')",
            [key],
        )
        .unwrap();
    }
    let capture = |id, topic| {
        telegram.message(id, topic, &format!("capture-{id}"));
        wait(|| {
            db.query_row(
            "SELECT COUNT(*) FROM task_payloads p JOIN task_runs r ON r.id=p.task_run_id WHERE r.update_id=?1 AND p.delivered=1",
            [id.to_string()], |r| r.get::<_,i64>(0)).unwrap() == 1
        });
    };
    capture(1, Some(7));
    capture(2, Some(8));
    capture(3, None);
    stop(&mut runtime);
    // Startup side-effect reconciliation and its projection use the persisted
    // source turn, not whichever session next asks for context.
    db.execute("INSERT INTO turns(ts,chat_id,input_type,user_text,provider_raw,telegram_reply,status,channel,side_effects_applied) VALUES('now','telegram:321951227#7','text','recovered','TELEGRAM_REPLY: recovered\nTASK_APPEND: topic7-recovered-task-sentinel','recovered','ok','telegram',0)", []).unwrap();
    let mut restarted = spawn(dir, &telegram, &provider);
    capture(4, Some(8));
    capture(5, Some(7));
    capture(6, None);
    stop(&mut restarted);
    telegram.topics.store(false, Ordering::SeqCst);
    let mut ordinary = spawn(dir, &telegram, &provider);
    capture(7, None);
    stop(&mut ordinary);
    telegram.topics.store(true, Ordering::SeqCst);
    let mut resumed = spawn(dir, &telegram, &provider);
    capture(8, Some(8));
    capture(9, None);
    capture(10, Some(7));
    stop(&mut resumed);
    let captures: Vec<_> = (1..=10)
        .map(|id| fs::read_to_string(dir.join(format!("captured-{id}"))).unwrap())
        .collect();
    let pending = fs::read_to_string(dir.join("TASKS/pending.md")).unwrap();
    if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
        let artifacts = PathBuf::from(artifacts).join("session-task-context");
        fs::create_dir_all(&artifacts).unwrap();
        for (index, prompt) in captures.iter().enumerate() {
            fs::write(
                artifacts.join(format!("captured-{}.txt", index + 1)),
                prompt,
            )
            .unwrap();
            fs::copy(
                dir.join(format!("capture-args-{}", index + 1)),
                artifacts.join(format!("args-{}.txt", index + 1)),
            )
            .unwrap();
        }
        fs::write(artifacts.join("operator-pending.md"), &pending).unwrap();
        fs::write(
            artifacts.join("transcript.json"),
            serde_json::to_string_pretty(&*telegram.transcript.lock().unwrap()).unwrap(),
        )
        .unwrap();
    }
    assert!(pending.starts_with(prefix));
    assert!(pending.ends_with(suffix));
    assert!(pending.contains("topic7-private-task-sentinel"));
    assert!(pending.contains("topic7-recovered-task-sentinel"));
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM kv WHERE value='pre-isolation-native-sentinel'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    for id in 1..=10 {
        assert!(
            !fs::read_to_string(dir.join(format!("capture-args-{id}")))
                .unwrap()
                .contains("pre-isolation-native-sentinel"),
            "pre-fix shared context resumed into capture {id}"
        );
    }
    assert!(
        fs::read_to_string(dir.join("capture-args-4"))
            .unwrap()
            .contains("scoped-native-2")
    );
    assert!(
        fs::read_to_string(dir.join("capture-args-7"))
            .unwrap()
            .contains("scoped-native-6")
    );
    assert!(
        fs::read_to_string(dir.join("capture-args-10"))
            .unwrap()
            .contains("scoped-native-5")
    );
    for (index, prompt) in captures.iter().enumerate() {
        let id = index + 1;
        assert!(
            !prompt.contains("manual-private-sentinel"),
            "manual bytes implicitly injected into capture {id}"
        );
        assert!(!prompt.contains("manual-footer-sentinel"));
        let owner = matches!(id, 5 | 10);
        assert_eq!(
            prompt.contains("topic7-private-task-sentinel"),
            owner,
            "task crossed session in capture {id}"
        );
        assert_eq!(
            prompt.contains("topic7-recovered-task-sentinel"),
            owner,
            "recovered task crossed session in capture {id}"
        );
    }
}

#[test]
fn polling_native_rich_prompt_contract_is_independent_of_regular_parse_mode() {
    let mut captures = Vec::new();
    let markdown = "# Heading\n\n| Name | Value |\n|---|---|\n| 🥥 | 2 |\n\n```rust\nlet x = 1;\n```\n\n$x^2$\n\n<details><summary>More</summary>\n\n- item\n\n</details>";
    for mode in ["off", "Html", "MarkdownV2"] {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        fs::write(
            dir.join("config.toml"),
            format!("TELEGRAM_PARSE_MODE = \"{mode}\"\n"),
        )
        .unwrap();
        let provider = dir.join("rich-prompt-provider.py");
        fs::write(
            &provider,
            format!(
                r#"import sys,pathlib
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
out=args[args.index("--output-last-message")+1]
(root/"captured-prompt").write_text(sys.stdin.read())
pathlib.Path(out).write_text("TELEGRAM_REPLY: "+{})
"#,
                serde_json::to_string(markdown).unwrap()
            ),
        )
        .unwrap();
        let telegram = Telegram::start();
        telegram.message(1, Some(7), "Please show a structured explanation.");
        let mut runtime = spawn(dir, &telegram, &provider);
        wait(|| {
            telegram
                .transcript
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry["method"] == "setMyCommands")
        });
        let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
        db.busy_timeout(Duration::from_secs(5)).unwrap();
        wait(|| {
            db.query_row(
                "SELECT COUNT(*) FROM task_payloads WHERE delivered=1",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
                == 1
        });
        stop(&mut runtime);
        let prompt = fs::read_to_string(dir.join("captured-prompt")).unwrap();
        let transcript = telegram.transcript.lock().unwrap().clone();
        if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
            let artifacts = PathBuf::from(artifacts).join("rich-prompt-contract");
            fs::create_dir_all(&artifacts).unwrap();
            fs::write(artifacts.join(format!("prompt-{mode}.txt")), &prompt).unwrap();
            fs::write(
                artifacts.join(format!("transcript-{mode}.json")),
                serde_json::to_string_pretty(&transcript).unwrap(),
            )
            .unwrap();
        }
        captures.push((mode, prompt, transcript));
    }
    for (mode, prompt, transcript) in captures {
        assert!(
            prompt.contains("Use native Rich Markdown inside TELEGRAM_REPLY"),
            "{mode}"
        );
        for example in [
            "**bold**",
            "# Heading",
            "| Name | Value |",
            "```rust",
            "$x^2$",
            "<details><summary>",
        ] {
            assert!(prompt.contains(example), "{mode}: {example}");
        }
        for marker in [
            "TELEGRAM_REPLY:",
            "SEND_PHOTO:",
            "SEND_DOCUMENT:",
            "SEND_VIDEO:",
            "MEMORY_APPEND:",
            "TASK_APPEND:",
            "DELEGATE_TASK:",
            "SEND_APPROVAL:",
        ] {
            assert!(prompt.contains(marker), "{mode}: {marker}");
        }
        for old in [
            "Do not use markdown",
            "Do not use CommonMark syntax",
            "MarkdownV2 is enabled",
            "automatically convert Markdown",
        ] {
            assert!(!prompt.contains(old), "{mode}: stale instruction {old}");
        }
        assert!(prompt.contains("Keep marker prefixes plain and unchanged"));
        assert!(prompt.contains("Do not embed media, buttons, callbacks"));
        let finals: Vec<_> = transcript
            .iter()
            .filter(|entry| entry["method"] == "sendRichMessage")
            .collect();
        assert_eq!(finals.len(), 1, "{mode}");
        assert_eq!(finals[0]["params"]["message_thread_id"], "7");
        assert!(finals[0]["params"].get("parse_mode").is_none());
        let rich: Value =
            serde_json::from_str(finals[0]["params"]["rich_message"].as_str().unwrap()).unwrap();
        assert_eq!(rich, json!({"markdown":markdown}));
    }
}

#[test]
fn polling_topic_approval_survives_restart_and_denies_other_topic() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let provider = dir.join("approval-provider.py");
    fs::write(&provider, r#"import sys,pathlib,os
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
out=args[args.index("--output-last-message")+1]
prompt=sys.stdin.read()
approved="Administrator approval granted" in prompt
name="approved" if approved else "initial"
(root/(name+"-prompt")).write_text(prompt)
(root/(name+"-args")).write_text("\n".join(args))
(root/(name+"-pid")).write_text(str(os.getpid()))
with (root/"calls").open("a") as calls: calls.write(name+"\n")
pathlib.Path(out).write_text("TELEGRAM_REPLY: approved result" if approved else "SEND_APPROVAL: Continue approved work?")
print('{"type":"thread.started","thread_id":"approval-native"}')
"#).unwrap();
    let telegram = Telegram::start();
    telegram.message(1, Some(7), "approval request");
    let mut runtime = spawn(dir, &telegram, &provider);
    wait(|| {
        telegram
            .transcript
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry["method"] == "setMyCommands")
    });
    let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    wait(|| {
        db.query_row("SELECT COUNT(*) FROM approvals a JOIN task_payloads p ON p.task_run_id=a.task_run_id WHERE a.status='pending' AND p.delivered=1",[],|r| r.get::<_,i64>(0)).unwrap()==1
    });
    let approval: i64 = db
        .query_row("SELECT id FROM approvals", [], |r| r.get(0))
        .unwrap();
    stop(&mut runtime);
    let mut restarted = spawn(dir, &telegram, &provider);
    telegram.message(2, Some(8), &format!("/approve {approval}"));
    wait(|| {
        telegram.transcript.lock().unwrap().iter().any(|entry| {
            entry["params"]["message_thread_id"] == "8"
                && reply_text(entry).contains("not authorized")
        })
    });
    assert_eq!(fs::read_to_string(dir.join("calls")).unwrap(), "initial\n");
    telegram.message(3, Some(7), &format!("/approve {approval}"));
    wait(|| {
        db.query_row("SELECT COUNT(*) FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id WHERE r.update_id=?1 AND p.delivered=1",
        [format!("approval:{approval}")],|r| r.get::<_,i64>(0)).unwrap()==1
    });
    wait(|| {
        db.query_row("SELECT COUNT(*) FROM telegram_ingress WHERE update_id IN ('2','3') AND ack_phase='sent'",[],|r|r.get::<_,i64>(0)).unwrap()==2
    });
    stop(&mut restarted);
    assert_eq!(
        fs::read_to_string(dir.join("calls")).unwrap(),
        "initial\napproved\n"
    );
    assert!(
        fs::read_to_string(dir.join("approved-args"))
            .unwrap()
            .contains("approval-native")
    );
    assert!(
        fs::read_to_string(dir.join("approved-prompt"))
            .unwrap()
            .contains("Administrator approval granted")
    );
    for stage in ["initial", "approved"] {
        let pid = fs::read_to_string(dir.join(format!("{stage}-pid"))).unwrap();
        assert!(!process_alive(&pid));
    }
    assert_eq!(
        db.query_row(
            "SELECT session_id FROM task_runs ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "telegram:321951227#7"
    );
    let transcript = telegram.transcript.lock().unwrap();
    let controls = transcript
        .iter()
        .filter(|entry| {
            entry["method"] == "sendRichMessage"
                && (reply_text(entry) == "You are not authorized to approve this request."
                    || reply_text(entry).starts_with("Approval recorded. Resuming as task #"))
        })
        .collect::<Vec<_>>();
    assert_eq!(controls.len(), 2);
    assert!(controls.iter().any(
        |e| e["params"]["message_thread_id"] == "8" && reply_text(e).contains("not authorized")
    ));
    assert!(
        controls
            .iter()
            .any(|e| e["params"]["message_thread_id"] == "7"
                && reply_text(e).starts_with("Approval recorded."))
    );
    let finals: Vec<_> = transcript
        .iter()
        .filter(|entry| entry["method"] == "sendRichMessage" && !controls.contains(entry))
        .collect();
    assert_eq!(finals.len(), 2);
    assert!(
        finals
            .iter()
            .all(|entry| entry["params"]["message_thread_id"] == "7")
    );
    if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
        let artifacts = PathBuf::from(artifacts).join("approval-restart");
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(
            artifacts.join("transcript.json"),
            serde_json::to_string_pretty(&*transcript).unwrap(),
        )
        .unwrap();
        for file in [
            "initial-prompt",
            "initial-args",
            "approved-prompt",
            "approved-args",
        ] {
            fs::copy(dir.join(file), artifacts.join(file)).unwrap();
        }
        fs::write(
            artifacts.join("result.json"),
            json!({"provider_calls":2,"wrong_topic_provider_calls":0,"final_live_providers":0})
                .to_string(),
        )
        .unwrap();
    }
}

#[test]
fn polling_retry_after_preserves_full_interval_for_rich_and_regular_across_restart() {
    let mut results = Vec::new();
    for (kind, method) in [("rich", "sendRichMessage"), ("regular", "editMessageText")] {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        let telegram = Telegram::start_with_retry_after(Some(method));
        let provider = Path::new("/nonexistent/retry-test-must-not-execute-provider");
        let mut initial = spawn(dir, &telegram, provider);
        wait(|| {
            telegram
                .transcript
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry["method"] == "setMyCommands")
        });
        stop(&mut initial);
        let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
        db.busy_timeout(Duration::from_secs(5)).unwrap();
        let old_deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3;
        let request = json!({
            "session_id":"telegram:321951227#7","channel":"telegram","source":"telegram",
            "input_type":"text","user_text":"saved retry test",
            "dispatch":{"kind":"telegram","chat_id":"321951227","message_thread_id":"7"},
            "progress_message_id": if kind=="regular" {json!("77")} else {Value::Null}
        });
        let state = if kind == "rich" {
            json!({"completed_ops":[],"text_plan":{"kind":"rich","markdown":"saved reply","phase":"ready","attempts":0,"retry_at":old_deadline}})
        } else {
            json!({"completed_ops":[],"text_plan":{"kind":"chunks","chunks":["saved reply"],"parse_mode":"off","fallback_plain":false},
                "operations":{"telegram:text:chunk:0":{"phase":"ready","attempts":0,"retry_at":old_deadline}}})
        };
        db.execute("INSERT INTO task_runs(session_id,channel,prompt,status,created_at) VALUES('telegram:321951227#7','telegram','saved retry test','completed','now')",[]).unwrap();
        let id = db.last_insert_rowid();
        db.execute("INSERT INTO task_payloads(task_run_id,request,output,delivery_state) VALUES(?1,?2,'TELEGRAM_REPLY: saved reply',?3)",
            rusqlite::params![id,request.to_string(),state.to_string()]).unwrap();
        let read_state = || {
            let raw: String = db
                .query_row(
                    "SELECT delivery_state FROM task_payloads WHERE task_run_id=?1",
                    [id],
                    |row| row.get(0),
                )
                .unwrap();
            serde_json::from_str::<Value>(&raw).unwrap()
        };
        let operation = |state: &Value| {
            if kind == "rich" {
                state["text_plan"].clone()
            } else {
                state["operations"]["telegram:text:chunk:0"].clone()
            }
        };
        let attempts = || operation(&read_state())["attempts"].as_u64().unwrap();
        let polls = || {
            telegram
                .transcript
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| entry["method"] == "getUpdates")
                .count()
        };
        let previous_polls = polls();
        let mut legacy = spawn(dir, &telegram, provider);
        wait(|| polls() >= previous_polls + 8);
        stop(&mut legacy);
        assert_eq!(attempts(), 0, "existing future deadline ignored for {kind}");
        assert_eq!(operation(&read_state())["retry_at"], old_deadline);

        let mut first = spawn(dir, &telegram, provider);
        wait(|| {
            let saved = operation(&read_state());
            saved["attempts"] == 1 && saved["phase"] == "ready"
        });
        let after_first = read_state();
        stop(&mut first);
        // Resume the newly persisted deadline amid the same busy intake.
        let mut resumed = spawn(dir, &telegram, provider);
        wait(|| {
            let saved = operation(&read_state());
            saved["attempts"] == 3 && saved["phase"] == "ready"
        });
        stop(&mut resumed);
        let final_state = read_state();
        let mut exhausted = spawn(dir, &telegram, provider);
        thread::sleep(Duration::from_millis(2100));
        stop(&mut exhausted);
        let transcript = telegram.transcript.lock().unwrap().clone();
        let receipts: Vec<_> = transcript
            .iter()
            .filter(|entry| {
                entry["method"] == method && entry["params"]["message_thread_id"] == "7"
            })
            .cloned()
            .collect();
        let intervals: Vec<_> = receipts
            .windows(2)
            .map(|pair| {
                pair[1]["request_monotonic"].as_f64().unwrap()
                    - pair[0]["response_monotonic"].as_f64().unwrap()
            })
            .collect();
        results.push(json!({"kind":kind,"legacy_deadline":old_deadline,"receipts":receipts,
            "minimum_intervals_seconds":intervals,"after_first":after_first,"final_state":final_state,
            "authorized_intake_polls":polls()}));
        if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
            let artifacts = PathBuf::from(artifacts).join(format!("retry-deadline-{kind}"));
            fs::create_dir_all(&artifacts).unwrap();
            fs::write(
                artifacts.join("transcript.json"),
                serde_json::to_string_pretty(&transcript).unwrap(),
            )
            .unwrap();
        }
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM task_runs", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(
            !db.query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [id],
                |row| row.get::<_, bool>(0)
            )
            .unwrap()
        );
    }
    if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
        fs::write(
            PathBuf::from(artifacts).join("retry-deadline-results.json"),
            serde_json::to_string_pretty(&results).unwrap(),
        )
        .unwrap();
    }
    for result in &results {
        let kind = result["kind"].as_str().unwrap();
        let receipts = result["receipts"].as_array().unwrap();
        assert_eq!(
            receipts.len(),
            3,
            "{kind}: three-attempt budget must survive restart"
        );
        assert!(
            receipts[0]["request_wall"].as_f64().unwrap()
                >= result["legacy_deadline"].as_f64().unwrap()
        );
        for interval in result["minimum_intervals_seconds"].as_array().unwrap() {
            assert!(
                interval.as_f64().unwrap() >= 1.0,
                "{kind}: retry_after=1 retried after {interval} seconds"
            );
        }
        assert!(
            receipts
                .iter()
                .all(|receipt| receipt["params"]["message_thread_id"] == "7")
        );
        let saved = if kind == "rich" {
            &result["after_first"]["text_plan"]
        } else {
            &result["after_first"]["operations"]["telegram:text:chunk:0"]
        };
        assert!(
            saved["retry_at"].as_f64().unwrap()
                >= receipts[0]["response_wall"].as_f64().unwrap() + 1.0,
            "{kind}: persisted deadline lost fractional response time"
        );
    }
}

fn capture_content_round_trip(
    group: &str,
    name: &str,
    markdown: &str,
    provider_output: &str,
    envelope: bool,
) -> Value {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    fs::write(dir.join("provider-output.txt"), provider_output).unwrap();
    if envelope {
        fs::write(dir.join("use-stdout"), "").unwrap();
    }
    let provider = dir.join("content-provider.py");
    fs::write(
        &provider,
        r#"import sys,pathlib
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
(root/"captured-prompt").write_text(sys.stdin.read())
with (root/"calls").open("a") as calls: calls.write("called\n")
raw=(root/"provider-output.txt").read_bytes()
if (root/"use-stdout").exists():
    sys.stdout.buffer.write(raw)
else:
    pathlib.Path(args[args.index("--output-last-message")+1]).write_bytes(raw)
"#,
    )
    .unwrap();
    let telegram = Telegram::start();
    telegram.message(1, Some(7), "Show the synthetic content fixture.");
    let mut runtime = spawn(dir, &telegram, &provider);
    wait(|| {
        telegram
            .transcript
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry["method"] == "setMyCommands")
    });
    let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    wait(|| {
        db.query_row(
            "SELECT COUNT(*) FROM task_payloads WHERE delivered=1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
            == 1
    });
    stop(&mut runtime);
    let (raw, reply): (String,String) = db.query_row("SELECT provider_raw,telegram_reply FROM turns WHERE status IN ('ok','parse_recovered')",[],|row| Ok((row.get(0)?,row.get(1)?))).unwrap();
    let (request, saved_output, first_state): (String, String, String) = db
        .query_row(
            "SELECT request,output,delivery_state FROM task_payloads",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let mut request: Value = serde_json::from_str(&request).unwrap();
    request["progress_message_id"] = Value::Null;
    // Replay the actual saved marker output without a selected plan. An
    // acknowledged original must not be resent when this new outbox runs.
    db.execute("INSERT INTO task_runs(session_id,channel,prompt,status,created_at) VALUES('telegram:321951227#7','telegram','saved content replay','completed','now')",[]).unwrap();
    let replay_id = db.last_insert_rowid();
    db.execute(
        "INSERT INTO task_payloads(task_run_id,request,output) VALUES(?1,?2,?3)",
        rusqlite::params![replay_id, request.to_string(), saved_output],
    )
    .unwrap();
    telegram.message(2, Some(8), "/tasks");
    let mut replay = spawn(dir, &telegram, &provider);
    wait(|| {
        db.query_row(
            "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
            [replay_id],
            |row| row.get::<_, bool>(0),
        )
        .unwrap()
    });
    stop(&mut replay);
    let replay_state: String = db
        .query_row(
            "SELECT delivery_state FROM task_payloads WHERE task_run_id=?1",
            [replay_id],
            |row| row.get(0),
        )
        .unwrap();
    let transcript = telegram.transcript.lock().unwrap().clone();
    let finals: Vec<_> = transcript
        .iter()
        .filter(|entry| {
            (entry["method"] == "sendRichMessage" && entry["params"]["message_thread_id"] == "7")
                || (matches!(
                    entry["method"].as_str(),
                    Some("sendMessage" | "editMessageText")
                ) && entry["params"]["text"] == reply)
        })
        .cloned()
        .collect();
    let result = json!({"case":name,"expected":markdown,"provider_output":provider_output,
            "provider_raw":raw,"telegram_reply":reply,"saved_output":saved_output,
            "first_state":serde_json::from_str::<Value>(&first_state).unwrap(),
            "replay_state":serde_json::from_str::<Value>(&replay_state).unwrap(),
            "final_requests":finals,"provider_calls":fs::read_to_string(dir.join("calls")).unwrap()});
    if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
        let artifacts = PathBuf::from(artifacts).join(group).join(name);
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(
            artifacts.join("results.json"),
            serde_json::to_string_pretty(&result).unwrap(),
        )
        .unwrap();
        fs::write(
            artifacts.join("transcript.json"),
            serde_json::to_string_pretty(&transcript).unwrap(),
        )
        .unwrap();
        fs::copy(dir.join("captured-prompt"), artifacts.join("stdin.txt")).unwrap();
        fs::copy(
            dir.join("provider-output.txt"),
            artifacts.join("provider-output.txt"),
        )
        .unwrap();
    }
    result
}

#[test]
fn polling_native_markdown_content_survives_provider_persistence_and_saved_replay() {
    let multiline = concat!(r"$\theta + \nu + \rho$", "\n\n", r"Code: `\t \n \r` 🥥");
    let mut results = Vec::new();
    for (name, markdown, envelope) in [
        ("single-line-math", r"$\theta + \nu$", false),
        (
            "inline-code",
            r"Escapes: `\t`, `\n`, `\r`, `\\n`, and `C:\temp\notes`.",
            false,
        ),
        ("multiline", multiline, false),
        ("json-envelope", multiline, true),
    ] {
        let provider_output = if envelope {
            json!({"type":"item.completed","item":{"type":"agent_message","text":markdown}})
                .to_string()
        } else {
            format!("TELEGRAM_REPLY: {markdown}")
        };
        results.push(capture_content_round_trip(
            "native-content",
            name,
            markdown,
            &provider_output,
            envelope,
        ));
    }
    for result in results {
        let name = result["case"].as_str().unwrap();
        let expected = result["expected"].as_str().unwrap();
        assert_eq!(
            result["provider_calls"], "called\n",
            "{name}: saved replay must not execute a provider"
        );
        assert_eq!(result["provider_raw"], result["provider_output"], "{name}");
        assert_eq!(result["telegram_reply"], expected, "{name}");
        assert_eq!(
            result["saved_output"],
            format!("TELEGRAM_REPLY: {expected}\n"),
            "{name}"
        );
        for state in ["first_state", "replay_state"] {
            assert_eq!(
                result[state]["text_plan"]["markdown"], expected,
                "{name}: {state}"
            );
            assert_eq!(
                result[state]["completed_ops"],
                json!(["telegram:text"]),
                "{name}: {state}"
            );
        }
        let finals = result["final_requests"].as_array().unwrap();
        assert_eq!(
            finals.len(),
            2,
            "{name}: one original and one distinct saved replay, no duplicate"
        );
        for request in finals {
            assert_eq!(request["params"]["chat_id"], "321951227", "{name}");
            assert_eq!(request["params"]["message_thread_id"], "7", "{name}");
            assert!(request["params"].get("parse_mode").is_none());
            let rich: Value =
                serde_json::from_str(request["params"]["rich_message"].as_str().unwrap()).unwrap();
            assert_eq!(rich, json!({"markdown":expected}), "{name}");
        }
    }
}

#[test]
fn polling_literal_thinking_tags_survive_provider_persistence_and_saved_replay() {
    let paired = "Show this XML literally:\n\n```xml\n<think>keep this exact code</think>\n```";
    let unpaired =
        "Show this XML literally:\n\n```xml\n<think>keep this unclosed code\n```\nKeep the tail.";
    let closing = "Show this XML literally:\n\n```xml\n</think>\n```\nKeep the tail.";
    let json_assistant = |reply| {
        format!(
            "{}\n{}",
            json!({"type":"item.completed","item":{"type":"reasoning","text":"not the final answer"}}),
            json!({"type":"item.completed","item":{"type":"agent_message","text":reply}})
        )
    };
    let mut results = Vec::new();
    for (name, reply, raw, stdout, kind) in [
        (
            "paired-marker",
            paired,
            format!("TELEGRAM_REPLY: {paired}"),
            false,
            "chunks",
        ),
        (
            "unpaired-marker",
            unpaired,
            format!("TELEGRAM_REPLY: {unpaired}"),
            false,
            "chunks",
        ),
        (
            "closing-marker",
            closing,
            format!("TELEGRAM_REPLY: {closing}"),
            false,
            "chunks",
        ),
        (
            "recovered-plain",
            paired,
            paired.to_string(),
            false,
            "chunks",
        ),
        (
            "recovered-json-paired",
            paired,
            json_assistant(paired),
            true,
            "chunks",
        ),
        (
            "recovered-json-unpaired",
            unpaired,
            json_assistant(unpaired),
            true,
            "chunks",
        ),
        (
            "prefix-envelope",
            "Final envelope answer.",
            "<think>synthetic private reasoning</think>\nTELEGRAM_REPLY: Final envelope answer."
                .into(),
            false,
            "rich",
        ),
        (
            "prefix-envelope-literal",
            paired,
            format!("<think>synthetic private reasoning</think>\nTELEGRAM_REPLY: {paired}"),
            false,
            "chunks",
        ),
    ] {
        results.push((
            kind,
            capture_content_round_trip("thinking-content", name, reply, &raw, stdout),
        ));
    }
    for (kind, result) in results {
        let name = result["case"].as_str().unwrap();
        let expected = result["expected"].as_str().unwrap();
        assert_eq!(result["provider_raw"], result["provider_output"], "{name}");
        assert_eq!(result["telegram_reply"], expected, "{name}");
        assert_eq!(
            result["saved_output"],
            format!("TELEGRAM_REPLY: {expected}\n"),
            "{name}"
        );
        assert_eq!(
            result["provider_calls"], "called\n",
            "{name}: replay launched another provider"
        );
        for state in ["first_state", "replay_state"] {
            let plan = &result[state]["text_plan"];
            assert_eq!(plan["kind"], kind, "{name}: markup gate changed");
            if kind == "chunks" {
                assert_eq!(plan["chunks"], json!([expected]), "{name}");
                assert_eq!(plan["parse_mode"], "off", "{name}");
            } else {
                assert_eq!(plan["markdown"], expected, "{name}");
            }
            assert!(
                result[state]["completed_ops"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("telegram:text"))
            );
        }
        let requests = result["final_requests"].as_array().unwrap();
        assert_eq!(
            requests.len(),
            2,
            "{name}: acknowledged original must not replay"
        );
        for request in requests {
            assert_eq!(request["params"]["chat_id"], "321951227", "{name}");
            assert_eq!(request["params"]["message_thread_id"], "7", "{name}");
            assert!(request["params"].get("parse_mode").is_none(), "{name}");
            if kind == "chunks" {
                assert!(matches!(
                    request["method"].as_str(),
                    Some("sendMessage" | "editMessageText")
                ));
                assert_eq!(request["params"]["text"], expected, "{name}");
                assert!(request["params"].get("rich_message").is_none());
            } else {
                assert_eq!(request["method"], "sendRichMessage", "{name}");
                let rich: Value =
                    serde_json::from_str(request["params"]["rich_message"].as_str().unwrap())
                        .unwrap();
                assert_eq!(rich, json!({"markdown":expected}), "{name}");
            }
        }
    }
}

#[test]
fn polling_raw_turn_recovery_excludes_reasoning_effects_in_both_crash_windows() {
    let mut results = Vec::new();
    for approval in [true, false] {
        for applied in [true, false] {
            let name = format!(
                "{}-sideeffects-{}",
                if approval { "approval" } else { "delegation" },
                u8::from(applied)
            );
            let temp = tempfile::tempdir().unwrap();
            let dir = temp.path();
            let raw = format!(
                "<think>\n{}DELEGATE_TASK: reasoning-only-delegation\nMEMORY_APPEND: reasoning-only-memory\nTASK_APPEND: reasoning-only-task\n</think>\nTELEGRAM_REPLY: Final safe answer.\nTASK_APPEND: final-task-sentinel",
                if approval {
                    "SEND_APPROVAL: reasoning-only-approval\n"
                } else {
                    ""
                }
            );
            fs::write(dir.join("provider-output.txt"), &raw).unwrap();
            let provider = dir.join("recovery-provider.py");
            fs::write(&provider, r#"import sys,pathlib
root=pathlib.Path(__file__).parent
args=sys.argv[1:]
(root/"captured-prompt").write_text(sys.stdin.read())
with (root/"calls").open("a") as calls: calls.write("called\n")
pathlib.Path(args[args.index("--output-last-message")+1]).write_bytes((root/"provider-output.txt").read_bytes())
"#).unwrap();
            let telegram = Telegram::start();
            telegram.message(1, Some(7), "Synthetic recovery control");
            let mut normal_runtime = spawn(dir, &telegram, &provider);
            wait(|| {
                telegram
                    .transcript
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["method"] == "setMyCommands")
            });
            let db = rusqlite::Connection::open(dir.join("state.db")).unwrap();
            db.busy_timeout(Duration::from_secs(5)).unwrap();
            wait(|| {
                db.query_row(
                    "SELECT COUNT(*) FROM task_payloads WHERE delivered=1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
                    == 1
            });
            stop(&mut normal_runtime);
            let original_prompt = fs::read_to_string(dir.join("captured-prompt")).unwrap();
            let (normal_id, request, normal_output): (i64, String, String) = db
                .query_row(
                    "SELECT task_run_id,request,output FROM task_payloads",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            let normal_turn: i64 = db
                .query_row(
                    "SELECT id FROM turns WHERE task_run_id=?1",
                    [normal_id],
                    |row| row.get(0),
                )
                .unwrap();
            let mut request: Value = serde_json::from_str(&request).unwrap();
            request["progress_message_id"] = Value::Null;
            request["update_id"] = "recovery-seed".into();
            let snapshot = |id| {
                let (output, state, delivered): (Option<String>,String,bool) = db.query_row(
                    "SELECT output,delivery_state,delivered FROM task_payloads WHERE task_run_id=?1",[id],
                    |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
                let runs: Vec<Value> = db.prepare("SELECT id,status,prompt FROM task_runs ORDER BY id").unwrap().query_map([],|row|
                    Ok(json!({"id":row.get::<_,i64>(0)?,"status":row.get::<_,String>(1)?,"prompt":row.get::<_,String>(2)?}))).unwrap()
                    .collect::<rusqlite::Result<_>>().unwrap();
                let tasks: Vec<Value> = db.prepare("SELECT turn_id,content FROM tasks ORDER BY id").unwrap().query_map([],|row|
                    Ok(json!({"turn_id":row.get::<_,i64>(0)?,"content":row.get::<_,String>(1)?}))).unwrap()
                    .collect::<rusqlite::Result<_>>().unwrap();
                let memory: Vec<String> = db
                    .prepare("SELECT content FROM memory_records ORDER BY id")
                    .unwrap()
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                let (provider_raw, reply, side_effects_applied): (String,String,bool) = db.query_row(
                    "SELECT provider_raw,telegram_reply,side_effects_applied FROM turns WHERE task_run_id=?1 ORDER BY id DESC LIMIT 1",[id],
                    |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
                json!({"output":output,"delivery_state":serde_json::from_str::<Value>(&state).unwrap(),"delivered":delivered,
                    "runs":runs,"tasks":tasks,"memory":memory,"approvals":db.query_row("SELECT COUNT(*) FROM approvals",[],|row| row.get::<_,i64>(0)).unwrap(),
                    "provider_raw":provider_raw,"reply":reply,"side_effects_applied":side_effects_applied,
                    "provider_calls":fs::read_to_string(dir.join("calls")).unwrap()})
            };
            let normal = snapshot(normal_id);
            assert_eq!(normal["approvals"], 0);
            assert_eq!(normal["tasks"].as_array().unwrap().len(), 1);
            assert_eq!(normal["tasks"][0]["content"], "final-task-sentinel");
            assert_eq!(normal["memory"], json!([]));
            assert_eq!(normal["provider_calls"], "called\n");
            db.execute("INSERT INTO task_runs(session_id,channel,source_chat_id,source_thread_id,update_id,prompt,status,created_at,started_at) VALUES('telegram:321951227#7','telegram','321951227','7','recovery-seed','Synthetic recovery control','running','now','now')",[]).unwrap();
            let id = db.last_insert_rowid();
            db.execute(
                "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
                rusqlite::params![id, request.to_string()],
            )
            .unwrap();
            db.execute("INSERT INTO turns(ts,chat_id,telegram_chat_id,telegram_message_thread_id,input_type,user_text,provider_raw,telegram_reply,status,task_run_id,side_effects_applied,channel) VALUES('now','telegram:321951227#7','321951227','7','text','Synthetic recovery control',?1,'Final safe answer.','ok',?2,?3,'telegram')",
                rusqlite::params![raw,id,applied]).unwrap();
            let turn_id = db.last_insert_rowid();
            if applied {
                // This window already committed the legitimate final append.
                db.execute("INSERT INTO tasks(ts,source,content,done,turn_id,append_index,managed_file) SELECT ts,source,content,done,?1,append_index,managed_file FROM tasks WHERE turn_id=?2",
                    rusqlite::params![turn_id,normal_turn]).unwrap();
            }
            let crash_seed = snapshot(id);
            let mut recovered_runtime = spawn(dir, &telegram, &provider);
            wait(|| {
                db.query_row("SELECT output IS NOT NULL AND delivered=1 FROM task_payloads WHERE task_run_id=?1",[id],|row| row.get::<_,bool>(0)).unwrap()
            });
            stop(&mut recovered_runtime);
            let recovered = snapshot(id);
            telegram.message(2, Some(8), "/tasks");
            let mut repeated_runtime = spawn(dir, &telegram, &provider);
            wait(|| {
                telegram.transcript.lock().unwrap().iter().any(|entry| {
                    entry["params"]["message_thread_id"] == "8"
                        && reply_text(entry) == "No active tasks."
                })
            });
            stop(&mut repeated_runtime);
            let repeated = snapshot(id);
            let transcript = telegram.transcript.lock().unwrap().clone();
            let finals: Vec<_> = transcript
                .iter()
                .filter(|entry| {
                    entry["method"] == "sendRichMessage"
                        && entry["params"]["message_thread_id"] == "7"
                })
                .cloned()
                .collect();
            let result = json!({"case":name,"raw":raw,"request":request,"normal":normal,"crash_seed":crash_seed,
                "recovered":recovered,"repeated":repeated,"final_requests":finals,"normal_output":normal_output,"recovered_turn_id":turn_id});
            if let Some(artifacts) = std::env::var_os("COCONUTCLAW_ACCEPTANCE_ARTIFACTS") {
                let artifacts = PathBuf::from(artifacts).join("raw-recovery").join(&name);
                fs::create_dir_all(&artifacts).unwrap();
                fs::write(
                    artifacts.join("results.json"),
                    serde_json::to_string_pretty(&result).unwrap(),
                )
                .unwrap();
                fs::write(
                    artifacts.join("transcript.json"),
                    serde_json::to_string_pretty(&transcript).unwrap(),
                )
                .unwrap();
                fs::write(artifacts.join("provider-output.txt"), &raw).unwrap();
                fs::write(artifacts.join("stdin.txt"), original_prompt).unwrap();
            }
            results.push(result);
        }
    }
    for result in results {
        let name = result["case"].as_str().unwrap();
        let recovered = &result["recovered"];
        assert_eq!(recovered["output"], result["normal_output"], "{name}");
        assert_eq!(
            recovered["delivery_state"], result["normal"]["delivery_state"],
            "{name}"
        );
        assert_eq!(recovered["provider_raw"], result["raw"], "{name}");
        assert_eq!(recovered["reply"], "Final safe answer.", "{name}");
        assert_eq!(recovered["side_effects_applied"], true, "{name}");
        assert_eq!(recovered["approvals"], 0, "{name}");
        assert_eq!(recovered["memory"], json!([]), "{name}");
        assert_eq!(
            recovered["provider_calls"], "called\n",
            "{name}: reasoning launched provider"
        );
        assert_eq!(
            recovered["runs"].as_array().unwrap().len(),
            2,
            "{name}: reasoning created delegation"
        );
        assert_eq!(recovered["runs"][1]["status"], "completed", "{name}");
        let tasks = recovered["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 2, "{name}");
        assert!(
            tasks
                .iter()
                .all(|task| task["content"] == "final-task-sentinel"),
            "{name}"
        );
        assert_eq!(tasks[1]["turn_id"], result["recovered_turn_id"], "{name}");
        assert_eq!(
            result["repeated"], *recovered,
            "{name}: repeated restart changed effects"
        );
        let finals = result["final_requests"].as_array().unwrap();
        assert_eq!(
            finals.len(),
            2,
            "{name}: acknowledged output replayed or delegated"
        );
        for request in finals {
            assert_eq!(request["params"]["chat_id"], "321951227");
            assert_eq!(request["params"]["message_thread_id"], "7");
            assert!(request["params"].get("parse_mode").is_none());
            let rich: Value =
                serde_json::from_str(request["params"]["rich_message"].as_str().unwrap()).unwrap();
            assert_eq!(rich, json!({"markdown":"Final safe answer."}), "{name}");
        }
    }
}
