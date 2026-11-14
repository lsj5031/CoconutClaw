use super::{
    parse_bin_with_env, parse_codex_progress_line, run_provider, shorten_status_text,
    split_command_spec,
};
use coconutclaw_config::RuntimeConfig;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

#[test]
#[cfg(unix)]
fn timeout_covers_large_stdin_when_descendant_keeps_only_input_open() {
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("stdin-holder.sh");
    // POSIX asynchronous jobs otherwise replace stdin with /dev/null before
    // their redirects. Preserve the real pipe on fd 3 before forking.
    fs::write(
        &script,
        "exec 3<&0\nsleep 3 <&3 >/dev/null 2>&1 &\nexit 0\n",
    )
    .unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    cfg.provider_timeout_secs = 1;
    let start = std::time::Instant::now();
    let output = run_provider(None, &cfg, &"X".repeat(32 * 1024 * 1024), None, None, None).unwrap();
    assert_eq!(output.exit_code, 124);
    assert!(start.elapsed() < std::time::Duration::from_secs(3));
}

#[test]
#[cfg(unix)]
fn deadline_and_cancellation_cover_pipes_after_normal_leader_exit() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    for cancelled in [false, true] {
        let mut cfg = RuntimeConfig::test_config();
        let script = cfg.tmp_dir.join("early-exit.sh");
        fs::write(&script, "cat >/dev/null\nsleep 3 &\nexit 0\n").unwrap();
        cfg.codex.bin = format!("sh \"{}\"", script.display());
        cfg.provider_timeout_secs = 1;
        let flag = Arc::new(AtomicBool::new(false));
        let flag2 = flag.clone();
        let signal = std::thread::spawn(move || {
            if cancelled {
                std::thread::sleep(std::time::Duration::from_millis(200));
                flag2.store(true, Ordering::SeqCst);
            }
        });
        let start = std::time::Instant::now();
        let output = run_provider(None, &cfg, "test", Some(&flag), None, None).unwrap();
        signal.join().unwrap();
        assert_eq!(output.exit_code, if cancelled { 130 } else { 124 });
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }
}

fn write_test_provider_script(
    dir: &Path,
    stem: &str,
    unix_body: &str,
    windows_body: &str,
) -> String {
    let (path, body, command) = if cfg!(windows) {
        let path = dir.join(format!("{stem}.ps1"));
        let command = format!(
            "powershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
            path.display()
        );
        (path, windows_body, command)
    } else {
        let path = dir.join(format!("{stem}.sh"));
        let command = path.display().to_string();
        (path, unix_body, command)
    };

    fs::write(&path, body).expect("write fake provider script");
    #[cfg(unix)]
    {
        let mut perms = fs::metadata(&path)
            .expect("read fake provider metadata")
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).expect("chmod fake provider script");
    }

    command
}

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
fn run_codex_sends_large_context_over_stdin() {
    let mut config = RuntimeConfig::test_config();

    let log_path = config.root_dir.join("fake-codex.log");
    let script = format!(
        "#!/bin/sh\nlog=\"{}\"\nout_file=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    --output-last-message) out_file=\"$2\"; shift 2 ;;\n    *) printf 'arg:%s\\n' \"$1\" >> \"$log\"; shift ;;\n  esac\ndone\ninput=$(cat)\nprintf 'stdin_len:%s\\n' \"${{#input}}\" >> \"$log\"\nprintf 'TELEGRAM_REPLY: read %s bytes\\n' \"${{#input}}\" > \"$out_file\"\n",
        log_path.display()
    );
    let windows_script = format!(
        "$log = \"{}\"\n$outFile = \"\"\nfor ($i = 0; $i -lt $args.Count; $i++) {{\n    if ($args[$i] -eq '--output-last-message') {{\n        $outFile = $args[$i + 1]\n        $i++\n    }} else {{\n        Add-Content -Path $log -Value \"arg:$($args[$i])\"\n    }}\n}}\n$inputText = [Console]::In.ReadToEnd()\nAdd-Content -Path $log -Value \"stdin_len:$($inputText.Length)\"\nSet-Content -Path $outFile -Value \"TELEGRAM_REPLY: read $($inputText.Length) bytes\"\n",
        log_path.display()
    );
    config.codex.bin =
        write_test_provider_script(&config.root_dir, "fake-codex", &script, &windows_script);

    let context = "x".repeat(200_000);
    let output = run_provider(None, &config, &context, None, None, Some(5))
        .expect("run fake codex provider with large stdin context");

    assert!(output.success);
    assert_eq!(
        output.raw_output.trim(),
        "TELEGRAM_REPLY: read 200000 bytes"
    );
    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert!(
        log.contains("arg:-"),
        "expected '-' prompt marker in log:\n{log}"
    );
    assert!(
        log.contains("stdin_len:200000"),
        "expected large context on stdin, got log:\n{log}"
    );
    assert!(
        !log.contains(&context),
        "large context should not appear in argv log"
    );
}

#[test]
fn split_command_spec_preserves_quoted_segments() {
    assert_eq!(
        split_command_spec(r#"powershell -File "C:\tmp dir\fake provider.ps1" --flag"#),
        vec![
            "powershell".to_string(),
            "-File".to_string(),
            r#"C:\tmp dir\fake provider.ps1"#.to_string(),
            "--flag".to_string()
        ]
    );
}

#[test]
fn parse_bin_with_env_splits_env_command_and_args() {
    let (env_vars, bin, args) =
        parse_bin_with_env(r#"FOO=bar powershell -NoProfile -File "C:\tmp dir\fake provider.ps1""#);
    assert_eq!(env_vars.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(bin, "powershell");
    assert_eq!(
        args,
        vec![
            "-NoProfile".to_string(),
            "-File".to_string(),
            r#"C:\tmp dir\fake provider.ps1"#.to_string()
        ]
    );
}

#[test]
fn shorten_status_text_ascii_no_truncation() {
    assert_eq!(shorten_status_text("hello", 10), "hello");
}

#[test]
fn shorten_status_text_ascii_truncation() {
    assert_eq!(shorten_status_text("hello world", 8), "hello...");
}

#[test]
fn shorten_status_text_exact_boundary() {
    assert_eq!(shorten_status_text("hello", 5), "hello");
}

#[test]
fn shorten_status_text_multibyte_no_truncation() {
    let cjk = "日本語テスト";
    assert_eq!(shorten_status_text(cjk, 10), cjk);
}

#[test]
fn shorten_status_text_multibyte_truncation() {
    let cjk = "日本語テストです";
    assert_eq!(shorten_status_text(cjk, 5), "日本...");
}

#[test]
fn shorten_status_text_empty() {
    assert_eq!(shorten_status_text("", 5), "");
}

#[test]
#[cfg(unix)]
fn codex_explicit_resume_image_and_native_session_are_preserved() {
    use super::run_provider_session;
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("codex-resume-mock");
    let args = cfg.tmp_dir.join("args");
    fs::write(
        &script,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$@" > '{}'
cat >/dev/null
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
  shift
done
printf 'TELEGRAM_REPLY: resumed\n' > "$out"
printf '%s\n' '{{"type":"thread.started","thread_id":"chat-thread"}}'
"#,
            args.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    let image = cfg.tmp_dir.join("photo.PNG");
    fs::write(&image, []).unwrap();
    let output = run_provider_session(
        Some(&image),
        &cfg,
        "hello",
        None,
        None,
        None,
        Some("chat-thread"),
    )
    .unwrap();
    assert!(output.success);
    assert_eq!(output.session_id.as_deref(), Some("chat-thread"));
    let args = fs::read_to_string(args).unwrap();
    assert!(args.starts_with("exec\nresume\nchat-thread\n"));
    assert!(args.contains("--image\n"));
    assert!(args.contains("--json\n"));
    assert!(!args.contains("--last"));
    assert!(!args.contains("--dangerously-bypass"));
}

#[test]
#[cfg(unix)]
fn configured_timeout_stops_a_mock_codex_process() {
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("codex-timeout-mock");
    fs::write(
        &script,
        "#!/bin/sh\ntrap '' TERM\ncat >/dev/null\nexec sleep 30\n",
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    cfg.provider_timeout_secs = 1;
    let started = std::time::Instant::now();
    let output = run_provider(None, &cfg, "timeout", None, None, None).unwrap();
    assert!(!output.success);
    assert_eq!(output.exit_code, 124);
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[test]
#[cfg(unix)]
fn cancellation_terminates_descendants_even_if_group_leader_exits_first() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let mut cfg = RuntimeConfig::test_config();
    let script = cfg.tmp_dir.join("codex-cancel-mock");
    fs::write(
        &script,
        "#!/bin/sh\ncat >/dev/null\ntrap 'exit 0' TERM\n(trap '' TERM; sleep 30) &\nwait\n",
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    cfg.codex.bin = format!("sh \"{}\"", script.display());
    let flag = Arc::new(AtomicBool::new(false));
    let signal = flag.clone();
    let cancel = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(250));
        signal.store(true, Ordering::SeqCst);
    });
    let started = std::time::Instant::now();
    let output = run_provider(None, &cfg, "cancel", Some(&flag), None, None).unwrap();
    cancel.join().unwrap();
    assert_eq!(output.exit_code, 130);
    assert!(!output.success);
    assert!(started.elapsed() < std::time::Duration::from_secs(8));
}
