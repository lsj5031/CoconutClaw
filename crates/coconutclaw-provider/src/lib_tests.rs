use super::{
    extract_antigravity_json_final, extract_claude_json_final, extract_factory_json_final,
    extract_opencode_json_final, extract_pi_json_final, parse_antigravity_json_line,
    parse_bin_with_env, parse_claude_json_line, parse_codex_progress_line, parse_factory_json_line,
    parse_opencode_json_line, parse_pi_progress_line, run_provider, shorten_status_text,
    split_command_spec, tool_arg_summary,
};
use coconutclaw_config::{AgentProvider, RuntimeConfig};
use serde_json::Value;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

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
    config.provider = AgentProvider::Codex;

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
    let line = r#"{"type":"tool_execution_start","toolCallId":"t1","toolName":"bash","args":{}}"#;
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
    assert!(result.len() <= 64);
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
fn extract_factory_json_final_trims_prose_before_marker() {
    let raw = r#"{"type":"completion","finalText":"I detect implementation intent. Build succeeded. TELEGRAM_REPLY: Built and restarted both services"}
"#;
    let text = extract_factory_json_final(raw);
    assert_eq!(
        text.as_deref(),
        Some("TELEGRAM_REPLY: Built and restarted both services")
    );
}

#[test]
fn extract_factory_json_final_ignores_user_messages() {
    let raw = r#"{"type":"message","role":"user","id":"u1","text":"user input","timestamp":1}
"#;
    assert!(extract_factory_json_final(raw).is_none());
}

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
    let line = r#"{"type":"message","role":"assistant","id":"a1","text":"hello","timestamp":1}"#;
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

#[test]
fn extract_opencode_json_final_reads_part_text() {
    let raw = r#"{"type":"step_start","timestamp":1,"sessionID":"s1","part":{"type":"step-start"}}
{"type":"text","timestamp":2,"sessionID":"s1","part":{"type":"text","text":"TELEGRAM_REPLY: hello from opencode"}}
{"type":"step_finish","timestamp":3,"sessionID":"s1","part":{"type":"step-finish","reason":"stop"}}
"#;
    let text = extract_opencode_json_final(raw);
    assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: hello from opencode"));
}

#[test]
fn extract_opencode_json_final_separates_multiple_text_events() {
    let raw = r#"{"type":"text","timestamp":1,"sessionID":"s1","part":{"type":"text","text":"TELEGRAM_REPLY: hello"}}
{"type":"text","timestamp":2,"sessionID":"s1","part":{"type":"text","text":"MEMORY_APPEND: some fact"}}
"#;
    let text = extract_opencode_json_final(raw);
    assert_eq!(
        text.as_deref(),
        Some("TELEGRAM_REPLY: hello\nMEMORY_APPEND: some fact")
    );
}

#[test]
fn extract_opencode_json_final_no_double_newline_if_text_ends_with_newline() {
    let raw = r#"{"type":"text","timestamp":1,"sessionID":"s1","part":{"type":"text","text":"TELEGRAM_REPLY: hello\n"}}
{"type":"text","timestamp":2,"sessionID":"s1","part":{"type":"text","text":"MEMORY_APPEND: fact"}}
"#;
    let text = extract_opencode_json_final(raw);
    assert_eq!(
        text.as_deref(),
        Some("TELEGRAM_REPLY: hello\nMEMORY_APPEND: fact")
    );
}

#[test]
fn extract_opencode_json_final_recovers_marker_from_reasoning() {
    let raw = r#"{"type":"reasoning","timestamp":1,"sessionID":"s1","part":{"type":"reasoning","text":"I will format the answer now.\nTELEGRAM_REPLY: short reply\nVOICE_REPLY: spoken"}}
{"type":"text","timestamp":2,"sessionID":"s1","part":{"type":"text","text":""}}
"#;
    let text = extract_opencode_json_final(raw);
    assert_eq!(
        text.as_deref(),
        Some("TELEGRAM_REPLY: short reply\nVOICE_REPLY: spoken")
    );
}

#[test]
fn extract_opencode_json_final_prefers_reasoning_markers_over_followup_chatter() {
    let raw = r#"{"type":"reasoning","timestamp":1,"sessionID":"s1","part":{"type":"reasoning","text":"I will format the answer now.\nTELEGRAM_REPLY: short reply\nVOICE_REPLY: spoken"}}
{"type":"text","timestamp":2,"sessionID":"s1","part":{"type":"text","text":"Done, I've sent it."}}
"#;
    let text = extract_opencode_json_final(raw);
    assert_eq!(
        text.as_deref(),
        Some("TELEGRAM_REPLY: short reply\nVOICE_REPLY: spoken")
    );
}

#[test]
fn parse_claude_system_init() {
    let line = r#"{"type":"system","subtype":"init","session_id":"s1","tools":["Bash","Read"]}"#;
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

#[test]
fn parse_opencode_step_start() {
    let line = r#"{"type":"step_start","timestamp":1767036059338,"sessionID":"ses_1","part":{"type":"step-start"}}"#;
    assert_eq!(
        parse_opencode_json_line(line).as_deref(),
        Some("Processing...")
    );
}

#[test]
fn parse_opencode_text() {
    let line = r#"{"type":"text","timestamp":1767036061000,"sessionID":"ses_1","part":{"id":"part_1","type":"text","text":"TELEGRAM_REPLY: hello","time":{"start":1,"end":2}}}"#;
    assert_eq!(
        parse_opencode_json_line(line).as_deref(),
        Some("Drafting response...")
    );
}

#[test]
fn parse_opencode_reasoning() {
    let line = r#"{"type":"reasoning","timestamp":1767036061100,"sessionID":"ses_1","part":{"id":"part_2","type":"reasoning","text":"  Let me\n  think this through carefully.  ","time":{"start":1,"end":2}}}"#;
    assert_eq!(
        parse_opencode_json_line(line).as_deref(),
        Some("Reasoning: Let me think this through carefully.")
    );
}

#[test]
fn parse_opencode_tool_use_success() {
    let line = r#"{"type":"tool_use","timestamp":1767036061199,"sessionID":"ses_1","part":{"id":"part_3","type":"tool","callID":"call_1","tool":"bash","state":{"status":"completed","input":{"command":"echo hello"},"output":"hello\n","title":"Print hello","metadata":{"exit":0},"time":{"start":1,"end":2}}}}"#;
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
    let line = r#"{"type":"step_finish","timestamp":1767036061205,"sessionID":"ses_1","part":{"type":"step-finish","reason":"tool_calls"}}"#;
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

#[test]
fn run_opencode_keeps_permission_override_when_retrying_without_legacy_flag() {
    let mut config = RuntimeConfig::test_config();
    config.provider = AgentProvider::OpenCode;
    config.exec_policy = "yolo".to_string();
    config.opencode.skip_permissions = Some(true);

    let log_path = config.root_dir.join("fake-opencode.log");
    let expected_db = config.instance_dir.join("opencode.db");
    let script = format!(
        "#!/bin/sh\nlog=\"{}\"\nprintf 'args:%s\\n' \"$*\" >> \"$log\"\nprintf 'permission:%s\\n' \"${{OPENCODE_PERMISSION:-}}\" >> \"$log\"\nprintf 'db:%s\\n' \"${{OPENCODE_DB:-}}\" >> \"$log\"\nif printf '%s\\n' \"$*\" | grep -q -- '--dangerously-skip-permissions'; then\n  echo \"unexpected argument '--dangerously-skip-permissions'\" >&2\n  exit 1\nfi\ncat <<'EOF'\n{{\"type\":\"text\",\"timestamp\":1,\"sessionID\":\"s1\",\"part\":{{\"type\":\"text\",\"text\":\"TELEGRAM_REPLY: hello from fake opencode\"}}}}\nEOF\n",
        log_path.display()
    );
    let windows_script = format!(
        "$log = \"{}\"\nAdd-Content -Path $log -Value \"args:$($args -join ' ')\"\nAdd-Content -Path $log -Value \"permission:$env:OPENCODE_PERMISSION\"\nAdd-Content -Path $log -Value \"db:${{env:OPENCODE_DB}}\"\nif ($args -contains '--dangerously-skip-permissions') {{\n    [Console]::Error.WriteLine(\"unexpected argument '--dangerously-skip-permissions'\")\n    exit 1\n}}\nWrite-Output '{{\"type\":\"text\",\"timestamp\":1,\"sessionID\":\"s1\",\"part\":{{\"type\":\"text\",\"text\":\"TELEGRAM_REPLY: hello from fake opencode\"}}}}'\n",
        log_path.display()
    );

    config.opencode.bin =
        write_test_provider_script(&config.root_dir, "fake-opencode", &script, &windows_script);

    let output = run_provider(None, &config, "hello", None, None, Some(5))
        .expect("run fake opencode provider");
    assert!(output.success);
    assert_eq!(
        output.raw_output,
        "TELEGRAM_REPLY: hello from fake opencode"
    );

    let log = fs::read_to_string(&log_path).expect("read fake opencode log");
    assert!(log.contains("args:run --dangerously-skip-permissions --format json hello"));
    assert!(log.contains("args:run --format json hello"));
    assert_eq!(log.matches("permission:{\"*\":\"allow\"}").count(), 2);
    assert_eq!(
        log.matches(&format!("db:{}", expected_db.display()))
            .count(),
        2,
        "expected OPENCODE_DB={} on both invocations, got log:\n{log}",
        expected_db.display()
    );
}

#[test]
fn parse_antigravity_init() {
    let line = r#"{"type":"init","session_id":"abc","model":"antigravity-2.5-pro"}"#;
    assert_eq!(
        parse_antigravity_json_line(line).as_deref(),
        Some("Processing...")
    );
}

#[test]
fn parse_antigravity_tool_use() {
    let line = r#"{"type":"tool_use","name":"Bash","input":{"command":"ls -la"}}"#;
    assert_eq!(
        parse_antigravity_json_line(line).as_deref(),
        Some("▶ Bash: ls -la")
    );
}

#[test]
fn parse_antigravity_tool_result() {
    let line = r#"{"type":"tool_result","output":"hello\n"}"#;
    assert_eq!(
        parse_antigravity_json_line(line).as_deref(),
        Some("✓ tool: hello")
    );
}

#[test]
fn parse_antigravity_tool_result_with_name_and_input() {
    let line = r#"{"type":"tool_result","name":"run_shell_command","input":{"command":"ls -la"},"output":"done"}"#;
    assert_eq!(
        parse_antigravity_json_line(line).as_deref(),
        Some("✓ run_shell_command: ls -la")
    );
}

#[test]
fn parse_antigravity_assistant_message() {
    let line =
        r#"{"type":"message","role":"assistant","content":[{"type":"text","text":"hello"}]}"#;
    assert_eq!(
        parse_antigravity_json_line(line).as_deref(),
        Some("Drafting response...")
    );
}

#[test]
fn parse_antigravity_error() {
    let line = r#"{"type":"error","message":"API rate limit"}"#;
    assert_eq!(
        parse_antigravity_json_line(line).as_deref(),
        Some("⚠ API rate limit")
    );
}

#[test]
fn extract_antigravity_json_final_prefers_result() {
    let raw = r#"{"type":"init","session_id":"s1"}
{"type":"message","role":"assistant","content":[{"type":"text","text":"not-final"}]}
{"type":"result","response":"TELEGRAM_REPLY: final answer","stats":{}}
"#;
    let text = extract_antigravity_json_final(raw);
    assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: final answer"));
}

#[test]
fn extract_antigravity_json_final_falls_back_to_message() {
    let raw = r#"{"type":"init","session_id":"s1"}
{"type":"message","role":"assistant","content":[{"type":"text","text":"TELEGRAM_REPLY: from message"}]}
"#;
    let text = extract_antigravity_json_final(raw);
    assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: from message"));
}

#[test]
fn extract_antigravity_json_final_string_content() {
    let raw = r#"{"type":"message","role":"assistant","content":"TELEGRAM_REPLY: plain string"}
"#;
    let text = extract_antigravity_json_final(raw);
    assert_eq!(text.as_deref(), Some("TELEGRAM_REPLY: plain string"));
}

#[test]
fn extract_antigravity_json_final_accumulates_deltas() {
    let raw = r#"{"type":"init","session_id":"s1","model":"antigravity-3.1-pro-preview"}
{"type":"message","role":"user","content":"say hello"}
{"type":"message","role":"assistant","content":"TELEGRAM_REPLY: This is a test reply with","delta":true}
{"type":"message","role":"assistant","content":" multiple sentences. The quick brown fox.","delta":true}
{"type":"result","status":"success","stats":{"total_tokens":100}}
"#;
    let text = extract_antigravity_json_final(raw);
    assert_eq!(
        text.as_deref(),
        Some("TELEGRAM_REPLY: This is a test reply with multiple sentences. The quick brown fox.")
    );
}

#[test]
fn run_antigravity_prefers_stdout_over_stderr_banner_for_plain_text() {
    let mut config = RuntimeConfig::test_config();
    config.provider = AgentProvider::Antigravity;
    config.exec_policy = "yolo".to_string();

    let script = r#"#!/bin/sh
printf 'TELEGRAM_REPLY: hello from fake antigravity\n'
printf 'YOLO mode is enabled. All tool calls will be automatically approved.\n' >&2
printf 'YOLO mode is enabled. All tool calls will be automatically approved.\n' >&2
"#;
    let windows_script = r#"
Write-Output 'TELEGRAM_REPLY: hello from fake antigravity'
[Console]::Error.WriteLine('YOLO mode is enabled. All tool calls will be automatically approved.')
[Console]::Error.WriteLine('YOLO mode is enabled. All tool calls will be automatically approved.')
"#;

    config.antigravity.bin =
        write_test_provider_script(&config.root_dir, "fake-antigravity", script, windows_script);

    let output =
        run_provider(None, &config, "hello", None, None, Some(5)).expect("run fake antigravity");
    assert!(output.success);
    assert_eq!(
        output.raw_output.trim(),
        "TELEGRAM_REPLY: hello from fake antigravity"
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
fn run_opencode_sets_per_instance_db_path() {
    let mut config = RuntimeConfig::test_config();
    config.provider = AgentProvider::OpenCode;

    let log_path = config.root_dir.join("fake-opencode-db.log");
    let script = format!(
        "#!/bin/sh\nlog=\"{}\"\nprintf 'db:%s\\n' \"${{OPENCODE_DB:-}}\" >> \"$log\"\ncat <<'EOF'\n{{\"type\":\"text\",\"timestamp\":1,\"sessionID\":\"s1\",\"part\":{{\"type\":\"text\",\"text\":\"TELEGRAM_REPLY: hello\"}}}}\nEOF\n",
        log_path.display()
    );
    let windows_script = format!(
        "$log = \"{}\"\nAdd-Content -Path $log -Value \"db:${{env:OPENCODE_DB}}\"\nWrite-Output '{{\"type\":\"text\",\"timestamp\":1,\"sessionID\":\"s1\",\"part\":{{\"type\":\"text\",\"text\":\"TELEGRAM_REPLY: hello\"}}}}'\n",
        log_path.display()
    );

    config.opencode.bin = write_test_provider_script(
        &config.root_dir,
        "fake-opencode-db",
        &script,
        &windows_script,
    );

    let output = run_provider(None, &config, "hello", None, None, Some(5))
        .expect("run fake opencode provider");
    assert!(output.success);
    assert_eq!(output.raw_output, "TELEGRAM_REPLY: hello");

    let log = fs::read_to_string(&log_path).expect("read fake opencode log");
    let expected_db = config.instance_dir.join("opencode.db");
    assert!(
        log.contains(&format!("db:{}", expected_db.display())),
        "expected OPENCODE_DB={}, got log:\n{log}",
        expected_db.display()
    );
}

#[test]
fn shorten_status_text_empty() {
    assert_eq!(shorten_status_text("", 5), "");
}
