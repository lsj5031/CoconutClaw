use super::activity::{Activity, TimeoutPolicy};
use std::time::{Duration, Instant};

fn event(id: &str, kind: &str, phase: &str, fields: &str) -> String {
    format!(r#"{{"type":"item.{phase}","item":{{"id":"{id}","type":"{kind}",{fields}}}}}"#)
}

#[test]
fn default_policy_allows_progress_past_old_deadline_and_thirty_minutes() {
    let start = Instant::now();
    let policy = TimeoutPolicy::new(None, 1800, 7200);
    let mut activity = Activity::new(start);
    for seconds in [299, 301, 1801, 2701, 3601, 4501, 5401, 6301, 7201] {
        let now = start + Duration::from_secs(seconds);
        assert!(activity.timeout(policy, start, now).is_none());
        activity.observe(
            &event(
                &seconds.to_string(),
                "reasoning",
                "completed",
                r#""text":"next step""#,
            ),
            now,
        );
        assert!(activity.timeout(policy, start, now).is_none());
    }
    assert!(
        activity
            .timeout(policy, start, activity.last + Duration::from_secs(1799))
            .is_none()
    );
    let reason = activity
        .timeout(policy, start, activity.last + Duration::from_secs(1800))
        .unwrap();
    assert!(reason.contains("inactivity") && reason.contains("elapsed 9001.0s"));
}

#[test]
fn total_deadline_zero_none_and_exact_boundaries() {
    let start = Instant::now();
    let mut activity = Activity::new(start);
    let now = start + Duration::from_secs(86400);
    activity.observe(
        &event("r", "reasoning", "completed", r#""text":"working""#),
        now,
    );
    for total in [None, Some(0)] {
        assert!(
            activity
                .timeout(TimeoutPolicy::new(total, 1800, 7200), start, now)
                .is_none()
        );
    }
    let policy = TimeoutPolicy::new(Some(86400), 1800, 7200);
    assert!(
        activity
            .timeout(policy, start, now - Duration::from_nanos(1))
            .is_none()
    );
    assert!(
        activity
            .timeout(policy, start, now)
            .unwrap()
            .contains("configured total deadline of 86400s")
    );
}

#[test]
fn noise_replayed_events_and_unknown_fields_do_not_renew_activity() {
    let start = Instant::now();
    let mut activity = Activity::new(start);
    let started = event(
        "c",
        "command_execution",
        "started",
        r#""command":"sleep 1","status":"in_progress","aggregated_output":"""#,
    );
    activity.observe(&started, start);
    let later = start + Duration::from_secs(1799);
    for noise in [
        "junk",
        r#"{"type":"heartbeat"}"#,
        r#"{"type":"error","message":"Reconnecting... 1/5"}"#,
        r#"{"type":"item.updated","item":{"id":"x","type":"unknown","text":"hello"}}"#,
        r#"{"type":"item.completed","item":{"type":"reasoning","text":"missing id"}}"#,
        &started,
        &started.replace("item.started", "item.updated"),
        &started.replace(
            r#""command":"sleep 1""#,
            r#""timestamp":99,"command":"sleep 1""#,
        ),
    ] {
        activity.observe(noise, later);
        assert_eq!(activity.last, start, "{noise}");
    }
    assert!(
        activity
            .timeout(
                TimeoutPolicy::new(None, 1800, 7200),
                start,
                start + Duration::from_secs(7200)
            )
            .unwrap()
            .contains("pending tool: true")
    );
}

#[test]
fn overlapping_tools_completion_output_and_silent_reasoning_have_distinct_allowances() {
    let start = Instant::now();
    let policy = TimeoutPolicy::new(None, 1800, 7200);
    let mut activity = Activity::new(start);
    for (id, kind, fields) in [
        (
            "c",
            "command_execution",
            r#""command":"build","status":"in_progress","aggregated_output":"""#,
        ),
        (
            "m",
            "mcp_tool_call",
            r#""server":"local","tool":"wait","status":"in_progress""#,
        ),
    ] {
        activity.observe(&event(id, kind, "started", fields), start);
    }
    assert!(
        activity
            .timeout(policy, start, start + Duration::from_secs(3600))
            .is_none()
    );
    let output_time = start + Duration::from_secs(3600);
    activity.observe(
        &event(
            "c",
            "command_execution",
            "updated",
            r#""command":"build","aggregated_output":"compiled","status":"in_progress""#,
        ),
        output_time,
    );
    assert_eq!(activity.last, output_time);
    activity.observe(
        &event(
            "c",
            "command_execution",
            "completed",
            r#""command":"build","exit_code":0,"status":"completed""#,
        ),
        output_time,
    );
    assert!(
        activity
            .timeout(policy, start, output_time + Duration::from_secs(1800))
            .is_none()
    );
    let finished = output_time + Duration::from_secs(1800);
    activity.observe(
        &event(
            "m",
            "mcp_tool_call",
            "completed",
            r#""status":"completed","result":{}"#,
        ),
        finished,
    );
    activity.observe(
        &event("r", "reasoning", "started", r#""text":"thinking""#),
        finished,
    );
    activity.observe(
        &event("r", "reasoning", "updated", r#""text":"thinking more""#),
        finished + Duration::from_secs(1),
    );
    assert_eq!(activity.last, finished + Duration::from_secs(1));
    let later = finished + Duration::from_secs(1801);
    assert!(
        activity
            .timeout(policy, start, later)
            .unwrap()
            .contains("pending tool: false")
    );
    activity.observe(
        &event("m", "mcp_tool_call", "started", r#""status":"in_progress""#),
        later,
    );
    assert_eq!(
        activity.last,
        finished + Duration::from_secs(1),
        "completed tool cannot be reopened"
    );
}

#[test]
fn activity_is_per_invocation_and_turn_end_closes_pending_tools() {
    let start = Instant::now();
    let policy = TimeoutPolicy::new(None, 1800, 7200);
    let mut first = Activity::new(start);
    let second = Activity::new(start);
    let later = start + Duration::from_secs(1800);
    first.observe(
        &event("w", "web_search", "started", r#""query":"weather""#),
        later,
    );
    assert!(first.timeout(policy, start, later).is_none());
    assert!(second.timeout(policy, start, later).is_some());
    first.observe(r#"{"type":"turn.completed","usage":{}}"#, later);
    assert!(
        first
            .timeout(policy, start, later + Duration::from_secs(1800))
            .unwrap()
            .contains("pending tool: false")
    );
}

#[test]
fn older_unfinished_snapshots_cannot_extend_default_silence_indefinitely() {
    for (kind, first, second) in [
        (
            "command_execution",
            r#""command":"build","status":"in_progress","aggregated_output":"A""#,
            r#""command":"build","status":"in_progress","aggregated_output":"AB""#,
        ),
        (
            "mcp_tool_call",
            r#""tool":"wait","status":"in_progress","result":"A""#,
            r#""tool":"wait","status":"in_progress","result":"AB""#,
        ),
        ("web_search", r#""query":"A""#, r#""query":"AB""#),
        ("reasoning", r#""text":"A""#, r#""text":"AB""#),
        ("agent_message", r#""text":"A""#, r#""text":"AB""#),
        (
            "file_change",
            r#""changes":["A"],"status":"in_progress""#,
            r#""changes":["AB"],"status":"in_progress""#,
        ),
        ("todo_list", r#""items":["A"]"#, r#""items":["AB"]"#),
    ] {
        let start = Instant::now();
        let mut activity = Activity::new(start);
        let a = event("item", kind, "updated", first);
        let ab = event("item", kind, "updated", second);
        activity.observe(&a, start);
        activity.observe(&ab, start + Duration::from_secs(1));
        for seconds in (2..=14402).step_by(2) {
            activity.observe(&a, start + Duration::from_secs(seconds));
            activity.observe(&ab, start + Duration::from_secs(seconds + 1));
        }
        assert_eq!(activity.last, start + Duration::from_secs(1), "{kind}");
        let policy = TimeoutPolicy::new(None, 1800, 7200);
        let allowance = if matches!(kind, "command_execution" | "mcp_tool_call" | "web_search") {
            7200
        } else {
            1800
        };
        assert!(
            activity
                .timeout(policy, start, start + Duration::from_secs(allowance))
                .is_none()
        );
        assert!(
            activity
                .timeout(policy, start, start + Duration::from_secs(allowance + 1))
                .unwrap()
                .contains("inactivity")
        );
    }
}

#[test]
fn novel_snapshots_including_replaced_text_still_renew_activity() {
    let start = Instant::now();
    let mut activity = Activity::new(start);
    let policy = TimeoutPolicy::new(None, 1800, 7200);
    for (index, text) in ["A", "AB", "replacement", "C", "CD"].iter().enumerate() {
        let now = start + Duration::from_secs(index as u64 * 7000);
        assert!(activity.timeout(policy, start, now).is_none());
        activity.observe(
            &event(
                "c",
                "command_execution",
                "updated",
                &format!(r#""status":"in_progress","aggregated_output":"{text}""#),
            ),
            now,
        );
        assert_eq!(activity.last, now);
    }
    // An old snapshot must not prevent later genuine novelty either.
    activity.observe(
        &event(
            "c",
            "command_execution",
            "updated",
            r#""status":"in_progress","aggregated_output":"A""#,
        ),
        activity.last + Duration::from_secs(1),
    );
    let next = start + Duration::from_secs(35000);
    activity.observe(
        &event(
            "c",
            "command_execution",
            "updated",
            r#""status":"in_progress","aggregated_output":"CDE""#,
        ),
        next,
    );
    assert_eq!(activity.last, next);
}

#[test]
fn terminal_status_updates_are_sticky_and_overlapping_tools_stay_pending() {
    for status in ["completed", "failed"] {
        let start = Instant::now();
        let mut activity = Activity::new(start);
        let a = event(
            "a",
            "command_execution",
            "started",
            r#""command":"build","status":"in_progress""#,
        );
        let b = event(
            "b",
            "mcp_tool_call",
            "started",
            r#""tool":"wait","status":"in_progress""#,
        );
        activity.observe(&a, start);
        activity.observe(&b, start);
        let end = start + Duration::from_secs(1);
        activity.observe(
            &event(
                "a",
                "command_execution",
                "updated",
                &format!(r#""status":"{status}""#),
            ),
            end,
        );
        activity.observe(&a, end + Duration::from_secs(1));
        // Even an unseen stale payload must not reopen a terminal item.
        activity.observe(
            &event(
                "a",
                "command_execution",
                "updated",
                r#""status":"in_progress","aggregated_output":"late""#,
            ),
            end + Duration::from_secs(2),
        );
        assert_eq!(activity.last, end);
        let policy = TimeoutPolicy::new(None, 1800, 7200);
        assert!(
            activity
                .timeout(policy, start, end + Duration::from_secs(1800))
                .is_none()
        );
        activity.observe(
            &event(
                "b",
                "mcp_tool_call",
                "completed",
                r#""status":"failed","error":"synthetic""#,
            ),
            end,
        );
        activity.observe(&a, end + Duration::from_secs(3));
        activity.observe(&b, end + Duration::from_secs(4));
        assert_eq!(activity.last, end);
        assert!(
            activity
                .timeout(policy, start, end + Duration::from_secs(1800))
                .unwrap()
                .contains("pending tool: false")
        );
    }
}
