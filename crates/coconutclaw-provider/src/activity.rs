use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(super) struct TimeoutPolicy {
    pub total: Option<Duration>,
    pub idle: Duration,
    pub tool_idle: Duration,
}

impl TimeoutPolicy {
    pub fn new(total: Option<u64>, idle: u64, tool_idle: u64) -> Self {
        Self {
            total: total.filter(|secs| *secs != 0).map(Duration::from_secs),
            idle: Duration::from_secs(idle),
            tool_idle: Duration::from_secs(tool_idle),
        }
    }
}

pub(super) struct Activity {
    pub last: Instant,
    items: HashMap<String, ItemActivity>,
    thread_started: bool,
    turn_started: bool,
    turn_finished: bool,
}

#[derive(Default)]
struct ItemActivity {
    seen: HashSet<u64>,
    pending: bool,
    completed: bool,
}

impl Activity {
    pub fn new(now: Instant) -> Self {
        Self {
            last: now,
            items: HashMap::new(),
            thread_started: false,
            turn_started: false,
            turn_finished: false,
        }
    }

    // Only exec --json stdout is evidence. In particular, reconnect errors,
    // stderr bytes and repeated snapshots are not forward progress.
    pub fn observe(&mut self, line: &str, now: Instant) {
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            return;
        };
        let kind = event["type"].as_str().unwrap_or_default();
        let changed = match kind {
            "thread.started" if event["thread_id"].as_str().is_some() => {
                !std::mem::replace(&mut self.thread_started, true)
            }
            "turn.started" => !std::mem::replace(&mut self.turn_started, true),
            "turn.completed" | "turn.failed" => {
                let changed = !std::mem::replace(&mut self.turn_finished, true);
                for item in self.items.values_mut() {
                    item.pending = false;
                    item.seen.clear();
                }
                changed
            }
            "item.started" | "item.updated" | "item.completed" if !self.turn_finished => {
                self.observe_item(&event, kind)
            }
            _ => false,
        };
        if changed {
            self.last = now;
        }
    }

    fn observe_item(&mut self, event: &Value, phase: &str) -> bool {
        let item = &event["item"];
        let Some(id) = item["id"].as_str().filter(|id| !id.is_empty()) else {
            return false;
        };
        let Some(kind) = item["type"].as_str() else {
            return false;
        };
        let fields: &[&str] = match kind {
            "command_execution" => &["command", "status", "aggregated_output", "exit_code"],
            "mcp_tool_call" => &["server", "tool", "arguments", "status", "result", "error"],
            "web_search" => &["query", "action", "status"],
            "reasoning" | "agent_message" => &["text"],
            "file_change" => &["changes", "status"],
            "todo_list" => &["items"],
            _ => return false,
        };
        if fields.iter().all(|field| item[*field].is_null()) {
            return false;
        }
        // Keep terminal state sticky even when delivered as item.updated.
        let completed = phase == "item.completed"
            || matches!(item["status"].as_str(), Some("completed" | "failed"));
        let mut hash = DefaultHasher::new();
        kind.hash(&mut hash);
        completed.hash(&mut hash);
        for field in fields {
            item[*field].to_string().hash(&mut hash);
        }
        let fingerprint = hash.finish();
        let state = self.items.entry(id.to_owned()).or_default();
        // Retain all unfinished semantic fingerprints, not just the latest:
        // A, AB, A, AB must not renew silence or mutate pending state.
        // Never evict old fingerprints while the item can still be updated.
        if state.completed || !state.seen.insert(fingerprint) {
            return false;
        }
        state.pending =
            matches!(kind, "command_execution" | "mcp_tool_call" | "web_search") && !completed;
        state.completed = completed;
        if completed {
            // Only the terminal tombstone is needed from here on.
            state.seen = HashSet::new();
        }
        true
    }

    pub fn timeout(&self, policy: TimeoutPolicy, start: Instant, now: Instant) -> Option<String> {
        let elapsed = now.duration_since(start);
        if let Some(total) = policy.total
            && elapsed >= total
        {
            return Some(format!(
                "provider execution timed out: configured total deadline of {}s reached after {:.1}s",
                total.as_secs(),
                elapsed.as_secs_f64()
            ));
        }
        let pending = self.items.values().any(|item| item.pending);
        let allowance = if pending {
            policy.tool_idle
        } else {
            policy.idle
        };
        let silent = now.duration_since(self.last);
        (silent >= allowance).then(|| {
            format!(
                "provider execution timed out: inactivity, no meaningful Codex progress for {:.1}s (limit {}s, elapsed {:.1}s, pending tool: {pending}); silence does not prove a stall",
                silent.as_secs_f64(), allowance.as_secs(), elapsed.as_secs_f64()
            )
        })
    }
}
