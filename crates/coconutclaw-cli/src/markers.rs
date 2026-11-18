use serde_json::Value;

pub(crate) const MAX_OUTPUT_BYTES: usize = 512 * 1024;
pub(crate) const MAX_DELEGATIONS: usize = 4;
pub(crate) const MAX_MEDIA: usize = 8;
pub(crate) const MAX_MEMORY_APPENDS: usize = 8;
pub(crate) const MAX_TASK_APPENDS: usize = 16;

pub(crate) fn rejection(reason: &str) -> String {
    format!("Output rejected: {reason}. No requested effects were applied.")
}

/// Reject the whole outcome, never execute an accepted prefix of a bad batch.
pub(crate) fn validate_effects(effects: &[Effect], delegated: bool) -> Result<(), &'static str> {
    let (mut delegates, mut media, mut memory, mut tasks) = (0, 0, 0, 0);
    let mut bytes = 0usize;
    for effect in effects {
        bytes = bytes.saturating_add(effect.payload().len());
        let limit = match effect {
            Effect::DelegateTask(_) => {
                delegates += 1;
                if delegated {
                    return Err("delegated tasks cannot delegate");
                }
                16 * 1024
            }
            Effect::SendPhoto(_) | Effect::SendDocument(_) | Effect::SendVideo(_) => {
                media += 1;
                4096
            }
            Effect::MemoryAppend(_) => {
                memory += 1;
                2048
            }
            Effect::TaskAppend(_) => {
                tasks += 1;
                2048
            }
            Effect::TelegramReply(_) => 256 * 1024,
            Effect::SendApproval(_) | Effect::WaitForTask(_) => 4096,
        };
        if effect.payload().len() > limit {
            return Err("marker payload byte limit exceeded");
        }
    }
    if bytes > MAX_OUTPUT_BYTES {
        return Err("output byte limit exceeded");
    }
    if delegates > MAX_DELEGATIONS
        || media > MAX_MEDIA
        || memory > MAX_MEMORY_APPENDS
        || tasks > MAX_TASK_APPENDS
        || effects.len() > 48
    {
        return Err("marker count limit exceeded");
    }
    Ok(())
}

/// Typed output effect produced by the AI provider.
///
/// Every marker line the model emits (`TELEGRAM_REPLY:`, `SEND_PHOTO:`, …)
/// compiles to one `Effect`.  Transports iterate over `Effect`s instead of
/// inspecting raw marker fields — adding a new transport becomes "match one
/// enum variant", not "grep for marker name across the codebase".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Effect {
    TelegramReply(String),
    SendPhoto(String),
    SendDocument(String),
    SendVideo(String),
    SendApproval(String),
    DelegateTask(String),
    WaitForTask(String),
    MemoryAppend(String),
    TaskAppend(String),
}

impl Effect {
    /// Human-readable label for logging / debug.
    #[allow(dead_code)]
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::TelegramReply(_) => "telegram_reply",
            Self::SendPhoto(_) => "send_photo",
            Self::SendDocument(_) => "send_document",
            Self::SendVideo(_) => "send_video",
            Self::SendApproval(_) => "send_approval",
            Self::DelegateTask(_) => "delegate_task",
            Self::WaitForTask(_) => "wait_for_task",
            Self::MemoryAppend(_) => "memory_append",
            Self::TaskAppend(_) => "task_append",
        }
    }

    /// The payload string carried by the effect.
    #[allow(dead_code)]
    pub(crate) fn payload(&self) -> &str {
        match self {
            Self::TelegramReply(s)
            | Self::SendPhoto(s)
            | Self::SendDocument(s)
            | Self::SendVideo(s)
            | Self::DelegateTask(s)
            | Self::WaitForTask(s)
            | Self::SendApproval(s)
            | Self::MemoryAppend(s)
            | Self::TaskAppend(s) => s.as_str(),
        }
    }
}

/// Render a slice of `Effect`s back to the text-marker wire format.
/// Used when persisting or re-serialising marker output.
pub(crate) fn render_effects(effects: &[Effect]) -> String {
    let mut lines = Vec::new();
    for effect in effects {
        let marker = match effect {
            Effect::TelegramReply(_) => "TELEGRAM_REPLY: ",
            Effect::SendPhoto(_) => "SEND_PHOTO: ",
            Effect::SendDocument(_) => "SEND_DOCUMENT: ",
            Effect::SendVideo(_) => "SEND_VIDEO: ",
            Effect::SendApproval(_) => "SEND_APPROVAL: ",
            Effect::DelegateTask(_) => "DELEGATE_TASK: ",
            Effect::WaitForTask(_) => "WAIT_FOR_TASK: ",
            Effect::MemoryAppend(_) => "MEMORY_APPEND: ",
            Effect::TaskAppend(_) => "TASK_APPEND: ",
        };
        lines.push(format!("{marker}{}", effect.payload()));
    }
    if lines.is_empty() {
        String::new()
    } else {
        let mut out = lines.join("\n");
        out.push('\n');
        out
    }
}

#[derive(Debug, Default)]
pub(crate) struct ParsedMarkers {
    pub(crate) telegram_reply: Option<String>,
    pub(crate) send_photo: Vec<String>,
    pub(crate) send_document: Vec<String>,
    pub(crate) send_video: Vec<String>,
    pub(crate) send_approval: Vec<String>,
    pub(crate) memory_append: Vec<String>,
    pub(crate) task_append: Vec<String>,
    pub(crate) delegate_task: Vec<String>,
    pub(crate) wait_for_task: Vec<String>,
}

impl ParsedMarkers {
    /// Channel-agnostic accessor: returns the reply text regardless of which
    /// marker prefix (`REPLY:` or `TELEGRAM_REPLY:`) was used on input.
    pub(crate) fn reply(&self) -> Option<&String> {
        self.telegram_reply.as_ref()
    }

    /// Compile parsed markers into a typed `Vec<Effect>`.
    ///
    /// Call this once after parsing so transports can `match` on variants
    /// instead of reaching into marker fields.
    pub(crate) fn to_effects(&self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(ref text) = self.telegram_reply {
            effects.push(Effect::TelegramReply(text.clone()));
        }
        for path in &self.send_photo {
            effects.push(Effect::SendPhoto(path.clone()));
        }
        for path in &self.send_document {
            effects.push(Effect::SendDocument(path.clone()));
        }
        for path in &self.send_video {
            effects.push(Effect::SendVideo(path.clone()));
        }
        for text in &self.send_approval {
            effects.push(Effect::SendApproval(text.clone()));
        }
        for text in &self.memory_append {
            effects.push(Effect::MemoryAppend(text.clone()));
        }
        for text in &self.task_append {
            effects.push(Effect::TaskAppend(text.clone()));
        }
        for text in &self.delegate_task {
            effects.push(Effect::DelegateTask(text.clone()));
        }
        for text in &self.wait_for_task {
            effects.push(Effect::WaitForTask(text.clone()));
        }
        effects
    }

    /// True when the markers include at least one output the transport layer
    /// should act on (text or media).
    #[allow(dead_code)]
    pub(crate) fn has_output(&self) -> bool {
        self.telegram_reply.is_some()
            || !self.send_photo.is_empty()
            || !self.send_document.is_empty()
            || !self.send_video.is_empty()
    }
}

/// Render the retained output effects.
pub(crate) fn render_output(reply: &str, effects: &[Effect]) -> String {
    let mut all = Vec::with_capacity(effects.len() + 2);
    all.push(Effect::TelegramReply(reply.to_string()));
    for e in effects {
        if !matches!(e, Effect::TelegramReply(_)) {
            all.push(e.clone());
        }
    }
    render_effects(&all)
}

/// Compatibility envelope: closed leading reasoning blocks followed immediately
/// by an explicit final-reply marker. Never scan inside marker values or decoded
/// assistant text, and never truncate an unclosed block.
pub(crate) fn final_marker_payload(raw_output: &str) -> &str {
    let mut remaining = raw_output.trim_start();
    while let Some(thought) = remaining.strip_prefix("<think>") {
        let Some((_, rest)) = thought.split_once("</think>") else {
            return raw_output;
        };
        remaining = rest.trim_start();
    }
    if remaining.starts_with("TELEGRAM_REPLY:") || remaining.starts_with("REPLY:") {
        remaining
    } else {
        raw_output
    }
}

pub(crate) fn parse_markers(payload: &str) -> ParsedMarkers {
    if payload.len() > MAX_OUTPUT_BYTES {
        return rejected_markers("output byte limit exceeded");
    }
    let markers = parse_markers_unchecked(payload);
    if let Err(reason) = validate_effects(&markers.to_effects(), false) {
        return rejected_markers(reason);
    }
    markers
}

fn rejected_markers(reason: &str) -> ParsedMarkers {
    ParsedMarkers {
        telegram_reply: Some(rejection(reason)),
        ..Default::default()
    }
}

fn parse_markers_unchecked(payload: &str) -> ParsedMarkers {
    // Every effect compiler, including raw-turn recovery and legacy replay,
    // must exclude the same envelope as initial resolution.
    let payload = final_marker_payload(payload);
    let markers = parse_markers_line_anchored(payload);
    if markers.telegram_reply.is_some() {
        return markers;
    }

    if let Some(recovered_payload) = recover_embedded_marker_payload(payload) {
        let recovered = parse_markers_line_anchored(recovered_payload);
        if recovered.telegram_reply.is_some() {
            return recovered;
        }
    }

    markers
}

fn parse_markers_line_anchored(payload: &str) -> ParsedMarkers {
    let mut markers = ParsedMarkers::default();
    let mut telegram_blocks = Vec::new();

    let mut current_marker: Option<&str> = None;
    let mut current_block = String::new();

    for line in payload.lines() {
        let line_trimmed = line.trim_start();

        if let Some((marker, content)) = detect_any_marker(line_trimmed) {
            // Commit previous block
            commit_block(
                &mut markers,
                &mut telegram_blocks,
                current_marker,
                &current_block,
            );

            // Start new block
            current_marker = Some(marker);
            current_block = content.to_string();
        } else if current_marker.is_some() {
            // Append to current block
            if !current_block.is_empty() {
                current_block.push('\n');
            }
            current_block.push_str(line);
        }
        // Note: lines before any marker are ignored (not prepended to marker content)
    }

    // Final commit
    commit_block(
        &mut markers,
        &mut telegram_blocks,
        current_marker,
        &current_block,
    );

    // Merge multi-block text fields
    if !telegram_blocks.is_empty() {
        // Marker values are native text. Backslashes belong to Markdown/code,
        // not an encoded envelope; JSON escapes are decoded at JSON extraction.
        markers.telegram_reply = Some(telegram_blocks.join("\n\n"));
    }

    markers
}

fn recover_embedded_marker_payload(payload: &str) -> Option<&str> {
    let first_line = payload.lines().next()?;
    let first_line_trimmed = first_line.trim_start();
    if detect_any_marker(first_line_trimmed).is_some() {
        return None;
    }
    if !starts_with_wrapper(first_line_trimmed) {
        return None;
    }

    let first_line_end = payload.find('\n').unwrap_or(payload.len());
    let head = &payload[..first_line_end];
    let start = marker_prefixes()
        .iter()
        .filter_map(|prefix| head.find(prefix))
        .min()?;

    if start == 0 {
        None
    } else {
        Some(&payload[start..])
    }
}

fn starts_with_wrapper(line: &str) -> bool {
    ["\"\"\"", "'''", "```", "\"", "'", "`"]
        .iter()
        .any(|prefix| line.starts_with(prefix))
}

fn marker_prefixes() -> &'static [&'static str] {
    &[
        "REPLY:",
        "TELEGRAM_REPLY:",
        "VOICE_REPLY:",
        "SEND_PHOTO:",
        "SEND_DOCUMENT:",
        "SEND_VIDEO:",
        "SEND_APPROVAL:",
        "DELEGATE_TASK:",
        "WAIT_FOR_TASK:",
        "MEMORY_APPEND:",
        "TASK_APPEND:",
        "SCHEDULE_PROMPT:",
    ]
}

fn detect_any_marker(line: &str) -> Option<(&'static str, &str)> {
    const MARKERS: &[(&str, &str)] = &[
        ("TELEGRAM_REPLY", "REPLY:"),
        ("TELEGRAM_REPLY", "TELEGRAM_REPLY:"),
        ("VOICE_REPLY", "VOICE_REPLY:"),
        ("SEND_PHOTO", "SEND_PHOTO:"),
        ("SEND_DOCUMENT", "SEND_DOCUMENT:"),
        ("SEND_VIDEO", "SEND_VIDEO:"),
        ("SEND_APPROVAL", "SEND_APPROVAL:"),
        ("DELEGATE_TASK", "DELEGATE_TASK:"),
        ("WAIT_FOR_TASK", "WAIT_FOR_TASK:"),
        ("MEMORY_APPEND", "MEMORY_APPEND:"),
        ("TASK_APPEND", "TASK_APPEND:"),
        ("SCHEDULE_PROMPT", "SCHEDULE_PROMPT:"),
    ];

    for &(name, prefix) in MARKERS {
        if let Some(rest) = line.strip_prefix(prefix) {
            return Some((name, rest.trim_start()));
        }
    }
    None
}

fn commit_block(
    markers: &mut ParsedMarkers,
    telegram_blocks: &mut Vec<String>,
    marker: Option<&str>,
    block: &str,
) {
    let Some(m) = marker else { return };
    let content = block.trim();
    if content.is_empty() {
        return;
    }

    match m {
        "TELEGRAM_REPLY" => telegram_blocks.push(content.to_string()),
        "VOICE_REPLY" => {}
        "SEND_PHOTO" => markers.send_photo.push(content.to_string()),
        "SEND_DOCUMENT" => markers.send_document.push(content.to_string()),
        "SEND_VIDEO" => markers.send_video.push(content.to_string()),
        "SEND_APPROVAL" => markers.send_approval.push(content.to_string()),
        "MEMORY_APPEND" => markers.memory_append.push(content.to_string()),
        "TASK_APPEND" => markers.task_append.push(content.to_string()),
        "SCHEDULE_PROMPT" => {}
        "DELEGATE_TASK" => markers.delegate_task.push(content.to_string()),
        "WAIT_FOR_TASK" => markers.wait_for_task.push(content.to_string()),
        _ => {}
    }
}

pub(crate) fn recover_unstructured_reply(raw_output: &str) -> Option<String> {
    let trimmed = raw_output.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(reply) = extract_assistant_text_from_json_stream(trimmed)
        && !reply.trim().is_empty()
    {
        return Some(reply);
    }

    if looks_like_json_event_stream(trimmed) {
        return None;
    }

    Some(trimmed.to_string())
}

pub(crate) fn should_retry_provider_failure(raw_output: &str) -> bool {
    let needles = [
        "network failure",
        "connection reset",
        "connection refused",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "service unavailable",
        "too many requests",
        "rate limit",
        "api error: json parse error",
    ];

    // Check all JSON errors first
    let json_errors = extract_error_summaries(raw_output);
    for summary in &json_errors {
        let lower = summary.to_ascii_lowercase();
        if needles.iter().any(|needle| lower.contains(needle)) {
            return true;
        }
    }

    // If no JSON errors matched (or none were found), we only fall back to raw output
    // if there were no JSON error events found at all. This prevents us from matching
    // retryable keywords in non-error JSON output (like tool outputs) when a non-retryable
    // error caused the failure.
    if json_errors.is_empty() {
        let lower_raw = raw_output.to_ascii_lowercase();
        needles.iter().any(|needle| lower_raw.contains(needle))
    } else {
        false
    }
}

pub(crate) fn extract_error_summaries(payload: &str) -> Vec<String> {
    let mut found = Vec::new();

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        let candidate = match event_type {
            "agent_end" => value
                .get("error")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            "turn.failed" => value
                .get("error")
                .and_then(|node| {
                    node.get("message")
                        .and_then(Value::as_str)
                        .or_else(|| node.as_str())
                })
                .map(ToOwned::to_owned),
            "turn_end" => value
                .get("message")
                .and_then(|node| node.get("errorMessage"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            _ => None,
        };

        if let Some(text) = candidate
            && !text.trim().is_empty()
        {
            found.push(text);
        }
    }

    found
}

pub(crate) fn extract_error_summary(payload: &str) -> Option<String> {
    extract_error_summaries(payload).into_iter().last()
}

fn looks_like_json_event_stream(payload: &str) -> bool {
    let mut parsed_lines = 0usize;
    let mut typed_events = 0usize;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        parsed_lines += 1;
        if value.get("type").and_then(Value::as_str).is_some() {
            typed_events += 1;
        }
    }

    parsed_lines > 0 && typed_events > 0
}

pub(crate) fn extract_assistant_text_from_json_stream(payload: &str) -> Option<String> {
    let mut final_text: Option<String> = None;
    let mut parsed_json_lines = 0usize;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        parsed_json_lines += 1;
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        let candidate = match event_type {
            "item.completed" => value.get("item").and_then(|item| {
                if item.get("type")?.as_str()? != "agent_message" {
                    return None;
                }
                item.get("text")?.as_str().map(ToOwned::to_owned)
            }),
            "message_end" => {
                let role = value
                    .get("message")
                    .and_then(|node| node.get("role"))
                    .and_then(Value::as_str);
                if role == Some("assistant") {
                    value
                        .get("message")
                        .and_then(|node| node.get("content"))
                        .and_then(join_text_blocks)
                } else {
                    None
                }
            }
            "agent_end" => {
                let text_from_messages =
                    value
                        .get("messages")
                        .and_then(Value::as_array)
                        .and_then(|messages| {
                            let mut chunks = Vec::new();
                            for message in messages {
                                let role = message.get("role").and_then(Value::as_str);
                                if role != Some("assistant") {
                                    continue;
                                }
                                if let Some(content) =
                                    message.get("content").and_then(join_text_blocks)
                                {
                                    chunks.push(content);
                                }
                            }
                            if chunks.is_empty() {
                                None
                            } else {
                                Some(chunks.join("\n"))
                            }
                        });
                text_from_messages.or_else(|| {
                    value
                        .get("error")
                        .and_then(Value::as_str)
                        .filter(|e| !e.trim().is_empty())
                        .map(|e| format!("⚠️ Agent stopped: {e}"))
                })
            }
            _ => None,
        };

        if let Some(text) = candidate
            && !text.trim().is_empty()
        {
            final_text = Some(text);
        }
    }

    if parsed_json_lines == 0 {
        None
    } else {
        final_text
    }
}

fn join_text_blocks(node: &Value) -> Option<String> {
    if let Some(text) = node.as_str() {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        return Some(text.to_string());
    }

    let array = node.as_array()?;
    let mut chunks = Vec::new();
    for item in array {
        let item_type = item.get("type").and_then(Value::as_str);
        if item_type == Some("text")
            && let Some(text) = item.get("text").and_then(Value::as_str)
        {
            let text = text.trim();
            if !text.is_empty() {
                chunks.push(text.to_string());
            }
        }
    }

    if chunks.is_empty() {
        None
    } else {
        Some(chunks.join("\n"))
    }
}

#[cfg(test)]
#[path = "markers_tests.rs"]
mod tests;
