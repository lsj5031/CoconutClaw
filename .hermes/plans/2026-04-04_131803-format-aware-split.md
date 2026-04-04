# Format-Aware Message Splitting for CoconutClaw

> **For Hermes:** Use droid to implement this plan. Create a worktree from `origin/main` first.

**Goal:** Replace CoconutClaw's naive line-based `split_text_chunks` with a format-aware splitter that preserves code blocks and inline code across chunk boundaries, with a "look both ways" strategy that shifts split points forward past format boundaries instead of only backing up.

**Architecture:** The new splitter tracks fenced code block state and inline code parity. When a candidate split point falls inside a format region, it looks forward past the boundary to include the whole region in the current chunk (if it fits), rather than only backing up and wasting headroom. This produces fuller, more even chunks with format integrity preserved.

**Tech Stack:** Rust, existing CoconutClaw codebase, `pulldown-cmark` already available.

---

## Context

### Current Code
- **File:** `crates/coconutclaw-cli/src/telegram.rs`
- `split_text_chunks(text, max_chars)` — lines 539-591: naive line-based splitter
- `send_or_edit_text(client, cfg, chat_id, text, progress_message_id)` — lines 449-495: calls splitter, sends chunks
- `render_telegram_reply_text(cfg, text)` — line 694: renders to HTML or MarkdownV2 based on config
- `should_send_reply_as_document(text)` — line 497: if rendered text > 4096, sends as .md file instead

### Pipeline Order (same as Hermes)
```
AI output → parse_markers() → raw reply text
         → render_telegram_reply_text() → HTML or MarkdownV2 string
         → should_send_reply_as_document() → if > 4096, send as file
         → split_text_chunks() → send_or_edit_text() → Telegram API
```

The splitter operates on the **already-rendered** formatted text (HTML or MarkdownV2).

### Hermes Reference (Python, `gateway/platforms/base.py` lines 1422-1532)
Hermes's `truncate_message()` does:
1. Track fenced code blocks (` ``` `) — close at chunk end, reopen with lang tag at next chunk start
2. Check inline code backtick parity — if odd, back up to safe split point
3. Add `(1/3)` chunk indicators
4. Reserve 10 chars for indicators

### What's Different for CoconutClaw
- Supports **both HTML and MarkdownV2** parse modes (Hermes is MarkdownV2 only)
- HTML mode uses `<pre><code class="language-X">` for code blocks, not ` ``` `
- The "look forward" strategy is new — Hermes only backs up

---

## Task 1: Create worktree

Create a git worktree from `origin/main` for this work.

```bash
cd /home/leo/coconutclaw-runtime
git fetch origin
git worktree add ../coconutclaw-format-split origin/main
```

---

## Task 2: Write failing tests for the new splitter

**File:** `crates/coconutclaw-cli/src/main.rs` (test module at bottom, after line ~2100)

Add these tests. They should all FAIL against the current `split_text_chunks` (which doesn't handle format).

```rust
// Test 1: Code block preserved across chunks (MarkdownV2 mode)
#[test]
fn split_preserves_code_block_across_chunks_mdv2() {
    use crate::telegram::split_text_chunks;
    use coconutclaw_config::TelegramParseMode;
    
    // 3900 chars of text, then a code block that pushes past 4096
    let padding = "x".repeat(3900);
    let input = format!("{padding}\n```python\ndef hello():\n    print('world')\n```\nMore text after code");
    
    let chunks = split_text_chunks(&input, 4096, TelegramParseMode::MarkdownV2);
    
    assert!(chunks.len() > 1, "should split into multiple chunks");
    
    // First chunk should close the code block properly
    let first = &chunks[0];
    assert!(first.contains("```python"), "first chunk should open code block");
    assert!(first.contains("```"), "first chunk should close code block");
    
    // Second chunk should reopen the code block (or have content after it)
    let second = &chunks[1];
    // Should not have orphaned code — either complete block or reopened
    assert!(second.len() <= 4096, "each chunk must be <= 4096");
}

// Test 2: Code block preserved across chunks (HTML mode)
#[test]
fn split_preserves_code_block_across_chunks_html() {
    use crate::telegram::split_text_chunks;
    use coconutclaw_config::TelegramParseMode;
    
    let padding = "x".repeat(3900);
    let input = format!("{padding}\n<pre><code class=\"language-python\">def hello():\n    print('world')\n</code></pre>\nMore text");
    
    let chunks = split_text_chunks(&input, 4096, TelegramParseMode::Html);
    
    assert!(chunks.len() > 1);
    // First chunk should close the code block
    assert!(chunks[0].contains("</code></pre>"), "first chunk should close HTML code block");
    // Each chunk valid
    for chunk in &chunks {
        assert!(chunk.len() <= 4096);
    }
}

// Test 3: Chunk indicators added for multi-chunk output
#[test]
fn split_adds_chunk_indicators() {
    use crate::telegram::split_text_chunks;
    use coconutclaw_config::TelegramParseMode;
    
    let input = "line\n".repeat(1500); // ~9000 chars
    let chunks = split_text_chunks(&input, 4096, TelegramParseMode::Off);
    
    assert!(chunks.len() > 1);
    // Each chunk should end with (N/M) indicator
    for (i, chunk) in chunks.iter().enumerate() {
        let expected = format!("({}/{})", i + 1, chunks.len());
        assert!(chunk.contains(&expected), "chunk {} should contain {}", i, expected);
    }
}

// Test 4: Short message stays as single chunk, no indicators
#[test]
fn split_short_message_single_chunk() {
    use crate::telegram::split_text_chunks;
    use coconutclaw_config::TelegramParseMode;
    
    let input = "Hello, this is a short message.";
    let chunks = split_text_chunks(&input, 4096, TelegramParseMode::Off);
    
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], "Hello, this is a short message.");
    // No indicator for single chunk
    assert!(!chunks[0].contains("(1/"));
}

// Test 5: Look-forward strategy — include format boundary in current chunk
#[test]
fn split_looks_forward_past_inline_code() {
    use crate::telegram::split_text_chunks;
    use coconutclaw_config::TelegramParseMode;
    
    // 4090 chars, then an inline code span that's 20 chars — total 4110
    let padding = "x".repeat(4090);
    let input = format!("{padding}\n`some_code_here()`\nNext line");
    
    let chunks = split_text_chunks(&input, 4096, TelegramParseMode::MarkdownV2);
    
    assert!(chunks.len() > 1);
    // The inline code should NOT be broken — either fully in chunk 1 or fully in chunk 2
    for chunk in &chunks {
        // Count backticks — should be even (paired)
        let count = chunk.matches('`').count();
        assert_eq!(count % 2, 0, "backticks should be paired in every chunk, got {} in: {}", count, &chunk[..chunk.len().min(100)]);
    }
}

// Test 6: Oversized line still handled
#[test]
fn split_oversized_line_fallback() {
    use crate::telegram::split_text_chunks;
    use coconutclaw_config::TelegramParseMode;
    
    let long_line = "x".repeat(8000);
    let chunks = split_text_chunks(&long_line, 4096, TelegramParseMode::Off);
    
    assert!(chunks.len() >= 2);
    for chunk in &chunks {
        assert!(chunk.len() <= 4096, "each chunk must respect limit");
    }
}
```

**Verify:** Run `cargo test` — these should fail (function signature mismatch at minimum since we're adding `parse_mode` param).

---

## Task 3: Implement the new `split_text_chunks`

**File:** `crates/coconutclaw-cli/src/telegram.rs`

Replace the existing `split_text_chunks` (lines 539-591) with a format-aware version. The key design:

### Function Signature
```rust
pub(crate) fn split_text_chunks(
    text: &str, 
    max_chars: usize, 
    parse_mode: TelegramParseMode,
) -> Vec<String>
```

### Algorithm (pseudocode)

```
INDICATOR_RESERVE = 10 chars for "(XX/XX)"
CHUNK_OVERHEAD = INDICATOR_RESERVE

carry_state = None  // tracks if we're continuing a code block from previous chunk

while remaining text:
    prefix = "" 
    if carry_state is CodeBlock(lang):
        match parse_mode:
            MarkdownV2 => prefix = "```{lang}\n"
            Html => prefix = "<pre><code class=\"language-{lang}\">"
            Off => prefix = ""
    
    overhead = CHUNK_OVERHEAD + len(prefix) + potential_format_close_len
    headroom = max_chars - overhead
    
    if remaining fits in one chunk (with prefix):
        chunks.append(prefix + remaining)
        break
    
    // Find candidate split point
    region = remaining[..headroom]
    split_at = region.rfind('\n')  // prefer newline
    if split_at too small: split_at = region.rfind(' ')  // then space  
    if split_at too small: split_at = headroom  // hard cut
    
    // === FORMAT-AWARE ADJUSTMENT ===
    // Check if split falls inside a fenced code block (``` or <pre><code>)
    // Check if split falls inside inline code (backtick parity)
    
    // "Look both ways" strategy:
    // 1. Scan forward from split_at to find where the current format region ends
    //    e.g. find the closing ``` or closing </code></pre> or closing backtick
    // 2. If including up to that boundary keeps us under max_chars, extend split_at forward
    // 3. If it would exceed max_chars, back up split_at before the opening boundary instead
    
    // This ensures format regions are never split — they either fit wholly in this chunk
    // or they're deferred entirely to the next chunk
    
    chunk_body = remaining[..split_at]
    
    // Determine format state after this chunk
    // Walk chunk_body lines to track code block open/close
    // Track inline code backtick parity
    
    if ends_inside_code_block:
        // Close the code block in this chunk
        match parse_mode:
            MarkdownV2 => chunk_body += "\n```"
            Html => chunk_body += "</code></pre>"
        carry_state = CodeBlock(lang)
    else:
        carry_state = None
    
    chunks.append(prefix + chunk_body)

// Add (N/M) indicators
if chunks.len() > 1:
    for i, chunk in enumerate(chunks):
        indicator = format!(" ({}/{})", i+1, total)
        // MarkdownV2 needs () escaped
        if parse_mode == MarkdownV2:
            indicator = format!(" \\({}/\\)", ...) // escape parens
        chunks[i] += indicator
```

### Key Implementation Details

**Code block detection (MarkdownV2):**
- Opening: line starts with ` ``` ` optionally followed by lang tag
- Closing: line starts with ` ``` ` when inside a code block
- Same logic as Hermes: walk lines, track `in_code` bool and `lang` string

**Code block detection (HTML):**
- Opening: `<pre><code` or `<pre><code class="language-X">`
- Closing: `</code></pre>`
- Extract lang from class attribute for reopening

**Inline code protection (MarkdownV2 only):**
- Count backticks in the candidate chunk. If odd, we're inside an inline code span.
- Look forward from split_at for the closing backtick. If total distance keeps us under max_chars, extend. Otherwise, back up before the opening backtick.

**"Look forward" logic:**
```rust
fn find_safe_split(remaining: &str, initial_split: usize, max_chars: usize, parse_mode: TelegramParseMode) -> usize {
    let mut split = initial_split;
    
    // Check if we're inside a code block at split point
    if let Some(close_pos) = find_code_block_close(remaining, split) {
        let extended = close_pos + format_close_len(parse_mode);
        if extended <= max_chars {
            split = close_pos; // extend forward past the code block
        } else {
            split = find_code_block_open(remaining, split); // back up before it
        }
    }
    
    // Check inline code parity (MarkdownV2 only)
    if parse_mode == TelegramParseMode::MarkdownV2 {
        let backtick_parity = count_backticks(&remaining[..split]);
        if backtick_parity % 2 == 1 {
            // Inside inline code — look forward for closing backtick
            if let Some(close) = remaining[split..].find('`') {
                let extended = split + close + 1;
                if extended <= max_chars {
                    split = extended;
                } else {
                    // Back up before the opening backtick
                    split = find_last_backtick_before(&remaining[..split]);
                }
            }
        }
    }
    
    split
}
```

---

## Task 4: Update caller to pass `parse_mode`

**File:** `crates/coconutclaw-cli/src/telegram.rs`

In `send_or_edit_text()` (line 456), change:
```rust
let chunks = split_text_chunks(text, TELEGRAM_TEXT_CHAR_LIMIT);
```
to:
```rust
let chunks = split_text_chunks(text, TELEGRAM_TEXT_CHAR_LIMIT, cfg.telegram_parse_mode);
```

`send_or_edit_text` already has `cfg: &RuntimeConfig` as a parameter, so `cfg.telegram_parse_mode` is available.

---

## Task 5: Make `split_text_chunks` visible to tests

**File:** `crates/coconutclaw-cli/src/telegram.rs`

The function is currently `pub(crate)`. The tests in `main.rs` need access. Two options:
- Option A: Keep `pub(crate)` — tests in `main.rs` can access `crate::telegram::split_text_chunks` since they're in the same crate.
- Option B: Add `#[cfg(test)]` test module directly in `telegram.rs`.

Go with **Option A** since all existing tests live in `main.rs`.

---

## Task 6: Run tests and verify

```bash
cd /home/leo/coconutclaw-format-split
cargo test -- split_  # run just the new tests
cargo test             # full suite to ensure no regressions
```

All 6 new tests should pass. Existing tests should be unaffected (the only behavioral change is that long messages are now split more carefully, but the splitting is internal — callers don't change their API usage).

---

## Task 7: Commit and push

```bash
git add -A
git commit -m "feat: format-aware message splitting for Telegram

Replace naive line-based split_text_chunks with format-aware splitter that:
- Tracks fenced code blocks and closes/reopens them at chunk boundaries
- Protects inline code spans from being broken (MarkdownV2 mode)
- Uses 'look forward' strategy to include format boundaries in current chunk
  instead of only backing up and wasting headroom
- Supports both HTML and MarkdownV2 parse modes
- Adds (N/M) chunk indicators with proper MarkdownV2 escaping

Ported from Hermes agent's truncate_message() approach (Python), adapted
for CoconutClaw's dual parse mode support and Rust implementation."
git push -u origin HEAD
```

---

## Files to Change

| File | Change |
|------|--------|
| `crates/coconutclaw-cli/src/telegram.rs` | Rewrite `split_text_chunks` (lines 539-591), update `send_or_edit_text` call (line 456) |
| `crates/coconutclaw-cli/src/main.rs` | Add 6 unit tests for new splitter |

## Not Changed
- `crates/coconutclaw-config/src/lib.rs` — no config changes needed
- `dispatch_telegram_output` — no changes, it already calls `send_or_edit_text` correctly
- `render_html_reply` / `render_markdown_v2_reply` — no changes to rendering

## Risks
- The "look forward" strategy needs to be bounded — don't search forward more than ~500 chars or we risk infinite loops on malformed input. Fall back to backing up if no close found within a reasonable distance.
- HTML mode `<pre><code>` detection needs to handle both `<pre><code>` and `<pre><code class="language-X">` variants.
- Chunk indicator `(1/3)` in MarkdownV2 mode needs `()` escaped to `\(\)`. The indicator is appended AFTER splitting, so the escape must not affect char counting during split.
