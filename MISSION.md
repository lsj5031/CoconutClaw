You are working on CoconutClaw, a Rust project at /home/leo/coconutclaw-format-split.

## Task
Implement format-aware message splitting for Telegram. Read the full plan at .hermes/plans/2026-04-04_131803-format-aware-split.md and implement all tasks described there.

## Summary of what to do
1. Read the plan file first — it has all the details, pseudocode, and test cases.
2. Rewrite split_text_chunks in crates/coconutclaw-cli/src/telegram.rs (currently lines 539-591) to be format-aware:
   - Track fenced code blocks (triple-backtick for MarkdownV2, pre/code tags for HTML mode)
   - Close/reopen code blocks at chunk boundaries
   - Protect inline code from being broken (MarkdownV2 backtick parity)
   - Use "look forward" strategy: when a split point falls inside a format region, look forward to include the whole region in the current chunk if it fits, rather than only backing up
   - Add (N/M) chunk indicators, with () escaping for MarkdownV2 mode
3. Change function signature to split_text_chunks(text, max_chars, parse_mode: TelegramParseMode)
4. Update the caller in send_or_edit_text (line ~456) to pass cfg.telegram_parse_mode
5. Add 6 unit tests in crates/coconutclaw-cli/src/main.rs test module (see plan for test cases)
6. Run cargo test to verify everything compiles and passes

## Key files
- crates/coconutclaw-cli/src/telegram.rs — main implementation file
- crates/coconutclaw-cli/src/main.rs — add tests at the bottom test module
- crates/coconutclaw-config/src/lib.rs — TelegramParseMode enum already defined here

## Important constraints
- The function operates on already-rendered text (post HTML or MarkdownV2 conversion)
- Must support both Html and MarkdownV2 parse modes plus Off (plain text)
- Look-forward search must be bounded (~500 chars max) to avoid issues with malformed input
- Each chunk must stay under max_chars (4096)
- Don't change the render functions or config — only the splitting logic
- Import TelegramParseMode from coconutclaw_config in telegram.rs if not already imported
