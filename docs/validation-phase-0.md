# Phase 0 validation

## Baseline and scope

Implementation started on `main` at
`15592e43aad037f96bda5cdde74afda1a51376a7`, matching the local `origin/main`
reference. The offline baseline passed 307 tests before subtraction.
The trim was closed on main after the independently reviewed patch and saved
live acceptance were verified. No branch, deployment, service/timer change or
credential change was performed. Live acceptance below was performed in earlier
owner-authorized runs, not repeated during documentation closeout.

Removed runtime code includes alternate providers, Slack, inbound HTTP webhooks,
audio transcription/synthesis, local vision orchestration, recurring scheduling,
heartbeat/reflection commands and named-instance selection. Their direct
dependencies, obsolete fixtures and current-feature documentation were removed.
Legacy SQLite columns/tables and historical release/design documents remain.

## Initial writer checks (superseded by repair pass)

The independent review rejected the initial source for retained-behavior defects.
The counts below describe that initial pass, not independent acceptance.
Repair-pass source identity, complete command logs, regression results and the
twelve-item disposition are recorded in
`/tmp/coconutclaw-revival-20261114/fix-pass-1-report.md`.
These gates subsequently passed; see the closeout evidence below.

All commands passed offline with the locked dependency graph:

```sh
cargo fmt --all -- --check
cargo clippy --offline --workspace --all-targets --locked -- -D warnings
cargo test --offline --workspace --all-targets --locked
cargo test --offline --workspace --doc --locked
cargo build --offline --release --locked -p coconutclaw
bash -n .githooks/pre-commit
git diff --check
```

188 tests passed: 154 CLI unit/integration, 3 executable E2E, 14 configuration
and 17 provider tests. There are no executable documentation tests.
Existing retained tests cover context, markers, history, attachments, Telegram
operations, cancellation, service generation and storage. New regressions cover:

- Restarted approvals, cross-chat rejection and one-time acceptance/rejection.
- Delegation identity, isolated Codex context and origin-history results.
- Saved-result recovery without reexecution and outcome-unknown attempts.
- Partial delivery acknowledgments and owned attachment retention until delivery.
- Queued media/update-ID round trips and polling failure checkpoints/offsets.
- Historical audio columns and inactive schedule preservation.
- Removed config/commands, preserved `.env` and non-destructive duplicate-ID errors.
- Explicit native Codex resume/image arguments, timeout and descendant cancellation.
- Real `once`, `doctor` and polling entry points using isolated mocks.

The regressions exposed and repaired destructive legacy duplicate handling,
approval expiration, nullable historical Telegram replies and attachment/recovery
gaps. One mock executable encountered Linux `ETXTBSY`; the new provider fixtures
now use an explicit shell interpreter rather than executing freshly written files.
The final complete suite and release build passed after these fixes.

This host lacks a normal `cc`. Validation used already-cached Clang and Rust LLD
through a temporary linker wrapper, without installing packages or changing
system configuration. Temporary transformation/linker helpers were removed.
Normal builds still require the Rust/C toolchain documented in the README.

## Not validated or claimed

No service installation or general scheduler operation is claimed. Live
acceptance below verifies one ordinary turn, not all runtime features. Platform execution was tested
on Linux only. No mutation-testing tool was available.

Telegram send and SQLite acknowledgment cannot be atomic, so a crash in that
window can duplicate delivery. Cancellation cannot undo completed effects;
a hard runtime kill can leave an orphan process. Approval markers coordinate
with the model, not enforce a security sandbox. Operator-reviewed configuration,
old service/timer cleanup and live acceptance remain separate follow-up work.

## Reviewed closeout, 15 November 2026

The tracked binary diff SHA-256 before documentation changes exactly matched
`d4cfb2f647c2658eeea87ad01eaab330411815892abf88fcfb5e2a3529ed268e`.
HEAD was `15592e43aad037f96bda5cdde74afda1a51376a7`. The authorized
untracked Phase 0 tests and architecture/migration/validation documents are
included in the closeout commit. Independent final checks passed **210 tests**,
formatting and strict Clippy. The earlier 188 count above is historical.
Durable source-bound evidence and receipts:
`/home/leo/.openclaw/workspace/artifacts/coconutclaw-trim-20261115`.

- `terra-max-acceptance/REPORT.md`, `acceptance-result.json` and
  `verification.json`: real reviewed release `once`, Codex 0.153.2,
  8.639 seconds, exact nonce, persisted successful turn, SQLite quick check.
- `telegram-live-acceptance/REPORT.md` and `attempt-2/REPORT.md`,
  `verification.json`, `sqlite-assertions.json`, `events.json` and
  `effective-provider-args.json`: actual release `run`, one ordinary owner
  turn, one provider invocation, task completed, exact nonce delivered,
  graceful exit 0 after 74.31 seconds. Telegram message receipt **933** is
  persisted API acceptance, not a human read receipt. The owner separately
  confirmed the nonce response; that human confirmation is distinct from the
  machine receipt. No raw Telegram product database or message bodies retained.
- Both runs used exactly `--model gpt-6.1-terra` and one explicit
  `-c 'model_reasoning_effort="max"'` via invocation-local `CODEX_BIN` wrapper
  injection, **not** a product-native effort option. Persistent defaults did
  not change. Effective argv proves selection, not proxy internal routing.
- Both successful sessions emitted two structural diagnostic error items and
  no tool items. Diagnostic contents were not retained; no attribution or
  diagnostic-free claim is made. Terra's initial classifier incorrectly
  counted those diagnostics as tools; saved evidence was reverified without
  another provider call.
- The initial Telegram attempt failed because harness confinement omitted the
  existing Codex `thread-writer-locks/.coordination.lock` directory. Granting
  that existing runtime lock directory fixed the **harness**, not a product
  defect. An import-only harness `FileExistsError` was fixed before retry.
  Neither initial attempt is counted as ordinary-turn acceptance.

Closeout inspected saved receipts and source identity, ran `git diff --check`
and shell syntax validation. Unchanged Cargo suites and live acceptance were
not rerun solely for this documentation closeout. No evidence/credential files
were added to Git. Local memory work follows in a separate, uncommitted diff.
