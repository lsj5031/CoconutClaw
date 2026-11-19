# Provider execution timeouts

Codex fresh and explicit-resume executions use the same activity policy:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `PROVIDER_TIMEOUT_SECS` | `0` | Optional total wall-clock deadline. `0` disables it. An explicit nonzero value retains its previous total-deadline meaning. |
| `PROVIDER_IDLE_TIMEOUT_SECS` | `1800` | Maximum silence without meaningful Codex progress, including silent reasoning and startup. |
| `PROVIDER_TOOL_IDLE_TIMEOUT_SECS` | `7200` | Silence allowance while a command, MCP call, or web-search item is pending. Must be at least the ordinary allowance. |

All values are integer seconds, at most 86400; the two idle settings must be
positive. File/environment precedence is unchanged. The provider API's
`timeout_secs: None` inherits the configured total; `Some(0)` explicitly disables
it, and `Some(n)` sets a total deadline for that invocation.
For a paired task, the provider runs with the runner's host-local timeout
settings, not the coordinator's.

There is **no default total duration cap**. A task can run longer than 30 minutes,
or many hours, provided it continues making progress inside its applicable silence
allowance. Existing private configurations with `PROVIDER_TIMEOUT_SECS = 300`
still opt into a five-minute total deadline; change that setting to `0` after
deploying a reviewed binary. Old binaries reject `0`.

## What counts as activity

Only Codex `exec --json` **stdout** JSONL events count, regardless of whether a
Telegram progress subscriber exists. The protocol has `thread.started`,
`turn.started`, `turn.completed`/`turn.failed`, and `item.started`/`item.updated`/
`item.completed` with stable item IDs. Command items carry `command`, `status`,
`aggregated_output`, and `exit_code`; MCP items carry server/tool/arguments,
status and result/error. Reasoning and agent-message items carry text. File
changes carry changes/status, searches carry query (or action/status), and plans
carry checklist items.

First lifecycle boundaries and changed recognized item payloads renew activity.
Only semantic fields are compared, not arbitrary timestamp/heartbeat fields.
Previously seen snapshots (including alternating older unfinished snapshots),
completed-item replays, unknown types, malformed JSON, raw
stdout, stderr, stream errors/reconnect notices, and repeated turn-start markers
do not renew it. An item without an ID is not activity. Updates to command output
count when emitted as a novel `aggregated_output` snapshot, including growing
output or replacement text. Completion removes
that item's pending status; overlapping tools remain pending independently.
Terminal `completed`/`failed` status updates also close an item permanently.
Old snapshots cannot mutate pending state or reopen closed items.
Turn completion clears pending tools. No activity is shared across tasks.

Replay protection retains one 64-bit semantic fingerprint per distinct snapshot
of each unfinished item for that invocation, plus item-ID terminal tombstones.
It does not retain copies of output payloads. Fingerprints are not evicted:
bounded recent-history caches would allow sufficiently old replay to renew
silence again. Memory therefore grows with unique unfinished snapshots and item
IDs, not repeated snapshots; per-item fingerprint storage is released at item
completion, and all history is dropped when the invocation ends. This gives up a
fixed per-invocation memory bound for conservative replay rejection without a hidden
total duration cap. A fingerprint collision can suppress genuine activity, not
admit a known replay; this is not a cryptographic authenticity check. Streams
with genuinely novel content/IDs are still considered progress, not detectable
stalls by this policy.

The existing parser and an offline Codex schema derived from upstream
`rust-v0.77.0` (commit `112f40e91c12af0f7146d7e03f20283516a8af0b`)
were inspected for these event shapes. That schema shows reasoning, web search
and file changes often appear only at completion, commands/MCP at start/end,
and checklist updates as `item.updated`. It does **not** promise streaming
command output or continuous reasoning breadcrumbs. No live authenticated
Codex request was used to discover events.

## Limitations and termination

Silence **cannot prove a stall**. A legitimate command may emit only start/end,
and maximum-effort reasoning may be completely silent until it finishes.
Pending tool execution therefore gets two hours by default, rather than being
mistaken for ordinary reasoning silence. It is not an indefinite exemption: a
stuck pending tool eventually expires too. Increase idle allowances if legitimate
individual silent steps need longer. A silent reasoning step over 30 minutes or
a silent tool over two hours can still be stopped; there is no CPU/process-health
inference or claim that such work is dead.

Exit code 124 distinguishes **inactivity** (silence duration, applicable limit,
elapsed duration, and pending-tool state) from a **configured total deadline**
(limit and observed elapsed duration). Cancellation remains independent and is
checked before timeouts. Termination uses the owned process group on Unix or
the per-invocation [Windows Job Object](production-nodes.md#state-and-recovery).
Deadline checks continue through stdin writes and output drain
after leader exit. Pipe handling and join bounds are unchanged. No failed task
is retried just because it timed out.

Tests include deterministic clock-boundary checks beyond 300 and 1800 seconds,
local executable mocks, and an opt-in actual CLI regression lasting over five
minutes:

```sh
cargo test -p coconutclaw --test e2e_smoke activity_progress_survives_former_five_minute_deadline -- --ignored --nocapture
```
