# BigFur CI digest Automation

Control's private SQLite Durable Object owns the schedule, admission quota,
digest and send evidence. Automation uses the existing Control Operator
entrypoint and Telegram message format. The checked-in Worker configurations
leave Automation disabled. The local fixture supplies a repository allowlist,
a read-only metadata service binding and a synthetic Telegram service binding.

## Operator requests

Send HTTPS `POST` requests to `/v1/operator/automation/<action>` on the
`ControlOperator` service binding with the existing operator bearer.

| Action | JSON body | Effect |
| --- | --- | --- |
| `save` | `id`, `version`, `config` | Create with version `0`. An edit checks the current operator version, increments the schedule revision and starts paused. |
| `preview` | `id` | Return the current schedule and topic, the next five local and UTC times, and any skipped DST gaps in that preview. |
| `dry-run` | `config`, `metadata` | Validate and render a digest from the supplied fixture. |
| `run-once` | `id`, `revision`, `key` | Admit an audited manual run without enabling recurrence. Replay the same key and revision to read its current result. |
| `pause` | `id`, `version` | Stop new recurring admissions. Runs already admitted retain their configuration. |
| `resume` | `id`, `version`, `backfill` | Use `false` to advance to the next future occurrence, or `true` to retain the cursor for bounded backfill. |
| `history` | `id` | Return up to 50 recent runs with their pinned configuration, state and reason. |
| `freeze` | `generation`, `frozen` | Close or open Automation admission for the active generation. The gate starts frozen for each generation. |

Config consists of `repository`, `zone`, `time`, `gap`, `fold`, `chatId` and
`messageThreadId`. The cadence is one daily local `HH:mm` in an IANA time zone.
Use `gap: "skip"` and select `fold: "earlier"` or `fold: "later"`. A spring
gap has no UTC occurrence. A fall fold produces one UTC occurrence at the
selected instant. Both target IDs are required.

The metadata service accepts a `GET` for the one configured repository and
returns `{ "runs": [{ "name": "Build", "status": "success", "sha": "<40 hex>" }] }`.
Control accepts at most ten entries and an 8 KiB response. The boundary carries
workflow names, status and commit identifiers only. A hosted adapter must scope
repository credentials to CI metadata reads and return this same shape.

## Ledger and alarms

- A **schedule** has a stable ID, an operator version for edits and pause or
  resume, and a revision for its configuration and generation.
- An **occurrence** is keyed by schedule ID, revision and due UTC instant.
  A **manual run** uses a separate key namespace within the schedule. Manual
  keys stay bound to their original revision across edits.
- Admission checks the active production generation, its credential version,
  the Automation freeze, sender availability, and capacity. A generation has
  eight admissions per UTC day across manual and recurring runs. Each schedule
  has at most one unresolved run, including a send with an Unknown outcome.
  Generation retirement closes both Control admission and Automation
  dispatch. The Automation freeze is stored in Control. The Gateway's
  direct-ingress freeze covers Telegram updates.
- Admission, quota accounting, the next occurrence and the alarm update share
  the SQLite transaction. Catch-up is limited to 48 hours. A skipped range
  records its first skipped instant, including when an edit or next-future
  resume replaces the cursor. A wake advances at most four schedules and
  processes at most one admitted run. The alarm uses the earliest actionable
  time, with a 30-second watchdog during processing.
- Metadata and delivery have separate states. Work that reaches its ten-minute
  deadline before dispatch is closed as failed or blocked. The reservation for
  a possible send is persisted as Unknown before contacting Telegram.
  Only a positive receipt committed to the same run changes it to sent.
  A response lost after a possible send leaves the run Unknown and holds later
  admissions for that schedule. Pausing, editing and restarting retain this hold.

## Owner gates

1. Validate the private Worker and the pinned-revision ledger in hosted staging
   using a synthetic repository and Telegram sink. Exercise alarm redelivery,
   deploy with a pending run, and verify the same ledger holds an ambiguous send.
2. Agree on recovery interruption and state-loss targets. In an isolated drill,
   simulate abrupt Mars loss and a partition with the predecessor still able to
   reach Telegram. Restore Control off Mars with the active generation,
   placement registry, send reservations and Automation ledger. Fence the old
   sender before any new sender uses Telegram. Admit a new task on a surviving
   runner and deliver to its original test topic. Verify existing task IDs,
   Mars placement pins and Unknown records after the old host returns. Record
   time to first reply and any gap in recoverable state.
3. Obtain approval for the repository, daily time, time zone and Telegram topic.
   Roll out with recurrence paused. Check the preview and fixture dry run, then
   perform one separately authorized production `run-once` and confirm its
   ledger receipt and delivery to the original topic.
4. Enable recurrence only after the owner accepts the Control recovery drill
   and supervised production check. Keep recurrence paused if the pilot scope
   or send evidence cannot be verified.

The Control v1 authority snapshot contains runner and legacy send evidence.
Automation recovery must also preserve all Automation tables in the Durable
Object before hosted enablement. A procedure for full-state recovery and an
audited way to resolve Unknown sends require the hosted recovery milestone.

## Leo-Claw verification

Control was checkpointed at `7009b5a339ea9fcaa6ec4f8b857339c9aab74dbb`
with parent `2f99287fe1a743fbce37bb748ca22917c94eab25`. The following Node
commands ran from `crates/coconutclaw-gateway`. The full run used the existing
local WASM output and omitted optional executor, Codex, cutover CLI and legacy
interoperability injections.

| Command | Result |
| --- | --- |
| `node --test --test-timeout=20000 automation.test.mjs config.test.mjs control-worker.test.mjs` | 17 passed, exit 0 |
| `node --test --test-timeout=120000 authority-control.test.mjs authority-preflight.test.mjs automation.test.mjs config.test.mjs control-worker.test.mjs integration.test.mjs` | 66 passed, 17 skipped, exit 0 |
| `node "$stage_root/stage.mjs" control-staging`, then `GATEWAY_CONTROL_TEST_BUNDLE_DIR="$bundle" node --test --test-timeout=20000 control-worker.test.mjs` | 2 passed, both commands exited 0 |
| `cargo fmt --all -- --check` | Exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Exit 101 before linting. The host lacks `cc`. |
| `cargo test --workspace --all-targets --locked` | Exit 101 before running tests. The host lacks `cc`. |
| `git diff --check` | Exit 0 |

Four targeted JavaScript mutations exercised quota enforcement, alarm
installation, the freeze gate and fold selection. Each corresponding test
failed with exit 1. The mutation harness exited 0.
