# Cloud gateway protocol v1

`coconutclaw-gateway` builds a Workers Rust Wasm module. `cloudflare.config.ts`
defines one Worker and one SQLite Durable Object class. The single synthetic Bot
maps to the `local-fixture` object. The checked-in phase is `disabled`. The
integration harness sets `phase1-local` for the enabled cases and admits only
HTTPS requests to `127.0.0.1`. The sender targets `telegram.invalid` and the
harness intercepts its fetch. The existing polling runtime uses the same
private-chat topic parser.

## Wire contract

All three routes accept POST with UTF-8 JSON and a 16 KiB body cap, including
chunked bodies. Text is nonblank and at most 4 KiB in UTF-8. Time fields are
integer Unix milliseconds. `200` means the applicable storage writes have
committed. Errors use `{"error":"code"}` with `400` for invalid inputs, `401`
for invalid authentication, `404` for gated routes, `409` for state conflicts
or replays, `413` for oversized bodies, and `503` for storage or fixture
failures. Every request goes to the same Bot ledger.

| Route | Caller and input | Response |
| --- | --- | --- |
| `/v1/telegram` | Telegram-shaped update and `X-Telegram-Bot-Api-Secret-Token`. `update_id` is an integer from 0 through 2³¹−1. Synthetic admission requires a plain-text private message from actor `321` in chat `321`, with an optional positive `message_thread_id`. | `disposition` is `task`, `ignored` or `duplicate`. First admissions also return the stable `task` ID or `null`. |
| `/v1/agent` | Object with `device` (`alpha` or `beta`), `at_ms`, `sequence` and a `command` object tagged by `op`. `X-Agent-Signature` is 64 bytes of Ed25519 signature encoded as 128 hex characters. | Operation-specific response below. |
| `/v1/fixture` | Synthetic operator with `X-Fixture-Secret`. JSON object tagged by `op`. Fixture commands inject decisions, clock readings and failures in the local harness. | Current phase, schedule version or derived task ID. |

The signed bytes are the literal prefix
`coconutclaw-v1\nlocal-fixture\nPOST\n/v1/agent\n` followed by the exact request
body bytes. The envelope binds the device, instance, operation and replay data.
The DO verifies the configured device public key and a ±30 s timestamp window.
For state-changing commands it atomically consumes a strictly increasing
per-device sequence in the range `1..=2147483647`, including state conflicts
after validation. Retry a lost reply with a fresh sequence and the same command.
`hello` and `next` are read-only after schema initialization and use `sequence: 0`.
`hello` reports the accepted sequence floor, protocol `1`, instance, device
and the fixed `text` capability. `next` returns the earliest eligible task ID
or `null`. It writes no presence state.

A **task** is the stable unit of work. An **attempt** binds a task to one device.
A **claim** reserves an attempt for 1.5 seconds. A **Start grant** licenses only
that attempt, with a 1.5 second latest-start deadline measured from authorization.
The phase-1 grant is a fixture protocol response over the signed-request channel.
An **occurrence** is a schedule slot plus the scheduled instant.

## State table

| Record | Operation and precondition | Durable transition and response |
| --- | --- | --- |
| Inbox | Fresh `update_id` for the Bot | Insert its disposition. For admitted messages, atomically create `telegram:<update_id>` with original chat, thread and prompt. Derive in insertion order, including older numeric IDs. |
| Inbox | Existing `update_id` | Return `duplicate` and preserve the first disposition and payload projection. |
| Attempt | `claim {task}` on an eligible queued task, or an expired pre-grant claim | Create `claimed` with new attempt ID, owner and deadline. Mark a replaced attempt `expired` in the same transaction. An active same-owner retry returns the same claim with `task`, `attempt`, `lease_until`, `chat_id`, `message_thread_id` and `prompt`. A pinned task is eligible only for its named device. |
| Attempt | `start {attempt}` from the current owner with an active claim | Persist `authorized` and return `attempt` and `start_until`. A retry returns the original deadline while the attempt is authorized, uncancelled and inside the window. |
| Attempt | Grant expires or fixture `disconnect {attempt}` after authorization | Expiry closes the permission to acquire a grant response. Fixture disconnect records `unknown`. In both cases the task stays unavailable to other claimants. |
| Attempt | Same-owner `started {attempt}` from `authorized`, `started` or `unknown` | Persist `started` evidence and return `cancel_requested`. A late receipt is evidence about the original attempt. It issues no grant. |
| Attempt and outbox | Same-owner `result {attempt,text}` with a grant, including `unknown` | Complete the attempt and insert the one `pending` outbox row atomically. Identical result text is idempotent. Different text conflicts. |
| Task or attempt | Fixture `cancel {task}` before any grant | Cancel a queued task or record `cancelled_before_start` on the current claim. A delayed Start request conflicts. |
| Attempt | Fixture `cancel {task}` after a grant | Persist `cancel_requested`. `cancelled {attempt,evidence:"before_start"}` requires the owning device and absence of started evidence. `evidence:"process_exited"` confirms exit. The terminal phases are `cancelled_before_start` and `cancelled_after_exit`. Repeated matching confirmation is idempotent. A committed result wins over a later cancel. |
| Outbox | Fixture `send {task}` when `pending` and due | Reserve `in_flight` and increment sends. Wait for commit before attempting the single `sendMessage` fetch with the stored topic. |
| Outbox | Receipt for `in_flight` | Success with a message ID gives `delivered`. Structured `400`/`403` gives `failed`. Structured `429` with `retry_after` from 1 through 10 seconds permits one explicitly requested retry after the delay. A second `429` fails. Transport errors and unclassified responses give `unknown`. |
| Outbox | Fixture `recover_send {task}` on a stranded `in_flight` row | Record `unknown`. Both terminal and unknown rows are ineligible for sends. |
| Schedule | Fixture `schedule {version,first_at,interval_ms,count,message_thread_id,prompt,pin}` | Replace the single slot using its next version, starting at 1. Commit the definition and initial alarm together. Replay of the current identical definition preserves its watermark and enabled state. |
| Schedule | DO alarm or fixture `wake {at_ms}` | Read the current version. In one SQLite transaction advance the watermark, insert the newest eligible `occurrences` row and task, and update the alarm. Key both the deduplication and task ID on `fixture` plus scheduled instant, even across versions. |
| Schedule | Fixture `disable {version}` or exhaustion | Clear remaining runs and remove the alarm. Prior occurrences are retained. A stale version cannot disable a newer schedule. |

Fixture schedules represent UTC instants with a fixed 1–60 second elapsed
interval and 1–3 occurrences. Civil time and DST conversion are outside this
contract. The first instant must be 0.5–60 seconds in the future on creation.
On a late wake the newest due instant is eligible for up to 60 seconds after
its deadline. Older due instants are skipped. Idle objects have no periodic
alarms. A scheduled task follows the same claim protocol as an inbox task.

## Local evidence

Use Node 22.18 or later (CI uses Node 24), a Rust toolchain with
`wasm32-unknown-unknown`, and `wasm-bindgen` CLI `0.2.130` on `PATH`.
Run these commands from the repository root.

```sh
npm --prefix crates/coconutclaw-gateway ci --ignore-scripts
npm --prefix crates/coconutclaw-gateway run stage
npm --prefix crates/coconutclaw-gateway run test:config
npm --prefix crates/coconutclaw-gateway run check:cf
```

`stage` compiles the locked Rust target, runs wasm-bindgen, and uses Cloudflare's
config and Build Output libraries to stage `cloudflare.config.ts`, the Worker
entrypoint, generated JavaScript modules and Wasm as prebuilt output. The
config test checks the staged modules and disabled metadata, including the
self-binding to the single SQLite class and the disabled public and preview
URLs. The checked-in config is the source for both the dry run and Miniflare.

`check:cf` runs `CF_SEND_TELEMETRY=false cf deploy --prebuilt --dry-run` from
the gateway directory with the locked official CLI `1.0.0-beta.5`. This beta
reads `.cloudflare/output/v0/` with no mode recorded. Current `cf` documentation
describes `v1`. Recheck the output format when updating the CLI and its libraries.
The dry run validated the staged bundle and listed `BOTS` and `PHASE="disabled"`
without credentials or network connections. It does not measure cloud quotas,
verify account eligibility, apply a SQLite class lifecycle change, or start the
Worker. Miniflare exercises the Worker and local SQLite. The Mars CLI and
account checks are coordinator-owned. See the [Cloudflare CLI project commands](https://developers.cloudflare.com/cf/projects/).

`cf build` and `cf dev` delegate to a configured build tool. This Rust project
provides its own staging command and uses only the prebuilt dry-run path. Use the
following to run the five loopback cases together with the config test and
the Rust checks.

```sh
npm --prefix crates/coconutclaw-gateway test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy -p coconutclaw-gateway --target wasm32-unknown-unknown --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

The harness consumes the staged modules, bindings and SQLite class declaration.
It uses Miniflare `4.20260730.0` with workerd `1.20261123.1`.
The newer workerd is pinned because the bundled July runtime stalled on
workers-rs `0.9.0` requests. The test client accepts Miniflare's fixture
certificate on loopback and generates disposable Ed25519 keys. Restart
disposes the runtime and opens the same on-disk SQLite storage in a new
runtime. Five tests cover the following observable behavior.

| Injection | Observed |
| --- | --- |
| Disabled phase, invalid secret, invalid IDs, oversized declared and chunked bodies | Routes gated, invalid writes rejected, allowed input still admitted. |
| Concurrent webhook duplicates, rollback after insert, simulated failed reply after commit, restart and lower numeric IDs | Exactly one task for a duplicate. Retrying the rolled-back update first admits it. Acknowledged and committed updates deduplicate after restart. Claims preserve insertion order and original topics. Unsupported inputs durably deduplicate without creating tasks. |
| Wrong instance, stale signature, changed body, replay before and after restart, device absent | Authentication and replay checks hold. Unclaimed tasks wait. The expired pre-grant attempt cannot start and another device can claim it. |
| Simulated failed Start reply after commit, followed by same-owner retry, disconnect and expiry | Retry returns the original deadline. Restart preserves `unknown`. Both `unknown` and expired authorizations remain ineligible for reassignment. |
| Cancel before a grant, cancel after a grant, started evidence, and result preceding cancel | Late Start retries conflict. The DO records never-started and process-exited confirmations separately, rejects never-started after started evidence and preserves a committed result. |
| Repeated result, concurrent send, ambiguous response, and receipt discarded after mock acceptance | One delivery row. The mock receives the original topic and only one request from concurrent sends. Receipt loss and recovered `in_flight` both become `unknown` and cannot resend. A structured `429` permits one delayed send. |
| Actual one-shot alarm following restart, synthetic early and duplicate wakes, capped catch-up, stale disable | One pinned task from the real alarm. Early wake creates no task, duplicate wake creates no duplicate task, and disabled or too-old occurrences create no task. |
| Captured Miniflare logs and workerd stdout/stderr | Fixture secrets and the full log-canary prompt are absent on the exercised paths. |

The failure injections simulate transaction rollback and lost acknowledgements
at explicit boundaries. They use orderly runtime disposal. Worker kill at
arbitrary instructions, live Telegram receipt behavior, cloud CPU and storage
latency, actual executor process creation, and native process cancellation
remain outside this evidence.

Seven targeted cargo-mutants `27.1.0` variants compiled and failed their
corresponding integration assertions. They bypassed the disabled-phase gate,
text bounds, grant requirement, claim deadline, Start deadline, send retry cap,
or missed-run window. To repeat a variant, use
`cargo mutants --Zmutate-file <rust-file> --list --diff --json`, apply its
`diff` in a disposable copy, rebuild with `npm run stage` in the gateway
directory, and run the corresponding `node --test --test-name-pattern`
case in `integration.test.mjs`. This was a targeted sample.

## Free-plan accounting

The harness allows at most 72 HTTP calls and eight mock sends per test.
Each accepted HTTP call reaches one DO. Invalid phase, method or route
stops at the Worker. A local Bot retains at most 128 inbox entries,
32 schedule versions' worth of occurrences (up to 96 tasks), and 512
attempts. Each task has at most one outbox row and two send reservations.
The two agent sequence rows are updated only by signed mutations. The
maximum combined stored prompt and result bytes are about 1.75 MiB before
SQLite metadata, indexes and allocation overhead. There is no external
storage binding.

For a future pilot, budget each Worker request, DO invocation including
alarm wake or retry, SQLite rows read and written including index writes,
and DO active duration separately. A new admitted update writes an inbox
and task row plus index entries. `next` can scan the bounded task set.
A mutating agent call writes its sequence plus any transition rows.
Delivery adds the reserve and receipt writes and at most one subrequest
per reservation. Schedule transitions add a version or watermark update
and an alarm write where applicable. Measure these on an authorized pilot
before choosing a traffic cap.

Cloudflare's Free allowance lists 100,000 Worker requests per day and
10 ms Worker CPU per invocation. The SQLite DO allowance lists 100,000
requests, 13,000 GB-s, five million rows read and 100,000 rows written per
day, and 5 GB total storage with a 1 GB cap per object. These are account
and platform limits. The local fixture caps are separate from metered use.
The complete Worker upload is about 288 KiB gzipped in the local `cf` dry run.
[Workers limits](https://developers.cloudflare.com/workers/platform/limits/) · [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) · [DO limits](https://developers.cloudflare.com/durable-objects/platform/limits/)

## Phase 2 handoff

1. Get authorization for an isolated Bot and one device before opening a
   new phase. Provide separate configuration, device authorization and a
   stable gateway identity. Remove access to fixture routes in that phase.
   Replace the mock endpoint with the thin Bot API adapter, retaining
   the original topic and redacting tokens, signatures and task content.
2. Connect a real executor with durable local launch intent, single-use
   attempt consumption, and retained signed evidence. Specify how it learns
   about cancellation, fences delayed grants, and reconciles an ambiguous
   launch. Replace the fixture deadlines with measured pilot values. Persist
   receipts for audit, including the Telegram message ID on delivery.
   Support outbound reconnection with jitter and backoff. Keep `unknown`
   ineligible for automatic retries.
3. Inject network reply loss and crashes on both sides of actual process
   creation. Verify at most one process per attempt, cancel before delayed
   authorization and result before cancel. Exercise real Telegram topic
   delivery, Codex cancellation, offline inbox retention and same-device
   reconnect. Record observed timings and real free-tier usage.
4. Use the production phase's explicitly authorized schedule and routing
   rules. The UTC fixture, single text capability, fixed identities, capacity
   limits and synthetic clock are local protocol choices. Durable route and
   memory authorization, civil schedules, failover and migration have their
   own later phase gates.

The [Phase 2 loopback executor](cloud-gateway-phase-2-local.md) records the
next local slice and its remaining pilot gates.
