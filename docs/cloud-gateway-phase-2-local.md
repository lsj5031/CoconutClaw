# Cloud gateway Phase 2 loopback executor

`cloudflare.config.ts` still stages one Worker and one SQLite `BotLedger` class
with `PHASE=disabled`. The harness can select `phase2-local`, which uses one
synthetic Bot ledger named `local-executor-fixture`. Its object ID and signing
domain differ from Phase 1. The Worker admits only HTTPS loopback POSTs.
Synthetic Telegram intake and fixture operations use separate secrets.
Schedules stay gated in this phase. Fixture sends resolve to the harness mock.

The Unix executor requires both the `local-executor` build feature and
`--loopback-fixture`. It accepts an explicit `https://127.0.0.1:<port>` origin,
a fixture CA and a disposable Ed25519 seed. It disables proxies, redirects and
the default TLS roots. `init` creates a private journal directory and `resume`
opens that same journal with the same public key. An exclusive lock admits one
writer. The fixed child runs with an empty environment in that directory. It
reads the prompt as data and returns a bounded text response through stdout.
The runner uses the existing provider's spawn, pipe, timeout and process group
lifecycle. Telegram intake uses the Phase 1 Rust private-topic parser.

## Launch and evidence

The device signs the exact Phase 2 request bytes using the Phase 1 envelope
format and new instance name. The ledger admits only `alpha` with
`EXECUTOR_ALPHA_KEY`. `hello`, `next` and `status` use sequence zero. The
executor persists each successive mutation sequence before sending it and
advances to the ledger's floor on reconnect.

| Boundary | Durable behavior |
| --- | --- |
| Before requesting Start | Persist the claimed attempt and its launch intent with SQLite `synchronous=FULL`. |
| Start response | The ledger commits a five-second grant while the five-second claim is live. A Start retry within the window returns the original deadline and echoes the new request sequence. The executor checks the attempt and sequence over fixture TLS and reads current status before launch. |
| Last local checks | An observed cancel seals an unconsumed intent and stages signed `before_start` evidence. A delayed response or expired authorization cannot extend the deadline. The executor commits `intent → consumed` once and checks wall and monotonic deadlines at the provider's spawn boundary. |
| Crash on either side of spawn | Restart seals a consumed attempt and stages signed `unknown`. The ledger records `unknown` and keeps the task ineligible. Recovery does not infer process state from a PID or signal a recovered PID. |
| Running child | After a cancellation observation the provider terminates and waits for its owned process. The executor stages `started` followed by `process_exited` evidence. An unresolved exit stages `unknown`. |
| Successful child | Seal the attempt and stage signed `started` and `result` together before sending them. A committed result prevails over a subsequent cancel. |
| Retry and delivery | The device keeps the exact signed payload after acknowledgement. The ledger verifies and stores the first proof atomically with its transition, treats an exact proof retry as idempotent, and rejects a different proof of the same kind. Result creates one outbox entry in its original topic and arms delivery in the same transaction. |

`status {attempt}` reports the owning attempt's phase, original Start
deadline and cancellation flag. `report {proof}` carries the original JSON
payload and hex signature. Its separate signing domain is
`coconutclaw-evidence-v1\nlocal-executor-fixture\nalpha\n` followed by that
payload verbatim. Retrying `report` changes the outer sequence and timestamp
while preserving the inner proof.

Cancellation can reach the ledger between the executor's final status read
and process creation. That race is handled as running cancellation on the
next successful poll. A transport outage delays cancellation. Executor death
after spawn may leave the disposable child alive briefly. The test child is
bounded and the restart preserves `Unknown` without making an exit claim.
General child survival and containment across host or executor death remain
pilot gates. Production task #23 retains its existing `Unknown` record.

## Local validation

Run from the repository root using the Phase 1 staging prerequisites, Node
24, OpenSSL and a Unix host. The harness generates and removes its own
certificate authority, keys, process journal and Miniflare persistence.

```sh
cargo build --locked -p coconutclaw-gateway --features local-executor --bin gateway-local-executor
npm --prefix crates/coconutclaw-gateway run stage
GATEWAY_LOCAL_EXECUTOR="$PWD/target/debug/gateway-local-executor" \
  node --test crates/coconutclaw-gateway/{config,integration}.test.mjs
npm --prefix crates/coconutclaw-gateway run check:cf
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --locked -p coconutclaw-gateway --features local-executor --all-targets -- -D warnings
cargo clippy --locked -p coconutclaw-gateway --target wasm32-unknown-unknown -- -D warnings
cargo test --workspace --all-targets --locked
```

The lane supplied its compiler and wasm-bindgen via
`PATH="$PWD/tmp/gateway-tools:$PATH"` and the Wasm sysroot via
`RUSTFLAGS="--sysroot=$PWD/tmp/gateway-tools/sysroot"` for Wasm commands.
Workspace native checks used `tmp/gateway-cargo`, `target/native-gateway`,
and a short disposable `TMPDIR` for Unix socket paths.

| Injection | Observed on Linux |
| --- | --- |
| Executor link unavailable during intake, then result reply lost after commit | The offline update survived a ledger restart. One child ran. The same device restarted and retried the identical signed proof with a fresh sequence. The alarm produced one automatic reply. The mock saw chat `321`, topic `37`, and the expected text. Message ID `1` survived a second ledger restart. |
| Abrupt executor exit immediately before and immediately after spawn | The first attempt created zero children and the second created one. Both signed `unknown` on restart and neither relaunched. A cancel against the second stayed requested without an exit claim. The old signed Start request was rejected before and after ledger restart. |
| Start reply loss, same-attempt replayed response, late response, cancel before grant delivery | Retry kept the original deadline. The replay and late response created zero children. Cancel before receipt also created zero children and produced signed `before_start`. Running cancel created one child, produced signed `process_exited` and produced no result. |
| Result committed before cancellation, altered result proof | Cancel returned `completed`. A valid outer signature with an altered inner payload was rejected. A differently signed result for the same attempt conflicted. |

The prebuilt `cf` dry run reported `BOTS` and `PHASE="disabled"` with about
305 KiB gzipped. The three native cases, the five Phase 1 cases and the config
test passed locally. The native workspace reported 581 passing tests and
five ignored. See the Phase 1 notes for its Worker and alarm fault matrix.

For targeted mutations, list diffs with
`cargo mutants --Zmutate-file <rust-file> --list --diff --json`. Apply one
generated diff to a disposable copy and build its native executor with the
same `cargo build` command and a separate `CARGO_TARGET_DIR`. Point
`GATEWAY_LOCAL_EXECUTOR` to the mutant and
run `node --test --test-name-pattern <case> crates/coconutclaw-gateway/integration.test.mjs`
from the baseline repository. Three `cargo-mutants` 27.1.0 variants built
and failed the matching test.

| Mutation | Observed failure |
| --- | --- |
| Replace `||` with `&&` in the grant identity check in `executor/runner.rs` | The stale response bypassed the sequence check and the late-grant case failed to reach its expected reconciliation. |
| Replace `||` with `&&` in the cancellation check in `executor/runner.rs` | The launch log contained a child for the cancelled attempt. |
| Replace `Journal::consume` with `Ok(true)` in `executor/journal.rs` | The pre-spawn crash left `intent` in SQLite where the test requires `consumed`. |

## Automatic delivery

One alarm reserves at most one send. The single ledger commits `in_flight`,
the reservation number, a five-second watchdog and a four-second Bot-wide
send interval before the mock fetch. A successful receipt commits its
positive message ID and `delivered` together. A matching Bot API `429` with
`retry_after` from one to ten seconds allows one retry. A longer valid
`retry_after` or a second `429` ends that delivery as `failed` and still sets
the Bot-wide cooldown. A timeout, lost or indeterminate receipt, or an
expired `in_flight` reservation becomes `unknown`. Only `pending` work is
eligible for sending. Cooldowns, the watchdog and receipts survive restart.
An admitted request also bootstraps alarm recovery for older Phase 2 rows.
The Phase 2 fixture reads delivery state with `delivery_status`.

The loopback tests cover a restart with pending work, a replay after a lost
result acknowledgement, and restart after the mock accepts a send with its
response held. The next topic still delivers and the interrupted send stays
`unknown`. A separate test restarts after the first `429`, checks pacing on
another topic, and verifies terminal failure after the second rejection.
Four targeted `cargo-mutants` variants failed their matching tests. They
bypassed the task cap, inverted the watchdog deadline, allowed a retry
after a second rejection, or inverted the alarm admission check.

## Pilot budget

The ledger admits 9,800 forwarded requests per UTC day against a shared
limit of 10,000 admitted requests and alarms. The remaining 200 admissions
are reserved for alarms. All forwarded Phase 2 routes charge the same
counter before protocol work. A denial returns `429` with `Retry-After` and
leaves the command unapplied. At the shared limit the alarm keeps pending
work and schedules the next UTC boundary. A future stored quota day fails
closed. Twenty distinct new tasks may be admitted per day. The task charge
commits with intake and rolls back with it. A duplicate can still be
acknowledged while ingress has room. Local tests exercise concurrent final
admissions, restart, a backward clock, UTC rollover and the hard alarm cap.

At a 10-second idle floor one device uses at most 8,640 idle `next` calls
per day, leaving 1,160 ingress admissions for other traffic. Active status
polls, handshakes, fixture calls and retries share that budget. At the request
limit the executor observes `Retry-After`. Remote cancellation and result
acknowledgement can therefore be delayed until quota resets. The local child
still has its eight-second execution bound. Each task can reserve two
sends, so 20 newly admitted tasks can contribute up to 40 sends. A retained
backlog can contribute more on a later day. The hard backlog ceiling is
128 inbox rows and 256 total send reservations across those rows. The
executor attempt ceiling is 512 and text and body limits are 4 and 16 KiB.

The full-ledger Miniflare test exercised a 128-task scan, the last of 512
attempt slots, maximum ASCII text, signed proof replay and delivery. Local SQL
cursor counters peaked at 271 reads on agent requests. Writes peaked at
eight warm and 33 during schema initialization. The successful alarm sample
peaked at 15 reads and seven writes. The retained SQLite file used 1,941,504
bytes, including 8,192 bytes for the two partial outbox indexes in their
terminal state. Local wall time varies with runtime load. The full run
sampled a 14 ms successful mock alarm. The regression envelope is 320 reads,
ten warm writes and 40 writes including initialization per sampled
invocation. For 10,000 admitted invocations that projects 3.2 million
reads and 100,000 warm writes. A cold initialization on every admitted
invocation would project 400,000 writes. These figures use workerd SQL
counters and exclude platform alarm bookkeeping and denied invocations.

The platform counts raw Worker and DO invocations before this admission
logic runs. An isolated ingress still needs a pre-forward traffic bound for
invalid traffic and retries, and hosted measurement of charged requests,
row and index writes, storage, alarm traffic, CPU and duration. Confirm
headroom against the authorized test account's limits before a pilot.
The official prebuilt `cf` dry run reported 316.32 KiB gzipped and the
same `BOTS` and `PHASE="disabled"` bindings.

## Remaining pilot gates

- For real Codex execution, identify the separately authorized test-provider
  profile. Wire it into the loopback-only executor using the existing provider
  and a fresh workspace, journal, device key and provider state directory.
  Bound the run to two attempts, one success and one cancellation observed
  after process creation, with a per-attempt deadline and provider usage
  ceiling. Verify exactly one same-topic mock receipt for the success,
  signed `process_exited` and no result for the cancelled attempt, and no
  relaunch after restart. The current child in this harness is synthetic.
- With separately authorized test Bot and gateway identities, verify webhook
  admission and same-topic Bot API delivery with fixture operations
  inaccessible. Measure hosted account headroom and Bot API pacing.
- Validate key rotation, clock skew, sleep and network changes, native
  background service lifecycle and containment through power or executor
  loss. Document reconciliation of `Unknown` and explicit retry with a
  new task ID. Production task #23 retains `Unknown`.
- Route and memory authorization, schedules, multi-device placement,
  Windows containment, failover and production migration have separate gates.
