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
| Retry and delivery | The device keeps the exact signed payload after acknowledgement. The ledger verifies and stores the first proof atomically with its transition, treats an exact proof retry as idempotent, and rejects a different proof of the same kind. Result creates one outbox entry in its original topic. The fixture drain retains a successful mock Telegram message ID with the receipt. |

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
| Executor link unavailable during intake, then result reply lost after commit | The offline update survived a ledger restart. One child ran. The same device restarted and retried the identical signed proof with a fresh sequence. Replayed result created one reply. The mock saw chat `321`, topic `37`, and the expected text. Message ID `1` survived a second ledger restart. |
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

## Capped pilot readiness

- Provision a separately authorized test Bot, device key and gateway identity
  with a production ingress that leaves every fixture operation inaccessible.
  Exercise real webhook receipt, same-topic Bot API delivery, Codex execution
  and cancellation in a disposable workspace. Implement durable automatic
  outbox draining with bounded retry and recovery of ambiguous delivery.
- Validate key revocation and rotation with retained audit history, clock
  skew and the latest-start window, real sleep and network changes, native
  background service lifecycle, and process containment through power and
  executor loss. The fixture only exercised Linux. Establish a recovery
  procedure for `Unknown` and explicit user retry with a new task ID.
- Enforce a pilot budget before admission. The current fixture caps bodies
  at 16 KiB, prompt and result text at 4 KiB, inbox entries at 128, and
  attempts at 512. Phase 2 permits one device and two outbox reservations per
  task. The harness caps 160 Worker calls and eight mock sends per test.
  Normal idle polling every 2–2.5 seconds costs up to 43,200 `next` calls/day
  per executor. Running cancellation polls target 250 ms and failed
  connections back off from 250 ms to at most 8 seconds including jitter.
- Use a pilot request proposal of at most 10,000 Worker and DO invocations
  each per day for one Bot, with at most 20 admitted tasks/day, an idle
  interval of at least 10 seconds and a shared request limiter covering
  retries, active polling, outbox and alarms. Idle polling at that floor
  alone allows 8,640 calls/day. Measure rows read and written, index overhead,
  retained evidence/storage, duration, CPU, and Bot API limits before
  approving the cap. Compare measured traffic and account headroom to the
  then-current platform limits. The local dry run supplies bundle size only.
- Route and memory authorization, schedules, multi-device placement,
  Windows containment, failover and production migration have separate
  acceptance gates.
