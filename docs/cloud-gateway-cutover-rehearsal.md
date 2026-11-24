# Synthetic cutover rehearsal

The test seeds a disposable legacy SQLite file with `sql/schema.sql` and stages a
versioned snapshot in the stopped Miniflare Worker’s SQLite ledger. The sole
legacy fixture writer commits one more task and closes its connection. A read-only
connection rejects ingress, schedule, dispatch, delivery, and result writes, then
exports the frozen projection. The final import verifies both SHA-256 digests,
requires an ordered append to the exact staged baseline, and commits the delta
and freeze record together. The test interrupts and retries both imports.

Synthetic `legacy:23` keeps run `24`, topic `37`, and the source record of the
Unknown outcome. Other imported rows include two running attempts, an in-flight
send, and a pending send. The Worker rejects claims and starts for the quarantined
attempts and keeps both sends Unknown with their original send counts.

The Worker accepts update `200` in topic `37`, commits its signed result, and
loses the acknowledgment. After restart the delta, update, and signed result are
replayed. The task has one attempt and produces one mock Telegram receipt.
The Worker then stops. A transaction returns the settled update, task, and result
to the legacy fixture as run `29` with completed ingress and a delivered payload.
It retains the gateway task ID, attempt, signed result, public key, and delivery
receipt. The test interrupts and retries that transaction, reopens the legacy file,
deduplicates update `200`, and accepts a fresh fixture update.

The forward delta covers appended rows in the fixture’s task projection. The
return covers one settled result. The legacy side uses SQLite connections and
fixture writes. The Worker side uses the real loopback protocol with a mock sender.

Run from `crates/coconutclaw-gateway` with the existing locally staged Worker
bundle and dependencies. If needed, `npm run stage` stages the bundle from
`cloudflare.config.ts` with the lane’s existing Wasm tools and cache. The test
prints the fixture version, frozen inbox sequence, digests, and signed evidence.

```sh
env -u GATEWAY_LOCAL_EXECUTOR node --test --test-concurrency=1 \
  --test-name-pattern='synthetic cutover' integration.test.mjs
```

## Legacy admission fence

The Rust runtime reads `legacy_admission_freeze` inside the existing SQLite
writer transactions for raw Telegram custody and schedule materialization.
On an initialized disposable database, a separate writer activates the fence by
committing `INSERT OR IGNORE INTO legacy_admission_freeze(singleton) VALUES (1)`.
The insert is idempotent and an aborted insert leaves admission open. An already
admitted transaction can commit first. Once the insert commits, both admission
paths reject new work, including exact Telegram replays, on existing and reopened
connections.

| Path | Covered | Remaining |
| --- | --- | --- |
| Ingress | `Store::admit_telegram` fences custody, cursor advancement, and legacy checkpoint import. `claim_telegram` fences new reducer claims. | The running poll loop can fetch using the frozen cursor. Already-claimed reducers and the poll loop's retention writes drain when the service stops. |
| Schedule materialization | The freeze precedes occurrence allocation, inbox and task creation, and due-cursor advancement. The scheduler fences occurrence dispatch. | `create_schedule`, `edit_schedule` and `set_schedule_state` remain administrative writers. |
| Dispatch and Start | `enqueue` and dependency resumption fence durable admission. `dispatch_ready` checks before reserving a lane. The worker rechecks in its durable Start transaction. `nodes::execute` checks in the assignment transaction that precedes `Begin`. `Runner::admit` checks its own database before creating a new signed Start. The executor seal requires signed terminal evidence for each reserved `Begin`. | A delayed `Begin` can still start on an unfrozen runner while the seal is blocked. Legacy grants have no expiry or revocation protocol for disconnected runners. |
| Outbox claim and send | Task replies, ingress acknowledgments, and notifications use the shared durable sender. The freeze orders against `sending` for each rich, chunk, or media operation and the checkpoints for UI callbacks and standalone progress cleanup. The immediate ingress progress message has its own fenced `sending` write. | A reserved operation can finish after freeze, including bounded fallback after a rejected edit and cleanup attached to an acknowledged reply. Active provider progress edits drain with the started task. |
| Result custody | `Begin` and `Status` replies through `nodes::accept`, including `nodes reconcile`, retain the verified Start and optional Result under the exact original run and assignment. Frozen receipt custody writes to `node_signed_receipts`; `apply_runtime_results` leaves the legacy projection untouched. | Already-started local tasks can persist ordinary results during drain. Late receipts require export, acknowledgment at the new authority, and continued reconciliation after final delta. The development task adapter uses its own receipt journal and has no frozen ingress into the local receipt buffer. |

### Local seal

Activate the existing freeze on the initialized legacy database, then stop the
local coordinator and wait for it to exit. The systemd and launchd units invoke
`run`. Frozen `run` and `once` refuse startup before provider or Telegram calls.
Keep administrative writers stopped for the final projection.

`coconutclaw --data-dir DATA_DIR seal-legacy-cutover` takes the same instance
lock as `run` and `once`. It checks the freeze and executor evidence in one
SQLite writer transaction and records `legacy_executor_seal(singleton=1)`.
Rerunning the command rechecks the evidence. The older `legacy_cutover_seal`
row only records the local ownership boundary.

A legacy `Begin` carries a signed assignment and can arrive after the
coordinator stops. Every `node_assignments` row is treated as potentially
issued because the coordinator commits it before transport. The runner commits
a signed Start to `node_claims` before scheduling the provider process.
A signed Start alone leaves a
possible future launch. The seal needs the exact signed terminal Result chained
to that Start and the original reservation. It checks the late receipt buffer
and the historical result projection, requiring agreement where both carry
evidence. A terminal `Unknown` spends the one-time launch authority while
preserving the unknown outcome. The runner returns existing evidence when the
same `Begin` reappears. `nodes start` and desktop runner activation hold the
runner's instance lock through shutdown and reuse the same claims after restart.

A disconnected runner with missing or Start-only evidence blocks the seal.
Elapsed time, transport failure, and a cancellation request supply no terminal
evidence. A remote reservation without a tracked `Begin` also blocks. Keep the
original databases, keys, and assignment rows intact. Restoring a runner from
before its claim or reusing its identity with another journal invalidates this
proof. If the original runner never consumed a grant, the current protocol has
no compatible terminal evidence for it. Leave the gate blocked until there is
verifiable revocation or the original authority is otherwise retired under a
separate proven procedure.

The freeze permits effects reserved before it and the seal preserves their
last durable evidence. Treat unfinished started runs, assignments with an
unconfirmed outcome, and `sending` operations without durable acknowledgment
as Unknown. Preserve their original IDs, receipts, and send counts. Hold queued
and pending work for explicit reconciliation. Frozen recovery holds queued
work and sends. Already-issued HTTP and already-started work can still complete.
The seal leaves synthetic task `#23` and the frozen projection intact.

Keep the original node assignment, signing authority, and receipt buffer
together for reconciliation. `nodes reconcile RUN_ID` can collect the exact
signed Start before a Result exists, and repeat after restart. It sends
`Status` only. Exact replays retain one record and conflicting evidence fails
validation. The buffer remains writable after the seal and is outside the
frozen business projection. Use the pinned legacy binaries and identities
for this exchange if a newer build rejects the old pairing revision. The seal
verifies the historical signatures from SQLite without a network request.

The HTTPS `gateway-local-executor` requires `--loopback-fixture`. Its signed
requests and signed receipts are scoped to that fixture. Its TLS grant reply
binds the attempt, request sequence, and launch deadline. The executor checks
the deadline again at the spawn boundary and consumes the attempt in its own
journal. Expiry leaves the effect Unknown when launch cannot be ruled out.
Production gateway-generation rotation and fencing old gateway grants across
restore and failback remain separate gates.

Run the following from the repository root with more than 15 GiB free on both
the checkout filesystem and `/tmp`. The existing `tmp/gateway-tools` supplies
`cc` and `ar` on the rehearsal host.

```sh
df -BG . /tmp
export PATH="$PWD/tmp/gateway-tools:$PATH"
export CARGO_HOME="$PWD/tmp/gateway-cargo" CARGO_TARGET_DIR="$PWD/target/native-gateway"
export CARGO_BUILD_JOBS=1
export ZIG_GLOBAL_CACHE_DIR="$PWD/tmp/zig-global" ZIG_LOCAL_CACHE_DIR="$PWD/tmp/zig-local"
export TMPDIR="$(mktemp -d /tmp/ccfreeze.XXXXXX)"
cargo test --offline --locked -p coconutclaw --bin coconutclaw \
  -- --test-threads=1 legacy_admission_ \
  ingress_concurrent_exact_webhook_replays_share_one_control_and_reply \
  calendar_occurrence_payload_cursor_and_inbox_roll_back_and_replay_together
cargo fmt --all -- --check
git diff --check
```

The ingress test attempts the freeze from a separate connection while the real
custody transaction is open. SQLite rejects the competing write with
`SQLITE_BUSY`. The admitted update and cursor then commit, the freeze succeeds,
and fresh and replayed input fail through `process_webhook_line` across reopen.
Synthetic task `#23` retains run `24`, topic `37`, and its original Unknown
outcome. The calendar test submits a due schedule to the materializer before
and after reopening the frozen database and checks that the due cursor and work
tables stay intact.

All four focused tests passed. Replacing `require_open_legacy_admission` with
`Ok(())`, as generated by `cargo mutants --Zmutate-file
crates/coconutclaw-cli/src/store/mod.rs --list --diff --json`, made both
`legacy_admission_` tests fail. The bypass accepted update `202` and
materialized the due occurrence.

`cargo fmt --all -- --check`, `git diff --check` and
`cargo clippy --offline --workspace --all-targets --locked -- -D warnings`
passed. The serialized `cargo test --offline --workspace --all-targets --locked`
run passed all 442 CLI unit tests, with four ignored. It stopped at
`successful_flush_survives_external_immediate_sigkill` in `coconutclaw-sync`
after the publisher timed out waiting for a flush acknowledgment. The same
compiled test passed on its isolated rerun.

### Local seal rehearsal

The focused tests use a disposable SQLite database, a loopback Telegram
sender, and a paired loopback runner with synthetic signing keys. One test
freezes during an HTTP send, loses the response, and verifies across restart
that the ambiguous send keeps its original attempt count and a second send
stays held. Another freezes before dispatch and re-enters the worker's Start
path with a pre-freeze request. The receipt test uses `nodes reconcile` across
reopen and checks a late Start, late Result, exact replay, valid but conflicting
evidence, and a separate pending projection that stays held.

Use the cache settings above. Before each build, run `df -B1 . /tmp` and
require at least 16,106,127,360 available bytes on both filesystems. Stop and
retain the test data if either check fails or the build reports `ENOSPC`.

```sh
cargo test --offline --locked -p coconutclaw --bin coconutclaw -- --test-threads=1 \
  legacy_cutover_ legacy_admission_ \
  ingress_concurrent_exact_webhook_replays_share_one_control_and_reply \
  calendar_occurrence_payload_cursor_and_inbox_roll_back_and_replay_together
cargo test --offline --locked -p coconutclaw --test nodes_cli -- legacy_cutover_
cargo test --offline --locked -p coconutclaw --test topics_rich -- \
  --exact polling_raw_turn_recovery_excludes_reasoning_effects_in_both_crash_windows
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --workspace --all-targets --locked -- -D warnings
cargo test --offline --workspace --all-targets --locked -- --test-threads=4
```

The authority rehearsal delivers a saved `Begin` to the real loopback runner
after the coordinator freezes. Both missing evidence and Start-only custody
block the seal. Once the terminal receipt is retained, the seal succeeds and
an exact `Begin` replay after runner reopen creates no second process. The
receipt test also checks a terminal `Unknown` and historical projected evidence.
The CLI test holds an untracked reservation across process invocations and
checks that the older marker cannot authorize the seal or restart the runtime.
`cargo-mutants` bypassed the authority verifier and both authority tests
failed. The staged loopback Worker cutover and native executor late-grant
rehearsals passed.

The earlier local ownership rehearsal passed seven focused tests and the
reply-recovery regression. Formatting, diff checks, and Clippy passed. Its
workspace suite passed 586 tests, with five ignored. The local Worker
rehearsal passed using the staged bundle and mock transport, with synthetic
task `#23` still Unknown.
`cargo mutants` caught the `Store::custody_node_receipt` replacement with
`Ok(())` in `legacy_cutover_reconcile`. When using the existing target cache,
wait for the entire workspace suite before starting a mutation run. Several
process tests relaunch their own binary.

```sh
cargo-mutants mutants --in-place --baseline skip --build-timeout 180 --timeout 30 \
  --output "$TMPDIR" --file crates/coconutclaw-cli/src/store/runtime_results.rs \
  --re 'replace Store::custody_node_receipt .* with Ok\(\(\)\)' \
  -C=--offline -C=--locked --test-package coconutclaw \
  --cargo-test-arg=--bin --cargo-test-arg=coconutclaw -- legacy_cutover_reconcile
```

## BigFur Readiness

Leo has authorized the BigFur cutover. Readiness gates still apply. Usage, quota,
and billing queries are out of scope.

- Prove the live writer fence across ingress, schedules, dispatch, sends, and
  result handling. Route or buffer late signed receipts durably. Fence old
  gateway generations and Start grants, including for disconnected executors.
- Export and reconcile the real final delta, including changed rows, stable IDs,
  routes, attempts, signed receipts, outbox state, and schedule watermarks.
  Verify and retain the original live `#23` Unknown record.
- Verify real Telegram webhook and same-topic delivery and real Codex
  execution and cancellation through the isolated pilot gates.
- Rehearse live failback with credential and generation rotation. Reconcile
  unsettled effects before resuming consumption. Record versions, high-water
  marks, signed evidence, recovery time, and the acknowledged-update loss window.
