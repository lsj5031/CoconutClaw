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
| Ingress | Poll processing and the webhook reducer share `Store::admit_telegram`. The check precedes raw custody and `last_update_id` advancement. The same check rejects legacy `inflight_update_json` import before its checkpoint is cleared. | The poll loop can still fetch using the frozen cursor. A handler that already committed custody can continue through `claim_telegram` and its effects. Startup recovery and retention can still write. Local `once` and other direct `SessionScheduler::enqueue` calls bypass raw Telegram custody. |
| Schedule materialization | `Store::due_schedule_events` checks before occurrence allocation, inbox and task creation, or any `next_due` advancement, including missed-run skips. | `create_schedule`, `edit_schedule` and `set_schedule_state` still write. An occurrence committed before the fence can still be queued and dispatched. |
| Dispatch and Start | | `dispatch_ready` reserves a lane and releases its transaction before `spawn_task`. The worker can write `update_task_run_started` and invoke Codex. `nodes::execute` can persist a remote assignment and issue `Begin` to a runner with separate state. Runner admission and already issued Starts still need an authority and generation fence. |
| Outbox claim and send | | `retry_deliveries` and `deliver_telegram_acks` still select and send work. The delivery plan's `sending` transition and immediate progress and callback calls are outside the fence. HTTP already issued can still take effect and return late. |
| Result custody | | Local `persist_result` still writes. The normal remote path through `nodes::accept`, including `nodes reconcile`, verifies the signed Start and Result, commits them to `runtime_remote_results` with their `runtime_inbox` event, and can then project them through `apply_runtime_results`. Those writes still target the legacy database. Cutover needs a durable destination for late verified receipts, coverage for receipts that arrive after the final delta, and a separate guard on result projection. |

The fence establishes an order for the two covered database transactions. A
final snapshot still requires stopping the remaining business writers and
reconciling external effects already in flight.

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
