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
