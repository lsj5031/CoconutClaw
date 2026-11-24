# Synthetic cutover rehearsal

The test seeds an in-memory database with the legacy `sql/schema.sql` and imports a
versioned, SHA-256-checked projection into the existing Miniflare SQLite ledger.
The import runs while the disposable local Worker is stopped. It commits the
snapshot and ledger rows together. The test interrupts a partial import, retries it,
then replays the same snapshot after the Worker has accepted new work. A different
snapshot is rejected.

The stable ID uses the legacy logical task number. Synthetic `legacy:23` keeps run
`24`, original topic `37`, and the source record of the Unknown outcome. The ledger
also quarantines a running task, an in-flight send, and a pending send. Imported
attempts have no eligible executor or Start grant. Imported deliveries retain their
send counts with phase `unknown`. After import and restart, the Worker rejects claims
and starts for the quarantined tasks and deduplicates the old update even when the
replay supplies a different topic. The only mock send is a reply to a new update in
the original topic.

Run from `crates/coconutclaw-gateway` with the existing locally staged Worker
bundle and dependencies. The local staging entry point is `npm run stage`, which
uses `cloudflare.config.ts`.

```sh
env -u GATEWAY_LOCAL_EXECUTOR node --test --test-concurrency=1 \
  --test-name-pattern='synthetic cutover' integration.test.mjs
```

## Real cutover gates

- Obtain separate approval for live cutover. Prove that the old authority is
  fenced from ingress, schedules, dispatch, and sends. Buffer or route late
  signed receipts durably, then import and compare the final delta before
  switching the webhook.
- Design a consistent export and import for the real schema. Reconcile actual
  stable IDs, attempts, routes, inbox and outbox evidence, schedule watermarks,
  and signed receipts. Verify and retain the original live `#23` Unknown record.
  The fixture maps four logical tasks and retains selected source fields.
- Fence old gateway generations and authorizations, and reconcile any ambiguous
  effects using independent evidence before releasing them. Exercise live
  Telegram delivery and Codex cancellation under separate authorization.
- Rehearse failback with updates, tasks, results, and receipts accepted by the
  new authority. Record versions, high-water marks, times, and signed evidence.
  Measure quotas, recovery time, and the acknowledged-update loss window.
