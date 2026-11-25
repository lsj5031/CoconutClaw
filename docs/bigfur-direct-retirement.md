# BigFur direct retirement

Start with a new BigFur ledger and retain the legacy records in place. Keep
live `#23` at its original Unknown outcome. Exclude legacy tasks from
execution on BigFur.

## Topology

- Cloudflare serves `bigfur.liu.nz` from the existing `coconutclaw-phase1-test`
  Worker and SQLite `BotLedger` namespace. Production uses the `bigfur-production`
  object. The retired synthetic pilot has a distinct object.
- Leo-Claw runs one enrolled Linux `alpha` runner with its private signing key,
  durable journal and scoped Codex provider. The Worker queues updates while
  Leo-Claw is off.
- An existing always-on host independent of Leo-Claw serves the control
  service over HTTPS with its own SQLite database and exclusive Bot API send
  credential. Agent polling, claim, Start, evidence, cancellation and outbound
  delivery require the service. Ingress uses the webhook secret and Cloudflare
  SQLite. [Control host runbook](gateway-authority-control.md)

The existing domain, Worker, SQLite Durable Object and two host roles need
no paid Cloudflare binding.

## Production candidate

Stage the accepted revision on Mars with the official `cf` CLI and
`cloudflare.config.ts`. Reuse the existing account, domain and SQLite class.
Review the prebuilt candidate with `npm run stage:production` and
`npm run check:cf:production` from `crates/coconutclaw-gateway`. The explicit
non-preview `phase2-production` mode supplies the domain and
`CF_VERSION_METADATA`. The default and preview modes are disabled.

For authorized staging use a host-private secrets file with
`cf deploy --prebuilt --mode phase2-production --secrets-file`. Set
`CUTOVER_CANCEL_SECRET` and keep the webhook closed while identifying the
production object. Send `POST /v1/production/identity` at `bigfur.liu.nz` with
the `x-cutover-operator` header to obtain the ID without creating a ledger.
An absent or invalid `CUTOVER_TARGET` leaves the data routes closed.

Use the object ID, committed revision, hostname, executor public key and
credential version in `CUTOVER_TARGET`. Use generation `1` for an independently
verified empty control store. Its `custodyDigest` is a fresh 64-character
lowercase hex publication identifier for the direct retirement. Use the
same identifier in the control publication. Record the actual Cloudflare
Worker version ID after installing the final bindings.

Set `PRODUCTION_TELEGRAM_SCOPE` to the verified private numeric
`chat_id:101550`. Use a distinct `WEBHOOK_SECRET` for Telegram's
`X-Telegram-Bot-Api-Secret-Token` header. Keep secrets in the host's private
paths. Set `CUTOVER_AUTHORITY_ORIGIN` and `CUTOVER_AUTHORITY_BEARER` for the
independent HTTPS control host. An authenticated `/v1/cancel` request for a
nonexistent task can exercise the control observer before the first Telegram
update. Use the resulting object incarnation and the one runner incarnation
in the control publication. Supply retirement, grant and exclusive
credential evidence from the direct-retirement cutover.

## Cutover and limits

1. Verify the Cloudflare domain and covering rules, TLS and control latency.
   Confirm the legacy unit, reactivation paths, in-flight effects and Bot
   identity. Stop and disable legacy polling, sending and automatic
   reactivation. Establish exclusive credential custody or invalidate old
   credentials. Compare Telegram's pending-update boundary with recent
   legacy update IDs before handing off. The new ledger deduplicates from its
   first admitted update. Resolve any concrete overlap before opening ingress.
2. Point the whole Bot webhook to `https://bigfur.liu.nz/v1/telegram` with
   `drop_pending_updates=false` and read back the URL. The first valid in-scope
   update atomically creates the generation and queues the task. Plain-text
   messages from the configured private chat owner in topic `101550` create
   work. Other authenticated update types are ignored.
3. Verify a real same-topic request and reply, native Codex success and
   cancellation, and durable ingestion with Leo-Claw stopped. Record
   redacted evidence. Keep the control host available for execution and sends.

Ingress has a 16 KiB body cap and two-second read deadline. The ledger caps
new tasks at 20 per UTC day, inbox sequence at 128, and attempt IDs at 512.
For supervised operation, cap admissions at **16 new tasks per UTC day** and
close admission when either the highest inbox sequence reaches **96** or
the highest allocated attempt ID reaches **384** for the lifetime of the
ledger. Count expired claims toward the attempt ceiling. Verify the counts
from a current ledger inspection. Keep admission closed if the counts
cannot be verified or a cutoff has been reached. This leaves 32 inbox slots
and 128 attempt IDs of headroom. Preserve the ledger and its deduplication
history at the cutoff.

### Eviction blocker

Cloudflare reruns the constructor when an evicted object wakes. SQLite
contents survive normal eviction. Point-in-time recovery also restarts
the object and restores its SQL and KV contents. The documented bookmarks
identify points in the object's history. An eviction and a restore can
present the same object ID, Worker version and ledger contents.
[Lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/),
[SQLite recovery](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#pitr-point-in-time-recovery-api),
[Durable Object state](https://developers.cloudflare.com/durable-objects/api/state/).

Control pins a UUID generated in the live object. Both eviction and restore
replace it. Persisting that UUID in the ledger would also copy it into later
snapshots. Accepting a new UUID based on the target and SQLite state would
authorize a restored copy with the same inputs. The existing control store
records publication, retirement and send reservations. Safe recovery needs
a continuity signal outside the restored ledger, such as a trusted
platform restore epoch.

The production-mode loopback test cold-starts the original SQLite store
and separately opens a copy under the same target. In both cases control
denies a signed claim and a persisted alarm, leaving the queued task and
pending outbox intact. Only the original store receives a further update,
and it still deduplicates an admitted update. A temporary stable-incarnation
mutation makes the copied store's claim succeed. The test models eviction
with a process restart and restore with an offline snapshot because the
Cloudflare PITR API is unavailable locally.

Unattended operation remains blocked. Fresh ingress still works within
the ledger limits. Close admission separately when retiring authority.
Inspect the durable outbox after an authority failure at alarm time.
Ambiguous effects stay Unknown.

On November 25, 2026, 15 focused loopback and configuration checks passed,
including the stale-restore, retirement, and ambiguous-effect denials.
The offline WASM build, official `cf` production dry run, formatting,
workspace and WASM Clippy, and all 591 runnable workspace tests passed.
The five ignored workspace tests stayed ignored.
