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
update. Publish `"continuity": "fresh-object-only"` in place of
`objectIncarnation`. Supply the runner incarnation and retirement, grant
and exclusive credential evidence from the direct-retirement cutover.

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

### Eviction and recovery

This release excludes in-place restore of the active object and rollback
of the control store. Keep recovery operations under separate operator
custody. The enrolled object can cold-wake against its live SQLite ledger
without re-enrollment. Control still checks the published generation,
object ID, Worker version and runner incarnation before effects. An
out-of-band rollback under the active object ID and deployment is
indistinguishable from that cold wake. It can undo acknowledged ingress
and reopen older task state. Treat suspected rollback as an incident and
close ingress and effect authority.

1. Close ingress and retire the predecessor in the independent control
   store. Wait out permits, stop the old runner and reconcile outstanding
   effects before opening the successor. Keep ambiguous effects Unknown.
2. Set the next generation explicitly. Ask the authenticated identity
   endpoint for that generation using `x-cutover-generation`, and pin the
   returned ID with a fresh custody digest. Generation 1 routes to
   `bigfur-production`. Later generations route to
   `bigfur-production:<generation>`. Verify the new deployment routes to
   the fresh object and remove the predecessor's ingress route.
3. For a paired successor, freeze the predecessor's ingress and include its
   verified update IDs in the Control publication using the
   [Control runbook](gateway-authority-control.md#production-placement-registry).
   This covers updates whose HTTP acknowledgments were lost. Preserve the
   predecessor ledger for reconciliation and keep its tasks and outbox held.
   A legacy singleton successor still requires a separately verified
   Telegram pending-update boundary.
4. Publish the successor against the exact new route and Worker version.
   Enroll a fresh runner incarnation and a generation-scoped journal. Open
   ingress only after verifying the predecessor and retired runner are
   denied. Legacy singleton admission uses Cloudflare SQLite and the webhook
   secret. New paired admission also requires active Control.

The same ledger deduplicates repeated update IDs across ordinary eviction.
A temporary Control outage rearms alarms for pending work with zero sends.
Retirement holds the generation's deliveries. A lost send acknowledgment
spends the send reservation and records Unknown.
