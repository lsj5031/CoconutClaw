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

Ingress has a 16 KiB body cap and two-second read deadline. The pinned ledger
limits include 20 new tasks per UTC day, 128 admitted inbox IDs and 512
attempts. The lifetime caps need an operating limit or a retention design
before sustained traffic.

The control service fences the live DO incarnation. Worker replacement or
ordinary DO eviction can close execution and sends while the running
production generation continues to accept Telegram updates. The ledger is
bound to one generation. Re-enrollment on that ledger is a blocker for
unattended operation. Close webhook admission separately when retiring
production authority. If control is unavailable when a send alarm fires,
inspect the durable outbox before authorizing any action. Ambiguous sends
stay Unknown.
