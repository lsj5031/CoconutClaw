# BigFur control host

For the first BigFur deployment, follow the [direct-retirement runbook](bigfur-direct-retirement.md)
for ledger initialization and predecessor evidence.

Run `authority-control.mjs` and `authority-preflight.mjs` from
`crates/coconutclaw-gateway` on an independently administered, always-on host
with Node 24 (verified on 24.22.1). The service uses Node's SQLite and HTTP
libraries. Use a dedicated private directory on durable local storage
that honors SQLite locks and fsync. Keep it outside all Worker, DO and runner
restore domains. Use one serving process and one database. Give publication
and retirement access only to the independent control operator.

Expose the loopback listener through an HTTPS reverse proxy on the same
host. Forward only `POST /v1/check` and `POST /v1/send` with no cache, retries
or redirects. Keep TLS verification enabled, cap bodies at 32 KiB, and
exclude authorization, bodies and downstream Bot API URLs from logs and
diagnostics. Verify host and Worker clocks and proxy latency before activation.
The Worker has a 1.5-second request timeout. Checks return 900 ms permits and
the host gives the outgoing Telegram request 10 seconds. The service is
fail-closed when the host or its database is unavailable.

## Enrollment and retirement

1. Keep ingress, launch and effect credentials held. For a custody cutover, prepare the held ledger
   with the checked generation, custody digest and attempt floor. Match the
   actual account, Worker, deployment and `CF_VERSION_METADATA.id` to the
   preflight target. Preserve the original `#23` Unknown and quarantine
   predecessor work. Verify the
   [handoff evidence](cloud-gateway-cutover-rehearsal.md#production-handoff-preflight).
2. Use `node authority-control.mjs init "$DB"` once, for an explicitly
   authorized first publication with no prior control store. For a restart
   check `node authority-control.mjs status "$DB"` against the independent
   control record and follow the continuity gate below.
3. Put the complete preflight `target` and the verified `versionId` in a
   private `OBSERVE.json` as `{ "target": {...}, "versionId": "..." }`. Supply a
   fresh `CUTOVER_AUTHORITY_BEARER` (32 random bytes in lowercase hex) to
   the observer and candidate. Under exclusive candidate routing run
   `node authority-control.mjs observe OBSERVE.json "$PORT"` and make one
   controlled non-agent probe that reaches the intended object. The observer
   returns 503 and prints the first matching object UUID to its private
   stdout. Verify the probe's provenance, stop the observer and keep that
   object running. The bearer authenticates the caller and the operator
   establishes which deployment and object made the request.
4. Prepare private `RECORD.json` with `target`, `versionId`,
   `objectIncarnation`, `runnerIncarnation` and `retirement`. The last field
   has the preflight predecessor generation and verified evidence references.
   For BigFur under the [fresh-object recovery policy](bigfur-direct-retirement.md#eviction-and-recovery),
   replace `objectIncarnation` with `"continuity": "fresh-object-only"`. The
   observer can verify the candidate route without enrolling its process UUID.
   Issue an independent random 32-byte lowercase-hex `runnerIncarnation`
   to the runner outside its journal and backups. Compare the record with the
   independently collected preflight target (`CANDIDATE.json`) using
   `node authority-preflight.mjs RECORD.json CANDIDATE.json`. Then run
   `node authority-control.mjs publish "$DB" RECORD.json`. The next consecutive
   generation can be published once, after retirement of its predecessor.
   The command checks the shape and references. Verify the evidence separately.
5. Confirm exclusive token custody or issuer invalidation of the old token,
   including file access, before starting `serve`. Once the service is
   reachable, a persisted alarm can dispatch without public ingress.
   Run `node authority-control.mjs serve "$DB" "$PORT"` with
   `CUTOVER_AUTHORITY_BEARER`, `CONTROL_TELEGRAM_CREDENTIAL_VERSION` and
   `CONTROL_TELEGRAM_BOT_TOKEN` supplied privately by the host. The credential
   version must match the publication. Verify the exact active tuple and
   retired-path denials before opening public traffic.
6. Run `node authority-control.mjs retire "$DB" "$GENERATION"` to close the
   current generation. Confirm `status` reports it retired. Wait at least the
   Worker's two-second permit window plus verified clock skew **after** the
   retirement acknowledgment. Drain and reconcile in-flight handlers,
   outstanding grants and ambiguous effects before enrolling a successor.
   A busy or failed retirement has no acknowledgment. Keep ingress closed
   and recheck the named generation before repeating the command.

The service commits `(generation, task, send number)` before starting any
Telegram request. It then acquires the SQLite writer lock again, rechecks
authority and holds that lock through the one bounded outbound request and
receipt commit. Retirement either wins before dispatch begins or waits for
that request. An already-issued request can still complete at Telegram.
Duplicates never dispatch, even after restart. A positive message ID is a
committed receipt. The Worker records an authentic receipt even if Control
then becomes unavailable. A null message ID is a spent reservation with
an uncertain outcome.

Before reserving locally, the alarm uses `deliveryCheck: true` on `/v1/check`
to check readiness against the Control writer lock. A temporary failure rearms
pending work. After a Worker reservation expires without a receipt, the Worker
can ask `/v1/send` to `closeUnsent` for that exact generation, task and send
number. Control commits a row with message ID `0` under its writer lock only
if the slot was unused. The row permanently fences that send number, and a
repeat request for the same slot can recover the proof. A verified proof allows
one new send number within the existing two-attempt bound. With missing or
ambiguous proof, including a Control outage at expiry, the Worker durably records
Unknown with zero retries. If the original authenticated send response arrives
after expiry, a positive receipt with the matching request nonce reconciles
that exact generation, task and send number to Delivered without rearming
delivery. Retired targets can neither close an unused slot nor dispatch a
reply. Their in-flight rows can still expire locally. If the response is lost,
reconcile held deliveries against the Worker and private Control evidence before
any operator-authorized new effect. The reservation key is scoped to one
generation, so hold old work through the handoff.

Upgrade Control before the Worker and roll back the Worker before Control.
Control versions predating `deliveryCheck` reject that request with HTTP 400,
so a new Worker holds pending deliveries. Upgraded Control still accepts the
older Worker's ordinary checks and sends. Control versions that support
`deliveryCheck` but predate `closeUnsent` leave expired slots without proof.
Hold paired successor ingress if reverting to a Worker that predates inherited
inbox checks.

## Production placement registry

For an authorized multi-placement publication, use `pairing: "production-v1"`
in the target in place of `executorKey`. Copy that pin into `CUTOVER_TARGET`.
Use `continuity: "fresh-object-only"` in the publication and omit
`runnerIncarnation`. Publishing initializes the registry atomically in the
existing Control database after its predecessor is retired. A new production
installation can publish generation 1. A previously published generation
requires the next consecutive generation and its fresh object.

Before publishing a paired successor, close predecessor ingress, retire its
effect authority and drain issued permits. With the predecessor's actual
object still pinned, call `POST /v1/production/ingress-handoff` with the
`x-cutover-operator` header. The endpoint atomically freezes fresh ingress
and returns the generation, object ID, custody digest and committed update
IDs. Repeating it gives the same record after cold wake. Verify provenance
and continuity under operator custody. Keep the records private.

Include the verified records in the publication's top-level `ingress` array,
in generation order. Control requires an exact generation, object and custody
match for every previously published BigFur ledger whose IDs it does not
already hold. Previously imported records remain in the immutable
publication history. When a predecessor's records or their provenance are
unavailable, hold the paired publication. The original installations still
need the predecessor freeze/export route on the pinned object before moving
public ingress.

The Control operator reads a runner's public card and uses
`node authority-control.mjs issue "$DB" CARD.json` with `{"key":"<card id>"}`.
Include `"alias":"desk"` for a later runner. The private code can enroll its
exact signing key once within five minutes. An unredeemed code expires without
enrollment. Repeating the authenticated redemption of a spent code returns the
original receipt after expiry, runner retirement or generation retirement.
Recovering a retired receipt grants only reconciliation authority.
The first runner is `alpha` and the default. `beta` and `gamma` require unique
aliases. Admission consults the published registry through `/v1/check`.
The same check suppresses inherited IDs before creating a successor task.
An authenticated, in-scope update with an exact ID already in the local
inbox can be acknowledged even during a Control outage or after retirement.
Requests that require a registry lookup, including inherited IDs known only
to Control, fail closed while Control is unavailable.
Expose `POST /v1/pair` through the Control HTTPS proxy for authorized pairing,
with the same body cap and logging restrictions as the other Control routes.

Use `rename "$DB" RUNNER.json` with `placement`, `incarnation`, and the new
`alias`, or `retire-runner "$DB" RUNNER.json` with just `placement` and
`incarnation`. Both commands take an exact receipt identity in the current
generation. A retired slot and its alias remain reserved for that generation. Retiring
`alpha` closes unprefixed admission while the other aliases continue. Tasks
already pinned to a retired runner stay with it. Follow the same permit drain
window as generation retirement. Results accepted before retirement can still
have pending deliveries.

The holder of the retired enrollment's original private key and incarnation
can authenticate Hello, Status and signed evidence on the original route.
This reconciliation authority gives no Claim or Start grant and queues no
reply. A result first reported under this authority is retained as evidence
with an Unknown outcome. Evidence keeps its original signature domain.
Control retains retired production generations for this purpose. Exact
old-target checks and the original route's credentials still apply.

## Restore

Retire authority and close routes and effect custody before recovery. The
service rejects old generations and checks the exact object, Worker version
and runner enrollment. A UUID-pinned publication also rejects a new DO
incarnation. A `fresh-object-only` publication admits ordinary cold wakes of
the enrolled object. Recovery requires a newly routed object and a fresh
runner incarnation under the next generation. The control store rejects
reuse of previously enrolled identities when publishing under this policy.

If the **control store** is missing, has uncertain continuity, or may have
been rolled back, keep its listener, ingress and effect custody closed.
Verify freshness and continuity against independently retained current
publications and retirement evidence. Establish the disposition of every
possibly spent send and permit.
A same-generation backup can predate retirement or a send reservation even
when its high-water value matches. Recover the latest verified state under
the independent operator's control. Leave all potentially spent work held.
Reopen only after verified retirement and a higher authorized publication
backed by that independent evidence.

## Verification

Run from the checkout root with the cached disabled Worker bundle and native
fixtures. These tests use synthetic credentials, a loopback fake Telegram
endpoint, disposable databases and independently restored Worker state.

```sh
df -B1 . /tmp
node --test --test-concurrency=1 crates/coconutclaw-gateway/{authority-control,authority-preflight}.test.mjs
GATEWAY_LOCAL_EXECUTOR="$PWD/target/native-gateway/debug/gateway-local-executor" \
COCONUTCLAW_CUTOVER_CLI="$PWD/target/native-gateway/debug/coconutclaw" \
  node --test --test-concurrency=1 \
  crates/coconutclaw-gateway/{authority-control,authority-preflight,config,integration}.test.mjs
```

On November 25, 2026 all 28 gateway tests passed. The tests also detected
four targeted mutations to generation continuity, target matching,
reservation replay and the dispatch lock.
