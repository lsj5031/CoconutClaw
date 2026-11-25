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
the host gives the outgoing Telegram request 1 second. The service is
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
Duplicates never dispatch, even after restart. A timeout, lost acknowledgment,
or crash leaves a spent reservation. A null receipt means the outcome may
be Unknown. A concurrent send can receive 503 while the writer is busy.
Reconcile held deliveries against the Worker and private control evidence
before any operator-authorized new effect. The reservation key is scoped to
one generation, so hold old work through the handoff.

## Restore

Retire authority and close routes and effect custody before restoring a
Worker, DO, or runner. The service rejects old generations and different
DO incarnations. Ordinary DO eviction also loses its incarnation and
requires an explicitly authorized higher generation.

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
