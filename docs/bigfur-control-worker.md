# BigFur Control Worker

Control runs in a separate Worker. Its singleton SQLite Durable Object,
`ControlLedger` named `bigfur-control-v1`, owns publications, retirement,
placement enrollment, and send evidence. The Gateway retains its existing
`BotLedger`, including ingress freeze, signed runner evidence, task state,
and outbox. The production Gateway name and namespace stay the same.

## Staging access

From `crates/coconutclaw-gateway`, `npm run stage:control-staging` packages
`coconutclaw-bigfur-control-staging` under `control/.cloudflare`.
`npm run stage:control-staging-gateway` packages
`coconutclaw-bigfur-gateway-staging` under `.cloudflare`. Control stages
independently of Rust. `npm run stage:control` and
`npm run stage:production-control` package the corresponding production
artifacts. Keep production and staging namespaces and credentials separate.

For a separately authorized staging rehearsal, the Cloudflare operator needs
Workers version and deployment access, permission to create the staging
SQLite class and configure both service bindings, and private secret custody.
The Control operator needs a private caller bound to the named
`ControlOperator` entrypoint and its own `CONTROL_OPERATOR_BEARER`. The Gateway
binds only to Control's default entrypoint and holds only
`CUTOVER_AUTHORITY_BEARER` for Control. Keep the same bearer at both ends. A
private probe caller must be able to invoke the staging Gateway service binding
with `https://bigfur.liu.nz` as the request origin. Neither staging Worker has
a public route. Require an authenticated private operator path to that probe
caller. Use a synthetic webhook secret, runner keys, topic scope, and
credential-version label. Use `held` as the required token secret's inert
value for hosted checks without sends. The local fixture intercepts every
Telegram call for send checks.

Obtain the actual candidate Gateway `CF_VERSION_METADATA.id` from the uploaded
version and verify its account, Worker, existing production `BotLedger`
namespace, route, and `CUTOVER_TARGET` independently. Pin that exact version
in the import. A version that merely has the same Git revision cannot
substitute. Changes to Gateway bindings or secrets can create a different
version. Collect the ID after configuring the candidate. Pairing callers
that previously used the Control host can use the existing signed
`POST /v1/pair` protocol at the Gateway origin. Preserve the existing
report and agent routes and runner credentials.

## Single-sender handoff

1. Record the source high-water generation, active or retired status, full
   publication history, active deployment/version/object/custody, enrolled
   runner placements, and Gateway inbox and outbox positions. Keep the live
   Gateway ledger and the selected `CUTOVER_TARGET` unchanged for an active
   `fresh-object-only` migration. Keep work already marked Unknown held.
   An active UUID-pinned publication requires retirement and a fresh
   successor. A retired source imports as retired.
2. Close old ingress and runner claim/start routes at the routing boundary.
   Use the reversible routing hold for a same-generation move. The Gateway's
   durable ingress freeze is reserved for successor handoff. Stop admitting
   old Control requests, including through the old proxy, and stop every
   publisher, registry writer, and sender. Drain issued permits for at least
   the two-second Gateway window plus measured clock skew. Drain the bounded
   Telegram request and verify the serving process is stopped before taking
   the export. Reconcile each reserved Gateway send against independently
   captured evidence, keeping every possibly spent attempt held. Account for
   requests already issued to Telegram that may still complete there.
3. Establish exclusive custody of the existing Bot API credential with
   evidence of removal from every old sender, restart supervisor, executable
   configuration, and access path. Keep the old endpoint unreachable and
   the old process fenced across restart. Use the inert `held` value until
   the new token can enter exclusive custody.
   If exclusive custody cannot be established, keep both senders held. An
   issuer-invalidated credential must go through retirement and an authorized
   successor with its new credential version.
4. Export the quiesced SQLite database under independent operator custody.
   The export checks database integrity and foreign keys and reads all five
   tables in one transaction. Keep the original DB and export private.

   ```sh
   umask 077
   node authority-control.mjs status "$DB"
   node authority-control.mjs export "$DB" > "$SOURCE"
   sha256sum "$SOURCE"
   ```

   For an active source, verify the export's freshness and provenance against
   the independent control record while ingress and writers are held. Supply
   that verified export and its independently recorded SHA-256 as `SOURCE`
   and `SOURCE_SHA256`. In the private JSON `GATEWAY_SECRETS_FILE`, set
   `CUTOVER_TARGET` to the JSON-encoded active publication target with only
   `deployment` removed. The CLI compares the parsed objects, so whitespace
   and member order can differ. The credential version label is part of the
   comparison.

   ```sh
   npm run stage:production-control
   npm run preflight:production-control -- "$SOURCE" "$SOURCE_SHA256" "$GATEWAY_SECRETS_FILE"
   ```

   Run these gateway package commands from `crates/coconutclaw-gateway`.
   For an authorized upload with traffic held, capture the candidate UUID
   and verify the exact version in the project account.

   ```sh
   GATEWAY_VERSION=$(npm run --silent release:production-control -- "$SOURCE" "$SOURCE_SHA256" "$GATEWAY_SECRETS_FILE") &&
     ./node_modules/.bin/cf workers versions get "$GATEWAY_VERSION" --worker-id coconutclaw-phase1-test
   ```

   The release command repeats the preflight before calling `cf workers
   versions create --prebuilt --mode phase2-production-control --secrets-file`
   with a private copy of the checked secrets. It uploads a version only and
   prints its UUID. A failed preflight blocks `cf`. If the upload fails or
   produces no verified UUID, check upload state before retrying. Freshness
   depends on the independent record and keeping writers held through release.
   For a preflight against a post-migration snapshot, verify its
   `sourceSha256` independently and save `JSON.stringify(source)` to the private
   source file without a trailing newline.

   After confirming the uploaded Gateway version, prepare the import.

   ```sh
   node --input-type=module - "$SOURCE" "$GATEWAY_VERSION" > "$IMPORT" <<'JS'
   import { readFileSync } from "node:fs";
   import { createHash } from "node:crypto";
   const [path, version] = process.argv.slice(2);
   const bytes = readFileSync(path);
   process.stdout.write(JSON.stringify({
       source: JSON.parse(bytes),
       sourceSha256: createHash("sha256").update(bytes).digest("hex"),
       gatewayVersion: version || null,
   }));
   JS
   ```

   Use an empty `GATEWAY_VERSION` for a retired source. Import bodies are
   bounded at 1 MiB. A larger source stays held pending a reviewed migration
   procedure. The source contains invitation hashes and enrollment details.
5. Through the private operator caller, `POST /v1/operator/status` and verify
   `highWater: 0`, `migration: null`, `senderConfigured: false`. `POST` the
   import file to `/v1/operator/import` with the operator bearer. Compare its
   returned digest, generation, and selected version to the independent record.
   `POST /v1/operator/snapshot` and compare `sourceSha256` and all five tables
   byte-for-byte after serializing the returned `source` with
   `JSON.stringify`. Count send rows separately for positive message IDs,
   zero and null. Verify the status high-water and retirement flag. A repeat
   with the same digest and version returns the original migration record.
   A different import is rejected. An active version change is recorded
   alongside the immutable publication and leaves generation, object, custody,
   runner instances, signature domains, and send keys intact. Confirm that
   both readbacks show `migration.gatewayVersion` equal to the verified
   `GATEWAY_VERSION`. An existing import cannot bind another version under
   the same import record. Hold activation until the authorized Control
   publication or initial migration binds the exact candidate.
6. After the Control bind and readback, explicitly activate the candidate at
   100% with the existing production route and its ingress, runner and sender
   holds still in place. Verify that the first deployment in the list serves
   exactly the candidate UUID at 100%.

   ```sh
   versions=$(printf '[{"version_id":"%s","percentage":100}]' "$GATEWAY_VERSION") &&
     ./node_modules/.bin/cf workers deployments create --worker coconutclaw-phase1-test \
       --strategy percentage --versions "$versions" &&
     ./node_modules/.bin/cf workers deployments list --worker coconutclaw-phase1-test
   ```

   While ingress is held, verify the candidate can make the expected
   authority check, the old version is denied even with an exact previous
   pin, and the old endpoint and old sender are still fenced. Verify the
   existing and retired runner receipts through signed requests, fixed
   placement, and unauthenticated ingress denial. Activate only the private
   sender with the verified credential version. A Gateway alarm can send
   as soon as that credential is configured. Verify the chosen Gateway
   version is the exclusive route before lifting ingress and runner holds.

Keep the independent timestamped artifact and deployment IDs, object ID,
source and readback hashes and row counts, credential-custody evidence,
retirement or process-drain acknowledgment and timing, route-denial results,
and exact staging commands, counts, and exits. Store secret-bearing artifacts
privately. Record which slots remain Unknown.

## Send and retirement

A send first commits `(generation, task, send number)` with a null message ID.
Control waits for durable storage, rechecks authority, and issues one
bounded Telegram request. It commits positive receipt evidence before
replying with the request nonce. Once reserved, the same slot never sends
again. A lost reply, failure, or reset can leave a null row. Zero is durable
proof that an unused slot was closed before dispatch. Only this proof may
allow the existing Gateway logic to reserve the next send number within its
two-attempt bound. Positive evidence can reconcile the matching original
Gateway attempt. Null is spent and ambiguous.

`POST /v1/operator/retire` with `{"generation":N}` acknowledges the exact
current generation. During in-flight dispatch Control returns 503 and
leaves retirement unacknowledged. Check status and retry only for the same
named generation after the request drains. Once retirement is acknowledged,
wait the full permit window plus clock skew and reconcile any already-issued
requests. Old runners retain only the reconciliation permissions of their
original authority records. The normal generation handoff still requires
Gateway ingress freeze and export on the predecessor, fresh successor
identity, and frozen inbox records on publication.

The operator routes all use `POST` and the operator bearer. `status` and
`snapshot` take no body. `publish`, `issue`, `rename`, and `retire-runner`
take the corresponding record shapes from the host runbook. The Gateway
never proxies these routes.

## Gateway ledger queries

Use the project's authenticated Cloudflare `cf` context. Verify that
`BOT_LEDGER_NAMESPACE_ID` names the production `BotLedger` namespace and
`GATEWAY_OBJECT_ID` is the exact object from the verified publication for
the generation under investigation. Run from `crates/coconutclaw-gateway`.

```sh
queries=$(node operator-query.mjs task-status "$TASK_ID") &&
  ./node_modules/.bin/cf durable-objects namespaces query "$BOT_LEDGER_NAMESPACE_ID" \
    --durable-object-id "$GATEWAY_OBJECT_ID" --queries "$queries"

queries=$(node operator-query.mjs webhook-health) &&
  ./node_modules/.bin/cf durable-objects namespaces query "$BOT_LEDGER_NAMESPACE_ID" \
    --durable-object-id "$GATEWAY_OBJECT_ID" --queries "$queries"
```

`TASK_ID` is `telegram:<update-id>`. Both queries project only metadata using
fixed `SELECT` statements through the existing authenticated ledger API.

- `task-status` shows the stored execution phase and the Gateway outbox phase
  independently. `absent` means this object has no task row for that ID.
  `task_pin` is the fixed requested runner slot and is visible before a claim.
  `attempt_device` is the current attempt's runner slot. Each is `alpha`,
  `beta`, `gamma` or `null` when the corresponding slot is unavailable.
  `no_attempt` means no attempt is attached, `missing_attempt` means an
  attached attempt record is unavailable, and `no_outbox` means there is no
  local delivery record. `unknown` in either phase is durable uncertainty
  about that outcome. `send_reservations` counts local reservations.
  `delivery_receipt_recorded` reports Gateway receipt presence only. Control
  may hold send evidence that the Gateway has yet to record.
- The task timestamps are stored Unix milliseconds. They describe the claim
  lease expiry, Start grant expiry, per-row pending delivery readiness or
  in-flight reservation expiry when that phase applies. A deadline may
  already have elapsed. The event times are unavailable. Other pacing and
  authority checks can delay delivery beyond readiness.
- `webhook-health` shows the local generation, durable ingress freeze, retained
  inbox count, and latest committed sequence and disposition in ledger order.
  A zero count means this ledger has no committed inbox evidence. An unavailable
  query or uninitialized schema leaves the evidence unknown. Webhook event
  times, rejected requests, acknowledgments without a new inbox row, Telegram
  registration and current reachability are outside this recorded evidence.

## Recovery

- After a cold wake, compare Control status, the migration record, and send
  evidence with the independent checkpoint. The same Control DO continues
  to own all send numbers. The old host remains fenced. Retry an interrupted
  import with the same file and version while the sender is held. An empty
  store or a digest mismatch after activation is a continuity incident.
- Keep ingress, runner grants, and sender custody held if either store may
  have been rolled back. The old Gateway snapshot may contain pending work
  and alarms. Verify the stale version is denied and reconcile pending slots
  with the current Control evidence. Preserve signed evidence and Unknown.
  For recovery that requires restoring or replacing the Gateway object,
  retire the current generation and use the fresh-object successor procedure.
- Recover Control only from the latest independently verified SQLite DO
  state. Verify publications, retirements, registry, and each possibly
  spent send against independent evidence. Same-generation backups can
  predate a retirement or a reservation. Keep all possibly spent work held,
  then require verified retirement and a higher authorized publication before
  reopening. A legacy source export predates all new effects and cannot
  safely replace the current Control ledger. Once new Control may have sent,
  restarting the old sender from its old DB is unsafe.

The separately authorized staging gate must demonstrate the hosted service
bindings and restricted routes, and record namespace, version, and secret
custody evidence. The local tests use isolated namespaces and synthetic
credentials and exercise storage and routing without a hosted account.

## Local verification

On November 27, 2026, the WASM build used the shared
`build-verification.lock`, cached 1.98.1 WASM sysroot, and `wasm-bindgen`
0.2.130. `sh crates/coconutclaw-gateway/build.sh` and all five local staging
commands exited 0.

```sh
(cd crates/coconutclaw-gateway && node stage.mjs phase2-control-staging && \
  node stage.mjs phase2-production-control && node stage.mjs && \
  node stage.mjs control-staging && node stage.mjs control)
GATEWAY_CONTROL_TEST_BUNDLE_DIR="$PWD/crates/coconutclaw-gateway/control/.cloudflare/output/v0/workers/default/bundle" \
  node --test --test-concurrency=1 crates/coconutclaw-gateway/{authority-control,authority-preflight,config,control-worker}.test.mjs
node --test --test-concurrency=1 --test-name-pattern='Cloudflare Control|BigFur .*paired|BigFur production pairing keeps|BigFur authority fences|BigFur delivery across|BigFur late receipt|BigFur retirement at' \
  crates/coconutclaw-gateway/integration.test.mjs
```

- Config, legacy authority, and staged Control Worker passed 15 of 15, exit 0.
- Gateway integration passed 18 of 18, exit 0. The fixture stopped the old
  authority, reopened a cloned Gateway snapshot and the separate Control
  store, and observed one delivered reply and one ambiguous slot that
  stayed Unknown with one send. It checked the freeze again after restart.
- Three staged mutation probes bypassed duplicate reservation denial,
  allowed retirement during dispatch, and accepted the stale version.
  Running `control-worker.test.mjs` with `GATEWAY_CONTROL_TEST_BUNDLE_DIR`
  pointed at each modified bundle exited 1 with 2, 1, and 1 failing
  assertions respectively.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` exited 0.
  `cargo test --workspace --all-targets --locked` ran 18 suites with 591
  passed and 5 ignored, exit 0. Both used the same shared build lock and
  cached Zig linker. `cargo fmt --all -- --check` and `git diff --check`
  exited 0.

In development, an old pin with the wrong generation caused the Gateway to
reject a Telegram webhook with HTTP 503. After the pin was corrected and the
matching Control Worker version was in use, the retry succeeded and delivery
was verified. Full power-off of the old host remains unverified.
