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

   For an active source, use the verified export and set `CUTOVER_TARGET_FILE`
   to a new path in the private staging directory. Derive the staged pin from
   the active publication's target with only `deployment` omitted. Run the
   check against the exact file you will use for `CUTOVER_TARGET` immediately
   before upload. The check compares the parsed JSON objects, so whitespace
   and member order can differ. Exit status `0` means they match. Any failure,
   including a wrong generation, blocks upload. Neither command prints values.

   ```sh
   cutover_pin() {
       node --input-type=module - "$SOURCE" "$CUTOVER_TARGET_FILE" "$1" <<'JS'
   import { readFileSync, writeFileSync } from "node:fs";
   import { isDeepStrictEqual } from "node:util";

   const [sourcePath, pinPath, mode] = process.argv.slice(2);
   try {
       const source = JSON.parse(readFileSync(sourcePath, "utf8"));
       const active = source.publications.at(-1);
       if (!active || source.retirements.some(row => row.generation === active.generation)) {
           throw new Error();
       }
       const { target } = JSON.parse(active.publication);
       if (target.generation !== active.generation || !Object.hasOwn(target, "deployment")) {
           throw new Error();
       }
       const { deployment, ...expected } = target;
       if (mode === "derive") {
           writeFileSync(pinPath, JSON.stringify(expected), { flag: "wx", mode: 0o600 });
       } else if (mode !== "check" ||
           !isDeepStrictEqual(JSON.parse(readFileSync(pinPath, "utf8")), expected)) {
           throw new Error();
       }
   } catch {
       console.error("CUTOVER_TARGET pre-upload check failed");
       process.exitCode = 1;
   }
   JS
   }
   cutover_pin derive && cutover_pin check
   ```

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
   runner instances, signature domains, and send keys intact.
6. While ingress is held, verify the candidate can make the expected
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
