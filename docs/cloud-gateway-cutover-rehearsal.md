# Synthetic cutover rehearsal

The test stages a baseline from a disposable legacy SQLite database into a
stopped Miniflare Worker. Staging holds gateway admission closed. The legacy
fixture commits one more task, records delivery of an in-flight reply and
advances a schedule watermark. It commits the admission freeze before the
read-only final export. The final import verifies both SHA-256 digests against
the staged baseline and commits the appended task, delivery acknowledgment,
archived watermark and gateway activation together. The test interrupts and
retries both imports and rejects changed task identities and regressed watermarks.

Synthetic `legacy:23` keeps run `24`, topic `37` and its original Unknown
record. Two running attempts stay held. The acknowledged send for `legacy:26`
retains message `42` and its original send count. The pending send for
`legacy:27` stays Unknown at zero sends. The imported schedule watermark is
retained in the snapshot, with no active gateway schedule.

The Worker accepts update `200` in topic `37`, commits its signed result, and
loses the acknowledgment. After restart the delta, update, and signed result are
replayed. The task has one attempt and produces one mock Telegram receipt.
The gateway then freezes with a second issued grant for update `209`. The
frozen gateway accepts the exact signed Start and Result for custody. A lost
acknowledgment and restart leave one result and zero sends for that task.
After signed terminal verification, the next generation stays inactive on
the return leg. The old Worker rejects ingress when reopened. One transaction
returns both updates as runs `29` and `30`, preserving their gateway task IDs,
attempts, routes, signed Start and Result, and the settled delivery receipt.
Run `30` stays held as Unknown. An interrupted transaction keeps legacy
admission frozen. After commit and reopen, the legacy fixture deduplicates both
returned updates and accepts a fresh one.

The native executor rehearsal uses the original journal and a loopback TLS
fixture. An expired grant with only a signed Start blocks advancement. A late
Result arrives after freeze and is durably acknowledged on exact replay after
restart. The transition rejects a wrong verification key and rolls back on
an interrupted generation write. The reopened old Worker rejects requests,
and the new Worker rejects old signatures and attempts. The executor rejects
a grant carrying the previous generation even when its attempt and request
sequence match. Its persistent journal rejects a request to resume the old
generation after adoption. The new generation launches and delivers one
same-topic result.

The version `coconutclaw-synthetic-cutover-v2` fixture has frozen inbox
sequence `6`, schedule `7` advanced from `1000` to `2000`, and these digests.

- Baseline `be37ff556cc12db351e583f36f29cb4b632c919bd68b36824b30681afcc8e8a5`
- Final `fcbf0a6bc9eecbda10044839cfbed6ee8471f6fad3d4d6d3e2a07088df2e7c7a`

Run from `crates/coconutclaw-gateway` with the locally staged Worker,
dependencies and `gateway-local-executor` binary. Use the existing cache and
disk checks below for each build. `npm run stage` reads `cloudflare.config.ts`.
The test prints signed evidence and the run IDs.

```sh
GATEWAY_LOCAL_EXECUTOR="$PWD/../../target/native-gateway/debug/gateway-local-executor" \
  node --test --test-concurrency=1 \
  --test-name-pattern='synthetic cutover' integration.test.mjs
```

`cargo mutants` generated an `Executor::launch` mutation that allowed an old
generation grant through when the attempt and request sequence matched. The
generation rehearsal caught it at the executor's first request after receiving
the grant.

The November 25 rehearsal on Leo-Claw passed both cutover tests three times
and all 18 loopback and configuration tests. The legacy authority checks
passed four focused unit tests and one CLI test. The workspace suite passed
587 tests with five ignored. Rust formatting, native and Wasm Clippy, and
`git diff --check` passed.

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
| Ingress | `Store::admit_telegram` fences custody, cursor advancement, and legacy checkpoint import. `claim_telegram` fences new reducer claims. | The running poll loop can fetch using the frozen cursor. Already-claimed reducers and the poll loop's retention writes drain when the service stops. |
| Schedule materialization | The freeze precedes occurrence allocation, inbox and task creation, and due-cursor advancement. The scheduler fences occurrence dispatch. | `create_schedule`, `edit_schedule` and `set_schedule_state` remain administrative writers. |
| Dispatch and Start | `enqueue` and dependency resumption fence durable admission. `dispatch_ready` checks before reserving a lane. The worker rechecks in its durable Start transaction. `nodes::execute` checks in the assignment transaction that precedes `Begin`. `Runner::admit` checks its own database before creating a new signed Start. The executor seal requires signed terminal evidence for each reserved `Begin`. | A delayed `Begin` can still start on an unfrozen runner while the seal is blocked. Legacy grants have no expiry or revocation protocol for disconnected runners. |
| Outbox claim and send | Task replies, ingress acknowledgments, and notifications use the shared durable sender. The freeze orders against `sending` for each rich, chunk, or media operation and the checkpoints for UI callbacks and standalone progress cleanup. The immediate ingress progress message has its own fenced `sending` write. | A reserved operation can finish after freeze, including bounded fallback after a rejected edit and cleanup attached to an acknowledged reply. Active provider progress edits drain with the started task. |
| Result custody | `Begin` and `Status` replies through `nodes::accept`, including `nodes reconcile`, retain the verified Start and optional Result under the exact original run and assignment. Frozen receipt custody writes to `node_signed_receipts`; `apply_runtime_results` leaves the legacy projection untouched. | Already-started local tasks can persist ordinary results during drain. Late receipts require export, acknowledgment at the new authority, and continued reconciliation after final delta. The development task adapter uses its own receipt journal and has no frozen ingress into the local receipt buffer. |

### Local seal

Activate the existing freeze on the initialized legacy database, then stop the
local coordinator and wait for it to exit. The systemd and launchd units invoke
`run`. Frozen `run` and `once` refuse startup before provider or Telegram calls.
Keep administrative writers stopped for the final projection.

`coconutclaw --data-dir DATA_DIR seal-legacy-cutover` takes the same instance
lock as `run` and `once`. It checks the freeze and executor evidence in one
SQLite writer transaction and records `legacy_executor_seal(singleton=1)`.
Rerunning the command rechecks the evidence. The older `legacy_cutover_seal`
row only records the local ownership boundary.

A legacy `Begin` carries a signed assignment and can arrive after the
coordinator stops. Every `node_assignments` row is treated as potentially
issued because the coordinator commits it before transport. The runner commits
a signed Start to `node_claims` before scheduling the provider process.
A signed Start alone leaves a
possible future launch. The seal needs the exact signed terminal Result chained
to that Start and the original reservation. It checks the late receipt buffer
and the historical result projection, requiring agreement where both carry
evidence. A terminal `Unknown` spends the one-time launch authority while
preserving the unknown outcome. The runner returns existing evidence when the
same `Begin` reappears. `nodes start` and desktop runner activation hold the
runner's instance lock through shutdown and reuse the same claims after restart.

A disconnected runner with missing or Start-only evidence blocks the seal.
Elapsed time, transport failure, and a cancellation request supply no terminal
evidence. A remote reservation without a tracked `Begin` also blocks. Keep the
original databases, keys, and assignment rows intact. Restoring a runner from
before its claim or reusing its identity with another journal invalidates this
proof. If the original runner never consumed a grant, the current protocol has
no compatible terminal evidence for it. Leave the gate blocked until there is
verifiable revocation or the original authority is otherwise retired under a
separate proven procedure.

The freeze permits effects reserved before it and the seal preserves their
last durable evidence. Treat unfinished started runs, assignments with an
unconfirmed outcome, and `sending` operations without durable acknowledgment
as Unknown. Preserve their original IDs, receipts, and send counts. Hold queued
and pending work for explicit reconciliation. Frozen recovery holds queued
work and sends. Already-issued HTTP and already-started work can still complete.
The seal leaves synthetic task `#23` and the frozen projection intact.

Keep the original node assignment, signing authority, and receipt buffer
together for reconciliation. `nodes reconcile RUN_ID` can collect the exact
signed Start before a Result exists, and repeat after restart. It sends
`Status` only. Exact replays retain one record and conflicting evidence fails
validation. The buffer remains writable after the seal and is outside the
frozen business projection. Use the pinned legacy binaries and identities
for this exchange if a newer build rejects the old pairing revision. The seal
verifies the historical signatures from SQLite without a network request.

The HTTPS `gateway-local-executor` requires `--loopback-fixture`. Its signed
requests and receipts are scoped to the fixture generation. Its TLS grant
reply binds the generation, attempt, request sequence and launch deadline.
The executor checks the deadline again at the spawn boundary and consumes
the attempt in its journal. Expiry leaves the effect Unknown when launch
cannot be ruled out. Generation advancement requires the original journal
with terminal evidence acknowledged and no unresolved intent or consumption.

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

### Local seal rehearsal

The focused tests use a disposable SQLite database, a loopback Telegram
sender, and a paired loopback runner with synthetic signing keys. One test
freezes during an HTTP send, loses the response, and verifies across restart
that the ambiguous send keeps its original attempt count and a second send
stays held. Another freezes before dispatch and re-enters the worker's Start
path with a pre-freeze request. The receipt test uses `nodes reconcile` across
reopen and checks a late Start, late Result, exact replay, valid but conflicting
evidence, and a separate pending projection that stays held.

Use the cache settings above. Before each build, run `df -B1 . /tmp` and
require at least 16,106,127,360 available bytes on both filesystems. Stop and
retain the test data if either check fails or the build reports `ENOSPC`.

```sh
cargo test --offline --locked -p coconutclaw --bin coconutclaw -- --test-threads=1 \
  legacy_cutover_ legacy_admission_ \
  ingress_concurrent_exact_webhook_replays_share_one_control_and_reply \
  calendar_occurrence_payload_cursor_and_inbox_roll_back_and_replay_together
cargo test --offline --locked -p coconutclaw --test nodes_cli -- legacy_cutover_
cargo test --offline --locked -p coconutclaw --test topics_rich -- \
  --exact polling_raw_turn_recovery_excludes_reasoning_effects_in_both_crash_windows
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --workspace --all-targets --locked -- -D warnings
cargo test --offline --workspace --all-targets --locked -- --test-threads=4
```

The authority rehearsal delivers a saved `Begin` to the real loopback runner
after the coordinator freezes. Both missing evidence and Start-only custody
block the seal. Once the terminal receipt is retained, the seal succeeds and
an exact `Begin` replay after runner reopen creates no second process. The
receipt test also checks a terminal `Unknown` and historical projected evidence.
The CLI test holds an untracked reservation across process invocations and
checks that the older marker cannot authorize the seal or restart the runtime.
`cargo-mutants` bypassed the authority verifier and both authority tests
failed. The staged loopback Worker cutover and native executor late-grant
rehearsals passed.

The earlier local ownership rehearsal passed seven focused tests and the
reply-recovery regression. Formatting, diff checks, and Clippy passed. Its
workspace suite passed 586 tests, with five ignored. The local Worker
rehearsal passed using the staged bundle and mock transport, with synthetic
task `#23` still Unknown.
`cargo mutants` caught the `Store::custody_node_receipt` replacement with
`Ok(())` in `legacy_cutover_reconcile`. When using the existing target cache,
wait for the entire workspace suite before starting a mutation run. Several
process tests relaunch their own binary.

```sh
cargo-mutants mutants --in-place --baseline skip --build-timeout 180 --timeout 30 \
  --output "$TMPDIR" --file crates/coconutclaw-cli/src/store/runtime_results.rs \
  --re 'replace Store::custody_node_receipt .* with Ok\(\(\)\)' \
  -C=--offline -C=--locked --test-package coconutclaw \
  --cargo-test-arg=--bin --cargo-test-arg=coconutclaw -- legacy_cutover_reconcile
```

## Frozen production-schema export

`coconutclaw legacy-cutover` uses explicit paths and bypasses runtime setup.
Exercise it with standalone rollback-mode SQLite backups of a disposable
coordinator. `VACUUM INTO` from the fixture creates that form. The source must
be a complete, detached backup with no concurrent writers or SQLite sidecars.
Capture the baseline before the freeze. For the final backup, follow the
local seal procedure and stop administrative writers. Keep both backups and
the original runner journals.

```sh
coconutclaw legacy-cutover baseline --source baseline.sqlite \
  --source-id "$SOURCE_ID" --coordinator-key "$COORDINATOR_PUBLIC_KEY" \
  --output baseline.json
coconutclaw legacy-cutover stage --custody custody.sqlite \
  --baseline baseline.json --baseline-digest "$BASELINE_DIGEST"
coconutclaw legacy-cutover export-final --source frozen.sqlite \
  --baseline baseline.json --baseline-digest "$BASELINE_DIGEST" --output final.json
coconutclaw legacy-cutover import-final --custody custody.sqlite \
  --delta final.json --final-digest "$FINAL_DIGEST"
```

Pin the stable source ID, the original coordinator's public key, and the
printed BLAKE3 artifact digests through a trusted handoff. Only sources
without tracked remote assignments can omit the key. Protect the backups,
artifacts and custody database as private task data. The exporter leaves
scoped memory and native Codex session pointers in the legacy store.

The delta contains replacements for changed tables, with the original
logical tasks, runs, attempt ancestry, ingress, routes, schedule revisions,
occurrences, and outbox evidence. Both captures must contain the exact
Unknown record for task `#23`. The final export checks all three freeze and
seal markers and re-verifies the exact Start and terminal Result against
each reservation. Start-only or missing terminal evidence blocks export.
The importer verifies the signatures again against the pinned coordinator
and commits the final projection and signed bytes in one transaction. Its
acknowledgment identifies the source, baseline, final artifact, projection
and each signed receipt. Reopening the custody database and replaying the
exact artifact yields the same acknowledgment. The offline acknowledgment
records evidence custody only.

All archived work is held. Raw states such as `applying`, `running` and
`sending` retain their last durable evidence and represent ambiguous effects
until reconciled. `pending` ingress and delivery remain held. Schedule
watermarks and historical schedule rows remain inactive. Unknown schema,
unresolved legacy ingress checkpoints, runner claim journals, and untracked
remote reservations block this path. A receipt backfilled into the source
buffer after sealing can repeat terminal evidence already required by the
seal. Evidence changes require reconciliation.

The `frozen_final_delta` tests exercise the runtime's schema and a late signed
Unknown result from a disposable runner journal. They check precommit rollback,
postcommit acknowledgment replay, source byte preservation, and rejection of
missing, mismatched or corrupt authority and evidence.

On November 25, 2026, Leo-Claw passed nine focused tests, the offline CLI
rehearsal, formatting, workspace Clippy, and 590 workspace tests with five
ignored. `cargo mutants` bypassed `verify_receipts`. The tests caught the
bypass when the importer accepted corrupt evidence and the exporter accepted
an unpinned coordinator.

## Held gateway import

Use a disposable `phase2-local` SQLite ledger initialized by the loopback
Worker. Stop Miniflare and any fixture writers before import. Point `--ledger`
at the object database the Worker created. The importer requires the local
cutover schema and refuses a ledger with prior work or assigned authority.

```sh
coconutclaw legacy-cutover import-gateway-local --ledger "$LOCAL_LEDGER" \
  --baseline baseline.json --baseline-digest "$BASELINE_DIGEST" \
  --delta final.json --final-digest "$FINAL_DIGEST"
```

The command revalidates both pinned artifacts and every signed Start and
terminal Result. A single durable SQLite transaction installs inactive
generation 1, the final projection, the original artifacts and the exact
receipt bytes. A repeated command returns the same acknowledgment only when
the artifacts, projection, receipts and held authority still agree. The
archive is separate from the live queue. It preserves task `#23` as Unknown,
pending ingress, schedule watermarks and unresolved delivery evidence.
The Worker rejects requests and skips alarm work while the archive is present.
Keep the gateway database and the pinned artifacts as private task data.

Set `COCONUTCLAW_CUTOVER_CLI` to the freshly built `coconutclaw` binary for
the Miniflare integration test. It drives the CLI against a stopped object,
reopens the same SQLite store, checks the hold and compares the printed
acknowledgments across processes. The Rust fixture supplies genuine synthetic
runner signatures and checks a SQLite abort during receipt insertion, byte
preservation, conflicting replay and damaged custody.

On November 25, 2026, Leo-Claw passed formatting, workspace and Wasm Clippy,
and 591 workspace tests with five ignored. The disabled artifact passed 19
local tests and the separate pilot artifact passed six. Official `cf`
accepted both prebuilt artifacts in separate dry runs from
`cloudflare.config.ts`. The gateway import test caught the `cargo mutants`
variant that replaced receipt verification with `Ok(())`.

## Stale restore rehearsal

The loopback Worker requires an explicit, positive `LOCAL_GENERATION` binding.
A missing, malformed, zero or mismatched pin denies requests with
`503 inactive_generation` and prevents delivery alarm work. A ledger with a
legacy archive still returns `503 legacy_cutover_held` and skips alarm work.
The import test opens an independent pre-import backup without the archive.
None of the four unusable pin values admits the held ingress. The intact
archive retains the exact synthetic `#23` Unknown record, pending ingress,
empty live work tables and the same import acknowledgment on replay.

The generation test places its pin in a separate file outside the Worker and
executor snapshots. The fixture launcher reads it at each Worker startup.
A single fixture writer advances the pin from `1` to `2` using a flushed file,
atomic rename and directory sync. It stops the source, freezes it under the
existing SQLite authority row and advances a separate copy only after verifying
the original signed terminal evidence. It stops both Workers before publishing
the new pin, then opens the successor and the older active backup in independent
Miniflare stores. The executor journals are also independent. The successor
uses a fresh journal and synthetic signing key.

| Probe | Observed |
| --- | --- |
| Restore the generation 1 store with `active=1`, a pending outbox row and a claimed attempt | Authorized Telegram ingress, fixture cancellation and a freshly signed Start return `503 inactive_generation`. The inbox, request budget, agent sequence and attempt stay unchanged, with no launch deadline issued. |
| Wake its persisted, due delivery alarm | The alarm runs and writes zero ledger rows. The pending reply stays at zero sends with no receipt and no request to the mock Telegram endpoint. |
| Resume the separately restored generation 1 journal against the restored Worker | TLS Hello receives `503 inactive_generation` twice. The executor never reaches Start and adds no launch or journal evidence. |
| Point that journal at the successor with the predecessor signing key | The successor returns `401 unauthorized` on Hello with either generation. Even local adoption of generation 2 in the clean restored journal gives the old key no authority. The original journal remains byte-identical. |
| Admit update `216` at generation 2 | The new journal launches attempt `3` once. Its signed result is acknowledged and produces one mock Telegram receipt in topic `37`. Update `214` deduplicates and its outbox remains Unknown at zero sends. Attempt `2` remains Unknown with no grant. |

Run from `crates/coconutclaw-gateway` with the locally staged disabled bundle,
the existing cache and the disk check before each build.

```sh
GATEWAY_LOCAL_EXECUTOR="$PWD/../../target/native-gateway/debug/gateway-local-executor" \
COCONUTCLAW_CUTOVER_CLI="$PWD/../../target/native-gateway/debug/coconutclaw" \
  node --test --test-concurrency=1 \
  --test-name-pattern='stale restore|holds a legacy CLI import|synthetic cutover' integration.test.mjs
```

The baseline accepted update `202` on the pre-import backup when the pin was
missing. The regression now rejects it. A `cargo mutants` change from `||` to
`&&` in the alarm guard made the restored Worker send update `214`'s reply to
the mock endpoint. The stale alarm assertion caught the send.

On November 25, 2026, Leo-Claw passed 20 disabled-bundle tests, six
pilot-bundle tests and 591 workspace tests with five ignored. Formatting,
workspace Clippy, native executor Clippy and Wasm Clippy passed. Official `cf`
validated each bundle in a separate prebuilt dry run from `cloudflare.config.ts`.

The tested boundary is a stopped handoff with the current launcher pin and with
old workers and executors quiesced before the pin advances. Worker bindings
remain fixed for a running instance. Copies from the same generation share
the same eligibility, and restoring the pin or booting with old deployment
bindings can make the old ledger authoritative again unless startup and routing
exclude those copies. A restored executor also relies on trusted TLS routing
to the fenced gateway.

## Production Handoff Preflight

`authority-preflight.mjs` checks a proposed activation target against the
independently read current authority record for a stopped handoff. Run it
from `crates/coconutclaw-gateway` with Node 24.

```sh
node authority-preflight.mjs CURRENT_AUTHORITY.json CANDIDATE.json
```

The candidate identifies the exact `generation`, Git `revision`, Cloudflare
`deployment` (account/Worker/deployment/version), TLS `hostname`, SQLite DO
`objectId`, SHA-256 `custodyDigest` of the verified handoff manifest,
`telegramCredentialVersion` (an issuer or custody identifier), and
`executorKey` (the enrolled public key in hex). Collect candidate values from
the proposed artifact, routing and held ledger. The current authority record
has a `target` with those same fields and a `retirement` object with the
predecessor `generation` and four evidence references. Generation 1 uses
predecessor 0 for legacy-only authority. The command exits nonzero on missing,
malformed or conflicting data. Its success output reports the checked
generation and makes no runtime changes.

| Evidence reference | Required operator evidence |
| --- | --- |
| `writers` | Record the legacy freeze and terminal writer acknowledgments. Identify the account, Worker, deployment and version IDs, DO namespace/object, hosts/routes and bindings. Show that the old version has no traffic or callable route, including preview URLs, direct routes and service bindings, that old alarms and in-flight handlers are stopped or safely accounted for, and that restore and deploy principals cannot reactivate retired writers outside the handoff procedure. Verify the only destination for both webhook and executor TLS traffic is the chosen version and object. Record negative probes of the retired paths and controls. |
| `executorGrants` | Inventory every runner and outstanding grant. Preserve original signed terminal evidence, or prove exclusion of the old runner and expiry of its last possible launch authorization at a trusted clock, including skew. Keep possible prior launches Unknown until reconciled. Account for disconnected runners and downstream authorities, and verify the successor rejects predecessor keys and generation-scoped requests. |
| `telegram` | Record issuer-confirmed invalidation of the old Bot token for API and file access, exclusive custody of the replacement version, and verification of the exact HTTPS webhook and new webhook secret. Include intermediary access and pre-revocation requests or queued deliveries in reconciliation. |
| `custody` | Bind the frozen baseline, final delta, import acknowledgment, ledger identity and rechecked ingress/schedule watermarks to the candidate manifest digest. Record possible lost acknowledged updates and the disposition of each held attempt or delivery. Verify the original production `#23` record and preserve Unknown. Keep ambiguous work held for reconciliation. |

The checker compares identifiers and requires references. The operator must
authenticate and inspect the referenced evidence, including exclusion of any
older authority that could still act. The current record must come from a
single-writer control plane outside both restore domains with an independently
enforced high-water mark. Serialize publication and activation under that
control and recheck the current record immediately before opening traffic or
installing effect credentials. On any restore, including a same-generation
copy, leave routes and credentials closed until an independently authorized
higher generation completes the handoff. A restored copy of the current file,
a Worker binding, and a signed historical approval all lack freshness on
their own.

The loopback regression publishes the checked successor pin and rejects
startup of the restored predecessor with its old target. It also opens an
independent stale store under the current pin to exercise ingress, signed
Start, due delivery alarms and an independently restored executor journal.
Missing or malformed pins stop loopback ingress before DO forwarding and
stop the DO before ledger initialization on direct requests and alarms.
Requests for an unrecognized hostname or object are rejected. The checked-in
Cloudflare configuration keeps the cutover routes disabled and declares one
SQLite DO class. Production requires startup integration with the independent
record, the operator evidence above, and a separate end-to-end retirement and
restore drill under production routing.

Validate the disabled bundle from `crates/coconutclaw-gateway` after the disk
check and `npm run stage`.

```sh
GATEWAY_LOCAL_EXECUTOR="$PWD/../../target/native-gateway/debug/gateway-local-executor" \
COCONUTCLAW_CUTOVER_CLI="$PWD/../../target/native-gateway/debug/coconutclaw" \
  node --test --test-concurrency=1 authority-preflight.test.mjs config.test.mjs integration.test.mjs
CF_SEND_TELEMETRY=false cf deploy --prebuilt --dry-run
```

The cold-start test caught the `cargo mutants` replacement of `!=` with `==`
in `LocalPhase::authority_pin`. The mutation admitted update `41` without a
pin.

## BigFur Readiness

- Verify production Telegram ingress and same-topic Bot API delivery under
  separately authorized identities. Real Codex execution and cancellation
  require separate containment and lifecycle evidence.
- Reconcile the frozen production export across all live writer paths and
  verify `#23` directly from the authorized source. Gateway activation still
  requires a production authority transfer, a disposition for each held
  record and unresolved effect, and verification against the live gateway.
- Provision and verify the production authority and credential fence described
  in the stale restore rehearsal across independent stores, executor journals
  and deployments. A custody acknowledgment applies to the ledger that
  committed it. A disconnected legacy runner with an unresolved `Begin`
  blocks the seal until it supplies the original signed terminal evidence or
  a separately proven retirement mechanism.
- Rehearse live failback. Fence gateway writers and reconcile ingress
  cursors, schedule watermarks, signed executor evidence and unsettled sends
  before resuming legacy consumption. Record versions, recovery time and the
  acknowledged-update loss window.
