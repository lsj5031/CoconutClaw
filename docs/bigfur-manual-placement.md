# BigFur manual placement

## Local fixture

Set `PHASE=phase1-placement-local` in Miniflare with the fixture's webhook
and operator secrets and distinct synthetic Ed25519 public keys in
`DEVICE_ALPHA_KEY`, `DEVICE_BETA_KEY` and `DEVICE_GAMMA_KEY`. The existing
loopback HTTPS `/v1/telegram`, `/v1/agent` and `/v1/fixture` routes use the
`local-placement-fixture` object and signature domain. Telegram-shaped
updates require the synthetic private chat `321`, thread `37`, sender `321`
and an explicit `is_bot: false`. Fixture delivery uses the mocked Bot API.

| Prefix | Fixture device |
| --- | --- |
| `@leo-claw` | `alpha` |
| `@mars` | `beta` |
| `@mercury` | `gamma` |

Set `LOCAL_PLACEMENT_ALIASES=mac=mars,win=mercury` to add selectors. Aliases
use lowercase ASCII letters, digits and hyphens. An unknown target,
malformed entry or duplicate selector, including a canonical name, closes
admission until corrected. Omit the binding to use the three canonical names.
Put whitespace and a non-command coding prompt after the prefix. An unknown
or malformed leading `@` is ignored. A message without a selector keeps its
original prompt and pins to `alpha`.

The inbox transaction records the update ID, original reply topic, prompt
and device. Its `telegram:<update_id>` task keeps that device across alias
changes, redelivery, runner absence and claim expiry. A lease that expires
before Start can be reclaimed by the same device. A disconnected authorized
attempt can become Unknown and is never automatically reclaimed. Start,
result and cancellation evidence use the existing signed fixture commands
and the current attempt's device. The fixture's operator cancellation can
also cancel work while its device is absent. Scheduling is outside this mode.

With the gateway's pinned Node dependencies and wasm-bindgen installed, run
the isolated gateway build and focused tests in `crates/coconutclaw-gateway`:

```sh
npm run stage
node --test --test-concurrency=1 \
  --test-name-pattern='manual placement|webhook admission|signed handshake' \
  integration.test.mjs
```

## Control enrollment fixture

`phase2-placement-local` uses the scoped Telegram ingress and the independent
Control service on loopback. The Worker pin carries `executorKeys` by placement
and the Control publication carries `runnerIncarnations` for the same placements.
Both maps require `alpha` and distinct values. Control pins the observed object
incarnation and requires a fresh object and runner incarnations for a successor.
Ingress resolves selectors against the pinned keys without querying Control.
Each admitted task keeps its original placement and reply topic.

An agent supplies `x-executor-placement` and `x-executor-incarnation`. The signed
request and evidence bind the object generation, placement, and incarnation.
Control permits echo that placement and the exact target, including its keys.
Start returns the placement, attempt, request sequence, and original deadline.
The final check requires the same authorized attempt, task pin, and deadline.
An object restart closes this fixture's authority until a successor is published.
The production singleton keeps its `executorKey`, `runnerIncarnation`, and
original signature domains.

## Fresh-install fixture for administrators

Start with an empty, private fixture directory and `PHASE=phase2-pairing-local`.
Use the synthetic webhook secret and owner/topic scope from the test fixture.
The Worker pin uses hostname `127.0.0.1`, generation `1`, the ID of
`paired-placement-fixture:1`, and `"pairing": "fixture-v1"` in place of
`executorKeys`. The pin and version stay fixed for the life of the fixture.

Start Control with `createPairingAuthority` from `authority-control.mjs` and a
mock Telegram sender. Observe the Worker object's incarnation through an exact,
authenticated Control check and publish the installation with that incarnation.
Keep the Control handle and its bearer with the administrator. Review the
runner's public card, including its role and revision. Call
`authority.issue({ key: card.id })` for the first runner, or
`authority.issue({ key: card.id, alias: "desk" })` for a later runner. Supply the
returned code privately to that runner. Codes expire after five minutes.
Aliases are unique lowercase ASCII letters, digits and hyphens, up to 32
characters. Control records the redeemed code and enrollment in one write.

The first redeemed runner becomes the default. Send `@desk` followed by a
prompt to select that runner. The selected placement stays with the task.
An operator POST to `/v1/status` with `{"task":"telegram:45203"}` and the fixture
operator header reports runner presence and attempt phase separately. A runner
with no signed activity in the last 15 seconds reads **Offline runner**.
**Unknown attempt outcome** is the durable outcome of an uncertain attempt.
The runner does not relaunch it automatically.

The walkthrough starts a fresh Miniflare Worker, Control database and runner
journals. It builds cards with `nodes enroll` and redeems each through Control.
From the repository root, build the binaries and stage the Worker.

```sh
cargo build --locked -p coconutclaw
cargo build --locked -p coconutclaw-gateway --features local-executor --bin gateway-local-executor
npm --prefix crates/coconutclaw-gateway run stage
COCONUTCLAW_CUTOVER_CLI="$PWD/target/debug/coconutclaw" \
GATEWAY_LOCAL_EXECUTOR="$PWD/target/debug/gateway-local-executor" \
  node --test --test-concurrency=1 --test-name-pattern='BigFur fresh install' \
    crates/coconutclaw-gateway/integration.test.mjs
```

## Fresh-install fixture for runners

Use `nodes enroll` with a fresh, private fixture data directory and share only
`runner-card.json` with the administrator. Put their one-time code in a private
`pairing-code` file and use the loopback Control endpoint they provide.
Run this from `crates/coconutclaw-gateway` with `$fixture_data` and
`$fixture_workspace` pointing to owned disposable directories.

```sh
umask 077
coconutclaw --data-dir "$fixture_data" nodes enroll \
  --role runner --bind 127.0.0.1:49731 --workspace "$fixture_workspace" > runner-card.json
seed="$fixture_data/runtime/nodes/device-key"
node pair-runner-local.mjs redeem "$control_endpoint" "$seed" pairing-code > enrollment.json
```

Use the `generation`, `objectId`, `placement` and `incarnation` from the
receipt with `--enrollment` below. Keep the seed and original receipt to resume
the same journal. The fixture uses the administrator's loopback Worker origin
and disposable CA. A spent code cannot return another receipt.

## Native runner fixture

Build `gateway-local-executor` with `--features local-executor`. Supply the
synthetic values from the Control publication to `--loopback-fixture`.

```sh
gateway-local-executor --loopback-fixture init "$origin" "$ca" "$seed" "$journal" \
  --enrollment "$generation" "$object_id" "$placement" "$incarnation"
```

Each runner starts with a separate private journal. `resume` requires the same
generation, object ID, placement, incarnation, and public key. Use a fresh
journal for a new enrollment. The runner replays stored signatures verbatim
in their original domain.

The runner checks the Hello device and claim placement. Before consuming a
Start grant, it checks the instance, attempt, request sequence, placement, and
original deadline against the reply and a fresh status under the same signed
scope. The wire binds incarnation through the signed requests and Control
checks. An authorized attempt with a lost Start reply, or a consumed attempt
without a durable result, reconciles as Unknown without retrying the effect.

With the staged gateway and the native binary path in `GATEWAY_LOCAL_EXECUTOR`,
run the focused tests from `crates/coconutclaw-gateway`.

```sh
node --test --test-concurrency=1 \
  --test-name-pattern='native enrolled|native executor|enrolled placement binds' \
  integration.test.mjs
```

Host custody in Gate 4 requires the Linux supervisor, macOS parent-loss, and
Windows ACL and Job Object checks on their respective hosts. Production
placement also depends on the retirement and delivery-custody proof in Gate 2.

On Linux, an enrolled loopback runner uses the same pidfd and subreaper
supervisor as the production runner. The supervisor holds the journal lock
until it finishes draining the provider tree. The enrolled integration test
observes a live synthetic provider and a sleep descendant in a separate
session, then requires both PIDs to disappear after cancellation and after a
separate runner `SIGKILL`. Recovery must acknowledge signed Unknown from the
original enrollment, with one Start, one launch and zero deliveries for the
lost attempt.

On Mars, the enrolled loopback fixture runs the synthetic shell under a
guardian in the provider's dedicated process group. The guardian binds
`kqueue` exit notification to its current runner parent before launching the
shell and kills its own group on runner exit or provider cancellation.
The macOS custody integration test observes the shell and its sleeping child
alive, checks their exit after cancellation and after a separate runner
`SIGKILL`, then checks signed Unknown and one launch on journal recovery.
This check covers the fixture's group members. Live macOS acceptance still
requires private host credentials and workspace, separately authorized
enrollment, and a custody policy for tools that create their own groups or
sessions. Run the Linux enrolled custody test on Leo-Claw for native validation.
Windows accepts `--loopback-fixture` with an explicit `--enrollment`. Use
absolute paths on a Windows filesystem that supports ACLs. Put the synthetic
seed, CA and journal in disposable directories with the private DACL described
in [Windows onboarding](windows-onboarding.md). The runner checks those paths
for reparse points and checks the DACLs before reading the seed or opening the
journal, including existing lock, database and SQLite sidecars. `init` creates
the journal directory under the private parent. `resume` keeps the original
enrollment and signatures.

The Windows fixture starts its built-in synthetic provider inside the native
runner's noninheritable kill-on-close Job Object. Native tests retain process
handles for the provider and its descendant, check that both exit after
cancellation or abrupt runner termination, and verify signed Unknown after
resuming the original journal. The lost attempt has one Start, one launch,
and zero deliveries. Run the PowerShell fixture checks in
[Windows onboarding](windows-onboarding.md#bigfur-enrolled-loopback-fixture)
on Mercury to validate this behavior.

## BigFur integration gates

1. Scope production admission to its existing webhook, private owner and
   topic check. Resolve aliases against a published set of enrolled placements
   in the same transaction that records the immutable pin. Keep plain text
   on `alpha` for the legacy generation. Prove duplicate updates, alias changes,
   offline admission, and same-topic replies without consulting runner health
   at admission. The production handler currently pins every admitted task to
   `alpha`. [Gateway ingress](../crates/coconutclaw-gateway/src/worker_app.rs)
2. Carry the fixture's per-placement Control enrollment into a separately
   authorized production generation. Define independent runner retirement,
   including who can reconcile old evidence after retirement. Prove that copied
   or retired journals cannot acquire permits. Verify cold wake, predecessor
   fencing, exclusive delivery custody and stale-restore behavior for the
   production object lifecycle before opening the scope.
   [Control publication](../crates/coconutclaw-gateway/authority-control.mjs) ·
   [Worker permit verification](../crates/coconutclaw-gateway/src/runtime_authority.rs)
3. Teach the native runner to use its enrolled placement for Hello, request
   signatures, claim checks, grant consumption, and evidence. Bind each journal
   to the exact enrollment and keep preexisting signed evidence verifiable in
   its original domain. Exercise cross-placement replay, lost Start and result
   acknowledgments, both kinds of cancellation, restart and Unknown without
   effect retry through the native runner.
   [Wire](../crates/coconutclaw-gateway/src/executor_wire.rs) ·
   [Journal](../crates/coconutclaw-gateway/src/executor/journal.rs)
4. Prove native custody on each host with host-owned credentials, workspace
   and separate journal. Use synthetic prompts and processes for integration
   tests. Native same-topic Telegram acceptance and real host enrollment
   require a separately authorized run.

| Host | Native custody path |
| --- | --- |
| Linux Leo-Claw | BigFur's Linux-only `gateway-local-executor --production` uses an owned pidfd, subreaper and subtree drain through `owned_codex.py`. Keep service-account config, the private signing seed and incarnation outside the journal, workspace and Codex home. Prove grant consumption, process-exit cancellation and parent-loss cleanup per enrollment. [Linux runner](gateway-linux-runner.md) |
| macOS Mars | The existing native paired `coconutclaw nodes start` path keeps an owner-only `nodes` directory, a host-local claim Store and local Codex auth. The Unix provider owns a process group. Port or reuse these custody mechanisms under the BigFur grant and journal, and prove cancellation and abrupt runner loss on macOS. [Paired nodes](production-nodes.md) |
| Native Windows Mercury | The native paired `coconutclaw.exe nodes start` path uses a private ACL-checked `%LOCALAPPDATA%\CoconutClaw-native-runner` root with local key and SQLite journal. Its provider starts inside a noninheritable kill-on-close Job Object. Integrate native ACL and Job custody into the BigFur runner and run Windows tests for reparse points, unsafe ACLs, descendant exit and abrupt owner loss. [Windows custody](windows-onboarding.md) |
