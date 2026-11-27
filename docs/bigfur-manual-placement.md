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

1. The production router accepts the `production-v1` pairing marker under its
   existing webhook, private owner and topic guards. It resolves the published
   aliases inside the inbox transaction and persists the task pin and reply topic.
   New paired admission requires an active registry. The exact locally recorded
   update ID can be acknowledged during a Control outage or after retirement.
   Runner health is independent. Singleton production queues plain text on `alpha`.
   [Gateway ingress](../crates/coconutclaw-gateway/src/worker_app.rs)
2. Control publishes `production-v1` with `fresh-object-only` continuity in the
   existing generation ledger. A paired successor requires verified frozen inbox
   IDs from its BigFur predecessors. Loopback tests cover predecessor redelivery,
   cold wake during Control outage, retired predecessor restore, runner retirement at
   the Start permit boundary, and one same-topic dispatch. Recovery requires a
   fresh object and independently verified Control continuity.
   [Control runbook](gateway-authority-control.md)
3. The `--production` runner uses the pairing receipt for Hello, request
   signatures, claim and Start checks, and evidence. It checks fresh Status
   before consuming a Start grant. Its journal binds the enrollment, signing
   key and original lock file identity. The runner rejects a copied journal
   directory. The original journal resumes with its stored signatures.
   The macOS production integration uses a synthetic provider and mocked
   Telegram to exercise copied journals, mismatched placement, lost Start and result
   acknowledgments, both kinds of cancellation, abrupt loss, and retired
   journal reconciliation. Unknown produces zero deliveries or new launches.
   A full clone with the current exportable seed and enrollment cannot be
   distinguished on the wire. Host-bound identity would be required to make
   that distinction.
4. Native synthetic loopback custody was accepted on macOS, Linux and Windows
   in the base candidate. The production entry point ran on macOS and was
   cross-checked for Linux and Windows. A separately
   authorized rollout still needs native host admission with host-owned
   credentials and workspace, verified Control and Telegram custody, provenance
   for predecessor inbox exports, and same-topic Telegram acceptance.

| Host | Native custody path |
| --- | --- |
| Linux Leo-Claw | The production runner uses an owned pidfd, subreaper and subtree drain through `owned_codex.py`. [Linux runner](gateway-linux-runner.md) |
| macOS Mars | The guardian watches its runner parent with `kqueue`, holds the journal lock and contains Codex in its process group. Host custody still needs a policy for tools that create their own process group or session. |
| Native Windows Mercury | The production runner uses the private ACL and reparse-point checks and launches native Codex directly inside a noninheritable kill-on-close Job Object. [Windows custody](windows-onboarding.md) |

## Production runner setup

Use `coconutclaw nodes enroll --role runner` as in the fixture and give the
public card to the Control operator. Redeem their one-use code against the
published HTTPS Control origin. Keep the seed, code file and receipt in
owner-only storage. On Windows use the private ACL-checked root.

```sh
umask 077
node pair-runner-local.mjs redeem-production "$control_origin" "$seed" "$code_file" > "$receipt"
gateway-local-executor --production init "$private_config" "$journal"
```

The private runner JSON requires `origin` (the Worker HTTPS origin),
`signing_key` (the 32-byte seed path), `enrollment` (the receipt path),
`codex` (an absolute executable path), `codex_home`, `workspace`, and
`timeout_seconds` (1 through 3600). Local installations can supply `ca`
and an explicit `provider_env` allowlist. Keep configuration and signing
material outside the journal, workspace and Codex home. Use `resume` with
the original receipt and journal after restart. A retired runner drains
eligible signed evidence under its original enrollment and exits without
requesting new work.

If the pairing response is lost, repeat `redeem-production` with the same
seed and code file to recover the committed receipt.
