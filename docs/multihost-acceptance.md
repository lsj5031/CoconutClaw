# Disposable multihost acceptance

Historical development fixture, starting 16 November 2026. Its independent
review was **DEFERRED, NOT APPROVED**. Its receipts apply only to this fixture;
for ordinary Telegram task routing, see [paired execution nodes](production-nodes.md)
and the separate [live fleet record](fleet-verification.md).

The runnable role executable is the CLI's native **test harness**, launched only
at the exact ignored `multihost_role_entry`, not the normal `nodes` CLI.
`scripts/multihost-acceptance.py` builds, prepares,
launches and controls it on Linux/macOS using an existing Python 3 installation.
Never run the whole ignored suite with credentials.

## Build inputs and host preflight

Use the parent-provided source/build-input archive and its SHA256/manifest.
Archives exclude `.git`, artifacts, target, live configuration, keys, auth and
sessions. A separate vendored archive, when supplied, contains only the already
cached, lock-pinned upstream build sources and Cargo source replacement config.
Do not install tools or fetch dependencies on a cache miss. Record the exact
missing crate/tool instead. The pinned macOS graph declares Rust at most 1.91;
Mitori's 1.94.1 meets that declared requirement. Only an actual Mac build proves
target compatibility.

First staging actually built offline on Mitori 1.94.1 and the same arm64 binary
SHA256 was verified on Mars. Its first coordinator launch failed before ready or
any model call because `kern.hostuuid` is unsupported. The corrected build uses
the built-in `ioreg -rd1 -c IOPlatformExpertDevice` and validates exactly one
well-formed, non-placeholder `IOPlatformUUID`, hashing it without logging raw
hardware identifiers. Linux identity behavior is unchanged. Parser regression
coverage is not a substitute for a new actual Mac launch.

The corrected actual1201 launch reached Mars native Codex and produced the
correct nonce/sum42 fixture, exit0 and Terra/max/never context. Application
acceptance still failed Unknown: native0.167 recorded `custom_tool_call` with
`input` and a paired `custom_tool_call_output`; the scanner recognized only
`function_call`/`arguments`. Both forms now require a matching, same-kind output
`call_id` and an actual execution call referencing the fixture. File, nonce,
host, session, context and integrity checks are retained. Finish refusals persist
bounded safe phase/details in runner evidence and its task error; coordinator
failed receipts include only those safe failures, not arbitrary SDK bodies.
Failed1201 remains Unknown, never retry it. Parent receipts12:55 now prove the
separately fresh `cc-real-multihost-20261116-hc1250`: original Store Completed,
138-byte nonce/sum42 artifact, native session, one invocation, exact duplicate,
full two-role restart and identical signed event/artifact replay. Both roles
stopped. Parent built the initial HostCodex correction bundle; later packaging
adds the read-only diagnostic's serial-stdout parser regression and documentation,
not a different native result algorithm. No extra model invocation is needed
merely for those test/diagnostic/documentation changes.

For a stopped preserved runner root, the newly built native executable also
provides an explicit **read-only** diagnostic using the same BLAKE3 routine:

```sh
/usr/bin/python3 "$SCRIPT" integrity --root /tmp/cc-runner-1201 \
  --executable /tmp/cc-multihost-role-hostcodex-fixed
```

This reads the existing assignment1 integrity record, hashes shared config in
place and stats auth metadata, without reading auth contents, invoking Codex,
opening SQLite, creating keys/ports or writing the root/config/auth. It returns
config hashes and equality booleans, not config or credentials. Comparing the
stored **BLAKE3** hash to SHA256 is invalid; the old parent's SHA256 comparison
does not establish a config change. Parent's correct read-only diagnostic now
confirms failed1201 config/auth integrity unchanged; the fresh successful
fixture's integrity also remained unchanged. This does not establish broader
containment or runtime activation.

Mitori:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
rustc --version
cargo --version
/usr/bin/clang --version
/usr/bin/python3 --version
cd /tmp/cc-multihost-source
/usr/bin/python3 scripts/multihost-acceptance.py build --output /tmp/cc-multihost-role
shasum -a 256 /tmp/cc-multihost-role
```

The parent transfers that arm64 Mac executable and launcher to Mars, verifies the
same SHA256 there, and records actual host/node identities. Do not transfer any
role root, device key, auth file, native session state or live config.
Mitori can fetch the authorized source bundle through its existing `claw` SSH;
no gateway-to-Mac SSH is assumed.

## Explicit transport and real provider profile

The parent must supply either:

* actual mutually reachable host interface IPs, using `--bind "$HOST_IP:0"`, or
* one **explicit**, reachable relay URL on both roles, using
  `--bind 0.0.0.0:0 --relay-url "$RELAY_URL"`.

There is no default/public relay or address discovery. Test reachability from
both hosts, preserve errors, and do not infer it from loopback or model access.
The coordinator owns three ephemeral UDP endpoints (publisher, local mirror,
control client); the runner owns two (scoped records, control server). Ready files
contain their actual bound/pinned endpoints. Relay mode selects the declared
relay route explicitly. No firewall, tunnel or unrelated service changes.

On Mars, the parent creates a **nonsecret** profile outside the source archive:

```json
{
  "executable": "/Users/liu/.local/bin/codex",
  "python": "/usr/bin/python3",
  "auth_json": "/Users/liu/.codex/auth.json",
  "shared_config": "/Users/liu/.codex/config.toml",
  "provider_name": "EXISTING_NONSECRET_PROVIDER_NAME",
  "base_url": "EXISTING_NONSECRET_BASE_URL",
  "wire_api": "responses"
}
```

Obtain routing from the existing selected provider's safe projection, never
print/copy bearer tokens. URLs containing userinfo, query or fragments are
refused. This finite native route supports the existing local API-key
`auth.json` layout with `OPENAI_API_KEY`; unsupported auth layouts fail closed,
without login, copying credentials, fallback providers or automatic retries.
The wrapper reads that host-local file in place, gives the key to the native CLI
in memory, filters unrelated environment credentials, and uses a disposable
`CODEX_HOME`. Tool environment inheritance and external project instructions are
disabled. Shared config bytes and auth size/mtime are checked unchanged.
Configured auth is not proof of a successful real request.

## Launch and exchange only public ready files

Choose one shared random alphanumeric/hyphen nonce of at least 16 characters,
for example `RUN_ID=$(/usr/bin/uuidgen)`. Use short, fresh absolute roots under
`/tmp`, not the source checkout or an existing workspace. Unix socket paths must
fit the platform limit. The commands below use direct IPs; append the same
explicit `--relay-url "$RELAY_URL"` to each prepare command for relay mode.

Mitori, coordinator:

```sh
SCRIPT=/tmp/cc-multihost-source/scripts/multihost-acceptance.py
CORE=/tmp/cc-mh-core
/usr/bin/python3 "$SCRIPT" prepare --root "$CORE" --role coordinator \
  --run-id "$RUN_ID" --bind "$MITORI_IP:0"
/usr/bin/python3 "$SCRIPT" launch --root "$CORE" --executable /tmp/cc-multihost-role
```

Inspect its returned log and fresh `ready.json`. The parent transfers **only**
that public ready file to `/tmp/cc-core-ready.json` on Mars.

Mars, runner:

```sh
SCRIPT=/tmp/multihost-acceptance.py
RUNNER=/tmp/cc-mh-runner
/usr/bin/python3 "$SCRIPT" prepare --root "$RUNNER" --role runner \
  --run-id "$RUN_ID" --bind "$MARS_IP:0" \
  --core-ready /tmp/cc-core-ready.json --provider-profile /tmp/cc-mars-provider.json
/usr/bin/python3 "$SCRIPT" launch --root "$RUNNER" --executable /tmp/cc-multihost-role
```

The parent transfers the runner's fresh public `ready.json` to
`/tmp/cc-runner-ready.json` on Mitori.

Mitori:

```sh
/usr/bin/python3 "$SCRIPT" control --root "$CORE" run \
  --runner-ready /tmp/cc-runner-ready.json
```

Run that blocking control command through an owned background execution if the
parent tool needs it. There is no short real-provider total deadline; existing
activity/pending-tool limits remain. Separate `status` and `cancel` commands stay
available while work runs. Do not use `--allow-same-host` for remote acceptance;
it labels local regression receipts `multihost:false` and `fixture_only:true`.

The coordinator checks distinct machine-identity hashes and hostnames. It mints
only disposable fixture policy; role keys remain host-local. Both roles derive
the public synthetic namespace from the run nonce, not by transferring a private
namespace/device credential. Owner-signed endpoint/author policy and signed
origin still gate the restricted protocol. Native docs/blob capabilities remain
unavailable.

Assignment and Start/Result reach the actual remote scoped peer. Only after
durable Start and execution claim does the existing provider API invoke Codex.
The real tool reads the nonce/17/25 fixture and writes nonce/sum42, actual tool
PIDs and host. The remote actor signs a task-bound artifact manifest, publishes
the exact file through scoped iroh, then signs the current-head Result. The
coordinator persists both Completed and the verified artifact bytes in its
original SQLite Store. No parent-computed result or hash-only output fallback.

## Stop, restart and collect

First collect the coordinator's receipt and exact event hashes. Then:

```sh
/usr/bin/python3 "$SCRIPT" control --root "$CORE" stop
```

That requests runner cleanup, joins provider work, closes only owned endpoints
and acknowledges after coordinator cleanup. Verify both `stopped.json` files and
role process exits. Relaunch **both existing roots**, without `prepare`, using
`launch` on each host. Exchange their **new** ready files, then:

```sh
/usr/bin/python3 "$SCRIPT" control --root "$CORE" replay \
  --runner-ready /tmp/cc-runner-ready.json
/usr/bin/python3 "$SCRIPT" control --root "$CORE" stop
```

Replay must preserve the three exact signed event hashes, original Completed,
artifact bytes and one durable invocation; re-admission and Begin are refused.
Do not rerun `run` to repair an ambiguous outcome. Interruption recovers Unknown,
never new invocation permission. Preserve failed receipts/logs and use a new
owned nonce/root only for a separately explicit fresh fixture attempt.

Collect **selected files only**, never archive whole role roots:

* coordinator: `ready*.json`, `launch-*.json`, `receipt.json` (copy before replay),
  `origin.json`, `policy.json`, `signed-events.json`, exact `fixture-output.json`,
  `provider-evidence.json`, `stopped*.json`, role logs;
* runner: corresponding public ready/launch/stopped receipts and role logs,
  `fixture-input.json`, `workspace/1/fixture-output.json`,
  `provider-1/launch.json`, the public `integrity` diagnostic receipt,
  `provider-1/scoped-native-events.json`;
* actual native binary SHA256, source/archive manifests, host identities,
  command exits and before/after source/receipt hashes.

Never collect `*-key`, role configuration/profile, auth or whole `codex-home`.
The original Store remains under the coordinator's `source` directory;
`acceptance_artifact` binds assignment to exact bytes/manifest.

The native wrapper journals its actual launcher PID/PGID before starting the
installed CLI. A kernel Unix-socket parent lease triggers cleanup of that live
owned group after abrupt runner death, without killing stale journal PIDs.
The retained regression proves this for an owned blocked child, not every
possible native tool's independently created process group. Real abrupt-death
containment/session recovery, operational custody/admission and activation
remain separate gates. No multihost success is claimed without distinct-host
network, model/tool, signed artifact, Store and restart receipts.

## Controlled three-host preparation, 17 November

Gateway plus Mars and Mitori means **three hosts, two remote Mac hosts**, not
three remote runner nodes. At preparation, execution awaited parent orchestration;
the actual outcomes and subsequent repair checkpoints are recorded below.
Old stopped roots and old Mac binaries are historical evidence, not current proof.
The source-pinned parent handoff supplies fresh roots, manifests and commands.

There is a concrete limit, not a new product routing feature: `adapter_binding`
fixes one origin and session per Store; RuntimeDriver holds one RunnerClient;
remote Begin admits only assignment1 and an origin with exactly that runner.
Do **not** point two independent role pairs at the same Store or claim separate
Stores prove a shared-core multi-runner scheduler. The bounded stage exercises
concurrent independent original-Store routes and the following existing
application boundaries, not the requested shared-Store/two-topic case.

`profile` projects only nonsecret routing/paths from each host's existing config.
If its selected provider uses an auth command, the isolated native callback reads
and executes that original command in place, with its original CODEX_HOME.
Otherwise the existing API-key wrapper route is used. No auth arguments, tokens
or files are copied. Command-auth projection/callback requires an **already
installed Python with tomllib**; report absence rather than install or fall back.
Both routes use existing Terra/max/never and isolated native state.

```sh
python3 "$SCRIPT" profile --output "$HOST_PROFILE" \
  --executable "$HOST_CODEX" --python "$EXISTING_PYTHON" \
  --auth-json "$HOST_AUTH" --shared-config "$HOST_CONFIG" --controlled
```

Prepare/launch as above, exchanging only public ready files. `control check
--runner-ready ...` initializes real pinned iroh transport and both Stores but
allocates no task/invocation. At initialization/reconnect and before each runner
record import, a public nonce exercises both scoped-data directions. The gateway initiates
the first data connection as well as control, matching observed LAN reachability;
this is not a relay/firewall workaround or a successful remote proof by itself.
Require `scoped_data_roundtrip:true` from check. Stop both roles and relaunch the **same fresh**
roots with new ready files before running. Never use allow-same-host remotely.

The controlled prompt admits only six workspace files. Its one native Python
tool publishes nonce/PID/parent/PGID/hostname/start time then waits on owned
release/cancel events, bounded at600 seconds. No arbitrary delay is a task
input. On each runner:

```sh
python3 "$SCRIPT" fixture --root "$RUNNER" status
python3 "$SCRIPT" fixture --root "$RUNNER" release
```

Observe both actual tool-started records **before** either release to prove
overlap by causal gates, not synchronized host wall clocks. Hold the first
admitted route while releasing the other; native result files add actual
start/finish timestamps. The initial preparation budget was two jobs plus one
separately fresh cancellation job, never rerunning an uncertain assignment.

`control run-cut --runner-ready ...` uses the ordinary RuntimeDriver acceptance
path but returns a deliberate failed receipt with `owned_cut:true` after the
runner's signed result, verified artifact and coordinator publication/target
result are durable, **before** mirror transfer and original-Store acknowledgment.
The original source remains running at this cut. It is not an external delivery
receipt or proof of power-loss fsync behavior. Then:

```sh
python3 "$SCRIPT" control --root "$CORE" detach
# Core has exited; runner stays alive. Relaunch ONLY that core, same executable.
python3 "$SCRIPT" launch --root "$CORE" --executable "$ROLE"
python3 "$SCRIPT" control --root "$CORE" replay --runner-ready "$RUNNER_READY"
```

Exact origin/input/core-data reconnect is idempotent; altered bootstrap refuses.
Reconciliation uses durable proof, not Begin. Missing evidence retains existing
Unknown behavior. Normal `stop` still stops the runner; detach is allowed only
at this explicit cut. Never delete locks/sockets to manufacture restart.

`control run-cancel` admits a separately fresh controlled job. After observing
its actual tool start, `control cancel` persists cancellation through the same
core/runner path and places a cooperative cancel event in its owned workspace.
Require an actual cancelled receipt and actual PID/PGID disappearance; otherwise
retain failure/Unknown and hand off, never assume cleanup from a status string.
No parent sends signals to unrelated or merely recorded stale PIDs. Another
Mac's held job may then complete independently. `replay-cancel` validates the
cancelled original Store without invocation. Real cancellation/descendant
coverage remains pending until those node-side observations are collected.

Each settled/replayed route exercises three typed tampered-proposal fields at
the actual `accept_proposal` boundary. `proposal.json` exports exact public signed
bytes. Transfer a completed route's public proposal to another coordinator:

```sh
python3 "$SCRIPT" control --root "$OTHER_CORE" probe --proposal "$FOREIGN_PROPOSAL"
```

Require rejection plus unchanged history/tasks/invocations/**original outbox**.
Same-route exact bytes are Duplicate, not rejection. Deterministic existing
tests cover stale epoch/head branches; do not claim a live owner-epoch migration.
These role fixtures do not execute an external sender: unchanged delivered/outbox
state is not generalized external exactly-once or nonoccurrence evidence.

New private integrity baselines hash auth bytes in place as well as config bytes,
size/mtime/ctime/inode/device/mode. **Do not transfer integrity-before.json**,
auth/config/session directories, role keys or whole roots. Collect only the
read-only diagnostic's equality booleans/config hashes and public receipts.
Do not compare SHA256 to BLAKE3. Old baseline receipts remain untouched.

### Historical1140 gate findings and1154 remediation, 17 November

Both Macs built source da9ca9f7 successfully. The old missing-cache blocker is
resolved, not a current restriction. Three real invocations were consumed.
Control/scoped-data preflights and independent-route native overlap succeeded,
but A/B source completion failed schema validation; cancellation C failed its
transport. All roles are stopped, original roots/DBs retained unchanged.
Neither shared-original-Store concurrency nor remote acceptance is passed.

Ordinary artifacts have exactly five bound fields. Controlled artifacts have
exactly seven, additionally positive integer nanosecond start/finish timestamps
in nondecreasing order. Both native production and receiving validators enforce
the selected fixture schema, hash, size, nonce, sum, host and PID bindings.

Signed cancellation intents are generic scoped records, NOT domain events.
Only their exact typed prefix enters generic receipt, followed by the same
canonical/core-signature/origin/assignment-hash/source-run/runner/attempt verifier
used by cancellation projection. Domain-event receipt remains strict. A remote
cancel requires that verified intent in custody. Cancellation does not add a
domain-history event. Failed cancellation custody stops event allocation until
explicit reconciliation; it is not permission for another provider invocation.

The deterministic native fixture exercises actual control/iroh transport,
cancelled exit130 without an output artifact, one invocation, process death,
full role restart and exact signed cancellation replay. Missing successful
output remains an artifact error, never fabricated completion/cancellation.
The parent's manual cancel-file fallback was cleanup only, not cancellation
success. Original A/B remain running and C cancel_requested.

That repair required new native builds and did not authorize further real calls.
The later owner authorization below supersedes that budget restriction.
Read-only public artifacts can be schema-checked, but the public
evidence bundle alone lacks the signed proposal/result and full original domain
binding needed to prove safe original-Store replay. Retained-state recovery needs
separate verified evidence collection/authorization; never rewrite original DBs
or claim deterministic local repair as a fresh Mac acceptance.

### Actual1224 success evidence and1233 repair checkpoint

Both Macs verified and built b6293d06 source. Fresh real A/B routes succeeded:
B was held while A finished; A's runner remained alive across coordinator-only
restart after the signed-result-before-original-ACK cut, then replay completed
the original task with one invocation. B completed with exact duplicate receipt,
then full coordinator/runner restart replay preserved Completed and one invocation.
Cross-route probes rejected with unchanged histories/outboxes/invocations. These
successes are retained, not rerun or downgraded because cancellation C failed.
They prove independent-route overlap, not shared-original-Store scheduling.

C's signed cancellation failed at the runner's scoped-record fetch. The
`remote runner refused: deadline has elapsed` error is the existing10-second
per-fetch timeout, not an expired24-hour policy or a61-second tool deadline.
Its failure response was24.145 seconds after tool start; cleanup ended the tool
61.478 seconds after start. Parent's cancel-file fallback remains cleanup only.
Original C remains unreconciled and must never be re-admitted.

The initial data preflight did not guarantee a long-lived reverse UDP path after
native startup/tool wait. An isolated real-iroh regression retains assignment,
scope, authors and keys while losing the old data path. A loopback UDP gate
models the observed gateway-initiated path requirement, with no host firewall,
clock or configuration change. Before repair the signed cancellation import
failed with the same deadline after10.047 seconds and eight dropped packets.
This demonstrates the failure mechanism; it is not a historical packet capture
or proof of which LAN device expired the path.

Immediately before each sole record-import attempt, the gateway now performs
the existing authenticated scoped-data roundtrip, gateway first. It uses the
pinned current runner data address. No retry, policy renewal, increased timeout
or new provider admission is added. Transport failure still requires explicit
reconciliation. The native cancellation fixture holds a separate tool group past
10 seconds, then checks prompt signed cancellation, no output, native/tool/
wrapper/group death, durable Cancelled, full restart and exact duplicate replay.

The own-route probe's bytes were valid. A post-restart harness readmission-
refusal check entered the driver's mutation phase before rejecting already-
settled work, leaving ready=false. The earlier internal duplicate ran before
that check, explaining the apparently inconsistent receipts. Admission now
rejects absent/started/settled assignments before clearing readiness; ambiguous
mutation/publication failures remain fail-closed. The real launcher/validator
regression proves own duplicate after full restart, plus foreign, tampered,
noncanonical and validly signed stale-attempt rejection with unchanged history,
task count, invocations and original outbox. Probe diagnostics expose the actual
rejection cause instead of conflating readiness failures with signature rejection.

The owner's12:18:46 Pacific/Auckland authorization permits further harmless
live calls and supersedes the older three-call limit. Parent alone orchestrates
fresh disposable acceptance; this worker initiates no remote provider calls,
never retries originals, changes no auth/config/security, and holds commit.
At that checkpoint, fresh native rebuilds and actual C cancellation/probe
acceptance remained required; the completed evidence is recorded below.
Independent review is DEFERRED NOT APPROVED. Shared-original-
Store concurrency remains CONSTRAINED: removing the singleton Store origin/
session binding and single RunnerClient/assignment1 fixture limitation requires
separate route-aware admission, custody and reconciliation implementation.

### Completed parent acceptance and local finalization, 17 November

The parent's `live-remediation-20261117-1233/parent-live-acceptance.json`
attributes both successful native builds to the exact231-file source manifest
`cc8f8bbfc82374e047c906105fe889c175c2934a7d05bf4a419deb3bd168fc1b`.
Mars binary SHA256 was
`d4b1701365976f4e645672f4436e3f543ab2c136b0a90522787f2ff3e8c94fb1`;
Mitori's was
`267b627a8ff0f653474c7631976773970e99e688fe637d6efec1b54f225a3363`.

Fresh C2 (`cc3h-C2-20261117-1316`) used actual iroh and one real provider
invocation. Signed cancellation persisted Cancelled in original source run2,
produced no output artifact, and refused re-admission. The same-host cancellation
marker-to-tool-end interval was **0.050103794 seconds**, not a cross-host clock
comparison. `fresh-C2-run.json` and `fresh-C2-full-replay.json` retain the exact
same three event hashes, one invocation and Cancelled across full coordinator/
runner restart. Own-route duplicate passed again after restart.

`fresh-C2-probe-summary.json` records positive own duplicate before and after
each foreign/binding, noncanonical and tampered-signature rejection, all with
unchanged history/tasks/invocations/original outbox. This is not a live owner-
epoch migration or generalized external-delivery exactly-once claim.

The public Mars/gateway cleanup receipts and old-B18779 absence receipt confirm
all owned old/new role, native, tool, wrapper PIDs/groups absent. Mars's read-only
integrity receipt reports auth bytes/config/metadata unchanged; prior A/B
integrity and success evidence is preserved. Original C remains unresolved,
never re-admitted. The earlier cc8f-C detached gateway launcher died before tool
start and remains a harness failure, not a product pass; C2 used durable tmux.

This finalization changes only this acceptance document. Implementation, tests,
launcher and formal inputs retain the accepted cc8f bytes. Existing498/0/5-
ignored Rust, strict Clippy/fmt/build/doctest, launcher/checker and12 affected
formal checks are reused on that identity, not rerun for a documentation edit.
The collected acceptance authorizes the bounded local task commit, not network
publication. Archived/read-only publication remains blocked, hosted native OS CI
is pending, shared-original-Store concurrency remains CONSTRAINED with the
route-aware gate above, and independent review remains DEFERRED NOT APPROVED.
