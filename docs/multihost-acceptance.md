# Disposable multihost acceptance

Development only. Review **DEFERRED, NOT APPROVED**. Phase 2 remains incomplete;
this does not activate networking or execution in the production executable.

The runnable role executable is the CLI's native **test harness**, launched only
at the exact ignored `multihost_role_entry`. No normal CLI networking flag or
sync dependency is introduced. `scripts/multihost-acceptance.py` builds, prepares,
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
  `provider-1/launch.json`, `provider-1/integrity-before.json`,
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
