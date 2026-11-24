# BigFur Linux runner

Build `gateway-local-executor` with the gateway's `local-executor` Cargo feature.
Run it in the foreground under a dedicated service account on Leo-Claw. The
Linux supervisor uses `/usr/bin/python3` (3.10 or newer) with pidfd support, `/proc`, and
Linux child subreaping. Provision a private Codex home with the scoped provider
and its injector-backed authentication command. Use the absolute path to the
native Codex executable.

Place `runner.json`, the raw 32-byte Ed25519 signing seed, and the independently
enrolled 64-character lowercase hex incarnation outside the journal, workspace
and Codex home. Give the service account ownership and mode `0600` on each
file. Exclude the incarnation from backups and logs. Enroll the verification
key and incarnation with the independent authority operator. This runner's
placement is `alpha`. Cutover ingress pins its admitted tasks to `alpha`, and
the cutover path excludes unpinned tasks. The Worker still applies the existing
fixture ingress identity and request budget.

```json
{
  "origin": "https://bigfur.example.invalid",
  "generation": 2,
  "object_id": "<64 lowercase hex digits>",
  "placement": "alpha",
  "signing_key": "/private/runner/ed25519.seed",
  "incarnation": "/private/runner/incarnation",
  "codex": "/absolute/path/to/codex",
  "codex_home": "/private/runner/codex-home",
  "workspace": "/absolute/path/to/assigned/workspace",
  "timeout_seconds": 300
}
```

An optional `ca` PEM path replaces the default TLS trust roots for this
connection. An optional `provider_env` list passes only the named variables
to the Codex supervisor. The runner connects directly over HTTPS and rejects
redirects. Codex uses workspace-write, disabled approvals, and a fresh
ephemeral session with the prompt on stdin.

```sh
gateway-local-executor --production init /private/runner/runner.json /durable/runner/journal
gateway-local-executor --production resume /private/runner/runner.json /durable/runner/journal
```

Use `init` for a new enrollment and `resume` for a restart. The journal holds
the generation and verification key, keeps its request sequence, and permits
one consecutive generation change only after all local attempts are sealed
and their evidence acknowledged. Provision the successor's private config
and credentials through the control host handoff. Check clock alignment with
the authority host because permits last at most two seconds.

The runner consumes the exact Start grant before the supervisor can launch.
A new status for that attempt must authorize the original deadline. The
supervisor checks both wall and monotonic deadlines at spawn. Cancellation
requires a fresh request-specific permit and signals the owned Codex PID via
a pidfd. The supervisor signs off on cancellation only after observing the
signal exit, reaping Codex, and draining its descendants, including those
that changed process groups. The prior provider helper waits on the leader
and signals its process group. The Linux supervisor adds parent-loss cleanup
and subtree exit proof. It retains the journal lock through cleanup and
drains the subtree on runner disconnect or local timeout. Use a service
manager that kills the service cgroup on stop to cover simultaneous loss of
the runner and its supervisor. After ambiguous interruption, the journal
reports Unknown and never relaunches that attempt.

## Local receipts

On November 25, 2026 the signed Worker path ran installed `codex-cli 0.170.0`
in a private scratch workspace. The test supplied `myprovider` and
`gpt-6.2-sol` through an isolated Codex config, with a local test auth command
and a loopback Responses server. It verified the Ed25519 receipts against
`bigfur:<generation>:<objectId>` and waited for their acknowledgment.

| Attempt | Receipt | Process observation |
| --- | --- | --- |
| 1 | `started`, `result` | One native Responses request and the expected bounded reply |
| 2 | `started`, `cancelled/process_exited` | The live Codex PID disappeared and its held HTTP stream closed |
| Resume | Same two signed Start requests total | Zero relaunches and zero additional Responses requests |

A separate executable fixture made exactly three starts after nine Start
requests. Mismatches in the grant sequence, instance, attempt or original
deadline, a lost grant response and an expired spawn permit each produced
Unknown with zero launches for those attempts. Expired cancel status left
the leader and detached descendant alive. Fresh cancel status produced
`process_exited` after both PIDs disappeared. Killing the runner during
the final attempt closed its lease and both owned processes disappeared.
The independently copied journal with the predecessor incarnation was
denied. The original journal resumed and acknowledged Unknown with no
additional Start.

The focused integration filter passed eight tests, including the native
runner, installed Codex, fixture recovery and authority regressions. The
authority control, preflight and configuration files passed eight more tests.
The production runner test caught both `cargo-mutants` replacements of `||`
with `&&` in the grant identity checks. The workspace suite passed 591 tests
with five ignored.

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked --workspace --all-targets -- -D warnings
cargo test --offline --locked --workspace --all-targets -- --test-threads=4
cargo build --offline --locked -p coconutclaw-gateway --features local-executor --bin gateway-local-executor
cargo clippy --offline --locked -p coconutclaw-gateway --features local-executor --all-targets -- -D warnings
cargo clippy --offline --locked -p coconutclaw-gateway --target wasm32-unknown-unknown -- -D warnings
npm --prefix crates/coconutclaw-gateway run stage
export GATEWAY_LOCAL_EXECUTOR="$PWD/target/native-gateway/debug/gateway-local-executor"
export COCONUTCLAW_CUTOVER_CLI="$PWD/target/native-gateway/debug/coconutclaw"
export GATEWAY_TEST_CODEX=/absolute/path/to/native/codex
node --test --test-concurrency=1 --test-name-pattern='BigFur|native executor|stale restore|late signed terminal' \
  crates/coconutclaw-gateway/integration.test.mjs
node --test --test-concurrency=1 crates/coconutclaw-gateway/{authority-control,authority-preflight,config}.test.mjs
```

The scoped injector route to the actual model remains unverified. The local
route used a synthetic auth command. The installed private provider
configuration pointed to a remote endpoint and was outside this run's
local-only, credential-free scope. Leo-Claw service operation and the
operator-witnessed same-topic Telegram acceptance remain open. The latter
requires production ingress identities and budgets, live authority and
credential custody, an activated ledger with the verified frozen archive
and attempt floor, and the separately authorized handoff, restore and
failback evidence. Live task `#23` remains Unknown.
