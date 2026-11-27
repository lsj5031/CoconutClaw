# Native Windows runner, PowerShell

Use the installed native Rust/C linker, locked offline dependencies and native
Codex already present. **No OpenClaw runner, extra OpenClaw setup or copied
authentication is needed.** This guide does not run or authenticate Codex during
build/enrollment. Codex auth remains under the existing Windows account.

This is an operator checklist, not evidence of manual fleet acceptance. Parent
must review the local candidate, publish it, collect exact-head Linux/macOS/
Windows CI, and authorize staging first. Do not change the accepted Linux
service or start a second Telegram poller. Windows is a runner for the existing
primary coordinator, not a second coordinator.

## 1. Build and record identity

In native PowerShell, edit only the nonsecret path/revision parameters:

```powershell
$ErrorActionPreference = 'Stop'
$Source = 'C:\path\to\reviewed-source'
$ExpectedRevision = '<parent-reviewed-full-40-character-sha>'
$CodexExe = 'C:\path\to\existing\codex.exe'
$Workspace = 'C:\path\to\dedicated-permitted-workspace'
$NodeData = Join-Path $env:LOCALAPPDATA 'CoconutClaw-native-runner'
$WinIP = '<Windows-reachable-LAN-IP>'
$WinBind = "${WinIP}:49732"
$CoreCard = Join-Path $env:TEMP 'coconutclaw-primary-public.json'
$WinCard = Join-Path $env:TEMP 'coconutclaw-windows-public.json'
if ($ExpectedRevision -notmatch '^[0-9a-f]{40}$') { throw 'Set the reviewed source SHA' }
if (-not (Test-Path -LiteralPath $CodexExe -PathType Leaf)) { throw 'Use the existing native executable path' }
if (-not (Test-Path -LiteralPath $Workspace -PathType Container)) { throw 'Use the permitted existing workspace' }
Set-Location -LiteralPath $Source
$env:CARGO_BUILD_JOBS = '2'
$env:CARGO_INCREMENTAL = '0'
# For a Git checkout, both checks must succeed and status must be empty:
$ActualRevision = (& git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $ActualRevision -ne $ExpectedRevision) { throw 'Source revision mismatch' }
$DirtySource = & git status --porcelain
if ($LASTEXITCODE -ne 0 -or $DirtySource) { throw 'Source must be clean' }
& cargo build --workspace --locked --offline
if ($LASTEXITCODE -ne 0) { throw 'Build failed; retain error, do not fetch/install' }
$Bin = Join-Path $Source 'target\debug\coconutclaw.exe'
Get-FileHash -LiteralPath $Bin -Algorithm SHA256
```

If an existing `CARGO_TARGET_DIR` is set, derive `$Bin` from that directory's
`debug\coconutclaw.exe` instead. For a verified Git-less source archive, replace
only the two Git checks with verification of the parent's archive/manifest hash
and set `$env:COCONUTCLAW_SOURCE_REVISION = $ExpectedRevision` before the build.
Never label unverified bytes with a source override. No internet installation is
part of these steps.

Run platform tests before operational acceptance:

```powershell
& cargo test -p coconutclaw-provider --locked --offline windows::tests
if ($LASTEXITCODE -ne 0) { throw 'Job custody tests failed' }
& cargo test -p coconutclaw-config --locked --offline windows_security::tests
if ($LASTEXITCODE -ne 0) { throw 'ACL tests failed' }
& cargo test -p coconutclaw --locked --offline nodes::tests
if ($LASTEXITCODE -ne 0) { throw 'Claim/replay/restart tests failed' }
& cargo test -p coconutclaw --locked --offline --test nodes_cli
if ($LASTEXITCODE -ne 0) { throw 'Actual CLI admission/ACL tests failed' }
```

These use owned native mocked processes, local iroh and test roots, never the
operator's Codex auth or Telegram. They assert original process-handle exit for
detached descendants after cancellation/timeout/normal exit and abrupt runner
owner death, plus shutdown and a fresh process afterward. ACL tests accept a
proper private DACL and reject broad access and null DACLs. Durable claims
restart as Unknown and replay cannot invoke again.

## 2. Create a fresh private root, enroll once

Do this only for a **new** dedicated runner root, not an existing identity.
All commands below are native PowerShell/Windows executables:

```powershell
if (Test-Path -LiteralPath $NodeData) { throw 'Preserve existing state; choose a fresh staging root' }
$null = New-Item -ItemType Directory -Path $NodeData
$AccountSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
& icacls.exe $NodeData /inheritance:r /grant:r "*$($AccountSid):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F'
if ($LASTEXITCODE -ne 0) { throw 'Private root ACL setup failed' }
# This is only an executable selection, not authentication:
$env:CODEX_BIN = '"' + $CodexExe + '"'
function Invoke-Node {
    & $Bin --data-dir $NodeData @args
    if ($LASTEXITCODE -ne 0) { throw 'Node command failed; preserve state' }
}
$CardText = Invoke-Node nodes enroll --role runner --bind $WinBind --workspace $Workspace
# PowerShell 5.1 ">" writes UTF-16; public cards require UTF-8 without a BOM.
[System.IO.File]::WriteAllText($WinCard, ($CardText -join "`n"), [System.Text.UTF8Encoding]::new($false))
Invoke-Node nodes card
Invoke-Node nodes status
```

Use the same Windows account and a local ACL-capable filesystem. The program
rejects reparse points, null/foreign/unsupported ACEs and noninheritable private
directory ACLs. Node commands inspect `config.toml` before using its settings
or creating layout. If `SQLITE_DB_PATH` points outside the default directory,
prepare its parent as a private directory first. The pairing database, its
parent, existing sidecars and the `tmp` output directory are checked before
runner admission.
It also validates temporary key-file custody **before** writing key bytes.
Never make a root public to fix an error. Never repair/adopt potentially exposed
existing identity state by blindly resetting its ACL, deleting its key or
resetting claims. Preserve it for parent-controlled recovery.

## 3. Primary connection and immutable pairing

Parent must run the **same candidate revision** on the primary and all paired
Mac/Windows nodes. Existing admission cards from another revision are rejected.
Do not delete old roots or evidence to bypass this; parent owns fresh staging
and any upgrade/migration decision.

The coordinator card must identify role `coordinator`, the exact revision and
its actual fixed reachable LAN UDP address (for example port 49731); Windows
advertises its actual fixed UDP 49732 address. Pinned direct iroh requires UDP
reachability between these two addresses, in both directions. A hostname,
successful SSH session or public card alone is not proof of that path. No relay,
discovery, new firewall rule or tunnel is configured automatically.

Exchange **only** `$WinCard` and `$CoreCard` through the parent's trusted tooling.
Independently verify IDs, roles, full source revision and addresses on both
hosts. Cards contain no private key or authentication. They are not short-lived
pair secrets. Never copy data roots, device-key, config, auth, sessions or SQLite.

Windows PowerShell, after the primary public card arrives as UTF-8:

```powershell
Invoke-Node nodes pair $CoreCard
Invoke-Node nodes status
```

The runner accepts exactly one pinned coordinator. Pairing conflicts preserve
the original admission; there is no implicit replacement or rotation.

**Parent's Linux primary side only, not Windows commands:** stop polling before
these administrative commands and use the primary's actual data root/binary:

```sh
"$CORE_BIN" --data-dir "$CORE_DATA" nodes pair "$WINDOWS_PUBLIC_CARD"
"$CORE_BIN" --data-dir "$CORE_DATA" nodes status --probe
```

No PowerShell `ssh` wrapper or OpenClaw endpoint is required. The parent owns
coordinator enrollment/upgrade and the subsequent single normal `run` process.

## 4. Start, observe, restart, cancel

Windows PowerShell, same account and `$env:CODEX_BIN` as above:

```powershell
Invoke-Node nodes start
```

This is foreground runner service, not polling. Wait for `Runner ready`.
Use another native PowerShell terminal with the same `$Bin`/`$NodeData`:

```powershell
& $Bin --data-dir $NodeData nodes status
if ($LASTEXITCODE -ne 0) { throw 'Status failed' }
```

Do not use `nodes status --probe` on the runner: that is a coordinator
administrative probe and must not compete with the identity-owning process.
Persisted contact is an observation, not a heartbeat guarantee. Parent probes
from the primary, then uses Home → Nodes → the Windows ID for a new ordinary
authenticated text task. Confirm it executes in the permitted Windows workspace
and that only the primary delivers the literal result. No injected ingress,
duplicate poller or direct API messaging is part of acceptance.

For a graceful restart, press **Ctrl+C** in the foreground runner terminal and
wait for the command to exit. It cancels owned workers, terminates their jobs
and joins them. Then repeat `Invoke-Node nodes start` with the **same** data root.
Do not enroll or pair again. Existing unfinished claims become Unknown and are
never reinvoked.

For a separately authorized **abrupt-death** acceptance test, first stop the
foreground runner. This PowerShell block retains the exact owned process:

```powershell
$Runner = Start-Process -FilePath $Bin -ArgumentList @('--data-dir', "`"$NodeData`"", 'nodes', 'start') -NoNewWindow -PassThru
# After Runner ready and the parent's explicitly owned in-flight test:
Stop-Process -Id $Runner.Id
$Runner.WaitForExit()
# Preserve all state; restarting does not resubmit the interrupted assignment.
Invoke-Node nodes start
```

Killing the runner is **not** a successful task cancellation receipt. Kernel
kill-on-close custody terminates its provider job descendants; signed/durable
effect status remains Unknown where completion cannot be established.

**Task cancel/reconcile belongs to the primary**, because RUN_ID and the signed
assignment live there. There is deliberately no runner-side `nodes cancel`
substitute. While the Windows runner remains up, the user may use the normal
authenticated task Cancel button. Alternatively parent stops primary polling
and runs the following **on Linux, not in Windows PowerShell**:

```sh
"$CORE_BIN" --data-dir "$CORE_DATA" nodes cancel RUN_ID
"$CORE_BIN" --data-dir "$CORE_DATA" nodes reconcile RUN_ID
```

Then observe on Windows with native `nodes status` as above. If cancellation was
offline, restart the runner with its original root and have the parent repeat
the same cancel/reconcile, never Begin or a resubmitted task. Late Completed
evidence cannot erase Unknown or authorize another provider invocation.

## 5. Manual acceptance receipt, distinct from CI

User relays nonsecret host OS/architecture, full revision, binary/source hashes,
public card identities, command exits, primary probe observation, actual native
provider invocation count, workspace selection, ordinary task delivery, and
before/during/late cancellation, disconnect/reconnect and full runner/primary
restart observations. A claim count is not an invocation count. Record exact
signed evidence hashes/replay outcome, not request/output bodies or raw Stores.
Observe only the fixture's owned process handles, never broad process-command
line dumps containing provider prompts. The mocked native custody tests above
provide repeatable descendant assertions without inspecting real credentials.

Native CI is required but is not manual authenticated Codex/Telegram fleet
acceptance. No service installation is implied. Parent owns Linux upgrade,
Mac transport/staging, exclusive primary polling and final fleet release.

## BigFur enrolled loopback fixture

For BigFur `gateway-local-executor --production`, the private `runner.json`
can set `"codex_sandbox": "danger-full-access"` to launch Codex without
its sandbox, with the Windows runner account's privileges. Approvals remain
disabled and Codex runs in the runner's Job Object. To roll back, stop the
runner, remove the field or set it to `"workspace-write"`, then resume with
the same receipt and journal. See [BigFur production setup](bigfur-manual-placement.md#production-runner-setup).

From a disposable checkout on native Windows, use the reviewed source revision
and the pre-provisioned Node, OpenSSL, wasm-bindgen and locked dependencies.
The test creates its own private roots, seeds, CA, journals and mocked Telegram
delivery ledger. The sandbox fixture launches a native Node stand-in for Codex.
The other executor fixtures use a synthetic provider built into the binary.

```powershell
$ErrorActionPreference = 'Stop'
function Invoke-FixtureCommand($Program) {
    & $Program @args
    if ($LASTEXITCODE -ne 0) { throw "$Program failed ($LASTEXITCODE)" }
}
Invoke-FixtureCommand git rev-parse HEAD
Invoke-FixtureCommand rustc -vV
Invoke-FixtureCommand node -p 'process.platform+String.fromCharCode(32)+process.arch'
Invoke-FixtureCommand cargo test --locked --offline -p coconutclaw-config windows_security::tests
Invoke-FixtureCommand cargo test --locked --offline -p coconutclaw-provider windows::tests
$Gateway = 'crates/coconutclaw-gateway'
Invoke-FixtureCommand npm --prefix $Gateway ci --no-audit --no-fund
$FixtureTarget = Join-Path (Get-Location) "$Gateway/build/native"
Invoke-FixtureCommand cargo build --locked --offline -p coconutclaw-gateway --features local-executor --bin gateway-local-executor --target-dir $FixtureTarget
Invoke-FixtureCommand cargo build --locked --offline --release -p coconutclaw-gateway --target wasm32-unknown-unknown --target-dir "$Gateway/build/target"
Invoke-FixtureCommand wasm-bindgen --target web --no-typescript --out-dir "$Gateway/build/worker" "$Gateway/build/target/wasm32-unknown-unknown/release/coconutclaw_gateway.wasm"
Invoke-FixtureCommand node "$Gateway/stage.mjs"
$env:GATEWAY_LOCAL_EXECUTOR = Join-Path $FixtureTarget 'debug/gateway-local-executor.exe'
Get-FileHash -LiteralPath $env:GATEWAY_LOCAL_EXECUTOR -Algorithm SHA256
Invoke-FixtureCommand node --test --test-concurrency=1 --test-name-pattern='BigFur native production runner passes the local Codex sandbox mode' "$Gateway/integration.test.mjs"
Invoke-FixtureCommand node --test --test-concurrency=1 --test-name-pattern='native enrolled' "$Gateway/integration.test.mjs"
```

Record the source revision, OS, architecture, executable hash and test results.
Require the sandbox fixture and both `native enrolled Windows` tests to pass.
The ACL and Job Object results require execution on Windows.
