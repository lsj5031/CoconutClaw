# Desktop validation, 20 November 2026

## Desktop controls

The packaged GTK companion ran on an isolated Xvfb display under the same account as its disposable runner and coordinator. The real file picker selected a public card from a path with spaces. Preview displayed the public fields and digest while the pairing table stayed empty. The fixture changed the selected file before pressing **Confirm pairing**. SQLite then contained the original reviewed identity and public fields.

The window started the runner and received readiness from its retained child. The existing coordinator CLI observed authenticated reachability after reciprocal CLI admission. The window stopped the runner through the inherited pipe, observed successful exit and verified the instance lock was available. An independent lock acquisition succeeded. A later failed coordinator probe retained the earlier contact timestamp. The executable Rust fixture also exercises foreground Ctrl+C on Linux, the inherited pipe, duplicate launch rejection, reconnection and lock release.

Local package archives, native accessibility captures, screenshots, build logs and coordinator exports are under `.desktop-attempt/functional-host/`. Native Windows operator acceptance and staging remain open. The Windows x64 companion ZIP was assembled on Linux with the pinned NSH runner. Closing the companion and stop failure handling must also be checked on the acceptance host. [Operator rollout gates](README.md#windows-rollout-gates)

## Earlier pairing and reachability follow-up

On Linux, the workspace format check and Clippy passed. The locked workspace suites passed 580 tests with five ignored. The executable fixture confirmed reviewed card bytes, private-root rejection, idempotent reimport and exactly one admission across conflicting concurrent confirmations. An isolated coordinator and runner exercised authenticated refusal before reciprocal pairing, reachability, disconnection, preservation of the last successful contact, reconnection and refresh by export. The retained fixture child handled Ctrl+C, exited cleanly and released the instance lock.

NSH 0.6.0 passed its type check and three headless tests. Substituting the last contact time for a failed probe's check time in a disposable copy failed the reachability test. The packaged Linux app launched and opened and cancelled the real GTK public-card picker on isolated X11. Accepted and rejected picker results were exercised headlessly.

## Initial package

Validated on Ubuntu 24.04.5 LTS x86-64. The companion uses TSX with NSH's native GTK and Win32 hosts and has no app-level package dependencies. NSH CLI and both prebuilt runners came from the same official v0.6.0 release, commit `554163758e30d2f62f814096a6327937dcb4fe49`. The CLI reported `nsh 0.6.0 (5541637)`. The release CLI pins TypeScript 7.0.2 and verifies its platform tarball against the integrity in its source. The compiler cache, Cargo output and extracted archives were isolated under `.desktop-attempt/`. Spotish informed the small NSH app/package layout and demo approach. [NSH release](https://github.com/longbridge/nsh/releases/tag/v0.6.0) · [Compiler pin](https://github.com/longbridge/nsh/blob/554163758e30d2f62f814096a6327937dcb4fe49/crates/nsh-cli/src/check.rs) · [Spotish reference](https://github.com/huacnlee/spotish/tree/a8d88114991b24bcff68286c55a6ccb8e38f3756)

## Reproduce the package and demo

Commands below run from the repository root on Linux x86-64. Extract the two official archives into the directories shown here. The hashes were checked before extraction.

```text
19d360eb8dd1ba8847a3b3ad06082f6f3705eb04ae8178357a8cdad31880f0d9  nsh-linux-x86_64.tar.gz
6ae2e430d84cfe5fcb1d2ab25e56c2685eaaa4abc963c93b52263f435791ebb1  nsh-windows-x86_64.zip
```

```sh
attempt="$PWD/.desktop-attempt"
mkdir -p "$attempt/nsh-linux" "$attempt/nsh-windows"
tar -xzf nsh-linux-x86_64.tar.gz -C "$attempt/nsh-linux"
python3 -m zipfile -e nsh-windows-x86_64.zip "$attempt/nsh-windows"
export NSH_CACHE="$attempt/nsh-cache"
nsh="$attempt/nsh-linux/nsh/bin/nsh"
test "$("$nsh" --version)" = 'nsh 0.6.0 (5541637)'
"$nsh" check desktop
"$nsh" test desktop
"$nsh" package desktop --target linux -o "$attempt/linux-package"
NSH_RUNTIME="$attempt/nsh-windows/nsh/runtime" \
  "$nsh" package desktop --target windows -o "$attempt/windows-package"
python3 -m zipfile -t "$attempt/windows-package/CoconutClaw.zip"
file "$attempt/windows-package/CoconutClaw/CoconutClaw.exe"
"$attempt/linux-package/CoconutClaw/CoconutClaw"
```

On native Windows x64 use `nsh/bin/nsh.exe` from that same pinned Windows archive. In PowerShell, run `& $Nsh --version` and compare to the version above. From the checkout root, run `& $Nsh run desktop --target windows` for the demo or `& $Nsh package desktop --target windows -o .desktop-attempt/windows-native` to package it. Keep the EXE and its adjacent `nsh` directory together when launching from the ZIP. [NSH Windows packaging](https://nsh.dev/docs/windows/#packaging)

For the two checked captures, NSH ran on GTK with an isolated, authenticated Xvfb display. The host compositor had a zero-size XWayland root and returned stale or transparent frames, so the captures came from the Xvfb run. Xvfb was extracted locally from Ubuntu's `xvfb_21.1.12-1ubuntu1.9_amd64.deb`, SHA-256 `2011271eabcc2eb460e99995c5ef26e46633e87d04f570e305b62f178f7b452c`. Run the following inside an isolated X11 display. [Ubuntu package](https://archive.ubuntu.com/ubuntu/pool/universe/x/xorg-server/xvfb_21.1.12-1ubuntu1.9_amd64.deb)

```sh
GDK_BACKEND=x11 GSK_RENDERER=cairo "$nsh" test desktop/App.tsx --target linux --foreground <<'JS'
await app.setViewport(820, 560);
expect(app.texts()).toContain("Reachable at last check");
await app.screenshot("docs/desktop/nodes-linux.png");
await app.press("Jobs");
expect(app.texts()).toContain("Unknown");
expect(app.texts()).toContain("No verified start");
await app.screenshot("docs/desktop/jobs-linux.png");
JS
```

## Test receipt

| Command or check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed. |
| `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` | Passed with Rust 1.98.1. |
| `cargo test --workspace --all-targets --locked --offline` | 578 passed, five ignored by the suites. Existing local mocks exercised signed evidence. An added assertion verifies that saved reachability and assignment coexist with Unknown and that late verified evidence preserves Unknown. |
| `coconutclaw desktop --database <synthetic database> --session 'telegram:101#4'` | Exit zero with a valid empty v1 export. Rust tests exercised exact topic selection and the read-only open path. |
| `nsh check desktop` and `nsh test desktop` | Type check passed. Both tests passed, including the mocked picker import/cancel/rejection and independent state fields. Two manual mutations in disposable copies were detected by the tests. |
| `nsh test desktop/App.tsx --target linux --foreground` | Passed the native GTK steps above. Both 820 × 560 PNGs were visually checked. |
| `nsh package desktop --target windows` | Produced portable x64 ZIP and Inno Setup script on Linux using the verified Windows release runner. ZIP CRC check passed. The packaged GUI PE32+ matches the release runner byte for byte. The package has the compiled bundle, the expected window sizes and the picker capability. |

The Rust commands used two build jobs, disabled incremental compilation and debug info, and reused the supplied offline [Linux linker shim](../offline-toolchain.md). NSH logged that its default status bar and application APIs were denied. The app needs only the user-selected file capability.

## Checked artifacts

| File | SHA-256 |
| --- | --- |
| [Windows ZIP](../../.desktop-attempt/windows-package/CoconutClaw.zip) | `42dac6a105ce2c90e75ce1fa82f98a79de0f7bf9414006920f6671f7ede24895` |
| Windows EXE inside ZIP | `c7c3ee00452c85287570cb82e4d596728a9a6558a3a72b00ae7b5f395a84eb83` |
| Windows compiled bundle | `ed768406c68e3e984991250186b4012ed4ad3b4547a9e91b10559557b10ae068` |
| [Inno Setup script](../../.desktop-attempt/windows-package/CoconutClaw.iss) | `b2f4913e1512c1c28df5f0b95d2b4d32f608d29deacf215b2ccfa0de902eb836` |
| [Linux TAR.GZ](../../.desktop-attempt/linux-package/CoconutClaw.tar.gz) | `1e408d05da8b5820afb7c9df9ab872c2fbde3f5ddd8a139f4445be9ce4a2be23` |
| [Native GTK Nodes](nodes-linux.png) | `a68962bdc54e4523bd1900cb5a52e115074a0ca17f4510fb49270d442de62c7b` |
| [Native GTK Jobs](jobs-linux.png) | `7f789144f15ec088848316d90238895bbf4f800cb3256d3d44a6fa34f6aaab59` |

Package bytes and full logs are local checkout artifacts. The screenshots are tracked. Native Windows launch, DPI, keyboard/UI Automation, picker and packaging on Windows remain open. Inno Setup 6 and Authenticode signing were unavailable here, so the ZIP is an unsigned portable package and there is no setup EXE. Native Windows pairing, local runner lifecycle and staging verification remain gated. [Operator rollout gates](README.md#windows-rollout-gates)
