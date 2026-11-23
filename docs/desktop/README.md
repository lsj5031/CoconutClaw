# CoconutClaw desktop companion

The NSH window starts with a synthetic fleet. Runner `51c2a740` was reachable at its last saved check. Run `1042` was assigned to that runner and is still **Unknown**, with no verified start or result. **Delivered** refers to the status message. Run `1041` has verified signed start and result records while its delivery is pending.

[Initial Nodes screenshot](nodes-linux.png) · [Initial Jobs screenshot](jobs-linux.png) · [Build and validation receipt](validation.md)

## Operator access

The existing coordinator owns admission, task routing, result custody and delivery. A coordinator operator can explicitly export one conversation or topic from the existing local SQLite file. The OS account's database access authorizes this export. The conversation argument selects rows and confers no chat or fleet permissions.

```sh
coconutclaw desktop --database /path/to/coordinator/state.db \
  --session 'telegram:123#4' > snapshot.json
```

Use the exact conversation ID, for example `telegram:123` for the chat, `telegram:123#4` for one topic, or `local:default` for a local scope. Run the export on the coordinator. It uses SQLite read-only mode and a single read transaction over the initialized store, with SQLite's usual WAL/SHM access requirements. It can run alongside the coordinator. **Open snapshot** reads the selected JSON through the native picker and holds it in memory until another file is opened or the app closes. Transfer the export only to an authorized operator. It contains public runner IDs, observation times, run IDs and status metadata. Prompts, output, memory, tokens, paths, addresses, session IDs and raw receipts are excluded.

Nodes shows up to 32 paired runners. The latest explicit probe includes its UTC check time and whether the paired runner returned an authenticated response. A failed probe preserves the last successful contact. Older exports may lack the check time. Jobs shows up to 50 current attempts for the selected scope in the coordinator's status order. Assigned computer is the frozen route. Verified evidence means the trusted coordinator recorded both signed events after checking the history. Start-only observations are not retained by the existing coordinator adapter. Snapshot files are operator-trusted exports and the viewer does not authenticate the file. Refresh means exporting and opening a new file.

Version 2 exports add **Routing** and **Schedules**. Routing shows where new Telegram text tasks go in the exported chat or topic. Schedules shows up to 50 durable schedules in the same order as Telegram, with saved state, planned time, cadence and policies, and any recorded run blocker. Local conversation exports include their own schedules. Version 1 files still open and report that routing and schedules were unavailable in that export. Cancelling the picker or selecting an invalid file preserves the open snapshot.

### Open local runner controls

Launch through the reviewed absolute CoconutClaw CLI, under the account that enrolled the runner. Use its existing protected data root and the absolute executable from the pinned NSH companion package. Keep the adjacent `nsh` directory with that executable.

~~~sh
"$BIN" --data-dir "$RUNNER_DATA" desktop-host --companion "$COMPANION"
~~~

~~~powershell
& $Bin --data-dir $RunnerData desktop-host --companion $Companion
~~~

The launcher uses the CLI it was started with and the operator's current environment. It stages the companion's bundle for this launch and gives NSH filesystem access to two disposable directories for local requests and replies. The native adapter accepts public card bytes, an explicit confirmation digest, Start and Stop. The launcher verifies the existing runner root and identity before accepting local actions. Opening the packaged app directly provides snapshot viewing and public-card review.

### Review and confirm pairing

In the launched companion, choose **Review public card** and select the coordinator's UTF-8 card. The CLI preview validates the role, identity, fixed UDP address and exact source revision for this runner. Compare these fields independently with the coordinator operator. **Confirm pairing** passes the held bytes and digest back to the reviewed CLI. Changing or replacing the selected file after preview leaves the held bytes intact. An identical import is idempotent, and a conflicting second coordinator leaves the first admission pinned.

The coordinator operator admits the runner's card independently using the coordinator CLI. The reviewed CLI pairing path is also available for foreground operation on either host.

~~~sh
"$BIN" --data-dir "$DATA" nodes pair "$PEER_CARD" --preview
"$BIN" --data-dir "$DATA" nodes pair "$PEER_CARD" --confirm "$REVIEWED_DIGEST"
~~~

### Start and stop the local runner

**Start local runner** starts the existing `nodes start` runtime for this root. The window reports ready after the launcher receives that runner's readiness line from the retained child and verifies the child is still alive. **Stop local runner** closes the original child's private inherited control pipe. The runner uses the same shutdown routine as foreground Ctrl+C. The window reports a successful stop after the original child exits successfully and the launcher acquires and releases the instance lock. It reports an unverified exit or lock if either check fails.

Closing the companion closes the control pipe of any child it owns. A reopened companion begins with fresh ownership. A foreground runner or one owned by another window can continue to hold the instance lock and must be stopped by its operator or owning window.

### Check a paired runner

The coordinator operator uses the existing [administrative probe procedure](../production-nodes.md) with the coordinator instance available for exclusive access. Export and open a fresh snapshot after the check.

~~~sh
"$CORE_BIN" --data-dir "$CORE_DATA" nodes status --probe
"$CORE_BIN" desktop --database "$COORDINATOR_DB" --session "$SESSION" > snapshot.json
~~~

## Windows rollout gates

| Area | Evidence required on native Windows before enabling fleet controls |
| --- | --- |
| Package and window | Use the exact pinned x64 package. Record Windows build, architecture, artifact hash, successful launch from a path with spaces, Nodes ↔ Jobs, keyboard and accessibility navigation, scrolling, minimum window size and 100%/150% scaling. Confirm the picker opens a valid export, keeps the previous view after cancel or rejection and holds data only for the current run. Record screenshots and command exits. |
| Nodes and reachability | Use a separately authorized staging coordinator and paired runner. Compare an unchecked node, successful authenticated contact, disconnect and reconnect against fresh exports. Confirm old snapshots stay dated, failed probes preserve the last successful contact, and neither pairing nor a saved reachable state supplies job evidence. The administrator owns any active probe through the existing coordinator procedure. |
| Public-card pairing | Stage a local preflight and explicit confirmation that present the public ID, expected opposite role, fixed address and exact revision for independent comparison. Pass only the selected public card to existing `nodes pair` on the configured host under its current operator account. The coordinator side remains an explicit coordinator-operator action. Verify UTF-8 card exchange, identical re-import, rejected wrong role/revision/identity and a conflicting second coordinator preserving the first admission. Prove the existing pair command enforces that invariant atomically under concurrent imports. Verify private-root and ACL failures precede admission. Keep production identity and private state in place. Follow the [native Windows enrollment guide](../windows-onboarding.md). |
| Local start | Verify that the Windows adapter uses the reviewed absolute CLI path, existing private runner root and the existing Windows account/SID. Verify direct process creation with `--data-dir <root> nodes start --desktop-control` and the runner's configured Codex environment. Preserve the runtime instance lock. Require a retained process handle plus `Runner ready` for that exact child before showing it as started. Test bad role, wrong account, ACL/reparse failures, absent configuration, startup failure, duplicate starts and the runner already owned by another launcher. Start applies only to this configured local runner. |
| Local stop | Verify the private inherited control pipe and the retained process handle for ownership and exit. The existing Ctrl+C path continues to use the same shutdown routine. Accept Stop only for the child that reached readiness under this adapter. Prove shutdown drains intake, joins workers and releases their owned Windows jobs and the instance lock. Verify timeout, stale handle, PID reuse, companion restart and unrelated look-alike processes. On timeout or lost ownership, report the observed state and leave stop to the owning console/operator. Never target a PID found by scan or use a machine-wide cancellation signal. |
| Jobs and recovery | Use disposable fixtures for trusted signed completion, signed cancellation, missing evidence on disconnect, late signed completion after Unknown, and each delivery state. Compare the same run ID against the coordinator and native window. Check the exact topic boundary, assignment staying fixed, delivery remaining independent of outcome and preserved Unknown after reconciliation. Evidence collection can use the existing mocked providers and native transport tests. |

The packaged GTK app has exercised the local controls with disposable identities. Windows packaging, accessibility, staging and operator receipts remain open. The coordinator continues to own active probing and exports. [Validation receipt](validation.md) · [Runner guide and recovery](../production-nodes.md)

## Telegram follow-up after parity

Open a separate change once the native Windows receipts satisfy the rollout gates and the operator confirms parity for every fleet action proposed for removal. Inventory duplicate node listings, status/detail surfaces and any routing controls first. Keep the current destination selector until an equivalent authorized path exists for the same chat/topic scope and new-run-only semantics. Preserve submission, task status, cancel and result delivery in Telegram. Test topic scoping, existing in-flight routes, stale callbacks and delivery across coordinator restart.

Keep brief recovery guidance alongside status and results. Record the Run ID for Unknown outcomes. The coordinator operator should check evidence with `nodes reconcile RUN_ID` through the existing recovery procedure before anyone repeats work. A cancellation request and a transport reconnect each leave the recorded outcome subject to signed evidence. Keep the CLI recovery path and restore the previous Telegram UI if the replacement fails acceptance. [Existing Telegram operations](../telegram-operations.md) · [Coordinator recovery](../production-nodes.md#state-and-recovery)
