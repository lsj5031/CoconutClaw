# Paired execution nodes

The ordinary-task path has live evidence with paired macOS and native Windows
runners. See [fleet verification](fleet-verification.md) for the exact
revisions, checks and limits. The older disposable multihost fixture does not
exercise this path.

One Linux coordinator polls Telegram. Explicitly paired runners accept text
tasks over the already pinned iroh QUIC implementation. They never poll Telegram,
receive its token, receive Codex authentication, or execute through SSH.
Each host uses its own Codex executable, authentication and dedicated permitted
workspace. No remote path, attachment or arbitrary filesystem API is accepted.

## Setup

Build the **same committed source revision** on each host using the existing
offline Rust dependencies. If building a parent-verified source archive without
Git metadata, set `COCONUTCLAW_SOURCE_REVISION` to its verified 40-character
commit. Never use that override to mislabel different source. Preserve binary
and source-manifest hashes in the fleet receipt.

These are templates for a new, explicitly authorized pairing:

```sh
# Coordinator, using its own data directory and actual reachable UDP address:
coconutclaw --data-dir "$CORE_DATA" nodes enroll \
  --role coordinator --bind "$CORE_IP:49731" > "$CORE_PUBLIC_CARD"

# Mac runner, dedicated data directory without Telegram credentials:
coconutclaw --data-dir "$NODE_DATA" nodes enroll \
  --role runner --bind "$NODE_IP:49732" --workspace "$PERMITTED_WORKSPACE" \
  > "$NODE_PUBLIC_CARD"
```

Exchange **only** those public cards through the parent's trusted tooling.
Independently compare their endpoint IDs, roles, revision and addresses before
admitting them. Neither a hostname nor a QUIC connection is enrollment authority.
Never transfer node roots, `device-key`, authentication, config, sessions or
SQLite files. The `nodes` directory is owner-only on Unix; operator custody of
the data directory remains required.

Windows uses the same CLI and native transport, with a private inheritable
current-user/SYSTEM/Administrators DACL checked before state is read or a peer
is admitted. This covers the device key, local workspace/card, configured
executable selection, SQLite coordinator allowlist and journal sidecars.
Reparse points, null DACLs, foreign trustees and unsupported ACE layouts fail
closed. Unsafe existing state is not silently repaired or adopted. See
[native PowerShell onboarding](windows-onboarding.md), not the POSIX templates
below, for Windows commands. No OpenClaw runner or credential transfer is needed.

```sh
# Coordinator:
coconutclaw --data-dir "$CORE_DATA" nodes pair "$NODE_PUBLIC_CARD"
# Runner:
coconutclaw --data-dir "$NODE_DATA" nodes pair "$CORE_PUBLIC_CARD"
coconutclaw --data-dir "$NODE_DATA" nodes start
# Coordinator, before starting polling:
coconutclaw --data-dir "$CORE_DATA" nodes status --probe
coconutclaw --data-dir "$CORE_DATA" run
```

Pairing is explicit and immutable. A runner admits exactly one coordinator.
One coordinator can pair multiple runners. Use fixed, reachable LAN UDP
addresses; no discovery, public relay, firewall edits or tunnel is introduced.
Clients share one endpoint per coordinator process. Only one process may own
that coordinator identity. Administrative network probes/reconciliation require
the core instance lock, so stop polling before running those commands.
`nodes status` without `--probe` only reads persisted observations.

On Telegram, open **Home → Nodes** or `/nodes` in the intended conversation/topic
and choose the **Use Runner** button with the matching short ID. Selection is
scoped to that conversation/topic and frozen when each new task is admitted.
Existing tasks cannot be stolen by changing the selection. **Use local**
affects only future tasks. There is no per-task destination override. A
[scheduled Telegram occurrence](inbox-scheduling.md) uses the selection in its
configured scope at admission, not at calendar creation.

Remote requests start fresh Codex sessions. For ordinary text tasks in an
explicit Telegram topic, the coordinator can include active, owner-approved
[memory](memory.md) from that exact chat and topic in the signed request to
the assigned runner. Root, delegated and scheduled requests do not get this
memory. Curated facts are considered first, then matching
snippets from the original user text. Only fact content goes into the prompt
as JSON data, not instructions. There is no parent-chat fallback, and no
coordinator history, preferences, memory provenance or database, credentials,
private supplemental context or quoted messages are sent. Attachments and
unsupported richer requests still fail explicitly.

The existing 2048-byte text and 4096-byte signed-event limits cover the whole
request, including any memory. Facts that do not fit are skipped, never at the
expense of the original user text. If none fit or apply, the signed text is
exactly the original. An oversized/unpublishable result becomes Unknown with a
bounded explanation, never a fabricated/truncated completion.

Correction and undo affect new assignments, not already signed or in-flight
requests. The exact signed input is kept in coordinator and runner evidence;
there is no runner memory index or session reuse, but changing a fact cannot
erase an older prompt, receipt or output. Do not approve secrets as memory:
the runtime does not classify or redact owner-approved fact content.

Mixed revisions remain unsupported. The signed request is still text, with no
new wire format or fallback for mismatched cards. Both identities and pairing
cards must pass the existing same-revision checks before exchanging work.

Remote Codex uses explicit `workspace-write` sandbox and no dangerous flag or
automatic compatibility retry, regardless of the coordinator's execution policy.
Tool environment inheritance is disabled. This is native Codex sandbox policy,
not a claim of malicious-host isolation. The runner must be trusted. Output is
literal text at the coordinator; marker-looking remote output cannot read a core
file, create children, mutate memory or send attachments. The original
coordinator delivery route and outbox remain authoritative.

## State and recovery

Each task has an independently owner-bound origin and signed Assignment,
runner-signed Start and runner-signed Result. This reuses the existing
`BoundOrigin`, `SignedEvent`, `History`, result inbox and literal delivery
projection. It does not promote the test-only RuntimeDriver, its shared Stores,
singleton session, global event head or four-lifetime-task cap.

The source Store freezes node/request ownership and signed assignment before
any network send. The runner's **separate host-local Store** commits the claim
and exact Start before returning an ephemeral invocation capability. Replaying
Begin returns evidence only. A restart cannot recreate that capability.
Cancellation is core-signed and bound to the complete assignment digest.
Pinned transport identity, locally admitted role, current revision, origin,
runner, attempt and complete event chain are checked before acceptance.

Nodes shows last successful contact, unavailable observations, selection and
conversation-scoped assignments. These are dated observations, not heartbeat
leases or guaranteed present liveness. Assignment does not prove execution;
the signed actual executor is not shown in the Telegram task result.

Use `/cancel [run ID]` or the task's inline Cancel control. Cancellation intent
is durable before dispatch and is not proof that external effects stopped. A
late completed result after cancellation remains Unknown, with signed evidence
retained. Graceful runner termination signals owned cancellation flags, shuts
its router, kills only the existing provider API's currently owned groups and
joins owned provider work. Abrupt process death leaves ambiguity; restart never
re-invokes. On Windows the native provider executable is created inside its
noninheritable, unnamed kill-on-close Job Object. A failed creation-time
assignment does not start a provider.
Cancellation, timeout, normal leader exit, shutdown and error cleanup terminate
the owned job, including detached descendants. Closing the runner process closes
its last job handles, so kernel custody survives an abrupt owner death without
a PID-tree sweep or stale-PID kill on restart. Descendant cleanup does not undo
already completed external effects or turn Unknown into Completed.

After a disconnect or coordinator/runner restart, do **not** resubmit the same
task as a repair. Stop the coordinator's polling process, retain all state, then:

```sh
coconutclaw --data-dir "$CORE_DATA" nodes reconcile RUN_ID
# If cancellation could not reach the runner:
coconutclaw --data-dir "$CORE_DATA" nodes cancel RUN_ID
coconutclaw --data-dir "$CORE_DATA" nodes reconcile RUN_ID
```

These commands retrieve evidence or repeat the same assignment-bound
cancellation intent, never Begin or provider execution. A durable result can
replay exactly, but local Unknown is not erased by late Completed evidence.
Replay preserves recorded Telegram acknowledgments. There is no supported
control to send another copy of an already acknowledged reply.
Only rerun ordinary polling after the administrative process has exited.
Do not restore a copied identity root while an old holder may still be alive.
Whole-backup freshness/adoption and coordinator replacement remain outside this
path; existing independent recovery/exclusion requirements are not relaxed.

Rollback: stop only the owned runner/core processes, select local for future
tasks while this executable still runs, retain all node keys, Stores and
uncertain-task evidence, then restore the previously recorded executable link.
Do not delete reservations, reset claims or replay uncertain work. An older
binary is not permission to retry it. Pairing/key rotation and cross-revision
node-state migration are deliberately unavailable, not silently inferred.

## Native evidence gate

For a new fleet acceptance check, record actual host/revision/binary
identity, permitted workspace, separate Store roots, native invocation count,
signed event and source-state hashes, successful ordinary Telegram selection/
delivery, before/during/late cancellation, disconnect/reconnect, full restart,
exact replay, unavailable node and cleanup observations. Mock tests cannot
satisfy that gate; a finite fixture cannot satisfy ordinary Telegram routing.

Windows production paths now implement Job Object descendant custody and
device-key/coordinator-allowlist ACL enforcement instead of an unconditional
platform refusal. Windows-native mocked-process tests cover cancellation,
timeout, normal leader exit, abrupt owner death, shutdown/restart and unsafe/
proper ACLs; CI requires their execution, not merely compilation. Existing
durable claim/replay/restart tests also run there. A Linux development run does
not establish Windows-native execution. For future candidates, require exact
hosted Windows CI and separate authorized fleet acceptance with host-local
Codex auth.
No Windows paired host or OpenClaw installation is required to develop/test this
source. No auth installation, export or copy is authorized by these docs.
