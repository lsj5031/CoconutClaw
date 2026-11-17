# Paired execution nodes

Implementation lane, **not deployed or accepted**. Native/live fleet acceptance
and independent review are separate. The old `multihost-acceptance.py` fixture
does not exercise this ordinary-task path.

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

The following are templates for the parent-owned fresh fixture, not permission
for this lane to launch against live authentication:

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

On Telegram, open **Home → Nodes → Use remote ID**. Selection is scoped to the
authenticated conversation/topic and frozen when each new task is admitted.
Existing tasks cannot be stolen by changing the selection. **Use local**
affects only future tasks. Ordinary text goes directly through the production
scheduler adapter, not through the finite checksum fixture.

Remote requests intentionally start fresh Codex sessions and do not forward
coordinator history, memory, preferences, private supplemental context or quoted
messages. Attachments and unsupported richer requests fail explicitly rather
than being silently stripped. The signed text/request envelope is bounded by
the existing domain validator (2048-byte text, 4096-byte signed event). An
oversized/unpublishable result becomes Unknown with a bounded explanation,
never a fabricated/truncated completion.

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
leases or guaranteed present liveness.

Use existing task cancellation buttons during normal work. Cancellation intent
is durable before dispatch and is not proof that external effects stopped. A
late completed result after cancellation remains Unknown, with signed evidence
retained. Graceful runner termination signals owned cancellation flags, shuts
its router, kills only the existing provider API's currently owned groups and
joins owned provider work. Abrupt process death leaves ambiguity; restart never
re-invokes. Native descendant containment needs parent host-side evidence.

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

Parent-owned Linux/Mac acceptance must record actual host/revision/binary
identity, permitted workspace, separate Store roots, native invocation count,
signed event and source-state hashes, successful ordinary Telegram selection/
delivery, before/during/late cancellation, disconnect/reconnect, full restart,
exact replay, unavailable node and cleanup observations. Mock tests cannot
satisfy that gate; a finite fixture cannot satisfy ordinary Telegram routing.

Native Windows remains blocked: parent reports no reachable Windows execution
node or route. In addition to a native Rust/C-linker/locked-dependency/Codex
fixture, private-key ACL and owned descendant cancellation/cleanup acceptance
are required before activation. Existing provider Windows cancellation kills
only its direct child, which is insufficient evidence for a runner tree.
Enrollment/start refuse explicitly rather than silently claiming Windows
production support. No Windows auth installation or copy is authorized.
