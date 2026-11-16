# Manual handoff: external authority exclusion

15 November 2026. **Design contract only, unreviewed, not deployed. Replacement
activation remains unavailable.** Independent backup-recovery review remains
**DEFERRED, NOT approved** (Leo, 15 November 2026 21:34). This document neither
authorizes live operations nor supplies operational evidence. No proof collector,
provider revocation integration, runtime adapter or activation path exists.

## Decision and boundary

Use a manual, fail-closed handoff. Prefer existing service-native credential and
session invalidation, with replacement credentials inaccessible to excluded
holders, plus independently enforced containment where credential invalidation
does not cover local execution. Reuse the repository's local stop, cancellation,
journals and quarantine as supporting controls, not proof of external exclusion.
Do not build a custom fencing/proxy service to compensate for unknown provider
semantics. No maintained repository mechanism currently establishes the complete
gate. Actual Telegram/Codex invalidation semantics have **not** been verified in
this offline increment; the contract specifies required evidence, not a claim
that either service exposes a suitable API or receipt.

An **old holder** is any old core, runner, child/tool, wrapper, copied credential,
refresh-capable session or principal able to recover that authority. A **fence**
is the exact owner-signed suspension event, not an active epoch. **Exclusion**
means those holders cannot initiate further effects within the handoff's complete
authority inventory. **Reconciliation** accounts separately for effects already
accepted, buffered or delegated before exclusion. **Activation** would grant
replacement execution authority; this implementation never does so.

The trust anchor must be outside both the rollbackable application stores and
the excluded holders' control: authenticated service authority for credential/
session invalidation; independently administered infrastructure for containment;
and an independently current owner checkpoint register for checkpoint selection.
The register is a required operational role, **not an implemented service**.
An owner signature authenticates intent, not service enforcement. An owner
declaration, PID absence, terminal job state, wall-clock age, peer agreement or
synthetic test cannot substitute for the corresponding authority's evidence.

## Actual effect paths and required coverage

Unqualified runtime paths below are under `crates/coconutclaw-cli/src/`;
provider `lib.rs` is `crates/coconutclaw-provider/src/lib.rs`, config `lib.rs` is
`crates/coconutclaw-config/src/lib.rs`, and sync paths are under
`crates/coconutclaw-sync/src/` (domain is
`synthetic/membership/domain.rs`). They describe inspected source, not live
configuration or credentials. Future operational inventory must cover every
core and runner, even those unreachable during handoff.

| Authority / actual caller | Existing mechanism and limit | Required operational exclusion and reconciliation |
| --- | --- | --- |
| Telegram token: `commands/run.rs::run_run` discovers identity and registers commands; `loops/poll.rs::run_poll_loop` calls `telegram.rs::fetch_poll_updates` / `fetch_updates` (`getUpdates`, offset, 25-second long poll). | Chat/topic allowlists and persisted inflight/update IDs constrain friendly local processing. No service-side epoch is sent. Another holder can poll/advance offsets without taking the local lock. | Service-authoritative invalidation of all old bot-token authority, including any configured API intermediaries, and exclusive custody of the replacement. Establish disposition of polls accepted before invalidation and the inbound checkpoint/gaps. A polling conflict or successful new poll is not old-token invalidation proof. |
| Telegram send/mutate: `telegram.rs::{dispatch_telegram_output,send_progress_message,spawn_progress_updater,send_or_edit_text,telegram_send_rich,telegram_send_media_file,telegram_delete_message,telegram_remove_keyboard,register_bot_commands}`; `commands/helpers.rs::dispatch_process_outcome`, webhook/poll outcomes, scheduler delivery. Text, rich text, photos/documents/video, edits, keyboard cleanup and command registration all use token authority. | Durable task delivery in `delivery/telegram.rs::{durable_op,deliver_text,dispatch_task_output}` saves `sending` before I/O and acknowledgment after it; ambiguous delivery suspends resend. Direct status/command/progress helpers are not all in that journal. | Invalidate token for **all** bot methods, not only polling; prevent old holders obtaining the new token. Account for buffered HTTP requests, uploads, intermediary queues and service-accepted sends/edits. Revocation must not be assumed to recall an accepted request. |
| Telegram disclosure: `telegram.rs::{telegram_download_file,telegram_api_base,telegram_file_base}` uses token-bearing API/file routes; bases can be configured separately. | Local request timeout/stop is not remote credential revocation or erasure of downloaded content. | Cover both API and file authority, any gateway credentials/caches, and retained capabilities with verified scope. Previously disclosed files cannot be recalled. Do not claim confidentiality restoration. |
| Codex invocation/auth: scheduler calls `turn.rs::process_turn_with_status`, which calls `coconutclaw_provider::run_provider_session`; `commands/once.rs::run_once` reaches it through `process_turn`. Provider `lib.rs::CodexRunner::build_cmd` invokes configured bin/wrapper with `exec`, optional `resume`, JSON and optional permission bypass. `new_provider_command` sets wrapper-supplied environment and otherwise inherits the process environment except `CLAUDECODE`. | SQLite stores native session IDs under context/boundary-specific keys. Session separation is context isolation, not credential revocation. No provider credential enumerator, revoke API or external epoch check exists. Retrying provider failures and permission-flag compatibility is not effect reconciliation. | Establish actual auth modes and all issuers/refresh paths under separate authorization. Independently invalidate old access **and** refresh/session authority across all holders, or prove independently administered containment covers every holder and recovery route. A new login/key or session-ID change alone does not invalidate an old one. |
| Codex running tools/children and their downstream authority: provider `run_provider_process`, `run_child_process`, `terminate_cancelled_child`, `kill_active_process_groups`, `configure_child_command`; scheduler `request_shutdown` / `shutdown`. | Unix process groups get TERM then KILL for cancellation (up to five seconds), immediate group KILL on timeout; shutdown escalates after eight seconds and refuses after ten. In-process group registry tracks only currently invoked owned groups. No host-wide/remote authority containment. | Prevent surviving/detached processes, wrapper escapes, offline filesystem effects and already submitted remote tools from acting. Provider-auth revocation alone does not stop a running tool or revoke its Git/cloud/service credentials. Inventory and exclude downstream effect credentials, shared workspace/storage write access and delegated remote operations, or keep the gate unavailable. |
| Local assignment, approval, memory/task materialization and delivery recovery: `scheduler.rs::{recover,retry_deliveries}`, `recovery/pending.rs::reconcile_pending_turn_side_effects`, `loops/webhook.rs::restore_inflight_update`. | Startup can reconstruct queued work, dispatch it and retry delivery; saved results avoid rerunning some work, started-without-result work becomes unknown. Local SQL is not a distributed authority fence. | Keep replacement executable stopped, credentials withheld, restored state quarantined/inert. Reconcile approvals, delegations, queued versus started attempts, pending effects and delivery before any future runtime admission. Do not start `run` just to inspect a restore. |
| Synthetic record admission: sync `synthetic/membership.rs` policy/scope boundary and `domain.rs::History` replay/fence validators. | Owner membership, scope, leases, connection revocation and exact chain/checkpoint validation protect inert records. No CLI dependency on sync; no external API effect is gated by this history. | Persist exact fence acknowledgment from every proposed runner before any future admission. Any old runner that cannot acknowledge still needs independent external exclusion. Dropping it from a role list does not withdraw credentials. |

This inventory covers known application paths, not unknown live secrets. Codex
can exercise arbitrary tool authority permitted by its host. If that authority
cannot be bounded, exclusion cannot be established. No private memory, auth
files, session stores, provider settings or tokens were inspected for this map.

## Why not use another existing control?

- `main.rs` takes `RuntimeConfig::acquire_instance_lock` from config `lib.rs`,
  an exclusive lock on one `runtime_dir/instance.lock`. Synthetic `writer.lock`
  similarly excludes cooperating local writers. Neither covers another directory,
  copied store, machine or credential user.
- `service.rs` already maintains systemd/launchd lifecycle. The generated Linux
  unit uses `Restart=always`; stop/status use `run_best_effort`, whose unsuccessful
  exit is warned about rather than made an error. A successful CLI exit is not
  verified service containment. Independently managed restart inhibition and
  host containment would need separate operational evidence. No service command
  was run here.
- Cancellation markers, scheduler flags, process groups, HTTP timeouts and
  activity-aware deadlines are useful bounded shutdown behavior. In
  `telegram.rs::execute_request`, a foreground shutdown can return while its
  HTTP worker is still executing. Even process exit cannot undo a request
  accepted by a remote service. Groups do not cover escaped/remote children.
- Policy high-water marks, `ScopedPeer::restore`, recovery manifests and
  `History::validate_fence_checkpoint` bind local history to independent input.
  They do not invalidate external credentials. The pending fence refuses **all**
  further domain actions, so reconciliation must be retained separately, not
  appended as a post-fence result or represented as a successful epoch 2.
- A new credential escrow/proxy would require routing **every** call and tool
  through an unbypassable maintained boundary, migrating/revoking existing
  credentials, durable admission and remote-effect reconciliation. None exists
  here. Adding one now would not exclude already partitioned holders.

## Exact handoff checkpoint and evidence record

The following is a reviewable operational record contract, **not** a new accepted
wire format, Rust type, boolean proof field or activation input.

One handoff ID binds all evidence to:

1. Exact owner public key, stable store ID, namespace and scope; verified
   `BoundOrigin::hash()`; original core and runner keys.
2. Fence sequence, BLAKE3 of the **complete canonical signed fence envelope**
   (the value compared by `validate_fence_checkpoint`), predecessor envelope hash,
   previous epoch 1, pending epoch 2, exact proposed core and runner set.
   Preserve origin and fence hash algorithms/domain separation as implemented;
   do not substitute a JSON-object hash or the event signing preimage.
3. Independently current membership generation and exact signed policy hash;
   independently retained recovery-manifest hash for any adopted backup, with
   exact application/protocol checkpoint identities and known limits. Synthetic
   recovery manifests do not yet bind real application state.
4. Authority inventory revision/digest: each affected service account/resource,
   credential **identifier/version, never its value**, auth/refresh lineage,
   holders, gateways, tool/workspace authorities and containment boundary.
   Record unknown coverage explicitly. Do not hash low-entropy secrets as IDs.
5. Exact digests of runner acknowledgments, attempts/effects reconciliation and
   service/infrastructure evidence, plus evidence issuer, authenticated retrieval
   method, enforcement scope, effective barrier, observation interval and
   invalidation/revalidation conditions. Sensitive raw evidence stays in a
   separately access-controlled owner store, not replicated history or this repo.

A runner acknowledgment must bind this tuple, runner identity and its durable
local fence/high-water checkpoint, report outstanding attempts, and demonstrate
that restart remains fenced before admission. It is a required future runtime
behavior, not supplied by friendly synthetic signature checks. Acknowledgment
does not prove loss of that runner's external credentials.

### Freshness and trust

Evidence must be collected **after** the exact fence is independently pinned and
after enforcement has become effective. It must cover all old credential
versions/refresh lineages through that barrier. An observation made before the
barrier or for another account, fence, proposed role set or inventory is invalid.
Authenticate the issuer through an owner-approved channel independent of old
holders; a signed application attachment saying “revoked” is not issuer evidence.
Where an issuer cannot bind a handoff nonce, the trusted collector must bind its
authenticated observation to this exact tuple and observation interval. No such
collector or trust configuration is implemented or presumed available.

Use service-established invalidation/propagation guarantees and independently
maintained containment, not a guessed TTL or sleep. A negative old-token probe
is supporting evidence at one endpoint/time only, not proof that refresh tokens,
alternate endpoints, cached sessions or other holders cannot act. A successful
replacement request proves only its own authority. Provider receipt/status
semantics, barriers for previously accepted work and remaining credential
issuance authority must be established before accepting evidence.

Freshness must remain continuous from the effective exclusion barrier through
any future activation and subsequent operation: the independent current register
must still select this fence, exclusion must still hold, and new credentials must
not become accessible to old holders. Revalidate immediately before any future
admission and after restart, restore, containment change, new credential issuance,
trust-anchor change or contradictory observation. Unknown propagation bounds,
clock rollback/uncertainty, lost continuity, register rollback, newer/conflicting
fence or unavailable issuer/containment evidence mean **unavailable**. No finite
local timestamp can detect an unseen newer owner decision.

## Serialized operational sequence (not authorized execution)

1. Separately authorize inventory and evidence collection, shutdown/containment,
   service credential/session invalidation, new credential custody and any
   downstream-tool intervention. Identify the trusted issuer/admin and exact
   scope first. Do not infer permission from this design.
2. Prepare the exact suspension checkpoint and independent register; keep all
   replacements inert. Stop new admission on reachable holders using maintained
   controls. Retain journals/results and unknowns. If an old holder is partitioned,
   do not wait for its cooperation as a substitute for external exclusion.
3. Apply service-native invalidation and independently controlled containment
   across the complete inventory. Withhold replacement credentials from every
   old holder, including old filesystem, environment, backups and refresh/issuance
   paths. Obtain authenticated effective-enforcement evidence. No exact Telegram
   rotation or Codex logout/revoke procedure is asserted without verified service
   semantics. If any credential family or holder cannot be excluded, stop here.
4. Reconcile accepted/in-flight effects and durable checkpoints after the barrier.
   Collect proposed-runner durable fence acknowledgments; exclude any remaining
   old runners independently. Verify exact history and anchored restore inputs
   without starting the runtime. Pin evidence digests and continuity externally.
5. Review all gates. **Today the result is still unavailable:** no trusted
   operational verifier, runtime fence admission or activation transition exists.
   Do not treat completing a checklist as a way to enable the replacement.

## Outstanding attempts and effects

Maintain a separate reconciliation ledger bound to the handoff tuple. For each
job/attempt, record assigning core, runner, start/session reference, durable
result and possible effect IDs. For each effect, record authority, target scope,
local operation/ack state, service receipt/reference where available, whether
accepted before exclusion, evidence of completion/non-execution/cancellation,
and unresolved uncertainty. Include queued delegations, approvals and inbound
updates/offset changes, not just running Codex leaders.

`delivery/telegram.rs` has no caller-supplied Telegram idempotency key: `sending`
or `unknown` cannot be converted to “not sent” on a timeout, crash or rotation.
Preserve acknowledged operations; do not resend, switch representation or
recreate an ambiguous effect automatically. Direct command/progress sends and
generic HTTP retries may lack durable per-operation evidence; missing receipts
increase uncertainty rather than proving absence. Service receipts and target
state inspection, where authorized and sufficient, must settle those cases.

`scheduler.rs::recover` preserves uncertain starts instead of automatically
rerunning them, but can run never-started queued work and send saved output.
Provider `turn.rs` retry loops and resumed native sessions are not proof of
exactly-once effects. Completed/Failed/Cancelled means neither that all child
effects terminated nor that all external operations were recorded.

Every consequential unresolved attempt/effect blocks future activation under
this contract. An owner risk acceptance may record a separate decision, but is
not proof of exclusion or reconciliation and must not silently satisfy this gate.
Unknown work remains Unknown; no automatic higher attempt or rewritten terminal
result. If an issuer cannot establish the fate of accepted work, preserve the
blocker rather than declaring the barrier drained.

## Restart, rollback and fail-closed result

Before verified exclusion, abort leaves replacements inert and does not authorize
resuming the old core. After invalidation, never restore old credentials or roll
back to epoch 1; revoked authority is not an application rollback mechanism.
Loss/crash midway through enforcement yields partial coverage, not success.
Re-establish the complete inventory/evidence under the same exact checkpoint or
an independently current subsequent authorized decision. Preserve earlier
receipts and unknowns without overwriting historical evidence.

On replacement restart or backup restore, start quarantined with credentials
withheld; obtain current independent inputs and revalidate containment/evidence.
A copied evidence bundle in the backup cannot establish its own freshness.
Missing application checkpoints, lost journals, conflicting history, stale
membership, incomplete runner acknowledgment, unbounded tool authority, exposed
replacement credentials or any unresolved consequential effect blocks admission.
An offline/unreachable issuer or old holder is not evidence of exclusion.

## What can be developed next

**Locally implementable:** an inert reconciliation-ledger contract and offline
validation fixtures for exact checkpoint/inventory binding, partial and conflicting
evidence, unknown attempts/effects, stale input and restart refusal. Such checks
may validate record structure and compare independently supplied identities;
they cannot authenticate imaginary provider receipts or establish real freshness.
Keep `History::validate_replacement_activation` unconditionally refusing.

**Operationally missing:** an owner-authorized, complete holder/tool-authority
inventory; verified Telegram token invalidation scope/propagation and accepted
request behavior; actual Codex auth/refresh/session revocation semantics; issuer-
authenticated evidence and independent containment covering surviving local and
remote tools; replacement secret custody; reconciliation of real outstanding
effects; independently current checkpoint continuity and durable runtime runner
fence admission. None was obtained in this increment.

The follow-on [inert reconciliation ledger](reconciliation-ledger.md) now maps
existing task payload/delivery states and implements mock-only shape/binding
checks without activation or runtime wiring. It does not verify real exclusion,
read runtime state or resolve unknown effects.

The descriptor follow-on now checks exact redacted application checkpoint/
observation bytes, source/schema identity and typed local links using artificial
SQLite interrupted-state fixtures. It supplies no real operational evidence.

The [recovery fixture correction](iroh-phase2.md#recovery-restart-fixture-identity-lifecycle)
uses controlled path evidence to separate copied-holder/client lifetimes,
retaining fail-closed assertions and the prior failed receipts. This is not
operational identity uniqueness or external exclusion.

The synthetic inventory follow-on now binds declared known/unjournaled effect
observations to exact authority/resource and holder aliases, retaining unknown
holders and explicit unmapped observations. A complete-looking declaration still
establishes no operational completeness, provenance, service enforcement or
exclusion. It checks pinned base-ledger observations, not application snapshot
preimages, live auth/refresh lineage or authenticated service evidence.

The mock-only composition now inspects exact application descriptors and inventory
coverage under one independently selected binding. Explicit unjournaled
observations remain opaque and Unknown, not validated application rows.
Complete-looking declarations still supply no service proof or permission.
**Next bounded development candidate**, requiring separate authorization: typed
redacted fixture preimages for those opaque references, not live collection,
issuer proof or runtime admission.
Collecting live evidence, changing/stopping
production, rotating/revoking credentials/sessions, provisioning a proof adapter,
network probes, runtime activation or deployment each needs separate owner
authorization and verification. Runtime adapters, quotas/multihost and remote
Codex remain later gates, not part of this result.
