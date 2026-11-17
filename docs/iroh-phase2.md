# iroh Phase 2, synthetic increments

The historical fixture status below predates the isolated production-node
implementation lane. See [paired execution nodes](production-nodes.md) for its
separate source path, activation limits and required native acceptance. Historical
fixture receipts are not evidence for that new path.

16 November 2026. **Partial, unreviewed, not deployed. Replicated application
state is not complete.** No production cross-host runner, CLI network configuration or live
production runtime adapters are enabled. `coconutclaw-sync` remains separate from the
shipped Telegram/Codex executable; the isolated Store adapter below links it
only as a CLI dev-dependency.

## Runnable multihost development roles, 16 November 11:03 authorization

The explicit ignored dev-role executable now reuses RuntimeDriver, actual Stores,
the runner signing/claim journal and existing provider API across pinned iroh
control and scoped record endpoints. Default loopback behavior remains unchanged;
only the disposable acceptance constructor accepts explicit host binds/relay.
Assignment, proposal, signed Result and bounded file artifact use actual scoped
record transfer. The source Store retains verified artifact bytes and a
runner-signed nonce/sum/session/hash manifest. Role keys stay on their own host;
native config/session state is isolated and existing host auth is read in place.

Local regressions cover distinct processes, full stop/reopen and exact replay
without a second invocation, framing/artifact bounds and owned-group parent-death
cleanup. The latter is not proof for every native tool-created process group.
Native in-flight session identity, broader admission/custody and operational
activation remain separate gates.

The parent built the offline source/dependency bundle on Mitori Rust1.94.1,
verified all26101 build inputs, and verified the same arm64 binary SHA256 on Mars.
The first Mac coordinator launch failed before ready/model because `kern.hostuuid`
is unsupported. The corrected lookup validates the built-in ioreg IOPlatformUUID,
hashes it without logging raw hardware identifiers and retains Linux behavior.
The corrected actual1201 launched both roles and Mars native Codex succeeded with
the correct fixture. Application acceptance failed Unknown: native0.167 emitted
custom tool-call/input/output events, while the scanner accepted function calls
only. The narrow correction accepts both execution formats with correlated
outputs and retains bounded safe Finish failures. Failed1201 is never retried.
Parent's correct read-only BLAKE3 check confirmed original config/auth unchanged;
the earlier SHA256 comparison was invalid. Fresh `hc1250` parent receipts now
prove actual Mitori→Mars original Store Completed, exact138-byte nonce/sum42
artifact, one invocation, full two-role restart and exact signed-event/artifact
replay. Both roles stopped. This proves the fixed disposable multihost fixture,
not Phase2 completion, production activation, custody or arbitrary containment.
Later changes are diagnostic/test/documentation only. Exact commands,
required public routing/endpoints and selective collection are in
[`multihost-acceptance.md`](multihost-acceptance.md).

## Owner amendment and actual Store adapter, 16 November 07:41

Owner authorization now explicitly continues ordinary isolated runtime adapter
development toward one remote Codex task. Earlier statements that runtime
adapters must wait for operational exclusion apply to **live admission**, not
code using disposable stores/mock executors. The prior suggestion to add more
unjournaled descriptor layers is superseded. Recovery review remains
**DEFERRED, NOT approved**; this is implementation, not review or deployment.

`coconutclaw-cli/src/store/task_adapter.rs` is compiled only under `cfg(test)`.
It uses actual `Store::open`, task insert/read/reservation/cancellation/restart
methods and the retained request parser/serializer. Its bounded supported slice
is explicitly delegated local text with stdout delivery, no attachments,
session credentials, supplemental/private context or Telegram routing. Unsupported
payload fields and lossy parser defaults refuse rather than being discarded.

The adapter pins an independently provisioned owner-bound domain origin and
exact local session mapping in the disposable SQLite store. Queued/unstarted
source runs are transactionally held with exact signed assignment bytes before
`ScopedPeer::publish_domain`. A failed publication can replay only those same
inert bytes; another export cannot skip a pending reservation. Lost local mapping
cannot allocate a new assignment for a source delegation already in domain
history. Local run IDs, domain assignment sequence and recipient run ID are
different identities, correlated explicitly with origin, source run and
delegation parent/index; only existing domain attempt 1 is accepted.

The existing `receive_domain` path performs policy-controlled loopback transfer.
The recipient validates the complete domain history and canonical application
request, then transactionally inserts/deduplicates an actual task/payload with
an origin/assignment-isolated session. Its status is **awaiting_approval**, with
no approval record, provider invocation or scheduler admission. Replays do not
reset started, cancelled, failed-unknown or conflicting recipient state.
History conflicts remain unavailable after restart. Restart quarantine, scoped
policy, epoch suspension and unconditional activation refusal are unchanged.
Only inert publication is retried, never consequential execution.

Seven behavior tests cover actual Store-to-transport receipt, reconnect/reopen,
duplicate correlation, request/attempt/scope conflicts, malformed input, injected
publication/inbox transaction failures, lost export mapping, cancellation and
unknown-outcome refusal. Fresh artificial data and loopback peers only. No
production schema migration, live runner flag, credential inspection/copy,
provider call, real task, takeover or deployment is added.

The following increment now adds isolated mock execution and persisted
Start/Result return. Continue toward runtime integration and one remote Codex
task without declaring these tests operational permission.

**Separate operational gates:** complete holder/tool inventory, verified
invalidation and accepted-effect semantics, trusted exclusion/containment and
replacement custody, independent checkpoint continuity, real runner admission
and live Codex/Telegram/takeover authority. None is supplied by fixtures.
Phase 2 stays incomplete; replacement activation stays unavailable.

## Deliberate mock Start/Result round trip, 16 November

**Test/dev-only, unreviewed, not deployed.** The Store adapter's `execution`
module extends actual persisted task rows, not a descriptor service or alternate
runtime. `start_mock` commits a signed domain Start and local started state
before publication. Only its ephemeral, non-cloneable dispatch token permits
one deliberate task-local mock invocation. The execution claim is committed
before that invocation. No Codex/provider process is launched.

Task completion, payload output and exact signed Result bytes commit together
before publication. The source applies returned Start/Result transactionally,
bound to its exact original signed assignment, origin, source run, assigned
runner and attempt 1. Duplicate starts cannot reset completion; duplicate
results do not overwrite state. Lost publication/transport or source persistence
can resend/reapply only durable exact bytes. Restart after a durable start but
without a durable result recovers **Unknown**, never another mock invocation.
An already-unknown source remains unknown even when late Completed evidence
arrives; that evidence is retained separately in the signed history/output.

Cancellation uses a narrow **core-signed command in existing scoped record
text**, bound to the assignment digest, origin, source run, runner and attempt.
It is cancellation intent, not exclusion proof. Keeping it outside the linear
domain event chain avoids a cancellation/late-result fork. No domain event,
signature, role, epoch, quarantine or activation rule is relaxed. Current policy
and complete-history validation precede interpretation of received commands.
Malformed/forged commands refuse before dispatch and do not partially cancel.

A pre-invocation cancellation skips the mock and returns a local Cancelled
disposition. Cancellation overlapping reported completion retains the result
but conservatively preserves Unknown in the task lifecycle. Tests exercise
signed cancellation transfer before invocation and with late return, plus a
local Store cancellation signal during the actual mock callback. This is not
a concurrent live runner or provider cancellation service.

Nine new behavior tests extend the seven existing Store/transport tests:
bidirectional persisted Start/Result, full peer/Store reopen, duplicate/conflict/
malformed/wrong-identity/attempt refusal, cancellation, lost return, injected
publication and result-commit failures, source restart uncertainty and exact
resend. All use disposable Stores and loopback peers. The fixture runner receives
an explicit docs write capability under the same scoped policy, not live runner
admission. The shipped executable's normal dependencies still exclude sync.

The following bounded driver now coordinates overlapping local mock callbacks.
The existing linear domain chain is not a multiwriter scheduler: conflicting
concurrent event publication still refuses, not last-writer-wins.

## Single-owner asynchronous mock runtime driver, 16 November

**Test/dev-only, unreviewed, not deployed.** `task_adapter/driver.rs` owns one
origin/session, both disposable actual Stores, a single scoped publication peer
and its loopback mirror. The coordinator holds fixture core/runner signing keys;
only it allocates signed Assignment/Start/Result sequences. Mock callbacks receive
only prompt, a cancellation watch channel and a bounded task-bound outcome
sender. They never receive Store connections, publication peers or signing keys.
This is local explicit sequencing, not a general queue, distributed consensus,
live runner admission or a new descriptor representation.

At most four tasks belong to this disposable driver, with at most two concurrent
mock futures. The existing adapter's durable non-cloneable invocation claim is
separated from result acceptance. Start and claim commit before invocation.
Completion acceptance reads the **current** validated history head, not the head
captured at invocation, so another assignment/start or out-of-order completion
does not fork the chain. Signed events still traverse actual scoped transport
and complete-history validation before source lifecycle application.

Reconciliation validates the entire candidate prefix from the existing adapter
outboxes before replay or new sequence allocation. Only missing events replay;
acknowledged history and terminal pairs are not republished wholesale. Publication,
receipt/projection and source-commit failures block further allocation until
explicit repair. Active local claims survive reconnect; orphaned starts after
restart become Unknown without another callback. Durable results resend exactly.
Missing source/execution correlations refuse, rather than reconstructing
execution permission. Reopened peers stay quarantined until explicit fixture
policy installation.

Signed cancellation intent uses the previous scoped command, then signals the
async callback. Pre-admission cancellation skips invocation; cooperative
cancellation settles Cancelled. Late Completed evidence after cancellation
remains in signed history/output while local lifecycle stays Unknown. A failed
cancellation publication replays the persisted exact intent and signals the
still-live callback. Duplicate outcome messages allocate nothing; conflicting
candidates refuse without signing an alternative Result. A disclosed validly
signed fork still makes the complete view unavailable.

Nine new behavior tests extend the sixteen adapter/roundtrip tests. The maximum
workload checks four completed tasks, two simultaneously awaited callbacks and
the exact twelve-event chain with out-of-order completion on a two-thread Tokio
runtime. Completion messages are drained after task join as well as before it,
so a send racing the join poll is not mistaken for missing output. Other checks cover
cancellation before/during work, duplicate/conflicting channel outcomes, failed
assignment/Start/Result persistence/publication, delayed receipt, failed source
commit, repeated full Store/peer reopen, quarantine, exact replay, lost
correlations, callback panic and no automatic invocation retry. Callback panic
stops the remaining disposable mock futures before orphaned claims can recover
Unknown; it never restarts them. Failures use disposable SQLite
triggers and channel barriers, not sleeps or test-suite serialization.

The following finite route supersedes the driver's shared signing ownership.
Neither slice demonstrates separate-host custody, remote admission or provider
cancellation enforcement. Remaining preparation needs process-boundary/custody/
admission design and cross-process failure drills. Richer
request scopes are not added here. Separately authorized operational inventory,
verified exclusion/invalidation/custody, accepted-effect reconciliation and
independent checkpoint continuity precede real admission and remote Codex
acceptance, followed by packaging/fresh-host acceptance. Phase 2 remains
incomplete; review is DEFERRED, NOT approved; activation remains unavailable.

## Runner-signed proposal route and current-head handshake, 16 November

**Test/dev-only, unreviewed, not deployed.** The existing RuntimeDriver now holds
only the core key and a runner channel client/public identity. A separate owned
Tokio runner actor holds the runner key and its disposable SQLite proposal/
signing journal. Start and Result signatures are requested through a narrow
adapter signing seam; legacy direct-key unit fixtures remain supported, not
used by the coordinator route. No new dependency or production migration.

Mock callbacks submit runner-signed outcome proposals. Before lifecycle
acceptance the coordinator checks canonical encoding, signature, fixed
origin/scope, exact assignment digest, source run/delegation parent/index,
assigned runner, attempt 1 and text/envelope bounds. Valid proposals cross
actual existing scoped transport from the runner-side peer to the coordinator;
the verified immutable receipt must match. They are not linear domain events.
Malformed/wrong-identity/stale-attempt candidates refuse; exact duplicates do not
allocate an event, and conflicting outcomes cannot overwrite the accepted result.

One sequencer selects the CURRENT complete-history head and asks the runner to
sign that exact final action. The runner revalidates the prefix and journaled
proposal; it refuses invented/changed outcomes and non-task signing requests.
Any previous signing response must appear in that prefix, or be the identical
pending challenge being replayed. This reservation is global across the bounded
runner's tasks, not just per task. Both sides use the unchanged domain validator;
the adapter verifies exact returned event equality and signature before commit.
No coordinator code signs as the runner.

Proposals and final signing responses commit before reply. Reconciliation
incorporates durable runner responses before allocating another sequence and
restores existing adapter rows without issuing another invocation token. Lost
Start response recovers Unknown without invoking. Lost Result response or a
proposal durable before coordinator crash retains exact signed output, while
the interrupted local acceptance stays Unknown. Already committed results
replay normally. Cancellation/late Completed evidence remains conservative.
Peers still reopen quarantined until explicit fixture policy installation.

Seven new tests accompany the existing twenty-five adapter/driver tests, now
using the separated signer route for all coordinated workloads. Tests cover
hostile bindings/signatures/canonicality/attempts, unchanged Stores on refusal,
unobserved/changed challenges, actual lost reply receivers, global pending-head
reservation, repeated Store/peer/runner-journal reopen, no duplicate invocation,
and maximum 2048-byte text versus oversized escaped envelopes. Existing
four-task/two-callback overlap, out-of-order completion, scoped cancellation,
duplicate/conflicting proposals and signed final-fork refusals remain.

**Concrete narrowing:** the current adapter formerly accepted SecretKey directly
inside its SQLite lifecycle methods. This increment extracts the smallest async
signing seam and uses a local owned actor instead of adding a test executable,
OS-process protocol or admission service. The actor/client are in one address
space; the coordinator retains fixture transport handles and application Stores.
Logical signing ownership and loopback transport are NOT process isolation,
multihost or custody proof. Remaining ordered gates: process/admission boundaries
and cross-process drills; separately authorized operational authority/effect/
checkpoint gates; explicitly authorized real remote Codex; packaging/fresh-host
recovery. Review DEFERRED NOT approved, Phase 2 incomplete, activation unavailable.

## Disposable OS-process mock runner, 16 November

**Linux, test/dev-only, unreviewed, not deployed.** The following path removes the
same-address-space limitation for mock execution/signing. It launches one exact
ignored child unit-test entry in the existing test binary. The child generates
and retains its own disposable runner key, signing/proposal journal and separate
actual execution Store. The parent obtains public identity/client only, not key
material. Its callback orchestrates RPC controls and forwards signed bytes;
the mock checksum computation, PID-bound output and signatures run in the child.
Existing actor/direct-key fixtures remain supported.

No dependency or production schema/service/admission flag is added. Existing
RuntimeDriver sequencing, adapter claims, proposal validation, scoped peer
transfer and source result persistence are reused. The parent cannot submit a
Completed/Failed process disposition: only child execution reports completion.
Child work derives from its exact journaled signed assignment, not an arbitrary
parent prompt/output. Existing domain signatures, global pending-head reservation,
fork refusal, quarantine and activation rules are unchanged.

The private Unix socket uses canonical version/boundary/origin-bound messages
with four-byte lengths and a 256 KiB frame cap. The runner still accepts only
four tasks/twelve history events and 4096-byte signed artifacts. At most four
RPC calls enter the blocking bridge, with one serialized request/response and
two pending child mocks. A fresh per-process boundary prevents old-client reuse.
Partial-frame/response I/O is bounded at ten seconds; idle child reads wait for
input/EOF, not a sleep loop. A child writer lock excludes another owner of the
same fixture. Unmarked nonempty directories refuse. The environment is cleared,
then only fixture socket/airgap metadata is supplied; no auth is copied.

Begin commits an invocation claim and actual started task before child work.
Release/cancellation commands deterministically advance pending mocks, so cancel
is not blocked behind an execution RPC waiting for completion. Reopening marks
unfinished child tasks Unknown. Existing claims or already observed/finalized
outcomes refuse Begin forever; recovery never creates an invocation token.
Durable signed Start/report/final response is recovered by the existing journal/
current-head route. Interrupted acceptance keeps local Unknown even when exact
Completed output survives. Cancellation before work invokes zero child tasks;
cooperative cancellation settles Cancelled, and late completion retains Unknown.

Four behavior tests cover a different-PID four-task/two-pending roundtrip and
out-of-order source results, malformed/oversized/version/origin/boundary refusal,
process death after durable Start/report/signature and an acknowledged active
claim, repeated journal/Store/peer reopen, no duplicate invocation, before/during/
late cancellation, graceful shutdown and Drop/forced cleanup. Crash points are
armed and acknowledged, not timed sleeps. An oversized header refuses before
body allocation. The helper entry is ignored in the default listing and is
explicitly executed by these tests; it is not skipped process coverage.

Shutdown attempts all peer closures even if the child is already dead. Child
ownership is guarded through startup errors, normal shutdown and Drop; Linux
pidfd polling bounds graceful exit before kill/reap. No descendants/provider
processes are created by the child. This proves a disposable OS-process mock
boundary, not malicious-host isolation, real custody, live effects or multihost
operation. Remaining ordered gates: actual admission requirements; separately
authorized operational authority/effect/exclusion/invalidation/custody/checkpoint
gates; explicitly authorized real remote Codex; packaging/fresh-host recovery.
Review DEFERRED NOT approved; Phase 2 incomplete; activation unavailable.

## Existing provider API and real local acceptance, 16 November

**Test/dev-only, unreviewed, not deployed.** Interrupted fake-provider work was
preserved, including four passing behavior tests and the failed compile receipt.
The child now calls existing `run_provider_session`, with actual task-bound
progress/session/output evidence. Provider work runs on an owned child thread;
a separate completion notice leaves control RPC free for cancellation. Start
and invocation claim commit before the provider API. Completion still uses the
runner proposal journal/current-head signature/scoped transport/source Store,
not a standalone CLI demonstration. Production provider behavior is unchanged.

Fake regressions retain explicit config/executable refusal, correct prompt/cwd,
new/resumed synthetic sessions/progress, exit7, before/during cancellation,
control Drop and shutdown cleanup, active/report/signature crash/reopen with no
second invocation, exact2048-byte output and refused2049 bytes. Refused output
stays local with Unknown, never digest fallback/truncation/retry.

Owner10:21 explicitly superseded the fake-only restriction for one harmless
real fixture. A separate ignored, opt-in acceptance uses the installed
`/home/leo/.local/lib/coconutclaw/codex-safe`, `gpt-6.1-terra`, wrapper max effort
and authorized yolo. Native scoped session metadata confirms model/effort.
Existing local auth is used in place; no auth copying, production Store, Telegram
or deployment. Native exec persisted fixture trust in shared config despite an
invocation trust override. Both native-only additions were removed only after
their removal reproduced the exact original config hash. The final route does
not assign shared CODEX_HOME to native exec: it generates non-secret routing in
a disposable home and forwards auth to the existing command reading original
configuration in place, without copying auth fields/tokens/files. Native session/
trust writes stay disposable. Final acceptance asserts unchanged global config
bytes and auth-file metadata. Default tests never invoke real Codex.

On **Leo-Claw**, final coordinator PID1917950 and runner PID1917952 completed the
nonce fixture through an actual Python tool (PID1918295), writing sum42.
Provider exit0/session/progress correlated to assignment1/source run2.
Runner-signed Completed returned over scoped peers and persisted at source.
Duplicate proposal acceptance was inert; full peer/Store/runner reopen retained
exact signed bytes, source Completed and one invocation. Runtime work took
25.336s, explicitly invoked command33.853s including compile. Global config and
auth metadata checks passed. No remote-host claim follows.

The first real receipt is retained: this integration initially set idle0,
mistaking it for disabled, and the existing API correctly returned immediate
inactivity/exit124 with no progress. The correction leaves existing30-minute
activity/two-hour pending-tool limits intact and disables only the optional
total deadline. The subsequent fresh nonce fixture passed; the failed assignment
was never re-invoked. Two subsequent model completions exposed the shared-config
trust side effect (one integrity assertion failed); the final fresh fixture
verified isolated configuration. These concrete corrections, not speculative
live repetitions, account for four explicit attempts; no assignment was retried.
No production timeout behavior changed.

Real during-cancel/abrupt-crash drills were not repeatedly run against the live
model; existing provider/fake regressions supplement, not prove, that boundary.
The next specific code gap is durable in-flight real provider process/session
identity plus abrupt-runner-death containment/reconciliation. Current provider
API returns session only on completion and exposes no in-flight PID/PGID;
fake parent-death cleanup cannot prove actual Codex descendants are contained.
Keep ambiguity Unknown and never automatically execute again. Also outstanding:
artifact-backed output above the2048-byte signed text cap, broader requests,
paired capability/workspace admission beyond this fixed fixture, runtime
activation, separately authorized operational inventory/exclusion/invalidation/
custody/effect/checkpoint gates, already-provisioned trusted remote-host
acceptance, multihost and packaging/fresh-host recovery. No remote auth was
installed or copied; only this authorized local host was exercised.
Review DEFERRED NOT approved; Phase2 incomplete; activation unavailable.

## Recovery-restart fixture identity lifecycle

**Synthetic test correction only, unreviewed, not deployed.** Recovery review
remains **DEFERRED, NOT approved** (Leo, 15 November 2026 21:34).
The descriptor increment's two default-suite `membership required` failures
were traced to a fixture routing error, not a loosened recovery gate.
The fixture opened current and rolled-back stores concurrently with the same
endpoint identity, and assumed each `EndpointAddr` selected that exact store.

Controlled diagnostics reversed only the two quarantine probe assertions.
The subsequent positive receive for the current holder then failed using the
rolled-back holder's loopback port. Path/handshake diagnostics identified the
still-quarantined holder as the rejecting endpoint. Original-order diagnostics
also showed the rollback-address negative probe reaching the current holder.
Thus a passing generic refusal had not necessarily tested the intended store.
This is consistent with cached iroh 1.4.1 source: `Endpoint::connect_with_opts`
resolves a remote identity, and `RemoteMap` keys address/actor state by
`EndpointId`, not backup-directory identity. No upstream bug/fix is presumed.

The existing four-process `recovery_restart` fixture now probes the rollback
copy with its own client, checks that refused receipt leaves already disclosed
history unchanged, and **awaits both endpoint shutdowns before opening the
current holder and fresh client**. The rollback probe runs first, retaining
the discriminating order that failed with concurrent holders. All stale-anchor,
old-policy, quarantine, injected commit failure, revoked-member, positive
projection and restarted duplicate checks remain. No assertions were disabled,
no sleeps/retries or suite serialization were added, and no library, transport,
membership or recovery implementation changed. Temporary diagnostics were
removed; their patches and failed receipts remain in increment evidence.
After removal, the focused regression and two DEFAULT workspace all-target runs
pass: each workspace run reports 325 passed, 0 failed, 1 preexisting ignored.

Supported fixture lifecycle: one live transport holder for an endpoint identity
at a time; copied-store probes use isolated client lifetimes. Shutdown is awaited
before identity reuse. Distinct concurrently active peers need distinct endpoint
identities. An address is a connectivity hint for an identity, not a storage-
incarnation selector. These are fixture preconditions, not library-enforced
global identity uniqueness, containment or external-credential exclusion.
Do not infer safe operational takeover or old-holder revocation from them.

The bounded synthetic inventory increment below follows this fixture correction.
Operational exclusion and Phase 2 completion remain unavailable.

## Increment 9: synthetic authority inventory

**Synthetic-only, unreviewed, not deployed.** The
[inventory descriptor](reconciliation-ledger.md#synthetic-authority-inventory-increment-9)
uses the existing inert ledger and canonical bounded parser to bind declared
recorded/unjournaled observations to authority/resource and holder aliases.
It refuses substitution, dangling/duplicate/conflicting links, foreign actors and
silent coverage omissions. Unknown holders and unmapped observations remain
explicit. Copied holders sharing a key remain distinct declarations.

All unjournaled observations, mapped or not, must retain `effect:unknown`.
Six new tests include actual post-fence retry/activation refusal with completed
history and complete-looking declarations, and the 4096/4097-byte boundary.
No issuer verification, real completeness, external exclusion, freshness or
runtime admission follows. Application snapshot preimages are not parsed by
this inspector; the existing application descriptor inspector remains separate.
No runtime, schema, dependency or recovery-fixture change is added.

## Increment 10: mock-only descriptor/inventory composition

**Unreviewed, not deployed.** The
[composed inspector](reconciliation-ledger.md#composed-application-and-inventory-inspection-increment-10)
reuses the exact application and inventory validators under one independently
bound bundle. Typed application rows remain distinct from explicitly unjournaled,
opaque observations. Both need exact coverage; unjournaled records stay
`effect:unknown`, including unmapped ones. Substitution and contradictory links
refuse the whole view even when mock hashes are recomputed. Complete-looking
inputs supply neither service proof nor freshness, retry permission or activation.
No runtime, live collection or service verifier is added.

The owner amendment above supersedes the prior suggested next descriptor layer:
continue actual isolated Store/runtime integration. External evidence and
operational gates remain separate; Phase 2 and activation remain
incomplete/unavailable.

## Increment 8: exact redacted fixture descriptors

**Synthetic-only, unreviewed, not deployed.** Independent backup-recovery review
remains **DEFERRED, NOT approved** (Leo, 15 November 2026 21:34).
The [ledger descriptor contract](reconciliation-ledger.md#exact-redacted-descriptors-increment-8)
adds canonical bounded application checkpoint/observation preimages, pins actual
schema/migration/mapping source bytes and checks exact descriptor digests through
the existing inert bundle binding. Artificial SQLite fixtures cover requests/
results, delivery attempts/acks, approvals/delegation and inbound/turn links.
Missing journals and interrupted windows remain Unknown. Conflicts, substitution
and malformed links refuse; terminal results/acks permit neither retry nor
activation. No runtime reader/exporter, adapter, dependency or service is added.
The existing activation validator is unchanged and unavailable.

The recovery-restart fixture correction above addresses the default-suite
failure with controlled causal evidence, retaining its prior failed receipts.
The synthetic authority inventory extension is described above. Operational exclusion,
trusted evidence/freshness and later runtime/multihost/remote-task gates remain
blocked separately. Phase 2 is not complete.

## Increment 7: inert reconciliation observations

**Synthetic-only, unreviewed, not deployed.** Independent backup-recovery review
remains **DEFERRED, NOT approved** (Leo, 15 November 2026 21:34).
The [reconciliation ledger contract](reconciliation-ledger.md) maps actual
runtime task statuses, payloads, delivery plans/operations, approvals, delegation
and unjournaled effects to a separate immutable observation bundle. It does not
read or export runtime data. Bounded local labels and descriptor/evidence digests
retain partial information and Unknown consequences, not a resolution decision.

The pure synthetic inspector reuses the existing canonical fence validator and
hash/serde types. It requires an independently supplied exact bundle hash and
matching origin/fence, application checkpoint, authority inventory, membership
and optional recovery anchors. Exact duplicate observations collapse; conflicts
at one immutable observation ID refuse the whole view. Tests retain Unknown,
refuse substitutions and post-fence results, and reload saved mock bytes against
reconstructed history to reject missing independent inputs and rollback.

This checks shape and binding only, not trust, freshness, inventory completeness,
semantic consistency of distinct evidence, service enforcement or real effects.
There is no operational issuer, resolver, runtime dependency, persistence service
or activation path. `validate_replacement_activation` is unchanged and unavailable.
Increment 8 above adds exact redacted application checkpoint/observation
descriptors against existing schema and interrupted task/result/delivery
windows, still without runtime admission. External exclusion
and all later runtime/multihost/remote-task gates remain blocked separately.

## Increment 6: manual epoch fence, replacement unavailable

**Synthetic-only, unreviewed, not deployed.** Independent recovery review remains
**DEFERRED, NOT approved** (Leo, 15 November 2026 21:34). Development is not
deployment acceptance. No live credentials, runtime adapters or production
stores were touched.

The smallest transition is suspension, not successful core transfer.
`Action::Fence` is an origin-owner-signed event in the existing complete canonical
chain. The immutable v1 origin and all pre-fence actions retain their original
meaning: implicit epoch 1, original core, exact assigned runner and attempt 1.
Only a transition from epoch 1 to pending epoch 2 is supported. It names a
different proposed core and at most 32 proposed runners, excluding that core.
Proposed roles are not authorized writers. The chain predecessor and origin hash
bind the fence to the exact historical prefix, including memory and job evidence.
No new origin, namespace, SQL authority, transport or dependency is introduced.

`PendingFence` means the accepted fixture prefix is suspended. After the fence,
**all** new actions refuse, including old/new-core memory mutation/assignment,
old runner starts/results, retries, second fences and rollback. Exact duplicate
bytes still collapse; conflicting owner transitions make the whole view
unavailable, not a winner selected by timestamp. Existing job attribution and
memory history remain inspectable, never executable. Outstanding, running or
unknown attempts are retained, not marked cancelled, reconciled or retried.
This intentionally does not implement a higher active epoch or runner rotation.

`validate_replacement_activation` always refuses. Outstanding/unknown attempts
give an unresolved-attempt/effect refusal; even a terminal or empty job history
gives an **external credential exclusion unproven** refusal. A terminal result,
including Cancelled, is not proof that effects or processes stopped. There is
no proof verifier, activation event, credential field, boolean attestation or
test-only activation escape hatch. Synthetic owner signatures cannot establish
that a partitioned old core lost Telegram/Codex credentials.

The [external authority exclusion contract](credential-exclusion.md) maps the
actual Telegram/Codex and tool-effect callers, compares maintained local controls,
and specifies checkpoint binding, trusted evidence continuity and reconciliation.
It selects service-native invalidation plus independent containment/custody where
required, not a custom proof service. This is **docs-only, unreviewed, not
deployed**; it establishes no real exclusion or activation authority.

The unresolved operational gate requires independently verified exclusion of
old-core external authority (Telegram polling/sending and Codex/provider access,
including retained sessions/processes), trusted current checkpoint continuity,
runner fence acknowledgment and reconciliation of ambiguous attempts/effects.
Actual service invalidation semantics, authenticated operational evidence and
complete holder/tool coverage remain unavailable. No live operation is authorized.
Until that concrete operational proof and its trusted adapter exist, replacement
**MUST remain unavailable**, regardless of signed epochs or fixture attestations.

`validate_fence_checkpoint(sequence, hash)` checks a caller's independently
retained current fence against the exact replayed canonical event. It rejects
old prefixes, missing fences and equal-sequence conflicting transitions. Supply
this checkpoint separately from rollbackable history/backup and arbitrary peers.
It is not a freshness oracle or a stored distributed high-water service: a
withheld unseen fence, stale independent input or wholly rolled-back checkpoint
cannot be detected locally. `domain_history` remains historical inspection, not
current-epoch admission. No prefix, even with a matching checkpoint, authorizes
execution or replacement activation. Scoped restart still starts quarantined;
existing anchored restore requirements remain unchanged.

New tests exercise owner-only authority, wrong/skipped/rollback epochs, duplicate
and conflicting transitions, stale core/runner and proposed-core refusal,
missing predecessors, absent/mismatched checkpoint and forged proof fields.
A public-boundary integration test uses three fresh processes for actual scoped
transfer (fence before predecessors), restart quarantine, exact replay, missing
credential-proof refusal with completed historical work, friendly stale-writer
refusal, hostile generic publication and persisted inert conflict refusal.
Prior publication, tombstone, receipt and backup/recovery tests are retained.

**Follow-on:** Increment 7 above adds the inert reconciliation ledger
against existing task/delivery states with mock-only refusal tests, without
activation or runtime wiring. The exclusion design contract is documented, but
obtaining and verifying operational exclusion remains blocked on the exact
evidence listed there. Proof collection/live credential changes require separate
owner authorization. Runtime scope/domain adapters and quotas/multihost follow
only after that operational gate, then remote Codex and packaging. None is
completed here.

## Increment 5: stable origins and inert domain transfer

**Implemented for synthetic fixtures only, unreviewed, not deployed.** Recovery
review remains **DEFERRED, NOT approved** under Leo's 15 November 2026 21:34
decision. Receipt recovery and this increment are also unreviewed. No rejected
review was retried, no provider controls changed and no deployment authorized.

The implementation first inspected `sql/memory.sql`, `sql/schema.sql`,
`store/memory.rs`, `store/turns.rs`, `store/task_runs.rs`, and `scheduler.rs`.
Runtime memory has immutable source-backed records/history, candidate versus
active state, core command promotion, active-only correction preserving curated
status/limits, and latest-eligible undo with transactional effective-state
checks. Runtime jobs have distinct durable task runs, persisted requests/results
and uncertain-start restart handling; approval resumes create a **new** task
run. There is no authoritative distributed attempt/epoch schema to import.

The isolated `synthetic::membership::domain` module implements this bounded
subset, not an alternate runtime:

- An independently locally provisioned `BoundOrigin` is signed by the existing
  offline owner key. It binds a 64-lowercase-hex stable store identifier, owner,
  namespace, exact synthetic scope, fixed application core signing key and at
  most 32 application runner signing keys. These application keys are distinct
  from endpoint/docs transport identities. They are not execution credentials.
  Origin/role-key rebinding and active core transfer are **not supported** by this
  fixed contract. Increment 6 adds only an owner suspension fence, not activation.
  A binding is trusted input, never installed from a peer.
- The stable event identity is `owner/store/sequence`. Its immutable envelope
  additionally binds the exact origin hash, actor, positive sequence, previous
  canonical-envelope hash and typed action. The existing iroh Ed25519 API signs
  domain-separated bytes and verifies signatures; no new dependency or crypto
  primitive. Exact serde struct/enum JSON serialization is the v1 canonical
  format. Unknown fields, whitespace/reordering, invalid signatures and any
  envelope over the existing **4096-byte artifact limit** are refused, never
  truncated. Valid runtime text can still exceed that serialized bound through
  escaping; export is explicitly unavailable in that case.
- A core-signed synthetic `Source` attests a bounded fixture reference. Candidate,
  remember and correction events require this predecessor. It does **not**
  authenticate or import a real turn, document, transcript or import slot.
  Only the bound core can attest sources or mutate memory history. Candidates
  are inactive; remember is curated and bounded to 512 bytes; other memory text
  is bounded to 2048 bytes with the runtime control-character rules. Correction
  requires an active predecessor and preserves curated status/512-byte limits.
  Undo names the latest non-undone change and reverses its exact old/new active
  state, including explicit restoration of a corrected predecessor. Replaying
  an old promotion after undo is a duplicate, never resurrection. **Unlike the
  runtime's explicit re-promotion command, fresh promotion of any spent ID is
  unavailable** here: no adapter yet proves fresh owner intent. Imported memory
  slot supersession/provenance is likewise unavailable, not guessed.
- Only the bound core assigns a new job (job ID equals assignment event ID,
  attempt 1). The assignment names an exact bound runner. Only that runner can
  report start, then one terminal completed/failed/cancelled/unknown result.
  The inert view retains exact assigning core, runner, attempt and result.
  Results without assignment/start, another runner's reports, second starts or
  results and higher/reused attempts fail. Cancellation intent, approvals,
  retries, delivery and reconciliation of unknown attempts have no proven
  distributed adapter and are unavailable, not auto-retried.

`History::replay` accepts out-of-order **input bytes** only when they form one
complete contiguous chain. Missing source/history/assignment predecessors and
different bytes at one stable event ID make the entire view unavailable.
Exact duplicate bytes, including those disclosed under new endpoint/docs keys,
collapse to one event. A valid signature is not proof that no unseen fork or
newer event exists; this is not a latestness/completeness oracle. Fixture
creation serializes the chain explicitly, including runner reports. No
distributed sequencer, live multiwriter admission or scheduler is introduced.

`publish_domain` validates the complete proposed history before publication,
and flushes existing local intents first so a local tombstone/conflict refuses
new publication. Exact signed bytes travel as the artifact of the existing
scoped inert record, using the existing body/hash/size verification, leased
owner membership, endpoint/author checks and publication/receipt journals.
`receive_domain` first retains the scoped disclosure, then validates the entire
domain history. An invalid domain disclosure remains inspectable inert evidence
but returns an error and makes the view unavailable, **not** a partially accepted
effective state or a record to execute. Friendly export validation is not
trusted: tests send a conflicting signed event through generic `publish`.

`domain_history` repairs the existing derived received SQL from receipts under
current policy and builds a pure historical view from complete verified receipt
and local publication bytes. There is no new authoritative SQL table or schema
migration. Losing derived SQL remains repairable; missing authority/journals,
current revocation, quarantine and byte conflicts retain Increment 4's refusals.
Old transport receipts must still be currently authorized after rotation;
new authorized copies do not launder a revoked old receipt.

**Historical, not effective runtime state:** exported bytes and `Memory.active`
describe only the validated historical fixture prefix. A transport tombstone
blocks fresh disclosure and local publication repair; already disclosed receipts
are not erased. This boundary cannot prove remote tombstone continuity,
undisclosed later history, real source provenance or owner-global latestness.
It must not be used to reconstruct effective runtime memory/jobs or re-export a
supposed complete application checkpoint. Where those proofs are required the
answer is **unavailable**. No FTS, memory promotion, task queue, provider, send,
credential or executable runtime table is wired to this view. No database file
is replicated or private runtime state read.

New tests cover memory promotion/correction/latest undo, no peer promotion or
spent-ID resurrection, source/predecessor gaps, exact job attribution and
transition refusals, owner/scope/namespace substitution, forks, signatures,
canonical-byte rejection and the exact 4096-byte escaped envelope boundary.
A scoped loopback test checks covering tombstones refuse domain disclosure and
new publication without adding an intent. The public `domain_transfer` check
uses **five fresh processes**: actual scoped transfer with an initially missing
predecessor, source-backed memory history and runner-signed job completion;
quarantined reopen and SQL-derived rebuild/replay; new endpoint/docs-author
transfer of byte-identical events with unchanged stable identity; malicious
fork, peer-promotion and owner-binding refusal; then restart with persisted
conflict/revocation refusal. Existing publication, tombstone, recovery and receipt
tests remain required. These are same-host synthetic drills, not multihost,
power-loss, runtime export or review approval.

**Next serialized increment:** manual owner-authorized core/runner epoch
fencing and a concrete proof of old-core external credential exclusion. Do not
activate a replacement if exclusion cannot be proved. Only after that gate:
runtime scope/domain adapters and quotas/multihost, then remote Codex and
packaging. No later gate is completed by this increment.

## Increment 4: durable scoped receipt and projection recovery

**Implemented for synthetic inert records, unreviewed, not deployed.**
Owner Leo's explicit 15 November 2026 21:34 deferral of the blocked independent
backup-recovery review permits development only. Increment 3's review debt
remains **deferred, NOT approved**; rejected reviews must not be retried or
reworded to bypass provider controls. The prior 299-test implementation snapshot
and evidence remain retained outside this increment.

Inspection found these concrete windows: publication already commits its SQL
intent before protocol writes, and reconciles both pending and acknowledged
rows; reception previously had only its final SQL projection. Loss of that
projection had no independently retained replay authority. A duplicate compared
only the saved hash column, so divergent body/artifact bytes could go unnoticed.
Ordinary scoped reopen could also silently initialize missing protocol or
authority files. These are not failures that a whole-checkpoint hash can repair.

### Authority and ordering

| Synthetic record/state | Authority | Repair or refusal |
| --- | --- | --- |
| Local publication, pending or acknowledged | Existing FULL-synchronous `projection.sqlite.publication` immutable intent (body and artifact) | Existing `flush` verifies every row, repairs blobs and individually absent metadata, commits both protocol barriers before acknowledgment |
| Received historical disclosure | New local `record-journal.sqlite.receipt`, bound to endpoint, owner and namespace | Current-policy scoped receive verifies bytes, commits complete receipt first, then derived `received` transaction |
| Received SQL projection | Derived `projection.sqlite.received`, not authority for lost receipts | Explicit `repair_projection` validates all retained receipts and current source-endpoint/author access, then atomically replays missing rows |
| Membership/high-water/identity | Existing local policy SQLite authority plus independently current owner input | Missing file or authority table refuses reopen; data journal cannot mint or roll back authority |
| Protocol deletion history and author keys | Existing docs store | Entire missing docs/default-author refuses reopen; no reconstruction from receipts, fresh namespace or author replacement |
| Artifact/body availability | Publication intent or verified scoped disclosure receipt contains original bounded bytes; blobs are separate protocol content | Sender may repair from intact intent; receiver may explicitly re-fetch through current scoped serving boundary. Missing bytes/conflicting history refuses, never metadata-only projection |

The receipt journal uses the existing rusqlite dependency, DELETE-journal FULL
synchronous commits and a synced initial directory entry. No database file is
replicated. The namespace is fixed by the journal binding; each immutable
author/key receipt retains source endpoint, body hash, complete body and artifact.
It is trusted local history established at the existing scoped receive boundary,
not a remotely supplied receipt, signature scheme or independent freshness oracle.
Its whole-store backup is included in the existing exact recovery manifest.

Projection insertion follows receipt commit. A failure or immediate process kill
between commits leaves a replayable receipt, not a partially projected record.
Both duplicate reception and replay compare hash **and complete body/artifact
bytes**. Conflicting SQL or journal rows fail closed without replacement.
SQL-ahead rows without receipts make replay explicitly unavailable; authorized
re-fetch may establish a matching missing receipt, but SQL alone cannot.
Replay validates the whole projection and journal within one SQL transaction,
rolling back all insertions on any conflict, corruption, lost authority or expired
lease. It checks current endpoint/author access, not an old disclosure's policy.
Revocation and restart quarantine therefore fence repair.
It never installs policy or activates recovery.

Derived `received` table loss is rebuildable with policy/identity and receipts
intact. Whole application SQL loss, missing publication history table, missing
docs deletion history, absent blob database or missing receipt journal is
**unavailable**, before mutable engine opens or missing-file recreation.
Known individual blob loss in an otherwise intact database is repairable only
from verified retained bytes, or explicit current-policy scoped re-fetch.
Individually missing publication metadata is repaired only from validated
publication intents with the existing covering-prefix/exact deletion checks.
Complete docs loss cannot prove absence of tombstones and is not repaired.
Ordinary reopen still requires caller-established intact authority continuity;
a present but wholly rolled-back database is not detectable locally and requires
the independent `restore` contract.

Historical disclosure receipts are not effective application state. Replaying
already disclosed inert history neither recreates docs entries nor reactivates
deleted memory/jobs nor authorizes a send. Existing projections remain locally
inspectable after revocation, as before. A docs tombstone prevents renewed
publication/disclosure; it cannot erase history already disclosed. Effective
runtime memory supersession/undo and job transitions require future runtime
adapters. Increment 5 adds only a synthetic historical domain validator, not
those adapters or any runtime execution path.

New unit tests cover receipt-before-projection failure, SQL behind/ahead,
byte-level conflicts despite matching hash, journal corruption and binding
mismatch, missing authority files/tables without recreation, revoked-author
replay, scoped missing-blob refusal/repair, and covering tombstone refusal.
The new public-boundary `projection_recovery` test uses four fresh processes:
external immediate SIGKILL after a durable receipt and failed SQL commit;
restart quarantine and atomic replay failure/success; derived table loss and
repeated replay with maximum-size artifacts; divergent/ahead SQL, revoked replay,
and whole SQL/docs loss refusal. Existing publication crash, acknowledged
metadata repair, prefix deletion, membership and anchored recovery tests remain
required. These are same-host process-crash tests, not power-loss or multihost
verification.

Older synthetic stores without a receipt journal are not silently upgraded.
There is no production store migration here. Without trusted history, reconstruct
nothing; keep old evidence and initialize new isolated fixtures for development.
Next serialized gates remain stable owner-bound event origins/device rotation,
application-domain export/transfer and transition validation, manual core
transfer/runner epoch fencing, old-core external credential exclusion, runtime
scope adapters, quotas/retention, host-level restore and multihost drills.
Do not deploy this or other unreviewed networking code to the live bot.

## Increment 3: exact backup adoption and independent authority

**Implemented, independent review deferred by owner 15 November 2026 21:34,
NOT approved.** This is a bounded synthetic
whole-backup restore contract, not application recovery or a latest-policy
service. It adds no runtime integration, configuration, credentials or network
discovery.

### Restore is not ordinary reopen

`ScopedPeer::reopen` means an ordinary restart of stores whose intact continuity
the trusted local caller has established. It is **not** backup adoption. A caller
with a copied, rolled-back, replaced or uncertain store must use
`ScopedPeer::restore`, even if every saved signature remains valid. No local
algorithm can distinguish a whole old backup from that store's earlier genuine
state. Misclassifying a backup as an ordinary restart is outside this contract;
the API cannot detect or prevent a malicious local operator deleting its latch.
The legacy static-pair `Peer` is not an alternative opening path.

`RecoveryManifest::capture` locks a **stopped** synthetic store. Its manifest
binds owner, persistent store endpoint identity, namespace, exact synthetic scope,
accepted policy generation, BLAKE3 policy-byte hash, and an exact backup inventory
hash. Store identity here is stable across synthetic restarts, not across device
key rotation and not an application core ID. Inventory hashing uses the already
pinned BLAKE3 implementation, streaming file content with a 64-KiB buffer.
Sorted relative paths, directory entries and every regular file's content hash
are included. Symlinks/special files are refused. The writer lock and the two
recovery-latch filenames are excluded, not application/protocol data. SQLite,
docs metadata, default author and blob database must all exist. Capture does
not sign, establish latestness, validate application history or repair anything.

The owner/operator must independently establish the current policy **and**
the intended exact data checkpoint, retaining the manifest hash through a
trusted channel **outside the rollbackable backup**. This is unavoidable
external input. A self-contained signed backup, new signature over stale data,
wall clock, first peer, or peer majority cannot supply it. The existing
owner-signed policy still authenticates the policy; the independent manifest
hash pins the exact recovery input. There is no custom signature scheme,
new service, or new dependency version (BLAKE3 1.8.7 was already locked).

`restore(..., Some((&manifest, independent_current_hash)))`:

1. Takes the same exclusive writer lock that will be retained by the peer.
2. Durably latches quarantine using synced temporary file, atomic rename and
   parent-directory sync, **before** checking recovery input or opening mutable
   storage engines. An interrupted temporary latch also means quarantine.
3. Refuses absent input, manifest/hash disagreement, owner/store/namespace/scope
   mismatch, missing stores and any exact checkpoint mismatch. No data replay,
   erasure, resurrection or missing-store reconstruction occurs on this path.
4. Persists a data-verification receipt and opens the peer **still quarantined**.

Authority is a separate call:
`install_recovery_policy(&signed_policy, &manifest, independent_current_hash)`.
It requires the independently supplied current hash again, the verified
checkpoint/identity, and the **exact** generation/policy hash bound by that
manifest. The existing owner signature, lease/scope checks, saved generation,
equal-generation fork refusal and commit-before-activation ordering still
apply. Failed activation cannot enable connections, publication or projection.
After activation, normal higher-generation policy updates and live revocation
remain available. Failure to update an already-active policy retains the prior
committed policy, as before; a failed update is not a successful revocation.

The receipt never acts as a freshness oracle. Every later reopen of a restored
store starts quarantined and refuses ordinary `install_policy` activation.
Independent recovery input is required again. If a newer policy was durably
accepted, an older recovery manifest cannot lower that high-water mark. A
separately trusted newer manifest may bind the same previously verified recovery
checkpoint to that newer exact policy. This authority reactivation assumes
ordinary intact continuity since data verification. A new data restore must
instead use `restore` and recheck its whole checkpoint before opening stores.
Storage-engine opens can change physical files: recapture a stopped snapshot
for a new data backup rather than reusing a no-longer-matching inventory.

**Trust limit:** if the supposedly independent latest hash is itself stale,
rolled back alongside the backup, or dishonestly supplied, this module cannot
discover that fact. Hash agreement proves input identity, not global latestness.
If current external input is unavailable, leave the store quarantined. Local
inspection of already disclosed inert projections is not remote disclosure or
erasure.

### Checks and remaining gates

Three new loopback tests cover whole generation-2-to-1 rollback, a still-valid
old policy, revoked-author requests with zero response chunks, revoked endpoint
reconnect refusal, missing/stale/mismatched inputs, identity/scope mismatches,
missing SQL/docs/blob files without recreation, exact policy binding,
equal-generation forks even with a separately pinned fork manifest, interrupted
latch writes, failed policy transactions, restart quarantine, monotonic
acceptance, positive authorized exchange and duplicate projection.
The new public-boundary `recovery_restart` integration test uses four fresh
processes: capture backups/revoke; reject rollback and inject activation failure;
reopen quarantined and activate independently; reopen and repeat with inert
duplicate projection. Its independent-input fixture lives outside the copied
backup, emulating, not implementing, the owner's trusted channel.

R1 publication barriers/acknowledgment ordering and R2 covering-prefix tombstone
checks are unchanged and retain their existing regressions. The exact-checkpoint
refusal is **not** a pending-publication/projection store-loss repair drill.
Increment 4 above adds bounded synthetic receipt replay, SQL behind/ahead checks,
blob refusal/repair and explicit unavailable classification for missing
authoritative stores. Power loss, domain application history reconciliation and
authoritative-data reconstruction beyond retained synthetic journals remain gates.
No full application-state transfer, stable runtime origin/device rotation,
manual core transfer, runner fencing, old-core credential exclusion, quotas,
multi-host or replicated-state completion claim follows.

## Increment 2: owner policy and scoped record boundary

`synthetic::membership::ScopedPeer` is a separate, opt-in synthetic boundary.
The original `Peer` below remains the static-pair transport fixture, **not**
an alternative membership API. Do not open a scoped store through that fixture.
No application configuration or deployed executable uses either API.

### Why native shared capabilities are not the boundary

Inspection of the exact cached pins established:

- `iroh-docs::net::handle_connection` has a namespace acceptance callback, but
  namespaces/read capabilities do not express endpoint-bound record scope.
- `BlobsProtocol` forwards the whole store to the standard provider. A known
  hash is sufficient to request its content; there is no record-policy callback.
- Gossip can originate background connections, so wrapping only incoming
  protocol handlers or public `connect` calls would be insufficient.
- iroh 1.4.1 publicly exports `EndpointHooks`. `before_connect` intercepts
  internal outgoing connections, and `after_handshake` intercepts both
  directions before returning a connection to the application. Weak connection
  handles support closing established connections without keeping them alive.

The concrete resolution is a dedicated `/coconutclaw/synthetic-record/1` ALPN.
**Native docs, gossip and blob protocols are not registered** on a scoped peer;
the endpoint hooks additionally deny those ALPNs in both directions. Retained
namespace capabilities or known hashes cannot bypass the record boundary.
Docs and blobs remain local durable publication stores, using the reviewed R1/R2
handoff and tombstone protections unchanged. There is no new crypto primitive,
dependency, automatic metadata reconciliation or shared-document wire access.

Each scoped store binds one owner public key, one namespace and the existing
`synthetic/chat-a` scope. It cannot serve arbitrary scopes, namespaces, blob
hashes or filesystem paths. A request names one immutable author/key. The server
reads only that namespace, validates the record and independently hashes its
bounded body/artifact before responding. The client validates both before one
FULL-synchronous SQLite projection transaction. Transport authenticates the
serving endpoint; the owner policy binds its permitted docs authors. This wire
format does not forward or independently verify a docs entry signature.
No store, raw endpoint, Doc or capability is returned by the scoped wrapper.

### Authority, freshness and revocation

An offline owner signs a complete snapshot using the pinned iroh key library's
Ed25519 signing and strict signature verification. Domain-separated signed bytes
bind namespace, exact scope, positive generation, expiration, member endpoints
and each endpoint's unique permitted authors. Membership is never inferred
from a QUIC handshake. Synthetic limits are 32 members, four authors per member
and a lease no longer than one day.

Snapshots are provisioned explicitly through a trusted **local** API on each
peer, not accepted from arbitrary remote peers. SQLite durably pins owner and
namespace, highest generation, exact policy bytes and observed wall time before
activation. Lower generations and different equal-generation snapshots fail,
including after restart. A failed policy transaction cannot activate access.
Unknown, invalid, wrong-owner, expired and wrong-scope snapshots fail closed.
An absent member/author is revoked. Reinstalling a lease cannot extend its
active monotonic deadline. A trustworthy local wall clock is required across
process restarts; observed clock rollback rejects installation.

Every policy installation closes all tracked connections before returning,
including connections to members retained in the new generation. Requests and
responses must use the current exact generation. Expiration closes established
connections on a monotonic timer. Authorization is checked again before sending
and before projection. Closing a connection fences subsequent traffic; bytes
already disclosed or handed to the transport before revocation are not erased.
Projection remains inert, never jobs, memory promotion, prompts or sends.

**Ordinary-reopen freshness limit:** a signature proves authority, not that no newer policy
exists elsewhere. There is no remote latest-policy oracle. Every reopen starts
quarantined, even with a saved valid policy, until the trusted local caller
installs independently established current policy. With an intact high-water
mark, stale policy replay is rejected. A wholly rolled-back backup also rolls
back its high-water mark. Increment 3 adds the explicit `restore` contract with
an independently trusted manifest hash; use that path for backup adoption, not
ordinary reopen. Automatically establishing global latestness remains impossible
without independent trusted input, not a property of signatures or reopen tests.

The sole enabled wire operation returns a bounded JSON bundle (65536 bytes
maximum; original 8192-byte body and 4096-byte artifact limits unchanged).
It handles one request per connection with a ten-second deadline. Wire format,
policy limits and lease duration are synthetic choices, not runtime adapters,
quotas, a general policy framework or a production protocol commitment.

### Behavioral checks

Six new tests exercise actual loopback connections: valid record/artifact
exchange and duplicate inert projection; wrong owner and tampering; unknown
policy with a raw client; rollback and equal-generation forks after reopen;
quarantined reopen; stale author replay at the server even when the caller
bypasses its local predicate; active partial-request revocation and reconnect;
lease expiry; forbidden scope/namespace/stale generation; raw-client native
docs/gossip/blob negotiation rejection despite retained identifiers; and
injected SQLite policy commit failure. Rejected record requests are checked for
**zero response chunks**, not merely failure to project. Positive exchange
after rejected requests ensures that refusal is not an inert implementation.
Scoped `flush` additionally repairs a pending publication after reopen with
the original write capability and current policy, while refusing repair during
quarantine and over a persisted covering deletion.
The existing crash, acknowledged repair and covering-prefix deletion tests
remain required and unchanged. These are same-host synthetic checks, not
multi-host membership, owner backup restore or full replicated-state completion.
Increment 3's separate checks above cover its bounded synthetic restore contract.

## Selected components and evidence

| Component | Exact direct pin | Enabled features |
| --- | --- | --- |
| iroh | 1.4.1 | tls-ring |
| iroh-docs | 0.101.0 | fs-store |
| iroh-blobs | 0.103.1 | fs-store |
| iroh-gossip | 0.101.0 | net |

Default features are disabled for all four. Their published manifests declare
Rust 1.91; development here uses 1.98.1. Cargo.lock supplies checksummed
transitive pins, including redb 4.6.0 and Tokio 1.54.1. The docs manifest requires
iroh `^1`, blobs `^0.103`, gossip `^0.101.0`. These pins have now compiled and
exchanged synthetic data, not merely passed manifest inspection.
The dev dependency additionally enables iroh's `test-utils` for the official
loopback relay fixture, not for the public library or CLI.

Ordinary `cargo info ...@...` and `cargo fetch` succeeded in this environment.
The first offline lookup failed because docs was not cached. No global
configuration, auth or registry changes were necessary. Subsequent builds/checks
use `--locked --offline`. Nine preexisting single-version packages moved because
of the selected dependency graph; attempted offline precise downgrades and
their concrete constraint failures are retained in the dependency receipt logs.

Official source evidence is the downloaded, Cargo-checksummed crate source:

- docs `examples/setup.rs`, `tests/util.rs`, `src/protocol.rs`, `src/api.rs`:
  router registration, persistent engine and read-only capabilities;
- docs `src/store.rs`: download policies do not bound by content size;
- blobs `src/protocol.rs`: bounded byte-range requests rounded to 1024-byte chunks;
- blobs `src/net_protocol.rs`: router shutdown already shuts down the blob store;
- iroh `src/endpoint/presets.rs`: Minimal avoids N0 relay/discovery defaults.

The earlier [roadmap sources](roadmap.md#sources) remain the published deployment
evidence, not newly refreshed web evidence. The cached blobs README explicitly
says this release is **not yet considered production quality**. Core iroh 1.x
does not change that protocol-layer warning.

## What runs

```sh
cargo test -p coconutclaw-sync --locked --offline
```

Tests create only fresh temporary directories, synthetic identities and
loopback listeners. Existing directories cannot be adopted by `create`.
`reopen` requires the synthetic marker, the same endpoint identity and an
exclusive file lock. The marker prevents accidental use, not malicious local
filesystem access. Endpoint secrets are caller-supplied, never serialized or
logged; tests use public, deterministic fixture keys that must never be used
outside these synthetic stores.

Each peer registers docs, blobs and gossip behind an authenticated endpoint-ID
allowlist. This static pair pin is **not membership, revocation or owner
authorization**. A read-only namespace capability prevents receiver writes.
The publisher must explicitly open its document after restart to activate sync.
Only explicit loopback IP addresses are accepted by public outbound methods;
relays, public discovery and port mapping are disabled there. A private
unit-test-only override permits exactly the fixture's local relay URL.

A synthetic record contains a version, docs-author-qualified monotonic SQLite
ID, fixed synthetic chat scope, at most 2048 UTF-8 text bytes and a separate
artifact hash/size. JSON is bounded to 8192 bytes; artifacts to 4096 bytes.
Unknown fields, scope/origin/key mismatches and invalid bounds are rejected.
The receiving SQLite table is inert: it is not runtime memory, provider context,
an executable job, a scheduler or a delivery queue. Marker-looking text has no
authority. There is no received-text promotion path.

## Store ownership and handoff

- `projection.sqlite` is the authoritative synthetic publication journal and
  local policy high-water store, with FULL synchronous transactions. Its
  `received` table is inert history. Scoped peers now derive that table from
  `record-journal.sqlite` as described in Increment 4; the legacy static-pair
  fixture still has only its original received projection.
- `docs/docs.redb` owns protocol metadata, namespaces/capabilities and authors;
  `docs/default-author` identifies the persistent default author. It is not SQL.
- `blobs/` owns separate blob metadata, content and verification state.
- `writer.lock` excludes a second cooperating store owner. It is not a distributed
  fence. No SQLite database file is replicated.

Publication commits bytes and artifact in a SQLite intent before adding blobs or
writing docs metadata. Flush reconciles **all** intents for the document, including
already acknowledged rows left by the original buffered-write ordering bug.
An indexed keyset scan reads one bounded intent at a time, with no fixed total
repair cap or persistent cursor that could skip a lost entry. Work is linear in
that document's retained history (indexed seeks per record); it is not a
production retention/quota design. A conflict stops the scan and returns an
error, rather than silently skipping the conflicting row or later history.
When an exact entry is absent, reconciliation checks every proper byte prefix
of its validated event key, including the empty prefix, for deletion markers
in the same namespace and author. Exact lookups alone cannot see covering
tombstones in the pinned API. This costs at most one indexed lookup per key
byte on the missing-entry path, independent of namespace metadata volume, and
does not cap historical repair. Existing matching entries are not recreated.

For each row, flush checks existing metadata (including deletion markers), adds
both original body and artifact, awaits blob `Store::sync_db()`, sets missing
metadata, awaits docs `SyncHandle::flush_store()`, then acknowledges SQLite.
Pinned `Docs::persistent` hides the sync handle, so the library constructs the
same persistent `Engine` through public APIs and retains its sync handle.
No persistence sleep or shutdown is used. Both barriers propagate failures;
an interrupted pre-ack handoff remains replayable from SQLite.

The blob database barrier commits **inline** data and outboards, not arbitrary
external blob files. The store explicitly configures 8192-byte inline data and
outboard thresholds; the existing record (8192) and artifact (4096) limits put
every supported publication and its Bao outboard inside that database. These
limits are unchanged, not a new truncation or weakening of the publication
contract. Increasing them requires revisiting durability. Reconciliation also
reimports both blobs when metadata exists, repairing older buffered blob loss.
No GC is enabled; temporary tags remain held through acknowledgment.

A crash between protocol commit and SQLite acknowledgment cannot replace the
event or duplicate its projection. Matching entries are not rewritten, preserving
their signed timestamp; different hash/length or a tombstone stops with an
immutable conflict rather than overwriting history or resurrecting a deletion.

Automatic content downloads are disabled. Explicit range requests are bounded,
including when metadata understates a blob's real size. Bao verifies downloaded
ranges; complete-store size and an independent full BLAKE3 rehash must agree
before parsing or projection. Artifact verification is also required before
the one SQLite projection transaction. No metadata-only projection occurs.
Duplicate reception after commit is a no-op; conflicting immutable history is
an error, not a memory correction.

These tests cover restart with a pending publication; replay after a lost
publication receipt; SQLite insertion failure before commit; receiver
disconnect with an event created offline; persistent peer reopen; duplicate
projection; malformed/oversized/dishonest-size content; forged scope/origin;
unapproved author; namespace mismatch; read-only write rejection; and unpaired
incoming connections on all three protocols. A separate integration test also
creates/exchanges/shuts down both stores in one child process and reopens them
in a fresh process, verifies the previous projection/author/sequence and
exchanges a second event. `publication_crash` additionally reuses independent
review R1's public-boundary recipe: clean initialization, fresh-process reopen
and publication, external immediate SIGKILL after successful flush, then
fresh-process reopen of intact stores. Three trials check metadata **before**
replay, fetch body and artifact before SQL can reconstruct them, and project
exactly once across repeated replay. Another public-boundary test repairs three
already acknowledged metadata-less intents, reopens and transfers them, and
rejects acknowledged body conflicts and tombstones unchanged. Both regressions
failed against the pre-fix implementation. No delay is a persistence barrier.
These are same-host process-crash tests, not power-loss or two-host recovery
drills.

The public-boundary `prefix_deletion` regression also persists deletions before
reopening in a fresh process, rejects repeated replay, and reopens again with a
fresh read-only receiver. It covers three events under author-qualified,
partial-byte and empty prefixes, plus an exact-key deletion control whose
uncovered events still transfer and project exactly once. Covering deletions
remain absent and cannot project. This regression failed against the R1-fixed
source before the prefix check was added.

## Application record map, next increment contract

This table is the intended application map, **not implemented runtime export**.
`store-id` must be a stable owner-bound core/store identity; local SQL row numbers
alone are not portable. Immutable event IDs must survive device-key rotation.
The original synthetic author-qualified scheme tests transport without meeting
that rotation requirement. Increment 5 wraps it with owner-bound signed fixture
events whose application IDs/bytes survive endpoint/docs-author rotation; it
does not implement runtime export or application signing-key rotation.

| Domain / immutable event key | Scope / authorized writer | Authoritative state / receipt |
| --- | --- | --- |
| `membership/<owner>/<generation>` | Owner-signed policy, authorized recipients only | Core SQLite policy high-water mark; future scoped snapshot distribution |
| `jobs/<store-id>/<job>/<attempt>/<event>` | Origin chat and assigned runner; core assigns, exact attempt reports | Core SQL assignment/delivery; runner SQL attempt/result; inert remote projection |
| `messages/<store-id>/<turn>` | Explicit origin chat grant; core writes | Core SQL turns; redacted opt-in records, no polling cursors |
| `memory/<store-id>/<event>` | Origin chat; core alone promotes/corrects/undoes | Core SQL immutable history/effective state; FTS derived; runner proposals stay untrusted |
| `artifacts/<store-id>/<record>` | Same readers as referenced job/chat; no sender-local paths | SQL descriptor/receipt; separately verified blobs retained by explicit references |

Credentials, owner/device secret keys, Codex sessions/auth, Telegram tokens,
delivery credentials and raw unreviewed transcript exports stay local. Job
arrival must never invoke the current scheduler. Attempt ownership needs its
own validated domain transition, not a docs author or QUIC handshake.

## Narrow docs fit check

Docs works for this immutable event transport and persistent reconciliation.
Concrete blockers to using it directly as application authority:

1. Namespace read/write capabilities are not per-peer, per-chat membership.
   Standard blob service serves known hashes to allowed endpoints without
   per-record scope checks. The static one-pair/synthetic-store boundary here
   must not become a multi-scope production boundary.
2. Docs timestamps allow replacing a key. Domain history and revocation cannot
   use "latest doc entry wins"; use immutable events plus SQL transition checks.
3. Download policy filters keys, not sizes or application authorization.
   Explicit verified bounded fetching is required.
4. redb, blob state and SQLite cannot commit together. Replayable journal
   handoff is required, including loss/restore tests beyond the modeled seams.

Resolve those directly before runtime wiring. No alternate-protocol comparison
is reopened. Production needs per-scope disclosure enforcement at *all* three
protocol boundaries, retention/GC pins and storage/metadata quotas; a trusted
but faulty peer can otherwise exhaust disk with metadata or many bounded blobs.
No automatic blob GC is enabled in this increment.

## Recovery and core transfer, required next

The scoped increments above test owner-signed snapshots, live revocation and
explicit independently anchored synthetic backup adoption, not application
membership roles or core transfer. Remaining application design direction,
**not implemented application recovery authority**:

- Separate offline owner signing authority from device endpoint/docs keys.
  Back up owner secret offline plus a signed recovery manifest containing stable
  store identity, latest policy generation, active core/epoch, revoked identities
  and durable state checkpoint hashes. Never replicate the secret.
- Grants bind device key, role, scopes, permitted authors and policy generation.
  Each receiver durably retains the highest accepted owner generation and
  revocation set. Data authors cannot grant membership. A revoked author's
  replayed old grant/event cannot reduce that generation or restore access.
- Revocation must close established connections as well as reject new ones,
  update projection policy, and rotate namespace/read capabilities where a
  revoked device retained them. It cannot erase content already learned.
  Restoring an old policy backup must not silently roll back revocations.
- Recovery requires an independently trusted latest signed policy/high-water
  mark, not just a stale backup or whichever peer connects first. If freshness
  cannot be established offline, remain quarantined. Test actual owner signing,
  device revocation, old-author replay and revoked-device backup restore.
- Core transfer is manual: stop and operationally exclude the old core, reconcile
  unknown attempts/effects, obtain an owner-signed higher epoch bound to the new
  core, persist the fence on all trusted runners, restore and verify application
  and protocol checkpoints, then enable the new core. No automatic failover.
  A partitioned or malicious old core holding Telegram credentials can still
  send effects; an application epoch is not external credential fencing.
  If old-core exclusion cannot be proved, do not activate the replacement.
- Increment 4 exercises synthetic pending/acknowledged publication and receipt
  replay, missing-store refusal, blob repair, SQLite behind/ahead and stale
  membership. Extend these to domain application-state transfer and effective
  memory/job transitions, with exact SQL consistency and no resurrection of
  undone/promoted memory or execution. Synthetic historical receipts cannot
  establish those runtime guarantees.

The scoped increment above implements bounded live connection revocation;
the static-pair fixture does not. These drills, runtime scope adapters, stable
owner-bound origins, broader process-kill/power-loss tests and retention limits remain open. No
replicated-state completion or owner-recovery claim follows from fixture keys.

## Relay and discovery

Prototype selection: explicit loopback direct addresses, Minimal preset,
RelayMode::Disabled, no address lookup. Direct QUIC is exercised deterministically.
The unit suite also runs the official in-process `iroh-relay` fixture, bound to
loopback, with both peers' IP transports removed and only relay addresses.
The same record, separate artifact, verification and SQL projection path passes
through that relay. The fixture's self-signed TLS certificate is accepted only
by its private test builder; public library methods have no TLS bypass.
This proves forced-relay operation, **not** automatic path failover during an
active transfer, public relay availability or real NAT traversal.

Public development relays are not selected or contacted. Eventual deployment selection is an
explicitly configured owner-controlled relay and explicit paired addresses
(no public discovery initially), subject to separate approval for hosting, DNS,
TLS, reachability, maintenance and spend. A relay is not an offline record
mailbox. No production relay choice is approved by this document.
