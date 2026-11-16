# CoconutClaw revival plan — revision 9

16 November 2026 · Owner's 12:37/12:40 always-on/scheduling amendment supersedes
the previous scheduling exclusion. Packaging waits for the foundation gates below.
Revision6 added explicit Markdown import
after completed bounded memory and Telegram topics/parallelism/native rich delivery,
before networking and remote execution.

## Direction

Owner11:03 authorized Mars and Mitori multihost development acceptance. Runnable
dev-only coordinator/runner roles now reuse actual Store/scoped iroh/runner/
provider code and return a signed file artifact. The parent proved the offline
Mitori1.94.1 build and identical binary transfer to Mars; the first launch exposed
unsupported `kern.hostuuid`, now replaced by validated ioreg IOPlatformUUID.
The corrected actual1201 reached Mars native Codex, exit0 and the correct file,
but application acceptance failed Unknown because native0.167 custom tool events
were not recognized. Both native execution formats now require correlated output,
and Finish refusals retain bounded safe phase/details. Preserve failed1201 without
retry. Parent's correct read-only BLAKE3 comparison confirmed config/auth unchanged.
Fresh `hc1250` actual Mitori→Mars receipts now prove original Store Completed,
the exact138-byte nonce/sum42 artifact, one invocation and full restart/exact
signed-event/artifact replay. Both roles stopped. Later packaging only adds
diagnostic/test/documentation corrections, not a new native result algorithm.
See [the exact handoff](multihost-acceptance.md). No production
networking flag, review approval, Phase2 completion or activation is implied.

Trim the existing application first. Use Codex for both conversation and delegated work. Adopt iroh as the networking direction, using published implementation/deployment evidence to avoid a separate comparison project. Keep local acceptance checks focused on CoconutClaw's integration.

The previous iroh-versus-p2panda experiment is removed. Phase 0 is the actual reduction of the product; Phase 1 is a short evidence, dependency and integration decision step. Neither phase includes building the complete distributed runtime.

## Product boundary

CoconutClaw owns communication, local memory and reliable delegation. Codex supplies the agent runtime. A local conversation session and a delegated task are different execution contexts, even though both use Codex.

One active core owns inbound messages, conversation ordering, shared-memory promotion and assignment. Several trusted runners execute assigned work. A machine may perform both roles. No automatic core failover initially.

## Always-on foundation, ahead of final packaging

The owner's amendment adds durable coordination, not a resident model loop or a
new runtime/framework. [The concrete boundaries and policies](inbox-scheduling.md)
define Event (deduplicated input), Session (conversation ordering/history), Task
(logical requested work) and Run (one execution/continuation). One core orders
each conversation; independent topics/conversations retain bounded concurrency.
Waiting/approval state belongs in SQLite and releases execution slots. Resume by
an event, never an unattended model loop. Keep instructions/preferences, factual
memory, structured task progress and notification history distinct.

Dependency order:

1. Reuse task request persistence, approval state, bounded lanes, recovery and
   outcome+outbox. Make request/result/approval consumption idempotent.
2. Atomically admit scheduled occurrence identity through that inbox. Rust/SQLite
   one-offs and IANA-zoned daily/weekly recurrence reuse existing chrono/chrono-tz;
   selectable fold/gap/missed policies with conservative legacy defaults.
3. Keep dispatch outside model slots. Preserve ambiguous model/effect/delivery
   outcomes without retry; failed/unknown scheduled work blocks future runs.
4. Extend the same owner to all raw Telegram commands and actual remote runner
   results, general waiting/resumption and independently runnable task lanes.
5. Add explicit reconciliation UX, changed-blocker notification dedup and
   preference/progress/notification interfaces, then richer calendar rules and
   operational acceptance. Only then return to final packaging.

**First useful slice implemented:** normal task-request inbox (including Telegram
model turns), local worker outcomes, approval continuation/rejection; explicit
local schedule CRUD, atomic occurrence+inbox+task+cursor, pause/resume/cancel/edit,
restart recovery, batch32 coalescing and no overlapping unresolved runs. Existing
deadline/event intake wakes calendars independently of Telegram fetch/retry waits. A bounded outbox
dispatcher frees model lanes before network delivery. Historical schedule rows
are never activated or imported. No actual user jobs/messages were created.

**Next finite slice implemented:** [durable raw Telegram ingress](telegram-ingress.md).
Canonical full updates, control results and command acknowledgments now survive
restart in SQLite. Exact replay does not repeat controls or sends; conflicting
payloads refuse. The cursor acknowledges durable raw custody, not effect
completion. SQLite-only controls commit effects and reply outbox together.
Interrupted runtime controls remain Unknown unless existing receipts prove
normal task admission or an approval transaction's rollback. The same bounded
dispatcher and delivery plans handle command replies, with no fake model runs.
Conversation admission ordering and topic/actor authority are enforced; other
conversations keep their slots. Old KV/legacy effects are never silently retried.

The finite [verified runtime result bridge](runtime-results.md) now routes
existing scoped runner receipts through normal durable custody, original-run
projection and the single outbox owner. Production peer admission stays gated.

The finite [persisted waiting/task graph foundation](runtime-task-graph.md) now
links logical tasks, physical attempts, sessions and triggering events. Approval
and same-session dependency waits release slots; distinct continuation attempts
advance one logical owner. Isolated task lanes share the five-provider cap and
retain origin conversation fences. Remote results still complete their exact
original signed run, never a manufactured continuation.

**Acceptance remains open:**
broader authoritative external-effect reconciliation, additional explicit wake kinds,
live product acceptance and packaging. Weekly recurrence and selectable calendar
policies now use the existing scheduler's bounded event/deadline intake, not the
Telegram polling cadence or another scheduler service.
Do not claim these bounded foundations complete always-on
coordination, scheduling, Phase2, review or activation.

**Owner's next-safe-handoff verification amendment (16 November14:55):**
[final source-wide formal and native-CI gates](final-verification-gates.md)
are mandatory after implementation and before packaging. Inventory explicit
and implicit machines across the final architecture, then source-link official
SANY/TLC safety, justified liveness and negative controls. Reuse existing native
Ubuntu/macOS/Windows CI, with critical portable lifecycle tests actually running.
Neither unfinished models nor dirty local bytes constitute that evidence.
The implementation-owned [finite formal gate](formal-verification.md) now maps
all repository module paths, including implicit combinations/platform branches,
to bounded source-linked contracts or explicit exclusions. Official SANY/TLC
safety/conditional-liveness checks and executable negative controls retain their
bounds, traces and refinement gaps. This is not arbitrary Rust proof or review.
Actual pinned hosted Linux/macOS/Windows results remain pending. Publishing is
not authorized; review stays DEFERRED NOT APPROVED.

The finite [authority-aware Unknown reconciliation](runtime-reconciliation.md)
slice now uses original-route commands and exact persisted sender receipts to
settle a known rich-text acknowledgment, with immutable audit/cause and no
provider outcome rewriting or resend. Unproven provider/control/progress effects
remain Unknown; no service nonoccurrence oracle or force-retry command exists.

The finite [context and notification separation](context-notifications.md)
foundation now provides authenticated durable preferences, scoped structured
task status and notification history. Core-derived changed-blocker identities
and decisions survive restart and share the existing delivery owner; repeated
unchanged observations do not create new notices. Facts and historical task
outcomes remain distinct and unmodified.

The finite [calendar/wake foundation](inbox-scheduling.md) now adds selected
weekdays, persisted selectable fold/gap/missed policies, backward-compatible
defaults and bounded calendar math. Its scheduler-owned Store/watcher/deadline
intake preserves occurrence atomicity and unresolved-run fences while waking
during blocked Telegram fetches or retry sleeps.

Dependency order now continues after the calendar and bounded formal slices:
actual source-pinned native-CI gate and any affected verification rechecks;
authorized acceptance/review, then
packaging. Scheduling remains in scope, never implicit actual user jobs.

## Phase 0 — Trim and establish a Codex-only baseline

### Keep

- Telegram only. Slack and Discord are removed from requirements.
- Conversation history and the existing useful memory substrate.
- Codex conversation sessions and Codex task execution.
- Durable tasks/results, progress, cancellation and delivery recovery.
- Minimal configuration, service lifecycle and doctor diagnostics.

### Remove from the supported implementation

- Droid/Factory, Claude, Pi, OpenCode and Antigravity adapters and their provider-selection configuration.
- Local vision orchestration, ASR/TTS pipelines and related configuration. Ordinary file/image forwarding remains where supported by Codex.
- Proactive heartbeats and nightly reflection. Historical general scheduling was
  cut in Phase0; the later owner amendment reintroduces only the explicit durable
  inbox/calendar capability above, never implicit legacy-row activation.
- Multiple named assistant-instance management.
- All Slack support and configuration.
- Dependencies, installation branches, tests and documentation used only by removed paths, after checking callers.

Do not replace these cuts with a plugin framework. Keep meaningful regression protection for retained behavior. Preserve user histories, memories and recoverable old configuration before removal; obsolete settings should produce a clear migration message rather than silently changing behavior.

### Codex runtime contract

Reuse the existing Codex integration first. Inspect current supported interfaces and authentication on the actual hosts. Prefer a maintained structured Codex interface when it materially improves sessions, events, cancellation and recovery; do not add ACP wrappers or a second model/tool runtime without a demonstrated need.

Maintain one conversation session per chat/thread and separate sessions/attempts for delegated tasks. Codex-only does not imply one process or one context shared across all work. Keep core bookkeeping and durable delivery outside model context.

A runtime conversation can request a delegation through a narrow coordinator interface. Choose its concrete mechanism from existing repository and Codex capabilities; avoid inventing a general tools platform. The result returns to the originating conversation with durable job identity.

### Sequence and gate

1. Inspect current instructions, checkout, configuration and data. Confirm the archived repository's revival path; preserve Git history and backups.
2. Establish a runnable baseline and resolve the previously observed missing C linker in the authorized development environment.
3. Remove feature groups in small verified changes, keeping a working Telegram/Codex path throughout.
4. Verify Codex conversation, a separate local delegated task, cancellation, restart recovery, persisted results and message delivery. Telegram is the only supported channel.

**Done:** a smaller Codex-only application builds, retained checks pass, removed settings have an explicit migration path, and the retained chat/delegation flow works. A cut list alone does not complete Phase 0.

## Phase 1 — Useful memory and state foundations

Prioritize this before networking and remote execution, as requested by the owner on 15 November.

Build and verify local bounded FTS5 retrieval, source-backed memory candidates, core-authorized promotion, supersession and undo. Use the existing Telegram/Codex path and SQLite; preserve restart behavior and avoid introducing a new runtime or queue dependency without a demonstrated gap.

Define explicit authorized record scopes and stable identities for memory, messages, jobs/results, membership and artifacts. Establish owner-key backup/recovery and revocation semantics before choosing how those records move. Keep secrets and agent authentication local. Replicate records rather than database files.

**Local completion gate:** source-backed recall, correction/supersession and undo work through the retained application path and after restart; record ownership/access and recovery semantics are documented and tested where locally enforceable.

**Deferred integration gate:** actual peer synchronization, peer revocation enforcement, owner restore/core-transfer drills and protocol-store/SQLite replay consistency require the Phase 2 network. Complete those checks immediately after the two-peer wiring, before declaring replicated state complete. Do not claim cross-machine verification from local tests.

## Phase 2 — Confirm iroh integration using existing evidence

### Decision

Use iroh. Stop the p2panda comparison. Review current official documentation, published deployment experience, examples and relevant known issues; pin compatible versions before integration.

Separate the choices:

- iroh supplies authenticated peer connectivity and relay fallback.
- iroh-docs is the default record-sync candidate, subject to a narrow fit check.
- iroh-blobs is the candidate for attachments and artifacts.
- SQLite remains application state/search. Explicitly account for iroh-docs' redb persistence and blob storage.

Do not build application membership, job ownership or exactly-once claims from networking guarantees. If the fit check finds a concrete blocker, document that blocker and resolve it directly rather than reopening a broad library tournament.

### Published evidence already found

- The official iroh 1.0 retrospective describes deployment of its networking/blob-transfer lineage in Delta Chat across desktop and mobile platforms. This supports choosing the ecosystem; it does not prove CoconutClaw's record synchronization or footprint.
- Official documents describe iroh-docs metadata reconciliation, authors/namespaces, persistent storage, and its blobs/gossip dependencies. Use unique event keys where immutable application history is needed; validate domain transitions separately.
- Current relay guidance says default public relays are intended for development/testing and recommends other arrangements for production. Record a self-hosted relay option and its operational needs. No paid-service commitment is part of this plan.

Avoid importing unattributed hole-punch success percentages or old throughput numbers as guarantees for our hosts and versions.

### Small required outputs

- A brief evidence/known-limitations note with exact selected dependency versions.
- A record map for membership, jobs/results, messages, memory and artifacts, with access scope and store ownership.
- An explicit choice of prototype and eventual relay/discovery configuration.
- A minimal two-peer check: connect, authenticate, exchange one record and artifact, disconnect/reconnect, and restart from persistent storage. Exercise relay fallback where feasible.

This check verifies our wiring. It is not a benchmark competition or a full distributed-failure research project. Owner-key backup/recovery must be designed here; full recovery drills belong with actual membership integration.

**Done:** selected compatible iroh components, documented storage/relay implications and a working narrow integration check. Published evidence can complete library selection quickly; it cannot substitute for this application check.

## Phase 3 — One remote Codex task

Connect the trimmed core to one trusted remote runner. Pair device identities, advertise minimal capabilities and workspace policy, assign a durable job/attempt, run Codex, persist the result and deliver it to the originating chat.

Test duplicate assignments, cancellation, restart and lost result delivery. Persisted acceptance is not exactly-once execution: reconcile the process/session after ambiguous crashes. Do not retry consequential work automatically when the runner's outcome is unknown.

**Done:** one real end-to-end remote task, inspectable result, cancellation and honest recovery/uncertainty behavior.

## Phase 4 — Release packaging

Complete Telegram setup, pairing, service lifecycle, backup and diagnostics. Measure actual release-build resource use and remove demonstrated overhead. Verify fresh-machine installation and recovery. No Discord or Slack implementation or channel abstraction is required.

## Immediate priority

The Phase 0 baseline, bounded memory, and Telegram topics on/off, five-worker
admission and native rich delivery are complete. The explicit one-time
[OpenClaw Markdown importer](openclaw-import.md) and activity-aware timeouts were
independently approved, committed in `0721155`, and deployed. The live bot remains
pinned to that activity-timeout release. Synthetic transport (`d78bd29`) and
membership (`d7fc98b`) were independently approved and committed, not deployed.
Preserve the completed disposable real cross-Mac task/restart evidence. Implement
the amended always-on inbox/scheduling foundation and its remaining acceptance
gates before final packaging, alongside the still-open Phase2 access/recovery
and activation boundaries.
No Slack, Discord or legacy runtime backend is added.

This revision changes the implementation order and scope. Phase 0 trim and real Telegram/Codex acceptance have passed; the baseline is closed in a separate main-branch commit (see Git history and docs/validation-phase-0.md). Only the isolated synthetic [Phase 2 increment](iroh-phase2.md) has networking; runtime networking and the replicated-state/access/recovery completion gates remain open. Repository changes must use the standing monitored Droid authoring workflow unless the user explicitly changes that workflow; Codex-only here describes the product's runtime/delegation backends.

The reviewed synthetic membership increment adds locally provisioned owner-signed, leased
membership with persisted policy high-water marks and live connection fencing.
Its scoped record ALPN deliberately replaces native shared-document/gossip/blob
wire access. The following uncommitted increment adds explicit exact synthetic
backup adoption, with a durable quarantine latch and independently current
manifest hash required separately from owner-policy signatures. **Owner Leo
explicitly deferred its blocked independent review on 15 November 2026 at 21:34.
Deferred is NOT approved.** This permits continued development only, not
deployment or a reviewed-completion claim. Do not retry rejected reviews or
alter prompts to bypass provider controls; preserve the 299-test recovery
implementation snapshot and evidence.

The next uncommitted synthetic increment implements a durable scoped receipt
journal and transactional inert projection replay after derived SQL loss.
Publication repair retains the existing pending/acknowledged intent journal,
blob barriers and prefix-tombstone checks. Missing policy/high-water or docs
deletion history is explicitly unavailable, not reconstructed. These increments
remain unreviewed and isolated from the executable. The next bounded increment
adds owner-bound stable fixture origins and signed canonical memory/job event
transfer over that same scoped boundary, with complete-chain transition checks,
duplicate replay, conflict refusal and fresh-process endpoint/docs rotation.
It is also **unreviewed, not deployed**, and only validates inert synthetic
history, not runtime export or effective-state activation. Unproven import
provenance, fresh re-promotion intent, higher attempts and remote tombstone
continuity remain explicitly unavailable. The next uncommitted increment adds
an origin-owner-signed manual epoch-1 suspension fence naming pending epoch-2
core/runner roles. It preserves fixed historical origins and refuses all
post-fence activity, stale actors, rollback and conflicting transitions.
Replacement activation is explicitly **unavailable**, even with synthetic
attestations and completed jobs: no operational external Telegram/Codex
credential exclusion proof verifier exists. Independent current fence checkpoints
reject stale historical prefixes but are not global freshness or execution
authority. This increment also remains **unreviewed, not deployed**.
The docs-only [external authority exclusion contract](credential-exclusion.md)
now maps actual effect paths and defines exact checkpoint/evidence binding,
freshness, Unknown reconciliation and restart refusal. It is **unreviewed, not
deployed** and supplies no operational proof. Service-native invalidation,
independent containment and replacement credential custody are the preferred
manual approach, conditional on verified service guarantees and complete holder/
tool coverage, not an invented proof provider. Real exclusion remains unavailable.
The next uncommitted [inert reconciliation ledger](reconciliation-ledger.md)
maps existing task/delivery states to bounded immutable observations and validates
exact fence/checkpoint/inventory binding, duplicates, conflicts and restart/
rollback with mock-only checks. It is **unreviewed, not deployed**, with no
activation/runtime wiring. Shape/binding checks prove neither evidence trust nor
freshness, complete inventory, semantic reconciliation or service enforcement.
The next uncommitted descriptor increment now binds exact redacted application
checkpoint/observation bytes to that ledger, with artificial SQLite interruption
fixtures, source/schema identity and malformed-link refusal. It is also
**unreviewed, not deployed**, inert and without real-store access.
The following [recovery-restart fixture correction](iroh-phase2.md#recovery-restart-fixture-identity-lifecycle)
proves that same-identity concurrent store copies could route to the wrong
holder. It isolates rollback-holder/client lifetimes before current-holder
restart, preserving all fail-closed assertions and prior failed receipts.
No sync/runtime implementation changed; this is **unreviewed, not deployed**.
The next synthetic inventory increment links recorded/unjournaled observations
to declared authority/resource and holder aliases, refuses malformed/conflicting
links and preserves unknown holders and coverage. Complete-looking declarations
cannot authorize retry or activation. It is **unreviewed, not deployed**, checks
base-ledger observations rather than application snapshot preimages, and adds no
runtime wiring or operational evidence.
The next uncommitted mock-only composition now reuses both inspectors under one
exact independently selected binding. It separates validated application rows
from opaque unjournaled observations, covers both with the inventory, and refuses
malformed/contradictory links without changing Unknown, history or activation
refusal. It is **unreviewed, not deployed**; no live collection, service proof or
runtime admission is added.

### Owner amendment, 16 November 2026 07:41

Continue useful isolated runtime integration toward one remote Codex task, not
an unbounded series of synthetic descriptor layers. Ordinary adapter development
with disposable data/mock executors is authorized now; it does **not** await
live exclusion evidence. This supersedes the preceding report's proposed next
descriptor gate and any earlier wording making operational exclusion a
prerequisite for isolated adapter code.

The next finite increment now bridges **actual application Store task rows** to
the existing owner-scoped domain/transport, compiled only in CLI tests. It
reserves a queued local text delegation, persists its exact signed assignment
before publication, exchanges it through the existing scoped peer boundary and
transactionally materializes one held recipient task. Stable owner/store/origin,
source run, delegation parent/index, assignment, runner and attempt 1 remain
explicit. Restart, duplicates, malformed/conflicting input, cancellation and
unknown-outcome refusal are tested with temporary Stores and loopback peers.
The recipient is inert `awaiting_approval`, not admitted runnable work; no
approval record or executable network flag is added.

The next finite increment now adds deliberate task-local mock execution:
durable signed Start before invocation, an at-most-once local execution claim,
atomic persisted Result/output and exact return correlation to the original
Store. Restart ambiguity becomes Unknown without execution retry; durable
results resend exactly. Signed scoped cancellation intent and late result
evidence remain separate, preserving cancellation/Unknown semantics without
forking the linear domain history. No actual Codex or runnable network flag.

The next finite increment now adds a test-only single-owner runtime driver:
one origin/session, two exclusively owned Stores and one publication peer with
a policy-controlled loopback mirror. Up to four tasks and two asynchronous
mock callbacks overlap; callbacks receive task-local completion/cancellation
channels, not Store connections, peers or signing keys. Accepted outcomes use
the current signed history head. Existing adapter outboxes supply exact replay;
orphaned starts become Unknown without callback retry. Duplicate outcomes are
idempotent; conflicting outcomes and incomplete correlations refuse. The
existing linear-history validator, quarantine and activation refusal are unchanged.

The following finite route now separates runner signing from the coordinator.
A disposable runner actor owns the runner key and durable proposal/signing
journal; the existing coordinator has only its public identity/channel client.
Signed task-bound proposals take the existing runner-peer-to-coordinator scoped
transport. The sole sequencer validates them, requests runner signatures for
the current-head final event and retains exact responses for replay. The runner
refuses challenges omitting an already reserved response, including across tasks.
Interrupted acceptance preserves Unknown and late signed output without another
invocation. No domain semantics, policy or fork refusal changes.

The next finite test/dev path now runs mock execution, runner signing and
journaling in a separate Linux child process. It generates its disposable key
inside the child, owns a separate execution Store and uses the existing runner
proposal/current-head protocol. The coordinator holds only public identity/client
for that runner and retains the sole publisher/scoped transport and application
Stores. Framed private Unix-socket controls have finite sizes/backpressure,
deterministic work/crash gates, cancellation and kill/reap cleanup. Exact replay
or conservative Unknown follows process crash; invocation is never retried.

Owner's 16 November 10:21 correction authorized a harmless **real Codex**
acceptance on Leo-Claw using existing local auth in place. The explicit opt-in
test/dev route now uses the existing provider API and codex-safe wrapper,
gpt-6.1-terra/max/yolo, inside the separate runner. A real tool read a generated
nonce/arithmetic fixture and wrote sum42; the signed result traversed scoped
transport and persisted Completed at the source. Exact duplicate delivery and
full restart retain one invocation and identical signed bytes. This is
single-host, separate-process acceptance, NOT multihost or runtime activation.
Useful fake-provider failure/cancellation/crash regressions remain; production
provider behavior and the live bot are unchanged.

Next concrete disposable runtime gap: real-provider abrupt-runner-death
containment/reconciliation. The provider API does not expose an in-flight
PID/PGID/session to the runner journal; mock parent-death cleanup does not prove
containment of the actual Codex process tree. Preserve Unknown/no retry until
that owned-process/session boundary is verified. Broader request/artifact output
and paired capability/workspace admission beyond the fixed fixture remain open.
This ordinary development does not require live exclusion permission. No generic
distributed queue or complete admission architecture is added.
Those were prospective gates in this earlier amendment. The separately
authorized disposable real cross-Mac task and exact restart replay are now
complete, as recorded above; that does not authorize production activation.
The latest 12:37/12:40 amendment puts the inbox/scheduling foundation and its
remaining acceptance gates before final packaging/fresh-host acceptance.
Ordinary disposable adapter development remains authorized.
Review remains **DEFERRED, NOT approved**; Phase 2 is incomplete.

Real credential invalidation/custody, live exclusion evidence, runtime runner
admission, real Codex/Telegram access and takeover/deployment still need separate
authority and verification. Complete authorized inventory, issuer/containment
evidence, accepted-effect reconciliation and trusted checkpoint continuity remain
operational blockers, not a reason to stop disposable adapter development.
None of those later gates is satisfied by synthetic history. See the
increment's authority table and remaining gates and the exact
[boundary and freshness limits](iroh-phase2.md#increment-2-owner-policy-and-scoped-record-boundary).

## Sources

- [iroh 1.0 retrospective](https://iroh.computer/blog/the-road-to-iroh-1-0)
- [iroh-docs official documentation](https://docs.iroh.computer/protocols/documents)
- [Relay deployment guidance](https://docs.iroh.computer/add-a-relay)

Sources checked 14 November 2026. Published deployment evidence and documentation are distinguished above from application verification still to be executed.
