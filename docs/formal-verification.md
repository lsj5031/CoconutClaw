# Source-linked bounded formal gate

16 November2026, implementation-owned sole writer. Independent review remains
**DEFERRED NOT APPROVED**. No publication, deployment, real effect or Phase2/release
completion is implied.

## Basis and coverage

The completed calendar implementation is pinned before models. Its158 live and
snapshot hashes and190 receipt hashes were independently rechecked; the prior
491-pass/0-fail/5-existing-ignored Rust receipts remain applicable because no
Rust, SQL, dependency, provider or application runtime input changed here.
The wider repository baseline contains171 paths, not just enums or `.rs` files.

[`formal/inventory.json`](../formal/inventory.json) explicitly classifies each
baseline path:application/shared runtime, test-only adapter/synthetic code,
evidence-only tests, documentation/config/build assets and unmodeled transitions.
It records declarations, SQL objects, platform/feature conditions and gaps.
This combines all-module declaration/conditional scans with concrete owner/action
body inspection. It is an exhaustive **best-effort path inventory**, not a claim
that every implicit machine, combination or line of implementation was audited.
New verification tooling/spec/config paths are separately pinned in check inputs.
Dependencies, target outputs, duplicate artifact snapshots, OS implementations and
private/production data are explicit exclusions, not silently omitted modules.

[`formal/source-map.json`](../formal/source-map.json) binds model actions to exact
file hashes and declaration locations. Each family records owner, state, guards,
transactions, durable/volatile boundaries and refinement gaps. The135 pinned
implementation/test/shared-library/build inputs include test-only networking and
remote process branches, not just the normal executable's dependency graph.

| Family | Independent state and checked boundary |
| --- | --- |
| Ingress | Exact payload custody, cursor, claim, separate normal-task receipt vs SQL-control effect/outbox, interrupted applying and Unknown |
| Tasks | Logical head/physical ordinal, workflow launch counts, active slots, approval/dependency cause, cancellation/recovery |
| Effects | Persisted sending vs wire acceptance vs receipt, definite rejection vs ambiguity, offer revision and audit |
| Calendar | Active/paused/cancelled, revision/cursor, occurrence/inbox/run sets, unresolved fences, cached deadline and blocked fetch |
| Context | Scoped preference provenance, separate facts, native snapshot generation, semantic blocker projection and immutable decisions |
| Replication | Membership generation/high-water, lease, stale requests/connections, partition, receipt-before-projection, restore quarantine/pin |
| Process | Durable claim/volatile dispatch token, leader vs pipes, cancellation/timeout, semantic activity fingerprints and owned registration |
| History | Reordered typed events, contiguous prefix, fixed owner/core/runner roles, source/candidate/promotion and assignment/Start/Result, suspension |
| RuntimeComposition | Calendar/raw custody -> request/run -> bounded provider admission -> result/outbox -> separate receipt/send |
| RemoteComposition | Original reservation excludes local provider; runner claim/sign -> custody -> original result/outbox; cancel/route/epoch races |
| WaitNoticeComposition | Frozen successful dependency -> current-head continuation; blocker/notice authorization vs in-flight ACK; preference generation |

Actions and guards are executable relations over independent variables, not
constants asserting implementation facts. For example, Effects can accept an
external operation and then lose its acknowledgment. Unknown does not imply
nonoccurrence; its retry mutant produces a second possible external acceptance.
The ingress task receipt can commit while raw custody still says applying,
distinct from a SQL-only control's atomic effect+reply commit.

## Bounds and interpretation

[`cases.json`](../formal/cases.json) is the exact reproducible registry:
22 small/expanded safety configurations, three conditional-liveness positives,
11 intended invariant mutants and two intended temporal mutants (38 total).
There are no state constraints, depth cutoffs or simulation substitutes.

- Ingress:up to3 events,2 scopes and2 opaque payload aliases.
- Tasks:up to2 logical tasks,2 physical ordinals each, abstract slot cap1/2.
  This does **not** inductively prove arbitrary tasks or the actual cap5.
- Effects:one rich-text operation,2/3 bounded attempts, three head revisions,
  independent acceptance/receipt/offer state. Arbitrary external effects excluded.
- Calendar:up to2 schedules,2 revisions,9 clock positions and a finite single/
  gap/fold slot oracle. Default and alternative policies are checked, not every
  policy Cartesian product. Real chrono/IANA arithmetic remains Rust-test evidence.
- Context:one scope with two revisions/blocker changes; two concurrent scopes
  with one change each. Actual returning blocker identities use fresh generations;
  the finite model's semantic progression is monotonic.
- Replication:up to2 record identities,3 policy generations,6 retained message
  identities and4 monotonic lease-clock positions; one abstract peer boundary.
- Process:one workflow, up to2 semantic fingerprint identities and3 clock
  positions; recovery cannot recreate consumed permission.
- History:up to3 typed records, four actor aliases; enough for
  source/candidate/promotion or assignment/Start/Result, not arbitrary histories.
- Compositions:one/two physical root jobs, one remote assignment and one waiter/
  dependency with one continuation. They check explicit cross-machine cuts,
  **not** one full Cartesian product of every subsystem/platform state.

Official TLC breadth-first exhaustion is required for positive checks, with
state counts, queue size, graph depth, action coverage and exits retained.
Fingerprints are probabilistic; logs retain their collision estimates. Neither
bounded checking nor a completed queue proves arbitrary Rust correctness.

## Safety versus conditional liveness

Safety allows reorder, duplicate/idempotent observations, partitions, crash cuts,
stale generation/owner/head, cancellation and ambiguous outcomes. Atomic actions
stand for the source-linked SQLite commits, not hypothetical partial SQL states.
OS I/O, HTTP and signed custody are separate from those transactions.

Two RuntimeComposition liveness bounds assume a continuing sole core, available
SQLite, finite accepted inputs, terminating providers and successful eventual
HTTP acknowledgment, with no crash/cancel/ambiguity and weak fairness per enabled
admission/start/finish/send/ack action. They check eventual completion/delivery.
The calendar liveness case holds fetch blocked forever and assumes a fair clock
and reducer, coalescing and no administrative mutation; admission must still
eventually occur. Its mutant incorrectly gates admission on fetch return.

These are explicit environmental obligations, not unconditional availability
under indefinite outage, frozen clocks, shutdown or permanent Unknown.
Other waiting/terminal states may stutter. General deadlock freedom and arbitrary
fairness are not claimed. Temporal mutants actually fail with counterexamples.

## Deliberate gaps and assumptions

- SQLite IMMEDIATE/commit isolation and filesystem durability, opaque equality
  of canonical bytes, signature/hash correctness and independently **current**
  external owner/checkpoint pins are trusted contracts. SANY/TLC does not verify
  cryptographic code, native databases or physical fsync/rename behavior.
- Full parser/serialization schemas, every command/media/chunk/fallback plan,
  Markdown/FTS/import/retention and memory correct/undo/spent-ID rules, synthetic
  reconciliation descriptor/unjournaled-holder completeness and byte-level
  authority inventory are inventoried but not fully model checked.
- Membership monotonic lease time is modeled; dual wall-clock rollback checks,
  real streams/reconnects, metadata tombstones and hardware rollback freshness
  are not proved. A policy generation is not application replacement activation.
  Epoch2 application fencing only suspends.
- Process ownership flags are not proof every descendant is dead. Unix process
  groups differ from non-Unix child-only termination; Windows pipe/thread and
  discovery/path/identity behavior still needs actual native execution. macOS
  identity/native-Codex correlation receipts are preserved, not rerun.
- One workflow dispatch is not one OS invocation:existing configured turn
  retries and textual CLI-flag rejection compatibility retry are outside that
  invariant. They do not prove arbitrary external nonoccurrence. Broader real
  external-effect reconciliation remains an explicit residual acceptance concern.
- Batch32/row33 fairness,100ms debounce,1s error backoff,30s rebase, actual waiting
  fetch/retry latency and full arbitrary task graphs require source/native tests;
  no hard real-time or unbounded starvation proof is claimed.
- Service management's systemd/launchd install/start/stop/uninstall and external
  unit-file effects are inventoried but not modeled or executed. Windows service
  management is explicitly unsupported. Third-party runtime/framework code is
  outside this application's finite model.

## Execution and next gate

Use the [offline runner](../formal/README.md). It rejects stale source mappings,
tool/parser/budget errors, unexpected mutant success, wrong counterexamples and
unexercised required actions. Actual official negative traces are required,
not only unit-test mocks. Temporary-copy gate controls additionally demonstrate
rejection of unexpected pass, unexpected failure, missing action and stale source.

Final source/spec/config/tool/command hashes and results are sealed under
`artifacts/coconutclaw-formal-gate-20261116-2132/`; the durable result distinguishes
model syntax/abstraction corrections from any actual implementation defects.
No implementation defect has been inferred merely from an abstraction failure.

After this coherent finite formal gate, the agreed **actual source-pinned hosted
Linux/macOS/Windows CI gate remains mandatory before packaging**. Reuse the
existing matrix and retained portable critical suites, never ignore them to
produce green Windows. Publication authority is absent; dirty local bytes have
no hosted-CI evidence. Authenticated Windows Codex remains unverified.
No independent review, remote dispatch, live provider or user schedule is started.

## 17 November controlled multihost preparation delta

The preceding receipt/count/publication statements describe the16 November gate,
not a new full-matrix or hosted-native run. The later publication attempt created
three commits but was refused by the archived/read-only repository; no CI run
exists for that attempted publication.

Current preparation changes only the test-only role/provider/driver harness,
launcher, tests and documentation. Eight original implementation pins are
refreshed after inspecting these changes; a ninth input, the host-local in-place
auth callback, is added (136 current pins). Additional direct driver/role anchors
bind the affected remote composition; the original owners and normal executable
remain unchanged. All Process, RuntimeComposition and RemoteComposition configs
are rechecked with the existing official offline runner. Unaffected model
owners/specs/configs are unchanged; their final03 results are explicitly reused.

The controlled fixture's file events, native command auth, physical iroh sockets,
coordinator detach/reconnect, before-original-ACK fault hook and OS descendants
are executable/native-test obligations, not newly modeled protocols. Their
abstract counterparts are reservation/claim/sign/custody/apply, cancellation and
partition/stuttering with durable proof retained. The model does not establish
Rust refinement, power-loss durability, external delivery exactly-once, or shared
original-Store scheduling across two runner actors. Local regression success
does not count as this round's pending Mars/Mitori provider acceptance.
Independent review remains **DEFERRED NOT APPROVED**; no new commit is authorized
before collecting the bounded real acceptance, and no push retry is part of this
preparation.

The zero-invocation preflight also exchanges an inert public nonce in both
directions through the real scoped data transport, gateway-initiated first.
These are not domain events or task admissions; this transport probe remains
an executable obligation rather than an added formal application transition.

## macOS/Linux CI follow-up, 17 November

The native-test runtime locator no longer assumes Cargo's test-artifact directory
depth. Five existing real-process lifecycle tests share it; a regression checks
both directory layouts and fail-closed explicit selection. The macOS lint fix
asserts valid descendant PIDs before the existing Linux-specific zombie checks.
Only the test profile optimizes the unchanged `curve25519-dalek` dependency,
whose unoptimized arithmetic dominated the measured 45-second fixture deadlines.
No signature check, timeout, domain action, runtime owner, SQL or protocol changes.

These inspected test/build changes refresh eight existing pins and add the
test-only locator (137 current inputs). All eleven model files, 38 configurations,
case inventory and action/declaration mappings remain unchanged. Their existing
finite-model results are qualified for reuse, not claimed as a new full formal
execution or native macOS/Windows result. The source-map checker and executable
regressions remain required. Independent review is still deferred, not approved.

PR #22's native macOS lint exposed a Linux-only gate around the Unix process
suite and its local orchestration helpers. The suite now also runs on macOS,
including signed replay, crash cuts, cancellation, provider and descendant
cleanup. Exit assertions use Linux pidfds or macOS kqueue process events.
The fake provider retains Linux parent-death signaling and uses its existing
control-socket/descendant-stdin EOF cleanup on macOS. The Linux-specific,
owner-authorized real-Codex acceptance remains opt-in and Linux-only. This
test-harness follow-up refreshes five inspected pins without changing model
actions or normal executable behavior; Linux results are not macOS evidence.

The next native test stage exposed physical-path assumptions on macOS.
Memory-import fixtures now select their canonical source root, preserving the
importer's rejection of all symlink components. Process fixtures likewise use
canonical paths, and overlap assertions use Unix process liveness rather than
Linux `/proc`. The calendar owner watches its canonical database parent and,
on macOS, uses kqueue vnode watches for the open database/WAL/journal files.
FSEvents alone did not report the live WAL writes within the existing wake
gates, even after canonicalizing the parent. Admission and wake policies are
unchanged. Disposable role readiness now waits for a reader
acknowledgment before closing its socket, retaining the existing finite deadline.
These changes refresh inspected pins and unchanged action declaration hashes,
not the bounded models or their action mappings.
Isolated fake-provider startup diagnostics retain the actual failure instead of
reporting only an expired handshake. Native macOS execution remains the gate.

The authorized PR #22 reconciliation merges completed owner Windows fixes from
main `2f47ba8`. The native-binary helper keeps its upstream callers and delegates
to the shared layout/override regression-tested locator; Windows environment
scrubbing and its 120-second hung-test bound are preserved, while Unix retains
the original 45-second bound. Inspected implementation pins and unchanged action
declaration locations are refreshed for the merged source. Affected small cases
are rechecked, not claimed as a new full formal gate or native OS acceptance.

Native run 44007997530 confirms the macOS vnode calendar wake fix and the merged
Windows/Linux gates. It also exposed overlapping first-open schema setup.
Store initialization now holds a database-specific OS file lock through schema,
migration and graph/memory initialization, released before normal operations.
The concurrent first-open regression retains separate live connections; existing
projection concurrency assertions are unchanged. This startup ownership is an
executable obligation, not a new model action or a proof of filesystem behavior.

Run 44010259517 confirms initialization ownership on all three OS jobs and narrows
the remaining macOS fixture startup failures to SDK-launcher environment injection.
The fake-provider profile resolves the explicit interpreter with the system
`xcrun --find python3` on macOS instead of executing `/usr/bin/python3`'s launcher.
The Linux-only parent-death fixture imports `ctypes` only on Linux. The original
closed environment allowlist is retained, with no ambient executable fallback.
Run 44011947680 confirms that bypassing the SDK launcher removes all four SDK
path variables. CoreFoundation still adds its UID/encoding/region metadata.
The fixture validates that metadata's closed numeric form and current UID before
removing it, then applies the original environment assertion. SDK paths and
unknown environment names remain rejected; no provider lifecycle test is skipped.
Run 44013736760 passes all native macOS unit cases, including the nine local
process/provider cases. Executable import fixtures also need physical temporary
roots; their source traversal/symlink refusal remains unchanged. The subsequent
Unix topic integration fixture now checks owned PIDs with `kill(pid, 0)` rather
than Linux `/proc`, retaining both overlap and cleanup assertions.

## 17 November button-first UI source reconciliation

This supersedes the old source counts/reuse statements for the local UI lane,
not the historical receipts, platform acceptance or publication restrictions.
Runtime remains exactly UI commit `3d67b70` with backend `94db7c7`; this follow-up
changes only formal contracts, mapping/inventory, verification tests and this
documentation. It does not incorporate unvalidated platform work.

`formal/reconciliation.json` records all 31 mismatched implementation pins,
their verified `a95880c` preimages, exact delta hashes and individual inspections.
Eighteen mismatches were inherited at approved base `2f47ba8`; nineteen inputs
also changed in the UI/data lane, with overlap. All declaration anchors were
checked, and relocated declarations reanchored. Three UI/backend source inputs
bring the pin set to 139. The path inventory refresh retains historical hashes
and adds those three inputs plus operations documentation and the previously
pinned multihost callback (formerly only verification-inventoried), for 176 paths.

The official runner now checks 50 configurations across 13 families: 31 positive
safety/conditional-liveness bounds, 17 intended invariant failures and two
intended temporal failures. The entire matrix is rechecked, not just new models.
Existing model invariants, budgets and negative classification are not weakened.

| Addition | Source-linked relation and finite scope |
| --- | --- |
| Ingress observations | One designated authenticated read-only/error event may pass older same-scope pending, applying or Unknown. Separate required actions witness all three cuts. It creates reply/interaction metadata, never a task receipt or domain/runtime effect. Reconciliation still bypasses only Unknown; ordinary mutations retain their fences. |
| UIAuthority | Two opaque controls, two actor aliases and one/two root/topic scopes; issued/expired/consumed state, route revocation, volatile invocation permission and durable Unknown. Exact current actor/scope authority is recorded at use; consumption precedes external invocation and survives the modeled crash. Observations are repeatable. |
| GuidedInput | Two fresh code identities, one/two scopes and valid/invalid nonblank data classes; one current code per scope, replacement, expiry, explicit submission, matching/stale cancel and unrelated messages. SQL input/effect/guide cleanup is atomic. |

Intentional new mutants permit observation effects, bypass callback authority,
reuse consumed runtime controls, swallow implicit input, cancel a replacement
from an old code, or accept expired input. Each must produce the specifically
named invariant counterexample. New small/expanded positives require every
declared action to execute, including replacement and stale cancellation.
Offline consistency tests also bind inventory bytes/declarations to all pins,
source-action anchors and case/action identities.

These are **bounded implementation-owned contracts, not a Rust refinement
proof**. Classification and actor admission are linked preconditions across
independent contracts, not an exhaustively composed parser/runtime model.
SQL claim/effect/reply commit atomically in source; the Ingress model's separate
claim/commit states conservatively overapproximate some SQL crash cuts.
Restart retention relies on SQLite's trusted durable transaction contract.
Token entropy/collision resistance, arbitrary code text/IDs, memory correctness,
real clock arithmetic/rollback, exact SQL snapshot joins/counts and rendering are
executable obligations, not newly proved algorithms.

Expiry/revocation model one monotonic validation episode. In particular, clock
rollback between observation classification and reduction is not modeled;
source-linked acceptance does not assert its impossibility. GuidedInput assumes
authenticated scope/actor at entry. Blank/mismatched input stutters; invalid-value
submission is nonblank data rejected by the existing command validator.
Effects' positive receipt covers UI acknowledged projection and known
not-modified navigation edits. Ephemeral spinner acknowledgment remains
one-shot mocked-wire evidence, not a modeled delivery/liveness requirement.
No genuine Telegram screenshot or live visual acceptance exists.

Inherited platform deltas include Unix-only test hooks/import fixtures, native
binary/env helpers, Windows synthetic provider/wait fixes, and Unix-only
directory-entry fsync in the synthetic membership store. None are edited here.
The Replication abstraction trusts durable store actions and does **not** prove
Windows directory/rename power-loss durability. External-kill tests do not
establish that property. Native OS CI and parent final evidence review remain
separate gates; this attempt performs no publication.

## 17 November integration onto validated main

The sections above are historical lane receipts. The current local integration
normally merges UI `c8b36e4` (including backend `94db7c7`) into main
`1d608f523eaed18926e882a1a12ed44efd5dee24`, after PR22's native OS gate.
That main gate does not establish hosted acceptance of the newly integrated UI.
The beginner README and all platform fixes are retained; no live operation,
deployment or publication is part of the local integration check.

`formal/integration-reconciliation.json` individually records 23 source-pin
deltas from the UI parent, both parent hashes, merged hashes and inspected
semantics. The validated main pins have no inherited mismatch. The union has
140 implementation inputs, including PR22's test-only binary locator; the
repository inventory has 177 paths. Historical UI reconciliation records remain
unchanged and mapping tests verify the hash chain through the integration.
All main action anchors are retained and declarations reanchored where needed.

All 13 models, 50 configurations, required-action lists and expected
counterexamples are byte-identical to reviewed UI `c8b36e4`. The complete official
runner and all required Rust/Python checks are rerun against the merged source,
not reused as if the old lane checks validated a new revision.

PR22 preserves Unix process coverage, Windows environment/layout handling and
120-second fixture bound, Unix's 45-second bound, signed arithmetic optimized
only in the test profile, canonical temp paths, portable owned-PID exit checks,
blocking accepted mock sockets, Ready read acknowledgment, explicit macOS Python
resolution and closed environment validation. Calendar kqueue vnode watches
implement the same observed-wakeup abstraction. Database-specific first-open
schema/migration ownership is established with an OS file lock, released before
normal operations. These physical mechanisms are native executable obligations,
not new modeled domain transitions or Rust/OS refinement proofs.

The UI's direct callback authority, observation-only bypass, immutable reports,
guided explicit-code input, compatibility and Unknown/no-blind-retry boundaries
remain unchanged. All earlier finite bounds and residual assumptions still
apply, including monotonic validation episodes and excluded wall-clock rollback.
Ordered production-handler/wire captures remain **mocked**, not genuine Telegram
screenshots. Independent review remains deferred and parent final integrated
diff/evidence review is required before any later authorized publication.

## 18 November five-lane review-fix integration

`formal/review-fixes-reconciliation.json` records the next exact delta from
`2e8eb2d`, preserving every earlier reconciliation layer. Each changed pin has
its preimage, integrated hash, diff hash, inspection and executable obligation.
New media, retention, startup, doctor, wording and regression inputs are
classified explicitly. Action owners and declarations are rechecked against
the integrated source, not replaced with a new arbitrary baseline.

The thirteen models and fifty finite configurations, invariants, required
actions, fairness assumptions and expected counterexamples remain unchanged.
Their full official gate is rerun for this integration. This checks the existing
abstract custody, authority, transaction, receipt and Unknown contracts, not a
Rust refinement proof. In particular, whole-output byte/count limits, outbox
canonicalization and opened-handle checks, operational retention, SQLite
bootstrap versus plain connect, read-only doctor, timezone rendering and
callback parser normalization are executable regression obligations. They are
not newly proved algorithms. The existing finite bounds and environmental
assumptions above still apply; no budget or invariant is weakened.

Routing restores a missing topic only from an authorized control's saved origin.
The observed root selection plus later topic task explains the missing route,
but the unavailable original update/control payload prevents establishing why
that selection was root-scoped. Synthetic missing-topic acceptance does not
establish the production root cause. Root controls and historical root selections
are never guessed into topics. Source/mock acceptance is not live acceptance.

Native Windows opened-handle media protection still needs the exact-head hosted
Windows gate. macOS tests do not establish Win32 behavior, live Telegram/provider
acceptance or fleet readiness. Parent independent review and hosted
Linux/macOS/Windows CI remain separate gates before publication. This local
integration performs no push, deployment or live operation.
