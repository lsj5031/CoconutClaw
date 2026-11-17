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
