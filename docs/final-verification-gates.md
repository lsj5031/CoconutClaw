# Final verification gates

Owner-authorized next-safe-handoff amendment,16 November2026, from
`next-handoff-verification-20261116-1455.md`. This is an obligation and execution
order, not a completed verification claim. Review DEFERRED NOT APPROVED,
Phase2 incomplete, activation unavailable. No publishing/packaging authority.

## After implementation, before packaging

Finish the architecture before modeling its final bytes. Preserve finite slice
receipts but do not mistake them for final-source verification. Pin the actual
final source manifest and revision; redo affected models/tests after any change.
Do not prematurely model unfinished code or launch independent review.

Inventory explicit AND implicit machines across all modules: persisted statuses,
combined flags, SQL/queue transitions, leases/epochs, callbacks/retries,
startup/shutdown, Event/Session/Task/Run, approval/cancel, schedule/occurrence,
inbox/outbox and Unknown delivery, replication/membership/revocation, artifacts,
recovery and provider lifecycle. This includes portable and platform-specific
branches, feature-gated code and composition, not only named enums.

For each machine record source-linked actions and owner, state, guards,
invariants, atomicity, durable/volatile boundaries and composition with other
machines. Record coverage, exclusions, assumptions and residual gaps explicitly.
Inventory completeness is an audited claim, not a guarantee that all implicit
machines were discovered.

Create source-linked TLA+ specifications/configurations and reproducible checks
using official SANY/TLC tooling. Preflight available tooling/models locally,
and Mars only if necessary; use existing provisioned tools/offline alternatives,
not a new application runtime/framework. Record tool/version/checksums, commands,
exit codes, safety versus justified liveness/fairness, finite bounds/state counts,
exhausted queue and artifacts. Cover duplicate/reordered events, crash/restart,
partition/stale owner, cancellation/approval races and ambiguous effects.

Negative controls must actually violate intended invariants and fail the gate.
Fix counterexamples with real regression coverage, then rerun affected checks on
the pinned final snapshot. Bounded model checking does not prove arbitrary Rust
correctness or guarantee every implicit machine was found.

## Native GitHub CI

Reuse `.github/workflows/ci.yml`'s Ubuntu/macOS/Windows matrix and existing release
targets. Critical portable lifecycle tests must execute on each OS, not disappear
behind `cfg(unix)`. Inspect current provider lifecycle, `e2e_smoke` and
`topics_rich` exclusions. Add disposable portable coverage for inbox/schedules/
restart/outbox, process transport/results, Windows path/identity/Codex discovery/
spawn/cancel/cleanup and SQLite/filesystem behavior. No credentials, live Telegram
or paid-provider secrets in CI.

Record the actual pinned published SHA, OS jobs, critical test counts/skips and
retained artifacts; fix actual Windows failures. Real authenticated Windows
Codex acceptance remains explicitly unverified if unavailable. Dirty local bytes
have no hosted-CI evidence. Inspect remote/status and obtain/reconcile publishing
authority before any commit/push. No publication is authorized by this gate.

## Current status and dependencies

The implementation-owned [bounded formal gate](formal-verification.md) now has a
whole-module best-effort inventory, source-linked executable contracts and
reproducible official SANY/TLC safety, conditional-liveness and negative controls.
Its finite bounds and explicit unmodeled/refinement gaps remain limitations, not
an arbitrary Rust correctness proof or independent review. Actual source-pinned
hosted Linux/macOS/Windows execution is **pending** and remains the next gate,
not a blocked excuse for omitting portable coverage. Existing cross-Mac
acceptance remains valid and must not be repeated without a new authorized gate.
Stage-ready manifests/exact commands take the place of waiting for absent SSH;
the parent owns paired-node operations and notifications.

Completed finite foundations: actual runtime result bridge and persisted
approval/dependency waiting, task/attempt/session/event linkage and isolated work
lanes under the original conversation owner. Their local receipts do not satisfy
the full final-source gates.

Further completed finite foundations: evidence-bound rich-text reconciliation
and [separate context/notification ownership](context-notifications.md), including
persisted changed-blocker suppression. These do not satisfy final verification.
The finite [calendar/wake foundation](inbox-scheduling.md) adds weekly recurrence,
selectable calendar policies and earliest-due event/deadline intake. Its portable
local receipts do not substitute for either final-source verification gate.
No actual schedule without an explicit user task/cadence. Packaging waits for
implementation acceptance and both gates.
