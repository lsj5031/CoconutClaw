# Final verification gates

The 16 November 2026 amendment set the following release checks. These are
requirements for a candidate, not publication or packaging authority. See the
[19 November baseline](#19-november-2026-baseline) for completed evidence.

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
retained artifacts. Distinguish native test execution from a build-only pass
and from live authenticated Codex acceptance. Dirty local bytes have no
hosted-CI evidence. Obtain publication authority before publishing.

## 19 November 2026 baseline

[Fleet verification](fleet-verification.md) records three separate gates:

- The exact merged commit `18c460c` passed hosted CI run `44455611317`,
  including native Windows tests and the required Windows rollup.
- The [bounded formal gate](formal-verification.md) was independently accepted
  on `3a55ae9`, with the same Git tree as `18c460c`. It retains finite bounds
  and explicit implementation gaps; it is not a proof of arbitrary Rust or
  native behavior.
- Live scoped memory was verified on `18c460c`. Earlier routing, calendar,
  recovery and cancellation cases belong to `746c3b8`, not the merge commit.

Carry those receipts only for their stated inputs and cases. Future candidates
need their own applicable checks and review. The fleet record lists uncovered
live cases and product gaps; it does not close broad Phase 2 replication or
release packaging. No actual schedule without an explicit user task/cadence.
