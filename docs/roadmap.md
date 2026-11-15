# CoconutClaw revival plan — revision 5

15 November 2026 · Supersedes revision 4. Owner added explicit Markdown import
after completed bounded memory and Telegram topics/parallelism/native rich delivery,
before networking and remote execution.

## Direction

Trim the existing application first. Use Codex for both conversation and delegated work. Adopt iroh as the networking direction, using published implementation/deployment evidence to avoid a separate comparison project. Keep local acceptance checks focused on CoconutClaw's integration.

The previous iroh-versus-p2panda experiment is removed. Phase 0 is the actual reduction of the product; Phase 1 is a short evidence, dependency and integration decision step. Neither phase includes building the complete distributed runtime.

## Product boundary

CoconutClaw owns communication, local memory and reliable delegation. Codex supplies the agent runtime. A local conversation session and a delegated task are different execution contexts, even though both use Codex.

One active core owns inbound messages, conversation ordering, shared-memory promotion and assignment. Several trusted runners execute assigned work. A machine may perform both roles. No automatic core failover initially.

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
- Proactive heartbeats, general scheduling and the existing nightly-reflection workflow.
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

The Phase 0 baseline, built-in bounded memory, and Telegram topics on/off,
five-worker admission and native rich delivery are complete. Next complete and
independently review the explicit one-time [OpenClaw Markdown importer](openclaw-import.md).
Only then wire and verify iroh **plus replicated-state synchronization/access/
recovery checks** (Phase 2), execute one remote Codex task (Phase 3), and finish
packaging (Phase 4). No Slack, Discord or legacy runtime backend is added.
Importer development remains uncommitted and undeployed until independent review.

This revision changes the implementation order and scope. Phase 0 trim and real Telegram/Codex acceptance have passed; the baseline is closed in a separate main-branch commit (see Git history and docs/validation-phase-0.md). Only the isolated synthetic [Phase 2 increment](iroh-phase2.md) has networking; runtime networking and the replicated-state/access/recovery completion gates remain open. Repository changes must use the standing monitored Droid authoring workflow unless the user explicitly changes that workflow; Codex-only here describes the product's runtime/delegation backends.

## Sources

- [iroh 1.0 retrospective](https://iroh.computer/blog/the-road-to-iroh-1-0)
- [iroh-docs official documentation](https://docs.iroh.computer/protocols/documents)
- [Relay deployment guidance](https://docs.iroh.computer/add-a-relay)

Sources checked 14 November 2026. Published deployment evidence and documentation are distinguished above from application verification still to be executed.
