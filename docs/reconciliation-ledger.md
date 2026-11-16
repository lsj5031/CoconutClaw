# Inert reconciliation ledger

The separate [runtime receipt reconciliation](runtime-reconciliation.md) now
implements a finite actual command path. It does not connect this synthetic
ledger as authority or change its external-exclusion/activation limits.

15 November 2026. **Synthetic-only, unreviewed, not deployed.** Independent
backup-recovery review remains **DEFERRED, NOT approved** (Leo, 21:34).
This implements inspection, not reconciliation authority. Phase 2 remains open.
The [external exclusion contract](credential-exclusion.md) still controls the
operational gate. No live inventory, credentials, sessions or runtime stores
were inspected.

## Smallest representation

`synthetic::membership::domain::reconciliation` in
`crates/coconutclaw-sync/src/synthetic/membership/domain/reconciliation.rs`
contains the base ledger's three serde structures and pure `inspect` function,
plus the bounded descriptor and inventory extensions below. It reuses
`History::validate_fence_checkpoint`, the existing `Hash` (BLAKE3), serde and
ordered collections. No dependency, database, signing scheme, issuer, workflow
engine, runtime export, executable dependency or service adapter is added.

A `Bundle` contains one `Binding` and immutable `Observation` records. It is
**separate from the suspended canonical domain chain**, not a post-fence result,
membership grant, publication journal, delivery receipt or recovery authority.

The binding pins:

| Field | Exact identity |
| --- | --- |
| `origin` | Existing `BoundOrigin::hash()` string. The verified fence's event origin must match. This transitively binds owner, stable store, namespace, scope and original roles. |
| `fence_sequence`, `fence_hash` | Existing sequence and BLAKE3 of the complete signed canonical fence envelope, checked against the replayed `History`. This binds predecessor, epoch 1 to pending 2, proposed core and runners. |
| `application_checkpoint` | BLAKE3 of an independently selected, exact redacted application inventory/checkpoint descriptor, including known missing journals. This is a fixture input, not an implemented application snapshot/export. |
| `inventory` | BLAKE3 of the exact redacted authority-inventory revision bytes, including authority/resource and credential-version identifiers, holders, lineage, tool/workspace boundaries and explicit coverage gaps. Never hash a secret to identify it. |
| `policy_generation`, `policy_hash` | Positive independently selected generation and existing recovery policy hash convention, BLAKE3 of domain-separated `policy_bytes(policy)`, not a hash of arbitrary JSON or the signature. The ledger compares inputs; it does not install or authenticate policy. |
| `recovery_manifest` | Optional existing `RecoveryManifest::hash()`. `None` means this fixture names no adopted backup, not that unknown restore provenance is acceptable operationally. Substituting `None` for an expected hash refuses. |

Each observation has an immutable `id`, bounded `local_state` label, `snapshot`
digest and zero or more `evidence` digests. IDs and labels are nonempty,
control-free, at most 256 UTF-8 bytes. Digest preimages are exact retained
redacted observation/evidence descriptor bytes, not secrets or raw private
payloads. Descriptors name the source checkpoint, subject, assigning core/runner
where known, local run/session references, result, possible effect IDs,
authority/resource, operation/target route, accepted-before-barrier disposition,
issuer/retrieval/barrier/interval and missing information as applicable.
Unknown attribution is explicit; never invent a distributed runner assignment
from a local SQL row. Sensitive originals belong in separately controlled owner
storage, not the repository or replicated bundle.

The fixture author chooses IDs such as `task/1/observation/1`,
`delivery/1/telegram:text:0/observation/1` and `unjournaled/progress/1`.
They are qualified by this bundle's origin and checkpoint, not global SQL IDs.
Fresh observations use new IDs and retain earlier records. Evidence added later
is another observation, not permission to rewrite an earlier Unknown. Related
observations reference each other's descriptor digests. No automatic resolver or
semantic contradiction adjudicator is implemented.

## Mapping the actual local state

These are required descriptor contents and conservative interpretations, **not
an implemented runtime reader/exporter**. The base ledger's `local_state` is an opaque inspection
label, not a new status schema. Existing CLI types are crate-private; coupling
this synthetic library to the executable merely to copy them is unnecessary.
No runtime SQL row or request is ingested by this increment.

Source references are under `crates/coconutclaw-cli/src/`, except
[`sql/schema.sql`](../sql/schema.sql).

| Actual records/callers | Separate ledger observation and retained uncertainty |
| --- | --- |
| `store/mod.rs::TaskRunStatus`, `store/task_runs.rs`: `queued`, `running`, `awaiting_approval`, `cancel_requested`, `completed`, `failed`, `cancelled`; `started_at`, `finished_at`, summaries and cancellation timestamp | Preserve exact local status and checkpoint-local run ID. Queued with no recorded start means only no start recorded, not proven non-execution. Cancellation request is intent. Terminal status is a local result, not proof of stopped tools or settled effects. There is **no runtime `Unknown` status**: restart may encode ambiguity as `failed` plus the unknown-outcome summary. Never normalize that to safely failed/retryable. |
| `task_payloads`: `request`, nullable `output`, `delivery_state`, `delivered`; `scheduler.rs::{persist_result,recover,retry_deliveries}` | Record request/result descriptor digests and exact presence/absence separately from execution status. Output present does not settle tools or delivery. Missing payload/turn/journal is missing evidence, not an empty task. `delivered=1` retains a local delivery acknowledgment only. `delivered=0` plus output is eligible for the **existing runtime** retry scan, but this ledger never invokes it. |
| `delivery/mod.rs::DeliveryState`, `persist_delivery_state`, `dispatch_task_output` | Retain completed operation IDs, operations map and text-plan descriptor. Legacy JSON arrays hold completed IDs only; they do not prove other operations were never sent. Missing or malformed state stays an explicit gap, not a new empty `[]` plan. |
| `delivery/telegram.rs::{durable_op,deliver_text}` operation `ready`, `sending`, `unknown`, attempt counts, `retry_at`; rich/document/chunk plans and completed IDs | `sending` is ambiguous after crash; `unknown` remains Unknown. `ready` records local eligibility/rejection handling, not globally proven non-execution. Attempts are **delivery attempts**, not Codex job attempts. Preserve acknowledged chunk/media IDs and the exact plan, including `telegram:text`, chunk/document IDs. Never resend, switch representation, clear attempts or regenerate a plan from this inspection. |
| `approval.rs`, `store/mod.rs::ApprovalRecord`, scheduler delegation and approval resume | Record approval ID/status, resume-descriptor reference and linked run IDs. Approval resume creates a new task run, not an inferred higher attempt on the old run. Delegation keys `delegation:{parent}:{index}` need parent/child links. Pending decisions and unstarted children remain inert, not automatic approval/cancellation/dispatch. |
| `turns`, pending side-effect recovery, inbound update IDs/`kv.last_update_id`, native provider-session references | Account for turn/result materialization, side-effect flags and poll checkpoint/gaps separately. Session identity is not proof that no process/tool survived. Never resume a session or advance a poll cursor to inspect it. |
| Direct status/command/progress/cleanup sends, downloads, polling and downstream tools lacking full per-effect journals | Add explicit unjournaled/coverage-gap observations by authority/target; no fabricated receipt or conclusion of no effects. Unknown inventory coverage itself blocks operational admission even if all known tasks have results. |

For the existing synthetic domain, `Job` already retains assigning core, exact
runner, attempt 1, start and optional result. Preserve that attribution alongside
the observation descriptor. `Outcome::Unknown`, absent result, pending assignment
or start never becomes a higher attempt, Cancelled or Completed through this
ledger. Synthetic history and actual runtime run IDs have no proven adapter.

## Inspection contract and refusal scenarios

`inspect(history, current_binding, independently_pinned_bundle, bytes)`:

1. Requires nonempty bytes no larger than the existing 65,536-byte synthetic
   bundle limit and an independently supplied BLAKE3 of those **exact bytes**.
2. Parses strict serde fields and requires exact serde reserialization. Whitespace
   or object reordering is not an alternative canonical encoding. This is a
   local fixture format, not a new accepted wire protocol.
3. Requires complete binding equality with the separately supplied current
   binding and a positive policy generation, then validates the actual historical
   fence and origin. No timestamp, first peer or copied bundle supplies freshness.
4. Validates bounded labels, collapses identical observations at the same ID, and
   refuses the **whole** view on different state, snapshot or evidence at one ID.
   No partial return, timestamp winner or overwrite.
5. Returns only inspectable observations. Empty/partial evidence is allowed
   **only as an inert record**, not as a satisfied gate. All external consequence
   reconciliation remains unproven, including acknowledged and terminal labels.

A digest authenticates neither its preimage nor an issuer. The base inspector
does not retrieve referenced descriptors, establish completeness of the inventory,
cross-check semantic contradictions across distinct observations, authenticate
service receipts or verify propagation, custody, containment or runner admission.
Missing descriptors, contradictory evidence and arbitrary labels cannot establish
non-execution or reconciliation. There is no `resolved`, `approved`, `complete`,
retry permit, effect command or activation result. Even a fully populated bundle
cannot establish external exclusion. `History::validate_replacement_activation`
is unchanged and always refuses.

The new tests in `domain/reconciliation/tests.rs` use deterministic mock keys,
digests, fresh temporary files and the actual existing `History` validator:

- Legitimate partial ledger inspection retains unknown attempt, sending and
  acknowledged delivery, pending approval and unjournaled effect. It neither
  changes history nor permits a post-fence result or activation.
- Substituting any binding component, including application/inventory checkpoint,
  policy or recovery anchor, refuses. Matching forged origin/fence inputs still
  refuse against actual history.
- Exact duplicates collapse; conflicting evidence/snapshot or relabeling Unknown
  at the same observation ID refuses the whole bundle.
- Reconstruct history and reload a persisted bundle, require the independent
  bundle hash again, and reject both a rolled-back bundle against the newer pin
  and a history prefix lacking the current fence.
- Malformed labels, oversize bytes and an injected approval field refuse.

These are shape/binding and prohibited-outcome checks, not mock service proofs.
No network fixture, runtime adapter, provider, credential or send is involved.

## Exact redacted descriptors, increment 8

**Synthetic-only, unreviewed, not deployed.** The child module
`domain/reconciliation/descriptors.rs` adds exact, bounded preimages for the
application checkpoint and observation snapshots. It accepts bytes, never a
database, path, connection, runtime request or provider. Its tests alone create
artificial in-memory SQLite databases using the actual
[`sql/schema.sql`](../sql/schema.sql). No runtime exporter or schema migration
is implemented or invoked.

The v1 format has two structures:

- `Descriptor { source, row }`, where `row` is one of six tagged, strict variants:
  Task, Payload, Delivery, Approval, Turn, Inbound.
- `Checkpoint { version: 1, source, missing, observations }`, where `missing` is
  an ordered set of unavailable journal families and `observations` is an
  ordered map from the derived local observation ID to its descriptor digest.

Canonical bytes are **exact serde struct/enum JSON serialization**. Unknown
fields, alternative JSON spacing/order and a noncanonical duplicate-key encoding
refuse. Maps/sets are ordered; descriptor input order does not change checkpoint
bytes. Each descriptor and checkpoint must be nonempty and at most the existing
4096-byte artifact limit. At most 64 descriptor inputs, including duplicates,
are accepted. This is a deliberately bounded fixture slice, not a full database
checkpoint, pagination mechanism or operational completeness claim.

`source_identity()` hashes the domain label
`CoconutClaw redacted fixture source v1` followed by a NUL byte, then, in the
fixed source-code roster order, each UTF-8 repository-relative path, NUL and
32-byte BLAKE3 of that file. The roster pins the schema SQL, migration source,
task/approval/kv stores, CLI types, scheduler, approval and delivery callers,
poll/webhook and pending-effect recovery, base ledger and descriptor source.
These are compile-time **source bytes**, not a runtime executable dependency,
schema introspection, deployed-state identity, signature or trusted producer.
The complete current roster is explicit in `source_identity`; changing these
sources invalidates old descriptor source identities.

### Redaction and links

There is no prompt, output text, raw request/resume JSON, attachment path,
session/token value or hash of private content in this representation.
Positive numeric request/output/resume references are **artificial fixture
aliases only**, not runtime IDs or commitments to actual content. Equality of
aliases makes no claim about real request/resume-body equality. JSON raw-content
fields refuse. Only fixed known status/plan/phase values and known numeric
Telegram operation-key forms are accepted. No free-form evidence text is added.

| Row | Redacted local information and checked links |
| --- | --- |
| Task | Positive checkpoint-local run ID; optional actual runtime status, recorded-start flag, explicit unknown-outcome flag; optional numeric inbound update or typed Delegation/Approval origin, never both. Known task statuses match the existing seven CLI statuses. An absent status means a missing task row, with no other claims. |
| Payload | Exact run link; optional artificial request/output aliases, local delivered bit, journal availability and Legacy/Rich/Document/Chunks plan kind. Missing request means missing payload row, not SQL NULL request; other payload claims then refuse. No delivered bit without recorded output. A null output remains Unknown even if a turn result exists. |
| Delivery | Exact run/payload link; numeric chunk/media or fixed text/document operation ID; optional Ready/Sending/Unknown phase and attempts; separate acknowledged bit. This preserves the real `durable_op` state where phase stays Sending after adding a completed ID. A phase-less entry can retain only a legacy local acknowledgment, not proof of no send. Rich text uses its plan phase/attempt count under `telegram:text`. |
| Approval | Positive approval ID; optional original run/status/resume alias. Known statuses include pending, approved, rejected and expired. Missing approval rows have no claims. Approval-resumed tasks reference that ID, not a higher attempt on the old run; known non-approved status cannot support a resume link. |
| Turn | Positive turn ID; optional exact run and numeric update links; known turn status and side-effect-applied bit, or an explicit missing row. Known task and turn numeric update IDs must agree. Recorded flags are not proof of exactly-once effects. |
| Inbound | Singleton `inbound` ID; optional last-update and inflight IDs. Missing keys remain null, including a partially retained pair. No cursor advancement, gap inference or external poll proof. |

Every referenced task/approval must have a descriptor, possibly an explicitly
missing one. Known delegation parents and original approval runs precede child/
resume run IDs, matching local AUTOINCREMENT creation, not a distributed
sequencer. Duplicate delegation parent/index, approval resume or numeric inbound
task/turn update identities refuse. Negative/zero row references, dangling links,
self-links, substituted turn updates and contradictory missing-journal claims
refuse. Entirely missing journal families must not contain invented known rows.
Individual missing rows do not imply a whole journal is missing.

Descriptor IDs are derived from row identity, using `task/{run}`, `payload/{run}`,
`delivery/{run}/{operation}`, `approval/{id}`, `turn/{id}`, or `inbound`.
`local_state()` is deterministically derived too. `descriptors::inspect` first
calls the unchanged base inspector, requires `Binding.application_checkpoint`
to equal BLAKE3 of exact checkpoint bytes, and requires the compiled source
identity. Then checkpoint entry, `Observation.snapshot` and exact descriptor
bytes must all agree; observation ID and label must match the derived values.
The sets must match exactly: no missing, extra or substituted descriptor or
observation. Identical duplicate descriptors collapse; differing descriptors at
one identity refuse the whole view. Exact source/schema substitution refuses
even when all caller-controlled bundle/checkpoint hashes are recomputed.

### What success still does not mean

Known null/missing/interrupted records remain Unknown in the operational sense.
`failed` with unknown-outcome evidence labels as `task:unknown`; Sending and
Unknown delivery are retained. Terminal task state, local delivered bits,
acknowledgments and side-effect flags remain only reported local facts.
No descriptor or label is an effect permit, retry/resend decision, exclusion
proof, runner acknowledgment or replacement activation input. Evidence-digest
authentication, real inventory coverage, source honesty and freshness are
still outside both inspectors. Matching rolled-back independent inputs remains
undetectable. No runtime store is read, written, opened, resumed or reconstructed.

New executable fixture tests exercise a legitimate linked path and byte reload,
result-before-payload materialization, rich Unknown delivery, legacy and chunk
acks, sequential journal loss, substitution, conflicting/duplicate descriptors,
malformed approval/delegation/inbound/turn links, unknown fields and size/count
refusals (including 64 accepted duplicate inputs and 65 refused).
Completed/Failed/Cancelled fixture statuses and acknowledgments still cannot
permit an actual post-fence attempt or replacement activation, even with
completed synthetic job history. The positive fixture emits exact artificial
specimen bytes under `--nocapture` for the increment evidence.

## Synthetic authority inventory, increment 9

**Synthetic-only, unreviewed, not deployed.** `domain/reconciliation/inventory.rs`
adds a v1 `Inventory` descriptor, inspected with the existing ledger boundary and
the application descriptor's strict canonical parser. It accepts exact bytes,
not paths, stores or service access. Nonempty canonical JSON is limited to
4096 bytes, including all nested lists. No dependency or runtime wiring is added.

The inventory repeats the exact origin, fixed synthetic scope and application
checkpoint; its complete canonical bytes must hash to `Binding.inventory`.
`inventory::inspect` also verifies the supplied `BoundOrigin`, requires its hash
to match the already validated ledger/fence origin, and returns **declarations
only**. Independent binding/bundle selection remains a caller obligation.

- Holders have positive artificial IDs and optional original core/runner keys.
  Other identities refuse; `None` preserves unknown attribution, including tool
  principals not represented by an application key. A key is not a credential
  possession attestation. Distinct copied holders may share a key and are never
  collapsed into one holder.
- Authority entries name Telegram API/file, Codex provider, tool or workspace
  authority, positive artificial resource/version aliases (unknown version is
  null), explicit holder links and an unknown-additional-holders declaration.
  These are not actual accounts, tokens, paths or private-content hashes.
  Known-empty holder coverage without an explicit unknown refuses.
- Each declared possible effect binds one exact observation ID/snapshot to an
  entry, matching authority kind/resource and an optional holder from that entry.
  `Recorded` means only a local recording declaration, never an authenticated
  service receipt. Known `task/` observations declare possible Codex invocation;
  `delivery/` and `inbound` declare Telegram API authority. This is not a runtime
  task-to-effect adapter. Aggregate work needs separate observations.
- Every `unjournaled/` observation, **including unmapped ones**, must retain
  `effect:unknown`. Linked ones must explicitly say `Unjournaled`. Neither
  relabeling nor moving the observation into a coverage gap can settle it.
- Every observation must appear exactly once: a possible-effect link or an
  `unmapped` ID whose authority coverage remains unknown. Removing a link needs
  an explicit gap; silently omitted, dangling, duplicate or conflicting coverage
  refuses the whole inspection. Duplicate holder/entry IDs, repeated holder
  links, duplicate authority kind/resource/version tuples, invalid aliases and
  target/holder/snapshot substitution also refuse, even for identical duplicates.
- `unknown_authorities` records possible authority outside this slice. False,
  empty gaps and all-known holders are **unverified declarations**, not real
  completeness, exclusion, freshness or resolved consequences.

The inspector checks inventory links against pinned base-ledger observations;
it does **not** parse application snapshot preimages or authenticate recording
provenance. The existing application descriptor inspector remains separate.
The inventory is a small synthetic slice, not the complete operational inventory
contract: auth/refresh lineage, gateways, containment boundaries and service
enforcement still require separately authorized investigation and evidence.
There are no issuer, excluded, resolved, ready or approved proof fields.

Six new tests reuse the existing ledger fixture and actual domain replay:
partial recorded/unjournaled coverage, unknown and copied holders, origin/scope/
checkpoint/identity substitution, malformed and duplicate links, explicit gaps,
and forged proof/raw-content fields. Exact 4096-byte inspection succeeds and
4097 bytes refuse. A coverage-gap relabeling regression failed before validation
was moved to all observations, then passed. Even completed synthetic history,
local acknowledgments, populated evidence digests and complete-looking inventory
cannot permit actual post-fence retry or replacement activation. History remains
unchanged. The positive fixture emits exact artificial inventory/bundle bytes.

## Restart, continuity and next gate

This module owns no files and silently loads nothing. Its file-reload test
models caller persistence, not a durable ledger service or crash-safe writer.
The independently current binding **and** exact bundle hash must survive outside
the backup/old holders. On restart/restore, lost input refuses inspection.
Matching a wholly rolled-back external register and bundle cannot be detected
locally. A digest chosen by the bundle's sender is not an independent input.
Inspection success never lifts runtime/scoped quarantine or permits epoch 2.

**Historical validation caveat:** this increment's default workspace run and its repeat
failed in unchanged `recovery_restart` at the first post-activation receive
(`membership required`), in different child stages. Isolated current/baseline
recovery tests passed; the full single-test-thread workspace run passed.
Serialization was not a fix. The subsequent
[fixture lifecycle correction](iroh-phase2.md#recovery-restart-fixture-identity-lifecycle)
proved wrong-holder routing with a controlled probe-order reversal, then stopped
the rollback holder and its client before opening the current holder/client.
All refusal assertions remain, with an added unchanged-disclosure check.
Prior failures remain preserved, not relabeled as passes.

## Composed application and inventory inspection, increment 10

**Mock-only, unreviewed, not deployed.** `inventory::inspect_application` accepts
the existing origin, history, independently selected binding/bundle hash, exact
bundle and inventory bytes, plus borrowed `descriptors::Application` checkpoint
and descriptor bytes. It calls the existing validators, not a second model of
rows, coverage or history. The standalone inspectors retain their prior contracts.

One bundle pin covers every observation. Its application checkpoint binds exactly
the typed application descriptor set, while its inventory digest binds coverage
of **all** observations. The origin/fence/policy/recovery binding remains exact.
Inventory and application checkpoints cannot be selected independently of that
binding. Every non-`unjournaled/` observation must validate as a typed application
row; unsupported opaque families refuse rather than disappear.

The returned `ApplicationInspection` separates:

- `application`: the existing validated typed descriptors.
- `unjournaled`: exact original `Observation` records with nonempty subjects and
  `effect:unknown`, including their unchanged snapshot and evidence digests.
- `inventory`: the existing declarations, including unknown attribution,
  copied holders, unknown additional holders/authorities and unmapped coverage.

Unjournaled digests remain **opaque ledger references**, not validated application
preimages or service receipts. They must be explicitly covered by an Unjournaled
effect declaration or an unmapped gap. They cannot appear in the application
checkpoint as invented rows. Rehashing contradictory/malformed links does not
help: inspection refuses the entire view, with no partial result. Identical
ledger/descriptor duplicates still collapse; duplicate coverage still refuses.
No absence, terminal row, local acknowledgment, evidence digest or complete-looking
inventory establishes exclusion, freshness, retry permission or activation.

Six public-boundary tests reuse the existing artificial SQLite/schema fixture
and domain history. They exercise the exact split, coverage gaps, unknown/copied
holders, independently pinned input substitution, rehashed contradictions,
missing journals, byte reload, forged descriptor content, unchanged history,
actual post-fence retry refusal and unconditional activation refusal. No live
store, collector, issuer verifier, runtime adapter or workflow service is added.

**Next bounded development candidate:** exact typed redacted fixture preimages
for the currently opaque unjournaled observation references, if separately
authorized, retaining Unknown and without a runtime exporter or service-proof
interpretation. This increment does not implement that later gate.

**Separate external blockers:** complete authorized holder/tool inventory,
verified service invalidation/accepted-effect guarantees, authenticated issuer
and containment evidence, replacement credential custody, real Unknown-effect
reconciliation, independent checkpoint continuity and durable runner admission.
They require separate operational authorization and verification. Deferred review
is not approval. No review retry, runtime activation, deployment, multihost,
quotas or remote Codex gate is completed here.
