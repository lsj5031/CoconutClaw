# Production runner source contract

Implementation-owned mapping, not an independent review or Rust refinement
proof. Exact-byte pins and declaration anchors are reconciled in
`production-reconciliation.json`, preserving every inherited pin and recording
each inspected production delta separately.

| Existing family | Production source action | Preserved cut / additional assumption |
| --- | --- | --- |
| Process Claim/Spawn | `nodes::Runner::admit`, `nodes::transport::Handler::dispatch`, `provider::run_workspace` | FULL-sync durable Start/claim precedes one nonserializable dispatch flag; no compatibility or restart reinvocation |
| Process Recover | `nodes::Runner::open` | orphan claim signs Unknown, never constructs another dispatch capability |
| RemoteComposition Reserve/LocalStart | `nodes::freeze_route`, `nodes::execute`, `Store::reserve_runtime_run` | session selection freezes with inbox admission; exact signed assignment and source route commit before send |
| RemoteComposition RunnerStart/Sign/Recover | `Runner::admit`, `Runner::finish`, unchanged sync `History::replay` | separate runner Store/key, per-assignment three-event history; shared-head/four-task fixture bounds do not apply |
| RemoteComposition Partition/Cancel | `transport::call`, `Cancellation::verify`, `Runner::cancel` | authenticated pinned coordinator plus assignment-bound signed intent; unavailable means Unknown, not local fallback |
| RemoteComposition Custody/Apply | `nodes::accept`, `Store::custody_runtime_result`, `Store::apply_runtime_results` | complete signed chain, original route/request verification, transactional inbox; Unknown is not erased by late Completed |
| UIAuthority Issue/Read/Consume | `telegram_ui::button`, `telegram_ui::handle` node selection | existing actor/session/expiry/consumed checks; future tasks only, frozen assignments unchanged |
| Tasks Recover | `nodes::recover`, `SessionScheduler::recover` | source restart leaves interrupted remote effects Unknown; durable pending result may apply before orphan marking |

Explicit gaps: the existing finite models do not prove native iroh or sandbox
implementation, tool-created descendants, key ACLs/custody, whole-backup
freshness, arbitrary fleet sizes, actual host network availability, continuous
heartbeat leases or external-provider exactly-once effects. Trusted nodes can
still lie about their own execution. Linux/Mac actual handlers/fake providers
and parent-owned native fixtures are separate evidence, not new model axioms.

Affected official configurations: Process, RemoteComposition,
RuntimeComposition, Tasks, UIAuthority and Ingress (all positive and mutant
cases). Exact pin/inventory/declaration reconciliation and official checks are
run on final formatted bytes. Old receipts are not evidence for the new handlers.
