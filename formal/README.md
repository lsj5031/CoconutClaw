# Bounded implementation-owned formal checks

These are hand-written TLA+ transition contracts linked to exact CoconutClaw
source bytes, not a Rust refinement proof or independent review.

The production runner lane adds `production-reconciliation.json`, preserving
every inherited pin and historical review record while separately recording
inspected production deltas/new inputs. `test_mapping.py` checks that complete
ancestry, not just current hashes. See `production-runners.md` for actual handler
actions and native evidence gaps. Affected official configurations are rechecked
on the final source identity; old harness receipts do not validate activation.

`windows-native-reconciliation.json` adds the next exact layer over published
114a1cf, retaining those historical records. It binds native Job Object/ACL
sources and tests while explicitly excluding Win32 behavior from the finite
proof claim. Windows CI execution and user fleet acceptance remain separate.

The five-lane review-fix integration adds `review-fixes-reconciliation.json`
over `2e8eb2d`, with individually inspected deltas and explicit executable gaps.
Historical ancestry is retained. The existing thirteen models and fifty cases
are unchanged and rerun, not widened into claims about media validation,
retention, parser/rendering behavior or native Windows file handles. See the
18 November section of the formal verification document.

Use the already provisioned official tools, offline:

```sh
python3 /home/leo/Code/personal/CoconutClaw/scripts/check-formal.py \
  --artifact-dir /home/leo/Code/personal/CoconutClaw/artifacts/formal-recheck-UNIQUE \
  --java /home/leo/.local/share/mise/installs/java/temurin-21.0.13+9.0.LTS/bin/java \
  --jar /home/leo/.local/share/tlaplus/v1.7.4/tla2tools.jar
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s /home/leo/Code/personal/CoconutClaw/formal -p 'test_*.py'
```

The artifact directory must be new. No installation, provider, Telegram, database
or network service is needed. The runner inherits the wrapper environment but
uses explicit Java and jar paths. It pins the official jar checksum, verifies
implementation hashes and declaration anchors, runs official SANY and sequential
TLC JVMs (512MiB heap, one worker, 90s default per JVM, configurable up to180s),
and records commands, input bytes/hashes, exits, elapsed time, state counts and
action coverage. A timeout or resource/tool error is incomplete, never a proof.
`--cases` selects named entries for affected rechecks, not a claim of the full gate.

- `implementation-hashes.json`:140 runtime/shared/test/build inputs. The original
 135 and multihost callback are joined by the UI reducer and two backend snapshot
 files, plus PR22's test-only native-binary locator. The test-profile arithmetic
 optimization changes neither signature validation nor model actions; no
 deadline is relaxed. `reconciliation.json` records the earlier UI inspection;
 `integration-reconciliation.json` records each inspected integration delta.
 The former records each of 31 inherited/UI pin
 deltas and its original committed preimage, not a blind hash refresh.
- `inventory.json`:177 repository paths (171 historical, four UI additions,
 the multihost callback and PR22 locator),
 including explicit excluded
  and evidence-only paths, declaration/SQL/conditional scans and individual gaps.
- `source-map.json`:owners, independent state, source declaration/hash anchors,
  guards, atomicity, durable/volatile cuts and per-family refinement gaps.
- `cases.json` and50 `.cfg` files:small/expanded safety,17 named invariant mutants,
  three conditional-liveness positives and two temporal mutants.
- Thirteen `.tla` files:ingress, tasks, effects, calendar, context, replication,
  process, immutable history, callback authority, guided input and three explicit
  cross-subsystem compositions.
- `test_runner.py`:supplementary fail-closed classification tests. Real official
  JVM negative controls, not these unit tests, establish that the mutants fail.
- `test_mapping.py`: current inventory/pin/declaration consistency, per-input
  reconciliation records, configuration identities and required-action presence.

The earlier multihost preparation rechecked all Process, RuntimeComposition and
RemoteComposition configurations against refreshed pins and added direct harness
anchors. Other model contracts/owners are unchanged, so their prior final03
receipts are explicitly reused, not presented as a new full-matrix execution.
Controlled native-tool gates/auth callback/OS cleanup and a shared Store with two
runner actors are not proved by these unchanged finite abstractions.
The UI reconciliation instead rechecks the **complete 50-case matrix**, including
all inherited families. New observation actions require actual coverage past
older pending, applying and Unknown events. UIAuthority checks finite actor/scope
authority and consumed-before-effect replay cuts; GuidedInput checks explicit
current-code input, replacement, expiry and stale cancellation. These independent
contracts are not one full parser/SQLite/runtime Cartesian product.

The positive gate requires a nontrivial fully exhausted finite graph, empty
queue, successful TLC exit and all declared required actions exercised.
Negative cases must exit nonzero with the intended named invariant trace, or
the sole configured temporal property's counterexample. Unexpected success,
wrong invariant, parser/resource failure or missing action coverage fails.
Each counterexample is retained. All models intentionally permit stuttering and
terminal/quarantined waits; `CHECK_DEADLOCK FALSE` is not a deadlock-freedom claim.
Only the explicitly fair, restricted liveness configurations assert progress.

See [scope, assumptions and remaining gaps](../docs/formal-verification.md).
Do not update an implementation pin to silence a failed mapping: inspect the
source change, update affected actions/models/tests, and rerun affected checks.
