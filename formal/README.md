# Bounded implementation-owned formal checks

These are hand-written TLA+ transition contracts linked to exact CoconutClaw
source bytes, not a Rust refinement proof or independent review.

Use the already provisioned official tools, offline:

```sh
python3 /home/leo/Code/personal/CoconutClaw/scripts/check-formal.py \
  --artifact-dir /home/leo/Code/personal/CoconutClaw/artifacts/formal-recheck-UNIQUE \
  --java /home/leo/.local/share/mise/installs/java/temurin-21.0.13+9.0.LTS/bin/java \
  --jar /home/leo/.local/share/tlaplus/v1.7.4/tla2tools.jar
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s /home/leo/Code/personal/CoconutClaw/formal -p test_runner.py
```

The artifact directory must be new. No installation, provider, Telegram, database
or network service is needed. The runner inherits the wrapper environment but
uses explicit Java and jar paths. It pins the official jar checksum, verifies
implementation hashes and declaration anchors, runs official SANY and sequential
TLC JVMs (512MiB heap, one worker, 90s default per JVM, configurable up to180s),
and records commands, input bytes/hashes, exits, elapsed time, state counts and
action coverage. A timeout or resource/tool error is incomplete, never a proof.
`--cases` selects named entries for affected rechecks, not a claim of the full gate.

- `implementation-hashes.json`:136 runtime/shared/test/build inputs. The original
 135 were pinned before modeling. The17 November test-only multihost delta
 refreshes eight changed pins and adds the host-local auth callback; it changes
 no normal executable/dependency/SQL behavior. See the source-map supplement.
- `inventory.json`:171 pre-formal repository paths, including explicit excluded
  and evidence-only paths, declaration/SQL/conditional scans and individual gaps.
- `source-map.json`:owners, independent state, source declaration/hash anchors,
  guards, atomicity, durable/volatile cuts and per-family refinement gaps.
- `cases.json` and38 `.cfg` files:small/expanded safety,11 named invariant mutants,
  three conditional-liveness positives and two temporal mutants.
- Eleven `.tla` files:ingress, tasks, effects, calendar, context, replication,
  process, immutable history and three explicit cross-subsystem compositions.
- `test_runner.py`:supplementary fail-closed classification tests. Real official
  JVM negative controls, not these unit tests, establish that the mutants fail.

The multihost preparation rechecks all Process, RuntimeComposition and
RemoteComposition configurations against refreshed pins and added direct harness
anchors. Other model contracts/owners are unchanged, so their prior final03
receipts are explicitly reused, not presented as a new full-matrix execution.
Controlled native-tool gates/auth callback/OS cleanup and a shared Store with two
runner actors are not proved by these unchanged finite abstractions.

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
