# iroh Phase 2, synthetic increments

15 November 2026. **Partial, synthetic-only, not deployed. Replicated application
state is not complete.** No remote execution, CLI configuration or runtime
adapters are enabled. `coconutclaw-sync` is a separate workspace library, not a
dependency of the Telegram/Codex executable.

## Increment 2: owner policy and scoped record boundary

`synthetic::membership::ScopedPeer` is a separate, opt-in synthetic boundary.
The original `Peer` below remains the static-pair transport fixture, **not**
an alternative membership API. Do not open a scoped store through that fixture.
No application configuration or deployed executable uses either API.

### Why native shared capabilities are not the boundary

Inspection of the exact cached pins established:

- `iroh-docs::net::handle_connection` has a namespace acceptance callback, but
  namespaces/read capabilities do not express endpoint-bound record scope.
- `BlobsProtocol` forwards the whole store to the standard provider. A known
  hash is sufficient to request its content; there is no record-policy callback.
- Gossip can originate background connections, so wrapping only incoming
  protocol handlers or public `connect` calls would be insufficient.
- iroh 1.4.1 publicly exports `EndpointHooks`. `before_connect` intercepts
  internal outgoing connections, and `after_handshake` intercepts both
  directions before returning a connection to the application. Weak connection
  handles support closing established connections without keeping them alive.

The concrete resolution is a dedicated `/coconutclaw/synthetic-record/1` ALPN.
**Native docs, gossip and blob protocols are not registered** on a scoped peer;
the endpoint hooks additionally deny those ALPNs in both directions. Retained
namespace capabilities or known hashes cannot bypass the record boundary.
Docs and blobs remain local durable publication stores, using the reviewed R1/R2
handoff and tombstone protections unchanged. There is no new crypto primitive,
dependency, automatic metadata reconciliation or shared-document wire access.

Each scoped store binds one owner public key, one namespace and the existing
`synthetic/chat-a` scope. It cannot serve arbitrary scopes, namespaces, blob
hashes or filesystem paths. A request names one immutable author/key. The server
reads only that namespace, validates the record and independently hashes its
bounded body/artifact before responding. The client validates both before one
FULL-synchronous SQLite projection transaction. Transport authenticates the
serving endpoint; the owner policy binds its permitted docs authors. This wire
format does not forward or independently verify a docs entry signature.
No store, raw endpoint, Doc or capability is returned by the scoped wrapper.

### Authority, freshness and revocation

An offline owner signs a complete snapshot using the pinned iroh key library's
Ed25519 signing and strict signature verification. Domain-separated signed bytes
bind namespace, exact scope, positive generation, expiration, member endpoints
and each endpoint's unique permitted authors. Membership is never inferred
from a QUIC handshake. Synthetic limits are 32 members, four authors per member
and a lease no longer than one day.

Snapshots are provisioned explicitly through a trusted **local** API on each
peer, not accepted from arbitrary remote peers. SQLite durably pins owner and
namespace, highest generation, exact policy bytes and observed wall time before
activation. Lower generations and different equal-generation snapshots fail,
including after restart. A failed policy transaction cannot activate access.
Unknown, invalid, wrong-owner, expired and wrong-scope snapshots fail closed.
An absent member/author is revoked. Reinstalling a lease cannot extend its
active monotonic deadline. A trustworthy local wall clock is required across
process restarts; observed clock rollback rejects installation.

Every policy installation closes all tracked connections before returning,
including connections to members retained in the new generation. Requests and
responses must use the current exact generation. Expiration closes established
connections on a monotonic timer. Authorization is checked again before sending
and before projection. Closing a connection fences subsequent traffic; bytes
already disclosed or handed to the transport before revocation are not erased.
Projection remains inert, never jobs, memory promotion, prompts or sends.

**Freshness limit:** a signature proves authority, not that no newer policy
exists elsewhere. There is no remote latest-policy oracle. Every reopen starts
quarantined, even with a saved valid policy, until the trusted local caller
installs independently established current policy. With an intact high-water
mark, stale policy replay is rejected. A wholly rolled-back backup also rolls
back its high-water mark: automatically establishing the latest policy and
preventing an operator from explicitly installing a still-valid stale snapshot
from that backup are **not implemented**. Do not use this API for backup
recovery. Full backup quarantine/latest-policy recovery remains a required gate,
not a property established by ordinary reopen tests.

The sole enabled wire operation returns a bounded JSON bundle (65536 bytes
maximum; original 8192-byte body and 4096-byte artifact limits unchanged).
It handles one request per connection with a ten-second deadline. Wire format,
policy limits and lease duration are synthetic choices, not runtime adapters,
quotas, a general policy framework or a production protocol commitment.

### Behavioral checks

Six new tests exercise actual loopback connections: valid record/artifact
exchange and duplicate inert projection; wrong owner and tampering; unknown
policy with a raw client; rollback and equal-generation forks after reopen;
quarantined reopen; stale author replay at the server even when the caller
bypasses its local predicate; active partial-request revocation and reconnect;
lease expiry; forbidden scope/namespace/stale generation; raw-client native
docs/gossip/blob negotiation rejection despite retained identifiers; and
injected SQLite policy commit failure. Rejected record requests are checked for
**zero response chunks**, not merely failure to project. Positive exchange
after rejected requests ensures that refusal is not an inert implementation.
Scoped `flush` additionally repairs a pending publication after reopen with
the original write capability and current policy, while refusing repair during
quarantine and over a persisted covering deletion.
The existing crash, acknowledged repair and covering-prefix deletion tests
remain required and unchanged. These are same-host synthetic checks, not
multi-host membership, owner backup restore or full replicated-state completion.

## Selected components and evidence

| Component | Exact direct pin | Enabled features |
| --- | --- | --- |
| iroh | 1.4.1 | tls-ring |
| iroh-docs | 0.101.0 | fs-store |
| iroh-blobs | 0.103.1 | fs-store |
| iroh-gossip | 0.101.0 | net |

Default features are disabled for all four. Their published manifests declare
Rust 1.91; development here uses 1.98.1. Cargo.lock supplies checksummed
transitive pins, including redb 4.6.0 and Tokio 1.54.1. The docs manifest requires
iroh `^1`, blobs `^0.103`, gossip `^0.101.0`. These pins have now compiled and
exchanged synthetic data, not merely passed manifest inspection.
The dev dependency additionally enables iroh's `test-utils` for the official
loopback relay fixture, not for the public library or CLI.

Ordinary `cargo info ...@...` and `cargo fetch` succeeded in this environment.
The first offline lookup failed because docs was not cached. No global
configuration, auth or registry changes were necessary. Subsequent builds/checks
use `--locked --offline`. Nine preexisting single-version packages moved because
of the selected dependency graph; attempted offline precise downgrades and
their concrete constraint failures are retained in the dependency receipt logs.

Official source evidence is the downloaded, Cargo-checksummed crate source:

- docs `examples/setup.rs`, `tests/util.rs`, `src/protocol.rs`, `src/api.rs`:
  router registration, persistent engine and read-only capabilities;
- docs `src/store.rs`: download policies do not bound by content size;
- blobs `src/protocol.rs`: bounded byte-range requests rounded to 1024-byte chunks;
- blobs `src/net_protocol.rs`: router shutdown already shuts down the blob store;
- iroh `src/endpoint/presets.rs`: Minimal avoids N0 relay/discovery defaults.

The earlier [roadmap sources](roadmap.md#sources) remain the published deployment
evidence, not newly refreshed web evidence. The cached blobs README explicitly
says this release is **not yet considered production quality**. Core iroh 1.x
does not change that protocol-layer warning.

## What runs

```sh
cargo test -p coconutclaw-sync --locked --offline
```

Tests create only fresh temporary directories, synthetic identities and
loopback listeners. Existing directories cannot be adopted by `create`.
`reopen` requires the synthetic marker, the same endpoint identity and an
exclusive file lock. The marker prevents accidental use, not malicious local
filesystem access. Endpoint secrets are caller-supplied, never serialized or
logged; tests use public, deterministic fixture keys that must never be used
outside these synthetic stores.

Each peer registers docs, blobs and gossip behind an authenticated endpoint-ID
allowlist. This static pair pin is **not membership, revocation or owner
authorization**. A read-only namespace capability prevents receiver writes.
The publisher must explicitly open its document after restart to activate sync.
Only explicit loopback IP addresses are accepted by public outbound methods;
relays, public discovery and port mapping are disabled there. A private
unit-test-only override permits exactly the fixture's local relay URL.

A synthetic record contains a version, docs-author-qualified monotonic SQLite
ID, fixed synthetic chat scope, at most 2048 UTF-8 text bytes and a separate
artifact hash/size. JSON is bounded to 8192 bytes; artifacts to 4096 bytes.
Unknown fields, scope/origin/key mismatches and invalid bounds are rejected.
The receiving SQLite table is inert: it is not runtime memory, provider context,
an executable job, a scheduler or a delivery queue. Marker-looking text has no
authority. There is no received-text promotion path.

## Store ownership and handoff

- `projection.sqlite` is the authoritative synthetic application journal and
  received-record projection, with FULL synchronous transactions.
- `docs/docs.redb` owns protocol metadata, namespaces/capabilities and authors;
  `docs/default-author` identifies the persistent default author. It is not SQL.
- `blobs/` owns separate blob metadata, content and verification state.
- `writer.lock` excludes a second cooperating store owner. It is not a distributed
  fence. No SQLite database file is replicated.

Publication commits bytes and artifact in a SQLite intent before adding blobs or
writing docs metadata. Flush reconciles **all** intents for the document, including
already acknowledged rows left by the original buffered-write ordering bug.
An indexed keyset scan reads one bounded intent at a time, with no fixed total
repair cap or persistent cursor that could skip a lost entry. Work is linear in
that document's retained history (indexed seeks per record); it is not a
production retention/quota design. A conflict stops the scan and returns an
error, rather than silently skipping the conflicting row or later history.
When an exact entry is absent, reconciliation checks every proper byte prefix
of its validated event key, including the empty prefix, for deletion markers
in the same namespace and author. Exact lookups alone cannot see covering
tombstones in the pinned API. This costs at most one indexed lookup per key
byte on the missing-entry path, independent of namespace metadata volume, and
does not cap historical repair. Existing matching entries are not recreated.

For each row, flush checks existing metadata (including deletion markers), adds
both original body and artifact, awaits blob `Store::sync_db()`, sets missing
metadata, awaits docs `SyncHandle::flush_store()`, then acknowledges SQLite.
Pinned `Docs::persistent` hides the sync handle, so the library constructs the
same persistent `Engine` through public APIs and retains its sync handle.
No persistence sleep or shutdown is used. Both barriers propagate failures;
an interrupted pre-ack handoff remains replayable from SQLite.

The blob database barrier commits **inline** data and outboards, not arbitrary
external blob files. The store explicitly configures 8192-byte inline data and
outboard thresholds; the existing record (8192) and artifact (4096) limits put
every supported publication and its Bao outboard inside that database. These
limits are unchanged, not a new truncation or weakening of the publication
contract. Increasing them requires revisiting durability. Reconciliation also
reimports both blobs when metadata exists, repairing older buffered blob loss.
No GC is enabled; temporary tags remain held through acknowledgment.

A crash between protocol commit and SQLite acknowledgment cannot replace the
event or duplicate its projection. Matching entries are not rewritten, preserving
their signed timestamp; different hash/length or a tombstone stops with an
immutable conflict rather than overwriting history or resurrecting a deletion.

Automatic content downloads are disabled. Explicit range requests are bounded,
including when metadata understates a blob's real size. Bao verifies downloaded
ranges; complete-store size and an independent full BLAKE3 rehash must agree
before parsing or projection. Artifact verification is also required before
the one SQLite projection transaction. No metadata-only projection occurs.
Duplicate reception after commit is a no-op; conflicting immutable history is
an error, not a memory correction.

These tests cover restart with a pending publication; replay after a lost
publication receipt; SQLite insertion failure before commit; receiver
disconnect with an event created offline; persistent peer reopen; duplicate
projection; malformed/oversized/dishonest-size content; forged scope/origin;
unapproved author; namespace mismatch; read-only write rejection; and unpaired
incoming connections on all three protocols. A separate integration test also
creates/exchanges/shuts down both stores in one child process and reopens them
in a fresh process, verifies the previous projection/author/sequence and
exchanges a second event. `publication_crash` additionally reuses independent
review R1's public-boundary recipe: clean initialization, fresh-process reopen
and publication, external immediate SIGKILL after successful flush, then
fresh-process reopen of intact stores. Three trials check metadata **before**
replay, fetch body and artifact before SQL can reconstruct them, and project
exactly once across repeated replay. Another public-boundary test repairs three
already acknowledged metadata-less intents, reopens and transfers them, and
rejects acknowledged body conflicts and tombstones unchanged. Both regressions
failed against the pre-fix implementation. No delay is a persistence barrier.
These are same-host process-crash tests, not power-loss or two-host recovery
drills.

The public-boundary `prefix_deletion` regression also persists deletions before
reopening in a fresh process, rejects repeated replay, and reopens again with a
fresh read-only receiver. It covers three events under author-qualified,
partial-byte and empty prefixes, plus an exact-key deletion control whose
uncovered events still transfer and project exactly once. Covering deletions
remain absent and cannot project. This regression failed against the R1-fixed
source before the prefix check was added.

## Application record map, next increment contract

This table is the intended application map, **not implemented runtime export**.
`store-id` must be a stable owner-bound core/store identity; local SQL row numbers
alone are not portable. Immutable event IDs must survive device-key rotation.
The synthetic author-qualified scheme tests transport without claiming to meet
that rotation requirement.

| Domain / immutable event key | Scope / authorized writer | Authoritative state / receipt |
| --- | --- | --- |
| `membership/<owner>/<generation>` | Owner-signed policy, authorized recipients only | Core SQLite policy high-water mark; future scoped snapshot distribution |
| `jobs/<store-id>/<job>/<attempt>/<event>` | Origin chat and assigned runner; core assigns, exact attempt reports | Core SQL assignment/delivery; runner SQL attempt/result; inert remote projection |
| `messages/<store-id>/<turn>` | Explicit origin chat grant; core writes | Core SQL turns; redacted opt-in records, no polling cursors |
| `memory/<store-id>/<event>` | Origin chat; core alone promotes/corrects/undoes | Core SQL immutable history/effective state; FTS derived; runner proposals stay untrusted |
| `artifacts/<store-id>/<record>` | Same readers as referenced job/chat; no sender-local paths | SQL descriptor/receipt; separately verified blobs retained by explicit references |

Credentials, owner/device secret keys, Codex sessions/auth, Telegram tokens,
delivery credentials and raw unreviewed transcript exports stay local. Job
arrival must never invoke the current scheduler. Attempt ownership needs its
own validated domain transition, not a docs author or QUIC handshake.

## Narrow docs fit check

Docs works for this immutable event transport and persistent reconciliation.
Concrete blockers to using it directly as application authority:

1. Namespace read/write capabilities are not per-peer, per-chat membership.
   Standard blob service serves known hashes to allowed endpoints without
   per-record scope checks. The static one-pair/synthetic-store boundary here
   must not become a multi-scope production boundary.
2. Docs timestamps allow replacing a key. Domain history and revocation cannot
   use "latest doc entry wins"; use immutable events plus SQL transition checks.
3. Download policy filters keys, not sizes or application authorization.
   Explicit verified bounded fetching is required.
4. redb, blob state and SQLite cannot commit together. Replayable journal
   handoff is required, including loss/restore tests beyond the modeled seams.

Resolve those directly before runtime wiring. No alternate-protocol comparison
is reopened. Production needs per-scope disclosure enforcement at *all* three
protocol boundaries, retention/GC pins and storage/metadata quotas; a trusted
but faulty peer can otherwise exhaust disk with metadata or many bounded blobs.
No automatic blob GC is enabled in this increment.

## Recovery and core transfer, required next

The scoped increment above tests owner-signed snapshots and live revocation,
not application membership roles, recovery or core transfer. Remaining design
direction, **not implemented recovery authority**:

- Separate offline owner signing authority from device endpoint/docs keys.
  Back up owner secret offline plus a signed recovery manifest containing stable
  store identity, latest policy generation, active core/epoch, revoked identities
  and durable state checkpoint hashes. Never replicate the secret.
- Grants bind device key, role, scopes, permitted authors and policy generation.
  Each receiver durably retains the highest accepted owner generation and
  revocation set. Data authors cannot grant membership. A revoked author's
  replayed old grant/event cannot reduce that generation or restore access.
- Revocation must close established connections as well as reject new ones,
  update projection policy, and rotate namespace/read capabilities where a
  revoked device retained them. It cannot erase content already learned.
  Restoring an old policy backup must not silently roll back revocations.
- Recovery requires an independently trusted latest signed policy/high-water
  mark, not just a stale backup or whichever peer connects first. If freshness
  cannot be established offline, remain quarantined. Test actual owner signing,
  device revocation, old-author replay and revoked-device backup restore.
- Core transfer is manual: stop and operationally exclude the old core, reconcile
  unknown attempts/effects, obtain an owner-signed higher epoch bound to the new
  core, persist the fence on all trusted runners, restore and verify application
  and protocol checkpoints, then enable the new core. No automatic failover.
  A partitioned or malicious old core holding Telegram credentials can still
  send effects; an application epoch is not external credential fencing.
  If old-core exclusion cannot be proved, do not activate the replacement.
- Test pending-publication and pending-projection restores with docs/redb missing,
  blobs missing, SQLite behind/ahead and stale membership. Require exact SQL
  replay consistency and no resurrection of undone/promoted memory or execution.

The scoped increment above implements bounded live connection revocation;
the static-pair fixture does not. These drills, runtime scope adapters, stable
owner-bound origins, broader process-kill/power-loss tests and retention limits remain open. No
replicated-state completion or owner-recovery claim follows from fixture keys.

## Relay and discovery

Prototype selection: explicit loopback direct addresses, Minimal preset,
RelayMode::Disabled, no address lookup. Direct QUIC is exercised deterministically.
The unit suite also runs the official in-process `iroh-relay` fixture, bound to
loopback, with both peers' IP transports removed and only relay addresses.
The same record, separate artifact, verification and SQL projection path passes
through that relay. The fixture's self-signed TLS certificate is accepted only
by its private test builder; public library methods have no TLS bypass.
This proves forced-relay operation, **not** automatic path failover during an
active transfer, public relay availability or real NAT traversal.

Public development relays are not selected or contacted. Eventual deployment selection is an
explicitly configured owner-controlled relay and explicit paired addresses
(no public discovery initially), subject to separate approval for hosting, DNS,
TLS, reachability, maintenance and spend. A relay is not an offline record
mailbox. No production relay choice is approved by this document.
