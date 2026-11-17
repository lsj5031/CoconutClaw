# Storage lifecycle

`Store::open` is startup/bootstrap: it creates the database, selects WAL mode,
applies schema/migrations and initializes the task graph and memory. A separate
`.initialize.lock` serializes that entire sequence across processes. It is not
the execution-owner lock. Startup may wait for another initializer or writer.

Runtime workers use `Store::connect`. It opens an existing database without
creating directories, taking the initialization lock, changing journal mode,
applying schema, migrating or backfilling data. Both paths install SQLite's
five-second busy timeout **before** any SQL. Foreign-key enforcement retains
the existing connection default; this change does not silently alter legacy
database semantics.

`doctor` uses configuration preview only. It creates no layout, templates,
database or lock files, takes no execution-owner lock, and makes no network or
authentication probes. It remains available while the listener owns the data
directory or SQLite has an active writer. Its configuration/binary-presence
checks are not database integrity checks or proof of runtime health.

## Online retention

The polling runtime performs bounded maintenance once per minute, including
the first iteration. Each transaction deletes at most 256 rows from each of:

* `telegram_ui_inputs` and `telegram_ui_controls`, only at/after their explicit
  expiry. Consumed but unexpired controls remain until expiry.
* `telegram_ui_observations` and `telegram_reconciliation_controls`, only when
  the associated ingress is `done`, acknowledgment is `sent`, progress is
  `none` or `sent`, delivery evidence is valid and has no `sending` or `unknown`
  phase, and the ingress is at least 30 days old. These tables are operational
  claim-bypass projections, not receipts or reconciliation evidence. Pending,
  applying and Unknown rows never qualify. Unparseable dates/evidence are kept.

The transaction is repeatable and safe alongside admission and delivery.
Maintenance errors are logged and retried at the next maintenance interval,
not by retrying any external effect. Expiry indexes bound capability selection.
No vacuum, checkpoint or external-file deletion runs in the polling loop.

## Evidence and archival

There is deliberately **no TTL** for Telegram ingress (including raw payloads
and update identities), inbox/event keys, schedule occurrence/creation keys,
task runs/payloads/attempts/waits, approvals, remote custody, notification
decisions, owner preferences, reconciliation offers/audit, or memory provenance
and command receipts. They are needed for exact replay/conflict detection,
dependencies, deduplication or immutable evidence, even after apparent success.
Unknown is never aged into success, deletion or retry permission.

Resolved operational projections are not an excuse to delete their source
rows. In particular, blocker generations and all immutable decision rows remain
online. Automatic pruning of task history or payloads is not implemented.

For long-term archival, stop the instance and all administrative writers, then
make and verify a complete SQLite backup (or a consistent SQLite backup through
an offline local database tool). Keep the full database, not selected table
exports; copying only the main file of a live WAL database is not a backup.
Treat backups as private user data with the same access controls as the source.
No automatic archive deletion or retention period is imposed. Restore as a
whole, retaining all replay/identity keys and evidence. Do not start a second
listener from a backup against the same Telegram identity. Online row archival
that removes receipts would need a separate durable tombstone/replay design
and is intentionally not inferred from elapsed time.
