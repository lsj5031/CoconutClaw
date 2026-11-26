use super::{Acknowledgment, CutoverReceipt, ValidatedFinal};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::path::Path;

const HELD_AUTHORITY: (i64, i64, i64) = (1, 0, 0);

pub(super) fn import(
    path: &Path,
    baseline: &[u8],
    delta: &[u8],
    final_state: ValidatedFinal,
) -> Result<Acknowledgment> {
    let mut db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    db.execute_batch(
        "PRAGMA trusted_schema=OFF; PRAGMA foreign_keys=ON; PRAGMA synchronous=EXTRA;",
    )?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let previous = tx
        .query_row(
            "SELECT baseline,delta,projection FROM local_legacy_cutover WHERE slot=1",
            [],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()
        .context("expected a stopped phase2-local gateway ledger")?;
    let has_work: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM inbox UNION ALL SELECT 1 FROM tasks
            UNION ALL SELECT 1 FROM attempts UNION ALL SELECT 1 FROM agent_sequences
            UNION ALL SELECT 1 FROM executor_evidence UNION ALL SELECT 1 FROM outbox
            UNION ALL SELECT 1 FROM delivery_receipts
            UNION ALL SELECT 1 FROM pilot_delivery WHERE not_before<>0
            UNION ALL SELECT 1 FROM schedule UNION ALL SELECT 1 FROM occurrences)",
        [],
        |row| row.get(0),
    )?;
    ensure!(!has_work, "gateway ledger already contains work");
    if let Some((saved_baseline, saved_delta, saved_projection)) = previous {
        ensure!(
            saved_baseline == baseline
                && saved_delta == delta
                && saved_projection == final_state.projection,
            "conflicting gateway cutover replay"
        );
    } else {
        ensure!(
            !tx.prepare("SELECT 1 FROM local_authority")?.exists([])?,
            "gateway authority is already assigned"
        );
        ensure!(
            !tx.prepare("SELECT 1 FROM local_legacy_receipts")?
                .exists([])?,
            "gateway contains unbound legacy receipts"
        );
        tx.execute(
            "INSERT INTO local_authority(slot,generation,attempt_floor,active) VALUES(1,?1,?2,?3)",
            params![HELD_AUTHORITY.0, HELD_AUTHORITY.1, HELD_AUTHORITY.2],
        )?;
        tx.execute(
            "INSERT INTO local_legacy_cutover(slot,baseline,delta,projection) VALUES(1,?1,?2,?3)",
            params![baseline, delta, final_state.projection],
        )?;
        for receipt in &final_state.receipts {
            tx.execute(
                "INSERT INTO local_legacy_receipts(run,start,result) VALUES(?1,?2,?3)",
                params![receipt.run, receipt.start, receipt.result],
            )?;
        }
    }
    let stored_authority: (i64, i64, i64) = tx.query_row(
        "SELECT generation,attempt_floor,active FROM local_authority WHERE slot=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(
        stored_authority == HELD_AUTHORITY,
        "gateway authority is no longer held"
    );
    let mut statement =
        tx.prepare("SELECT run,start,result FROM local_legacy_receipts ORDER BY run")?;
    let stored = statement
        .query_map([], |row| {
            Ok(CutoverReceipt {
                run: row.get(0)?,
                start: row.get(1)?,
                result: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    ensure!(
        stored == final_state.receipts,
        "stored receipt custody mismatch"
    );
    tx.commit()?;
    Ok(final_state.ack)
}
