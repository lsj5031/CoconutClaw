use crate::nodes::CutoverReceipt;
use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use iroh::EndpointId;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

mod projection;
use projection::{Projection, Replacements, capture, changes, receipt_binding, validate};

const VERSION: u32 = 1;
const CUSTODY_ID: i64 = 0x43434c43;

#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand, Debug)]
enum Action {
    /// Capture a baseline from a standalone SQLite backup.
    Baseline {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        source_id: String,
        #[arg(long)]
        coordinator_key: Option<EndpointId>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Export the frozen delta and exact signed terminal receipts.
    ExportFinal {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        baseline_digest: blake3::Hash,
        #[arg(long)]
        output: PathBuf,
    },
    /// Stage a pinned baseline in a new offline custody database.
    Stage {
        #[arg(long)]
        custody: PathBuf,
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        baseline_digest: blake3::Hash,
    },
    /// Commit a pinned final delta and acknowledge durable receipt custody.
    ImportFinal {
        #[arg(long)]
        custody: PathBuf,
        #[arg(long)]
        delta: PathBuf,
        #[arg(long)]
        final_digest: blake3::Hash,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Baseline {
    version: u32,
    source_id: String,
    coordinator_key: Option<EndpointId>,
    projection: Projection,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Delta {
    version: u32,
    source_id: String,
    baseline_digest: String,
    projection_digest: String,
    replacements: Replacements,
    receipts: Vec<CutoverReceipt>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Acknowledgment {
    source_id: String,
    baseline_digest: String,
    final_digest: String,
    projection_digest: String,
    receipt_digests: BTreeMap<i64, String>,
}

pub(crate) fn run(args: &Args) -> Result<()> {
    match &args.action {
        Action::Baseline {
            source,
            source_id,
            coordinator_key,
            output,
        } => {
            ensure!(!source_id.trim().is_empty(), "missing source identity");
            let mut source = open_source(source)?;
            let tx = source.transaction()?;
            let projection = capture(&tx, false)?;
            validate(&projection)?;
            let baseline = Baseline {
                version: VERSION,
                source_id: source_id.clone(),
                coordinator_key: *coordinator_key,
                projection,
            };
            print_digest(publish(output, &baseline)?)?;
        }
        Action::ExportFinal {
            source,
            baseline,
            baseline_digest,
            output,
        } => {
            let baseline = read_pinned(baseline, *baseline_digest)?;
            let delta = export_final(source, &parse_baseline(&baseline)?, *baseline_digest)?;
            print_digest(publish(output, &delta)?)?;
        }
        Action::Stage {
            custody,
            baseline,
            baseline_digest,
        } => {
            let baseline = read_pinned(baseline, *baseline_digest)?;
            stage(custody, &baseline, *baseline_digest)?;
            print_digest(*baseline_digest)?;
        }
        Action::ImportFinal {
            custody,
            delta,
            final_digest,
        } => {
            let raw = read_pinned(delta, *final_digest)?;
            let ack = import_final(custody, &raw, *final_digest)?;
            writeln!(std::io::stdout().lock(), "{}", serde_json::to_string(&ack)?)?;
        }
    }
    Ok(())
}

fn print_digest(digest: blake3::Hash) -> Result<()> {
    Ok(writeln!(std::io::stdout().lock(), "{digest}")?)
}

fn open_source(path: &Path) -> Result<Connection> {
    let mut header = [0; 20];
    File::open(path)?.read_exact(&mut header)?;
    ensure!(
        &header[..16] == b"SQLite format 3\0" && header[18..20] == [1, 1],
        "source must be a standalone rollback-mode SQLite backup"
    );
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        ensure!(
            !Path::new(&sidecar).try_exists()?,
            "source has a SQLite sidecar"
        );
    }
    let source = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source.execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF;")?;
    Ok(source)
}

fn read_pinned(path: &Path, expected: blake3::Hash) -> Result<Vec<u8>> {
    let raw = fs::read(path)?;
    ensure!(blake3::hash(&raw) == expected, "artifact digest mismatch");
    Ok(raw)
}

fn temp_beside(path: &Path) -> Result<tempfile::NamedTempFile> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    Ok(tempfile::NamedTempFile::new_in(
        parent.unwrap_or(Path::new(".")),
    )?)
}

fn publish(path: &Path, value: &impl Serialize) -> Result<blake3::Hash> {
    let raw = serde_json::to_vec(value)?;
    let mut temp = temp_beside(path)?;
    temp.write_all(&raw)?;
    temp.as_file().sync_all()?;
    temp.persist_noclobber(path)?;
    Ok(blake3::hash(&raw))
}

fn parse_baseline(raw: &[u8]) -> Result<Baseline> {
    let baseline: Baseline = serde_json::from_slice(raw)?;
    ensure!(
        baseline.version == VERSION && !baseline.source_id.trim().is_empty(),
        "unsupported baseline or missing source identity"
    );
    validate(&baseline.projection)?;
    Ok(baseline)
}

fn export_final(path: &Path, baseline: &Baseline, digest: blake3::Hash) -> Result<Delta> {
    let mut source = open_source(path)?;
    let tx = source.transaction()?;
    let final_state = capture(&tx, true)?;
    validate(&final_state)?;
    let replacements = changes(&baseline.projection, &final_state)?;
    let receipts = crate::nodes::cutover_receipts(&tx, baseline.coordinator_key)?;
    verify_receipts(&final_state, baseline.coordinator_key, &receipts)?;
    Ok(Delta {
        version: VERSION,
        source_id: baseline.source_id.clone(),
        baseline_digest: digest.to_string(),
        projection_digest: blake3::hash(&serde_json::to_vec(&final_state)?).to_string(),
        replacements,
        receipts,
    })
}

fn verify_receipts(
    projection: &Projection,
    coordinator: Option<EndpointId>,
    receipts: &[CutoverReceipt],
) -> Result<()> {
    let runs = projection::assignment_runs(projection)?;
    ensure!(
        runs.iter()
            .copied()
            .eq(receipts.iter().map(|receipt| receipt.run)),
        "terminal receipt coverage mismatch"
    );
    for receipt in receipts {
        let coordinator = coordinator.context("missing pinned coordinator key")?;
        let (bundle, origin, assignment) = receipt_binding(projection, receipt)?;
        receipt
            .verify(bundle, origin, assignment, Some(coordinator))
            .with_context(|| format!("verify receipt for source run {}", receipt.run))?;
    }
    Ok(())
}

fn stage(path: &Path, raw: &[u8], digest: blake3::Hash) -> Result<()> {
    parse_baseline(raw)?;
    let temp = temp_beside(path)?;
    {
        let mut db = Connection::open(temp.path())?;
        let tx = db.transaction()?;
        tx.pragma_update(None, "application_id", CUSTODY_ID)?;
        tx.execute_batch(
            "CREATE TABLE baseline(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                digest TEXT NOT NULL, artifact BLOB NOT NULL);
             CREATE TABLE final_delta(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                digest TEXT NOT NULL, artifact BLOB NOT NULL, projection BLOB NOT NULL);
             CREATE TABLE signed_receipts(run INTEGER PRIMARY KEY,
                start BLOB NOT NULL, result BLOB NOT NULL);",
        )?;
        tx.execute(
            "INSERT INTO baseline VALUES(1,?1,?2)",
            params![digest.to_string(), raw],
        )?;
        tx.commit()?;
    }
    temp.as_file().sync_all()?;
    match temp.persist_noclobber(path) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let db = open_custody(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            ensure!(
                staged_baseline(&db)? == (digest.to_string(), raw.to_vec()),
                "conflicting staged baseline"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn open_custody(path: &Path, flags: OpenFlags) -> Result<Connection> {
    let db = Connection::open_with_flags(path, flags)?;
    db.execute_batch("PRAGMA trusted_schema=OFF;")?;
    let app: i64 = db.pragma_query_value(None, "application_id", |row| row.get(0))?;
    ensure!(app == CUSTODY_ID, "expected an offline custody database");
    Ok(db)
}

fn staged_baseline(db: &Connection) -> Result<(String, Vec<u8>)> {
    Ok(db.query_row(
        "SELECT digest,artifact FROM baseline WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}

fn import_final(path: &Path, raw: &[u8], digest: blake3::Hash) -> Result<Acknowledgment> {
    let delta: Delta = serde_json::from_slice(raw)?;
    let mut db = open_custody(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    db.pragma_update(None, "synchronous", "EXTRA")?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (baseline_digest, baseline_raw) = staged_baseline(&tx)?;
    ensure!(
        blake3::hash(&baseline_raw).to_string() == baseline_digest
            && delta.baseline_digest == baseline_digest,
        "staged baseline digest mismatch"
    );
    let baseline = parse_baseline(&baseline_raw)?;
    ensure!(
        delta.version == VERSION && delta.source_id == baseline.source_id,
        "wrong source identity or delta version"
    );
    let mut final_state = baseline.projection.clone();
    final_state.extend(delta.replacements.clone());
    validate(&final_state)?;
    ensure!(
        changes(&baseline.projection, &final_state)? == delta.replacements,
        "invalid delta replacements"
    );
    let projection = serde_json::to_vec(&final_state)?;
    ensure!(
        blake3::hash(&projection).to_string() == delta.projection_digest,
        "final projection digest mismatch"
    );
    verify_receipts(&final_state, baseline.coordinator_key, &delta.receipts)?;
    let final_digest = digest.to_string();
    let previous: Option<(String, Vec<u8>, Vec<u8>)> = tx
        .query_row(
            "SELECT digest,artifact,projection FROM final_delta WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((stored_digest, stored_artifact, stored_projection)) = previous {
        ensure!(
            stored_digest == final_digest
                && stored_artifact == raw
                && stored_projection == projection,
            "conflicting final delta replay"
        );
    } else {
        tx.execute(
            "INSERT INTO final_delta VALUES(1,?1,?2,?3)",
            params![final_digest, raw, projection],
        )?;
        for receipt in &delta.receipts {
            tx.execute(
                "INSERT INTO signed_receipts VALUES(?1,?2,?3)",
                params![receipt.run, receipt.start, receipt.result],
            )?;
        }
    }
    let mut statement = tx.prepare("SELECT run,start,result FROM signed_receipts ORDER BY run")?;
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
    ensure!(stored == delta.receipts, "stored receipt custody mismatch");
    let receipt_digests = stored
        .iter()
        .map(|receipt| {
            Ok((
                receipt.run,
                blake3::hash(&serde_json::to_vec(receipt)?).to_string(),
            ))
        })
        .collect::<Result<_>>()?;
    tx.commit()?;
    Ok(Acknowledgment {
        source_id: delta.source_id,
        baseline_digest,
        final_digest,
        projection_digest: delta.projection_digest,
        receipt_digests,
    })
}

#[cfg(test)]
mod tests;
