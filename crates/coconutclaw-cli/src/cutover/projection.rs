use crate::nodes::CutoverReceipt;
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, Transaction, types::ValueRef};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(super) type Projection = BTreeMap<String, Vec<Row>>;
pub(super) type Replacements = Projection;
type Row = BTreeMap<String, Cell>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum Cell {
    Null,
    Integer(i64),
    Text(String),
    Blob(Vec<u8>),
}

impl Cell {
    fn integer(&self) -> Result<i64> {
        match self {
            Self::Integer(value) => Ok(*value),
            _ => bail!("expected integer"),
        }
    }

    fn text(&self) -> Result<&str> {
        match self {
            Self::Text(value) => Ok(value),
            _ => bail!("expected text"),
        }
    }

    fn blob(&self) -> Result<&[u8]> {
        match self {
            Self::Blob(value) => Ok(value),
            _ => bail!("expected blob"),
        }
    }
}

const MEMORY: &[&str] = &[
    "memory_records",
    "memory_active",
    "memory_history",
    "memory_command_receipts",
    "memory_import_origins",
    "memory_search",
    "memory_search_data",
    "memory_search_idx",
    "memory_search_content",
    "memory_search_docsize",
    "memory_search_config",
];
const NODES: &[&str] = &[
    "paired_nodes",
    "node_selection",
    "node_routes",
    "node_assignments",
];
const FENCES: &[&str] = &[
    "legacy_admission_freeze",
    "legacy_cutover_seal",
    "legacy_executor_seal",
];

#[derive(Debug, PartialEq, Eq)]
struct Column {
    name: String,
    kind: String,
    key: i64,
    required: bool,
}

fn tables(db: &Connection) -> Result<BTreeSet<String>> {
    let mut stmt = db.prepare("SELECT name FROM sqlite_schema WHERE type IN ('table','view')")?;
    Ok(stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

fn columns(db: &Connection, table: &str) -> Result<Vec<Column>> {
    let mut stmt = db.prepare(
        "SELECT name,type,pk,hidden,\"notnull\" FROM pragma_table_xinfo(?1) ORDER BY name",
    )?;
    let mut rows = stmt.query([table])?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next()? {
        ensure!(
            row.get::<_, i64>(3)? == 0,
            "unsupported hidden column in {table}"
        );
        columns.push(Column {
            name: row.get(0)?,
            kind: row.get(1)?,
            key: row.get(2)?,
            required: row.get(4)?,
        });
    }
    Ok(columns)
}

fn schema() -> Result<BTreeMap<String, Vec<Column>>> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(crate::store::SCHEMA_SQL)?;
    db.execute_batch("ALTER TABLE turns ADD COLUMN channel TEXT NOT NULL DEFAULT 'telegram';")?;
    db.execute_batch(crate::nodes::SCHEMA)?;
    db.execute_batch(crate::store::LEGACY_EXECUTOR_SEAL_SCHEMA)?;
    tables(&db)?
        .into_iter()
        .map(|table| {
            let columns = columns(&db, &table)?;
            Ok((table, columns))
        })
        .collect()
}

fn projected(table: &str) -> bool {
    !FENCES.contains(&table) && !matches!(table, "node_signed_receipts" | "node_claims")
}

fn key_columns<'a>(table: &str, columns: &'a [Column]) -> Vec<&'a str> {
    if table == "sqlite_sequence" {
        return vec!["name"];
    }
    let mut keys: Vec<_> = columns.iter().filter(|column| column.key > 0).collect();
    keys.sort_by_key(|column| column.key);
    keys.into_iter()
        .map(|column| column.name.as_str())
        .collect()
}

fn key<'a>(row: &'a Row, keys: &[&str]) -> Vec<&'a Cell> {
    keys.iter().map(|name| &row[*name]).collect()
}

pub(super) fn capture(tx: &Transaction<'_>, frozen: bool) -> Result<Projection> {
    let schema = schema()?;
    let present = tables(tx)?;
    for table in &present {
        ensure!(
            schema.contains_key(table) || MEMORY.contains(&table.as_str()),
            "unsupported source table or view {table}"
        );
    }
    for (table, expected) in &schema {
        if !present.contains(table) {
            ensure!(
                NODES.contains(&table.as_str())
                    || matches!(table.as_str(), "node_claims" | "legacy_executor_seal"),
                "missing source table {table}"
            );
        } else {
            ensure!(
                &columns(tx, table)? == expected,
                "unsupported columns in {table}"
            );
        }
    }
    for table in FENCES {
        let fences: Vec<i64> = if present.contains(*table) {
            tx.prepare(&format!("SELECT singleton FROM {table}"))?
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?
        } else {
            Vec::new()
        };
        ensure!(
            if frozen {
                fences == [1]
            } else {
                fences.is_empty()
            },
            "invalid {table} for {} capture",
            if frozen { "frozen final" } else { "baseline" }
        );
    }
    let violations = tx.prepare("PRAGMA foreign_key_check")?.exists([])?;
    ensure!(!violations, "source has broken foreign keys");
    if present.contains("node_claims") {
        ensure!(
            !tx.prepare("SELECT 1 FROM node_claims")?.exists([])?,
            "unsupported runner claims in source"
        );
    }
    if tx
        .prepare("SELECT 1 FROM node_signed_receipts")?
        .exists([])?
    {
        ensure!(
            present.contains("node_assignments"),
            "unbound signed receipts"
        );
        ensure!(
            !tx.prepare(
                "SELECT 1 FROM node_signed_receipts s
                WHERE NOT EXISTS(SELECT 1 FROM node_assignments a WHERE a.run=s.task_run_id)"
            )?
            .exists([])?,
            "unbound signed receipts"
        );
    }
    let mut projection = Projection::new();
    for (table, columns) in schema.into_iter().filter(|(table, _)| projected(table)) {
        let mut values = Vec::new();
        if present.contains(&table) {
            let names = columns
                .iter()
                .map(|column| format!("\"{}\"", column.name))
                .collect::<Vec<_>>()
                .join(",");
            let mut stmt = tx.prepare(&format!("SELECT {names} FROM \"{table}\""))?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let mut value = Row::new();
                for (index, column) in columns.iter().enumerate() {
                    let cell = match row.get_ref(index)? {
                        ValueRef::Null => Cell::Null,
                        ValueRef::Integer(value) => Cell::Integer(value),
                        ValueRef::Text(value) => Cell::Text(std::str::from_utf8(value)?.to_owned()),
                        ValueRef::Blob(value) => Cell::Blob(value.to_vec()),
                        ValueRef::Real(_) => {
                            bail!("unsupported real value in {table}.{}", column.name)
                        }
                    };
                    value.insert(column.name.clone(), cell);
                }
                match table.as_str() {
                    "sqlite_sequence" if MEMORY.contains(&value["name"].text()?) => continue,
                    "kv" if value["key"].text()?.starts_with("codex_session:") => continue,
                    _ => values.push(value),
                }
            }
        }
        let keys = key_columns(&table, &columns);
        values.sort_by(|left, right| key(left, &keys).cmp(&key(right, &keys)));
        projection.insert(table, values);
    }
    Ok(projection)
}

pub(super) fn validate(projection: &Projection) -> Result<()> {
    let schema = schema()?;
    ensure!(
        projection.keys().map(String::as_str).eq(schema
            .keys()
            .map(String::as_str)
            .filter(|table| projected(table))),
        "unknown or missing projection tables"
    );
    for (table, rows) in projection {
        let cols = &schema[table];
        let keys = key_columns(table, cols);
        let mut previous = None;
        for row in rows {
            ensure!(
                row.keys().eq(cols.iter().map(|column| &column.name)),
                "unsupported row in {table}"
            );
            for column in cols {
                let cell = &row[&column.name];
                let supported = match cell {
                    Cell::Null => {
                        !column.required
                            && !keys.contains(&column.name.as_str())
                            && table != "sqlite_sequence"
                    }
                    Cell::Integer(_) => {
                        column.kind == "INTEGER"
                            || column.name == "seq" && table == "sqlite_sequence"
                    }
                    Cell::Text(_) => {
                        column.kind == "TEXT" || column.name == "name" && table == "sqlite_sequence"
                    }
                    Cell::Blob(_) => column.kind == "BLOB",
                };
                ensure!(supported, "unsupported value in {table}.{}", column.name);
            }
            let identity = key(row, &keys);
            ensure!(
                !keys.is_empty() && previous.is_none_or(|last| last < identity),
                "invalid row order or duplicate key in {table}"
            );
            previous = Some(identity);
        }
    }
    let kv = &projection["kv"];
    ensure!(
        kv.iter()
            .any(|row| row["key"].text().ok() == Some("schema_version")
                && row["value"].text().ok() == Some("12"))
            && kv
                .iter()
                .any(|row| row["key"].text().ok() == Some("runtime_graph_v1")
                    && row["value"].text().ok() == Some("1")),
        "unsupported source schema version"
    );
    ensure!(
        !kv.iter().any(|row| row["key"]
            .text()
            .is_ok_and(|name| name.starts_with("inflight_"))),
        "unresolved legacy ingress checkpoint"
    );
    for row in kv {
        ensure!(
            matches!(
                row["key"].text()?,
                "schema_version"
                    | "runtime_graph_v1"
                    | "last_update_id"
                    | "last_ignored_update_id"
                    | "last_ignored_reason"
                    | "last_ignored_at"
                    | "blocker_observation_cursor"
            ),
            "unsupported legacy key/value record"
        );
    }
    poll_cursor(projection)?;
    validate_lineage(projection)?;
    unknown_23(projection)?;
    Ok(())
}

fn by_id(rows: &[Row], name: &str, id: &Cell) -> Option<usize> {
    rows.binary_search_by(|row| row[name].cmp(id)).ok()
}

fn lookup<'a>(projection: &'a Projection, table: &str, name: &str, id: &Cell) -> Result<&'a Row> {
    let rows = &projection[table];
    by_id(rows, name, id)
        .map(|index| &rows[index])
        .with_context(|| format!("missing {table} binding"))
}

fn poll_cursor(projection: &Projection) -> Result<u64> {
    lookup(
        projection,
        "kv",
        "key",
        &Cell::Text("last_update_id".into()),
    )?["value"]
        .text()?
        .parse()
        .context("invalid Telegram polling cursor")
}

fn validate_lineage(projection: &Projection) -> Result<()> {
    let attempts = &projection["runtime_attempts"];
    let mut current = BTreeMap::new();
    for attempt in attempts {
        let run = lookup(projection, "task_runs", "id", &attempt["run_id"])?;
        let task = lookup(projection, "runtime_tasks", "task_id", &attempt["task_id"])?;
        let ordinal = attempt["ordinal"].integer()?;
        if ordinal == 1 {
            ensure!(
                attempt["run_id"] == task["task_id"] && attempt["parent_run_id"] == Cell::Null,
                "invalid first attempt"
            );
        } else {
            let parent = lookup(
                projection,
                "runtime_attempts",
                "run_id",
                &attempt["parent_run_id"],
            )?;
            ensure!(
                ordinal > 1
                    && parent["ordinal"].integer()? == ordinal - 1
                    && parent["task_id"] == attempt["task_id"],
                "invalid attempt ancestry"
            );
        }
        let existing = current
            .entry(&attempt["task_id"])
            .or_insert((ordinal, &attempt["run_id"]));
        if ordinal > existing.0 {
            *existing = (ordinal, &attempt["run_id"]);
        }
        ensure!(
            run["session_id"] == task["session_id"] && run["channel"] == task["channel"],
            "attempt route disagrees with logical task"
        );
    }
    for task in &projection["runtime_tasks"] {
        ensure!(
            current
                .get(&task["task_id"])
                .is_some_and(|(_, run)| **run == task["current_run_id"]),
            "invalid current run"
        );
    }
    for run in &projection["task_runs"] {
        lookup(projection, "runtime_attempts", "run_id", &run["id"])?;
        ensure!(
            matches!(
                run["status"].text()?,
                "queued"
                    | "running"
                    | "awaiting_approval"
                    | "waiting_remote"
                    | "waiting_event"
                    | "cancel_requested"
                    | "completed"
                    | "failed"
                    | "cancelled"
            ),
            "unsupported task run status"
        );
    }
    Ok(())
}

fn unknown_23(projection: &Projection) -> Result<(&Row, &Row)> {
    let task = lookup(projection, "runtime_tasks", "task_id", &Cell::Integer(23))
        .context("source task #23 is absent")?;
    let run = lookup(projection, "task_runs", "id", &task["current_run_id"])?;
    ensure!(
        run["status"].text()? == "failed"
            && run["error_summary"]
                .text()
                .is_ok_and(|summary| summary.contains("outcome unknown")),
        "source task #23 lacks original Unknown evidence"
    );
    Ok((task, run))
}

pub(super) fn changes(baseline: &Projection, final_state: &Projection) -> Result<Replacements> {
    ensure!(
        unknown_23(baseline)? == unknown_23(final_state)?,
        "source task #23 Unknown record changed"
    );
    ensure!(
        poll_cursor(final_state)? >= poll_cursor(baseline)?,
        "Telegram polling cursor regressed"
    );
    for (table, keys, mutable) in [
        ("runtime_tasks", &["task_id"][..], &["current_run_id"][..]),
        ("runtime_attempts", &["run_id"][..], &[][..]),
        (
            "task_payloads",
            &["task_run_id"][..],
            &["output", "delivery_state", "delivered"][..],
        ),
        (
            "task_runs",
            &["id"][..],
            &[
                "status",
                "started_at",
                "finished_at",
                "cancel_requested_at",
                "progress_message_id",
                "last_progress",
                "error_summary",
                "result_summary",
            ][..],
        ),
        ("runtime_inbox", &["event_key"][..], &[][..]),
        (
            "telegram_ingress",
            &["seq"][..],
            &[
                "session_id",
                "state",
                "route",
                "output",
                "progress_message_id",
                "progress_state",
                "ack_phase",
                "delivery_state",
            ][..],
        ),
        (
            "runtime_schedules",
            &["id"][..],
            &["revision", "request", "timing", "state", "next_due"][..],
        ),
        (
            "schedule_occurrences",
            &["schedule_id", "revision", "due"][..],
            &[][..],
        ),
        (
            "runtime_schedule_runs",
            &["schedule_id", "task_run_id"][..],
            &[][..],
        ),
        ("node_routes", &["run"][..], &[][..]),
        ("node_assignments", &["run"][..], &[][..]),
        ("runtime_remote_runs", &["task_run_id"][..], &[][..]),
        ("runtime_remote_results", &["seq"][..], &["state"][..]),
        (
            "runtime_notifications",
            &["id"][..],
            &["disposition", "delivery_state"][..],
        ),
    ] {
        let final_rows = &final_state[table];
        for prior in &baseline[table] {
            let identity = key(prior, keys);
            let index = final_rows
                .binary_search_by(|row| key(row, keys).cmp(&identity))
                .ok()
                .with_context(|| format!("removed {table} identity"))?;
            let next = &final_rows[index];
            ensure!(
                prior
                    .iter()
                    .all(|(name, value)| mutable.contains(&name.as_str()) || next[name] == *value),
                "changed {table} identity"
            );
            if table == "runtime_schedules" {
                ensure!(
                    next["revision"] >= prior["revision"],
                    "schedule revision regressed"
                );
                if next["revision"] == prior["revision"]
                    && let (Cell::Integer(old), Cell::Integer(new)) =
                        (&prior["next_due"], &next["next_due"])
                {
                    ensure!(new >= old, "schedule due watermark regressed");
                }
            }
        }
    }
    for prior in &baseline["sqlite_sequence"] {
        let next = lookup(final_state, "sqlite_sequence", "name", &prior["name"])?;
        ensure!(next["seq"] >= prior["seq"], "source sequence regressed");
    }
    Ok(final_state
        .iter()
        .filter(|(table, rows)| baseline[*table] != **rows)
        .map(|(table, rows)| (table.clone(), rows.clone()))
        .collect())
}

pub(super) fn assignment_runs(projection: &Projection) -> Result<Vec<i64>> {
    let assignments = &projection["node_assignments"];
    let reservations = &projection["runtime_remote_runs"];
    ensure!(
        assignments
            .iter()
            .map(|row| &row["run"])
            .eq(reservations.iter().map(|row| &row["task_run_id"])),
        "remote reservations and tracked assignments disagree"
    );
    assignments.iter().map(|row| row["run"].integer()).collect()
}

pub(super) fn receipt_binding<'a>(
    projection: &'a Projection,
    receipt: &CutoverReceipt,
) -> Result<(&'a str, &'a str, &'a [u8])> {
    let run = Cell::Integer(receipt.run);
    let assignment = lookup(projection, "node_assignments", "run", &run)?;
    let reserved = lookup(projection, "runtime_remote_runs", "task_run_id", &run)?;
    let execution = lookup(projection, "task_runs", "id", &run)?;
    let payload = lookup(projection, "task_payloads", "task_run_id", &run)?;
    ensure!(
        reserved["session_id"] == execution["session_id"]
            && reserved["request"] == payload["request"],
        "remote source route or request changed"
    );
    if let Some(projected) = projection["runtime_remote_results"]
        .iter()
        .find(|row| row["task_run_id"] == run)
    {
        ensure!(
            projected["start"].blob()? == receipt.start
                && projected["result"].blob()? == receipt.result,
            "signed evidence differs from the frozen projection"
        );
    }
    Ok((
        assignment["bundle"].text()?,
        reserved["origin"].text()?,
        reserved["assignment"].blob()?,
    ))
}
