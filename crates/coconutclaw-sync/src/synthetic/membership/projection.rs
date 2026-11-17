//! Local immutable disclosure receipts, committed before the derived projection.
//! Not wire input, policy authority, or a replacement for a lost owner high-water.
use super::*;

const JOURNAL: &str = "record-journal.sqlite";

pub(super) fn create(
    root: &Path,
    store: EndpointId,
    owner: EndpointId,
    namespace: NamespaceId,
) -> Result<()> {
    let journal = Sqlite::open(root.join(JOURNAL))?;
    journal.execute_batch(
        "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
         CREATE TABLE binding(store TEXT NOT NULL, owner TEXT NOT NULL, namespace TEXT NOT NULL);
         CREATE TABLE receipt(
           author TEXT NOT NULL, key BLOB NOT NULL, source TEXT NOT NULL,
           hash TEXT NOT NULL, body BLOB NOT NULL, artifact BLOB NOT NULL,
           PRIMARY KEY(author,key));",
    )?;
    journal.execute(
        "INSERT INTO binding VALUES (?1,?2,?3)",
        params![store.to_string(), owner.to_string(), namespace.to_string()],
    )?;
    // Persist the new journal's directory entry as well as its SQLite commits.
    // Windows cannot open a directory handle, so this durability step is Unix-only.
    #[cfg(unix)]
    File::open(root)?.sync_all()?;
    Ok(())
}

pub(super) fn require_stores(root: &Path) -> Result<()> {
    for name in [
        "projection.sqlite",
        "docs/docs.redb",
        "docs/default-author",
        "blobs/blobs.db",
        JOURNAL,
    ] {
        ensure!(
            fs::symlink_metadata(root.join(name)).is_ok_and(|m| m.file_type().is_file()),
            "record recovery unavailable: missing or nonregular {name}"
        );
    }
    Ok(())
}

pub(super) fn open(
    root: &Path,
    store: EndpointId,
    owner: EndpointId,
    namespace: NamespaceId,
) -> Result<Sqlite> {
    let journal = Sqlite::open_with_flags(
        root.join(JOURNAL),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    journal.execute_batch("PRAGMA synchronous=FULL;")?;
    let binding: (String, String, String) =
        journal.query_row("SELECT store,owner,namespace FROM binding", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    ensure!(
        binding == (store.to_string(), owner.to_string(), namespace.to_string()),
        "record journal authority mismatch"
    );
    Ok(journal)
}

pub(super) fn require_authority(
    root: &Path,
    store: EndpointId,
    owner: EndpointId,
    namespace: NamespaceId,
) -> Result<()> {
    // A present but empty/replaced SQLite file is not continuity. Do not let
    // the ordinary schema initializer silently mint a new policy high-water.
    let sql = Sqlite::open_with_flags(
        root.join("projection.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let endpoint: String = sql.query_row("SELECT endpoint FROM identity", [], |r| r.get(0))?;
    let binding: (String, String) = sql.query_row(
        "SELECT owner,namespace FROM membership WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    ensure!(
        endpoint == store.to_string() && binding == (owner.to_string(), namespace.to_string()),
        "record recovery unavailable: authority continuity lost"
    );
    // Missing publication history is not an empty journal to initialize.
    sql.prepare("SELECT id,namespace,author,body,artifact,published FROM publication LIMIT 0")?;
    Ok(())
}

pub(super) fn validate(
    author: AuthorId,
    key: &[u8],
    hash: &str,
    body: &[u8],
    artifact: &[u8],
) -> Result<()> {
    ensure!(body.len() <= MAX_RECORD, "journal body too large");
    let record: Record = serde_json::from_slice(body)?;
    let artifact_hash = record.validate(author, key)?;
    ensure!(
        Hash::new(body).to_string() == hash
            && artifact.len() as u64 == record.artifact.size
            && Hash::new(artifact) == artifact_hash,
        "immutable record corruption"
    );
    Ok(())
}

// Compare all bytes, not only the hash column of a potentially divergent SQL row.
fn matching_projection(
    sql: &Sqlite,
    namespace: NamespaceId,
    author: AuthorId,
    key: &[u8],
    hash: &str,
    bundle: &Bundle,
) -> Result<bool> {
    let old = sql
        .query_row(
            "SELECT hash,body,artifact FROM received WHERE namespace=?1 AND author=?2 AND key=?3",
            params![namespace.to_string(), author.to_string(), key],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?;
    if let Some((saved_hash, body, artifact)) = old {
        ensure!(
            saved_hash == hash && body == bundle.body && artifact == bundle.artifact,
            "immutable projection conflict"
        );
        return Ok(true);
    }
    Ok(false)
}

fn project(
    sql: &Sqlite,
    namespace: NamespaceId,
    author: AuthorId,
    key: &[u8],
    hash: &str,
    bundle: &Bundle,
) -> Result<bool> {
    if matching_projection(sql, namespace, author, key, hash, bundle)? {
        return Ok(false);
    }
    sql.execute(
        "INSERT INTO received VALUES (?1,?2,?3,?4,?5,?6)",
        params![
            namespace.to_string(),
            author.to_string(),
            key,
            hash,
            bundle.body,
            bundle.artifact
        ],
    )?;
    Ok(true)
}

pub(super) fn commit(
    journal: &mut Sqlite,
    sql: &mut Sqlite,
    source: EndpointId,
    request: &Request,
    bundle: &Bundle,
    authorized: impl Fn() -> Result<()>,
) -> Result<bool> {
    let hash = Hash::new(&bundle.body).to_string();
    validate(
        request.author,
        request.key.as_bytes(),
        &hash,
        &bundle.body,
        &bundle.artifact,
    )?;
    // Detect SQL conflicts BEFORE adding authority to the journal.
    matching_projection(
        sql,
        request.namespace,
        request.author,
        request.key.as_bytes(),
        &hash,
        bundle,
    )?;
    let tx = journal.transaction()?;
    let old = tx
        .query_row(
            "SELECT hash,body,artifact FROM receipt WHERE author=?1 AND key=?2",
            params![request.author.to_string(), request.key.as_bytes()],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?;
    if let Some((saved, body, artifact)) = old {
        ensure!(
            saved == hash && body == bundle.body && artifact == bundle.artifact,
            "immutable journal conflict"
        );
    } else {
        tx.execute(
            "INSERT INTO receipt VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                request.author.to_string(),
                request.key.as_bytes(),
                source.to_string(),
                hash,
                bundle.body,
                bundle.artifact
            ],
        )?;
    }
    tx.commit()?;
    let tx = sql.transaction()?;
    let inserted = project(
        &tx,
        request.namespace,
        request.author,
        request.key.as_bytes(),
        &hash,
        bundle,
    )?;
    authorized()?;
    tx.commit()?;
    Ok(inserted)
}

impl ScopedPeer {
    /// Replay locally retained, verified disclosure receipts, never remote docs
    /// metadata. Current endpoint/author authority is required for every row.
    /// SQL-ahead history, corruption and conflicts abort the entire transaction.
    /// Whole SQL or docs loss cannot establish policy/deletion continuity.
    pub fn repair_projection(&mut self) -> Result<usize> {
        let state = self.boundary.lock()?;
        self.boundary.policy(&state)?;
        let tx = self.peer.sql.transaction()?;
        {
            let mut rows =
                tx.prepare("SELECT namespace,author,key,hash,body,artifact FROM received")?;
            let mut rows = rows.query([])?;
            while let Some(row) = rows.next()? {
                let namespace: String = row.get(0)?;
                let author: String = row.get(1)?;
                let key: Vec<u8> = row.get(2)?;
                let expected: Option<(String, Vec<u8>, Vec<u8>)> = self
                    .journal
                    .query_row(
                        "SELECT hash,body,artifact FROM receipt WHERE author=?1 AND key=?2",
                        params![author, key],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .optional()?;
                ensure!(
                    namespace == self.boundary.namespace.to_string()
                        && expected == Some((row.get(3)?, row.get(4)?, row.get(5)?)),
                    "record recovery unavailable: SQL ahead or conflicting"
                );
            }
        }
        let mut repaired = 0;
        let mut receipts = self.journal.prepare(
            "SELECT author,key,source,hash,body,artifact FROM receipt ORDER BY author,key",
        )?;
        let mut receipts = receipts.query([])?;
        while let Some(row) = receipts.next()? {
            let author: String = row.get(0)?;
            let author: AuthorId = author.parse()?;
            let key: Vec<u8> = row.get(1)?;
            let source: String = row.get(2)?;
            self.boundary.author(&state, source.parse()?, author)?;
            let hash: String = row.get(3)?;
            let bundle = Bundle {
                body: row.get(4)?,
                artifact: row.get(5)?,
            };
            validate(author, &key, &hash, &bundle.body, &bundle.artifact)?;
            repaired += usize::from(project(
                &tx,
                self.boundary.namespace,
                author,
                &key,
                &hash,
                &bundle,
            )?);
        }
        // The lease may have expired during a long replay.
        self.boundary.policy(&state)?;
        tx.commit()?;
        Ok(repaired)
    }
}
