use std::fs::{DirBuilder, File, OpenOptions};
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use coconutclaw_gateway::executor_wire::{Event, Evidence, SignedEvidence};
use ed25519_dalek::{Signer, SigningKey};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use super::link::Scope;

pub struct Intent {
    pub attempt: i64,
    pub prompt: String,
}

pub struct Pending {
    pub id: i64,
    pub event: String,
    pub proof: SignedEvidence,
}

pub struct Journal {
    db: Connection,
    _lock: File,
    scope: Scope,
}

const SCHEMA: &str = "
    PRAGMA application_id=1128482610;
    CREATE TABLE identity (
        singleton INTEGER PRIMARY KEY CHECK (singleton=1),
        instance TEXT NOT NULL,
        public_key TEXT NOT NULL,
        sequence INTEGER NOT NULL CHECK (sequence BETWEEN 0 AND 2147483647)
    ) STRICT;
    CREATE TABLE attempts (
        attempt INTEGER PRIMARY KEY CHECK (attempt BETWEEN 1 AND 512),
        prompt TEXT,
        phase TEXT NOT NULL CHECK (phase IN ('intent', 'consumed', 'sealed')),
        CHECK ((phase='sealed') = (prompt IS NULL))
    ) STRICT;
    CREATE UNIQUE INDEX one_active_attempt ON attempts((1)) WHERE phase!='sealed';
    CREATE TABLE evidence (
        id INTEGER PRIMARY KEY,
        attempt INTEGER NOT NULL REFERENCES attempts(attempt),
        event TEXT NOT NULL CHECK (event IN ('started', 'unknown', 'result', 'cancelled')),
        payload TEXT NOT NULL,
        signature TEXT NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0,1)),
        UNIQUE (attempt,event)
    ) STRICT;
";

impl Journal {
    pub fn open_scoped(
        dir: &Path,
        key: &SigningKey,
        init: bool,
        scope: Scope,
        predecessor: Option<&str>,
        rotate_key: bool,
    ) -> Result<Self> {
        if init {
            DirBuilder::new().mode(0o700).create(dir)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(init)
            .open(dir.join("executor.lock"))?;
        FileExt::try_lock_exclusive(&lock).context("executor journal is already in use")?;
        let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
        if init {
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
        }
        let mut db = Connection::open_with_flags(dir.join("executor.sqlite3"), flags)?;
        db.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        let instance = scope.journal_identity();
        let public_key = hex::encode(key.verifying_key().to_bytes());
        if init {
            let txn = db.transaction()?;
            txn.execute_batch(SCHEMA)?;
            txn.execute(
                "INSERT INTO identity VALUES(1,?,?,0)",
                params![instance, public_key],
            )?;
            txn.commit()?;
        }
        ensure!(
            db.query_row("PRAGMA application_id", [], |row| row.get::<_, i64>(0))? == 1128482610,
            "executor journal format mismatch"
        );
        let identity: (String, String) = db.query_row(
            "SELECT instance,public_key FROM identity WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if scope.placement().is_some() || identity.0 == instance {
            ensure!(
                identity == (instance, public_key),
                "executor journal identity mismatch"
            );
        } else {
            ensure!(
                predecessor.is_some_and(|previous| if rotate_key {
                    identity.0.strip_prefix(previous).is_some_and(|id| {
                        id.len() == 64
                            && id
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    })
                } else {
                    identity.0 == previous && identity.1 == public_key
                }),
                "executor generation must advance exactly once"
            );
            let txn = db.transaction()?;
            ensure!(
                txn.query_row(
                    "SELECT NOT EXISTS(SELECT 1 FROM attempts WHERE phase!='sealed')
                       AND NOT EXISTS(SELECT 1 FROM evidence WHERE acknowledged=0)",
                    [],
                    |row| row.get::<_, bool>(0)
                )?,
                "executor has unresolved launch authority or unacknowledged evidence"
            );
            txn.execute(
                "UPDATE identity SET instance=?,public_key=? WHERE singleton=1",
                params![instance, public_key],
            )?;
            txn.commit()?;
        }
        Ok(Self {
            db,
            _lock: lock,
            scope,
        })
    }

    #[cfg(target_os = "linux")]
    pub fn process_lock(&self) -> Result<File> {
        Ok(self._lock.try_clone()?)
    }

    pub fn sync_sequence(&self, floor: i64) -> Result<()> {
        self.db.execute(
            "UPDATE identity SET sequence=MAX(sequence,?) WHERE singleton=1",
            [floor],
        )?;
        Ok(())
    }

    pub fn next_sequence(&self) -> Result<i64> {
        let txn = self.db.unchecked_transaction()?;
        txn.execute(
            "UPDATE identity SET sequence=sequence+1 WHERE singleton=1",
            [],
        )?;
        let sequence = txn.query_row(
            "SELECT sequence FROM identity WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        txn.commit()?;
        Ok(sequence)
    }

    pub fn remember(&self, intent: &Intent) -> Result<()> {
        self.db.execute(
            "INSERT INTO attempts(attempt,prompt,phase) VALUES(?,?,'intent')",
            params![intent.attempt, intent.prompt],
        )?;
        Ok(())
    }

    pub fn intent(&self) -> Result<Option<Intent>> {
        Ok(self
            .db
            .query_row(
                "SELECT attempt,prompt FROM attempts WHERE phase='intent'",
                [],
                |row| {
                    Ok(Intent {
                        attempt: row.get(0)?,
                        prompt: row.get(1)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn consume(&self, attempt: i64) -> Result<bool> {
        Ok(self.db.execute(
            "UPDATE attempts SET phase='consumed' WHERE attempt=? AND phase='intent'",
            [attempt],
        )? == 1)
    }

    pub fn recover(&mut self, key: &SigningKey) -> Result<()> {
        let consumed = self
            .db
            .query_row(
                "SELECT attempt FROM attempts WHERE phase='consumed'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(attempt) = consumed {
            self.finish(key, attempt, [Event::Unknown])?;
        }
        Ok(())
    }

    pub fn finish(
        &mut self,
        key: &SigningKey,
        attempt: i64,
        events: impl IntoIterator<Item = Event>,
    ) -> Result<()> {
        let txn = self.db.transaction()?;
        ensure!(
            txn.execute(
                "UPDATE attempts SET phase='sealed',prompt=NULL WHERE attempt=? AND phase!='sealed'",
                [attempt],
            )? == 1,
            "attempt is already sealed"
        );
        for event in events {
            Self::record(&txn, &self.scope, key, attempt, event)?;
        }
        txn.commit()?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub fn started(&self, key: &SigningKey, attempt: i64) -> Result<()> {
        Self::record(&self.db, &self.scope, key, attempt, Event::Started)
    }

    fn record(
        db: &Connection,
        scope: &Scope,
        key: &SigningKey,
        attempt: i64,
        event: Event,
    ) -> Result<()> {
        let name = event.name();
        let payload = serde_json::to_string(&Evidence { attempt, event })?;
        let signature = hex::encode(key.sign(&scope.evidence_bytes(&payload)).to_bytes());
        db.execute(
            "INSERT INTO evidence(attempt,event,payload,signature) VALUES(?,?,?,?)",
            params![attempt, name, payload, signature],
        )?;
        Ok(())
    }

    pub fn pending(&self) -> Result<Option<Pending>> {
        Ok(self
            .db
            .query_row(
                "SELECT id,event,payload,signature FROM evidence WHERE acknowledged=0 ORDER BY id LIMIT 1",
                [],
                |row| {
                    Ok(Pending {
                        id: row.get(0)?,
                        event: row.get(1)?,
                        proof: SignedEvidence {
                            payload: row.get(2)?,
                            signature: row.get(3)?,
                        },
                    })
                },
            )
            .optional()?)
    }

    pub fn acknowledge(&self, id: i64) -> Result<()> {
        self.db
            .execute("UPDATE evidence SET acknowledged=1 WHERE id=?", [id])?;
        Ok(())
    }
}
