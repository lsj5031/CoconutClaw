//! Explicitly paired native runners. No discovery, credential transfer or remote
//! filesystem API. Each assignment has an independent three-event history.
use crate::store::{Store, TaskRunStatus};
use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use coconutclaw_config::RuntimeConfig;
use coconutclaw_sync::synthetic::{
    SCOPE,
    membership::domain::{Action, BoundOrigin, History, Origin, Outcome, SignedEvent},
};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, Signature, endpoint::presets};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub(crate) mod desktop;
#[cfg(test)]
mod tests;
mod transport;

const REVISION: &str = env!("COCONUTCLAW_REVISION");
const ALPN: &[u8] = b"/coconutclaw/runner/1";

#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    #[command(subcommand)]
    action: Command,
}
#[derive(Subcommand, Debug)]
enum Command {
    /// Create this computer's node identity and public pairing card.
    Enroll {
        #[arg(long)]
        role: Role,
        /// This computer's reachable IP address and fixed UDP port.
        #[arg(long)]
        bind: SocketAddr,
        /// Permitted working directory, required only for a runner.
        #[arg(long)]
        workspace: Option<PathBuf>,
    },
    /// Trust a public pairing card after verifying it independently.
    Pair { card: PathBuf },
    /// Show this node's public pairing card as JSON.
    Card,
    /// Accept tasks from paired coordinators on this runner.
    Start,
    /// Show saved node checks, not current online status.
    Status {
        /// Check reachability now and save the result.
        #[arg(long)]
        probe: bool,
    },
    /// Check stored run evidence without running the task again.
    Reconcile { run: i64 },
    /// Request cancellation of a remote run, not confirmation it stopped.
    Cancel { run: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Role {
    Coordinator,
    Runner,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Card {
    version: u8,
    role: Role,
    id: EndpointId,
    address: SocketAddr,
    revision: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Local {
    card: Card,
    workspace: Option<PathBuf>,
}

fn directory(cfg: &RuntimeConfig) -> PathBuf {
    cfg.runtime_dir.join("nodes")
}
fn read_json<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)?;
    ensure!(bytes.len() <= 65536, "node configuration exceeds bound");
    Ok(serde_json::from_slice(&bytes)?)
}
fn local(cfg: &RuntimeConfig) -> Result<Local> {
    validate_storage(cfg)?;
    read_local(cfg)
}
fn read_local(cfg: &RuntimeConfig) -> Result<Local> {
    let value: Local = read_json(&directory(cfg).join("local.json"))?;
    ensure!(
        value.card.version == 1 && value.card.revision == REVISION,
        "node revision mismatch; explicitly re-enroll after upgrade"
    );
    Ok(value)
}
fn key(cfg: &RuntimeConfig) -> Result<SecretKey> {
    let identity = local(cfg)?.card.id;
    let bytes: [u8; 32] = fs::read(directory(cfg).join("device-key"))?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid device key"))?;
    let key = SecretKey::from(bytes);
    ensure!(key.public() == identity, "node identity mismatch");
    Ok(key)
}
pub(crate) fn require_coordinator(cfg: &RuntimeConfig) -> Result<()> {
    if directory(cfg).join("local.json").exists() {
        ensure!(
            local(cfg)?.card.role == Role::Coordinator,
            "runner identities cannot poll Telegram or run coordinator turns; use nodes start"
        );
    }
    Ok(())
}
fn create_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(path.parent().context("node parent absent")?)?;
    #[cfg(windows)]
    coconutclaw_config::windows_security::validate_private(file.path())?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(path)
        .map_err(|_| anyhow::anyhow!("node file already exists"))?;
    Ok(())
}
#[cfg(windows)]
pub(super) fn load_checked(
    overrides: &coconutclaw_config::CliOverrides,
    command: &crate::Commands,
) -> Result<RuntimeConfig> {
    use coconutclaw_config::windows_security::{present, private_directory, validate_private};
    let mut node = matches!(command, crate::Commands::Nodes(_));
    let cfg = coconutclaw_config::preview_checked_config(overrides, |data| {
        node |= present(&data.join("runtime/nodes"))?;
        if node {
            private_directory(data, true)?;
            let config = data.join("config.toml");
            if present(&config)? {
                validate_private(&config)?;
            }
        }
        Ok(())
    })?;
    if node {
        let parent = cfg
            .sqlite_db_path
            .parent()
            .context("missing SQLite parent")?;
        private_directory(parent, parent == cfg.data_dir)?;
        for dir in [&cfg.runtime_dir, &cfg.tmp_dir, &cfg.tasks_dir, &cfg.log_dir] {
            private_directory(dir, true)?;
        }
        validate_existing(&cfg)?;
        for file in [
            cfg.instance_dir.join("SOUL.md"),
            cfg.tasks_dir.join("pending.md"),
        ] {
            if present(&file)? {
                validate_private(&file)?;
            }
        }
        if present(&directory(&cfg).join("local.json"))? {
            let identity = read_local(&cfg)?;
            if matches!(command, crate::Commands::Run(_) | crate::Commands::Once(_)) {
                ensure!(
                    identity.card.role == Role::Coordinator,
                    "runner identities cannot poll Telegram or run coordinator turns; use nodes start"
                );
            }
        }
    }
    coconutclaw_config::ensure_instance_layout(&cfg).map_err(|_| anyhow::anyhow!("failed to initialize configured directories/files; check path permissions (values redacted)"))?;
    if node {
        validate_storage(&cfg)?;
    }
    Ok(cfg)
}

fn validate_storage(_cfg: &RuntimeConfig) -> Result<()> {
    #[cfg(windows)]
    {
        use coconutclaw_config::windows_security::private_directory;
        private_directory(&_cfg.data_dir, false)?;
        private_directory(
            _cfg.sqlite_db_path
                .parent()
                .context("missing SQLite parent")?,
            false,
        )?;
        private_directory(&_cfg.tmp_dir, false)?;
        validate_existing(_cfg)?;
    }
    Ok(())
}

#[cfg(windows)]
fn validate_existing(cfg: &RuntimeConfig) -> Result<()> {
    use coconutclaw_config::windows_security::{present, validate_private};
    let mut paths = vec![
        cfg.runtime_dir.clone(),
        cfg.config_file_path.clone(),
        cfg.runtime_dir.join("instance.lock"),
        directory(cfg),
        directory(cfg).join("device-key"),
        directory(cfg).join("local.json"),
        cfg.sqlite_db_path.clone(),
    ];
    for suffix in ["-wal", "-shm", "-journal", ".initialize.lock"] {
        let mut path = cfg.sqlite_db_path.as_os_str().to_os_string();
        path.push(suffix);
        paths.push(path.into());
    }
    for path in paths {
        if present(&path)? {
            validate_private(&path)?;
        }
    }
    Ok(())
}
fn initialize(store: &Store) -> Result<()> {
    store.conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS paired_nodes(id TEXT PRIMARY KEY,card TEXT NOT NULL,last_contact TEXT,last_error TEXT);
         CREATE TABLE IF NOT EXISTS node_selection(session TEXT PRIMARY KEY,node TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS node_routes(run INTEGER PRIMARY KEY,node TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS node_assignments(run INTEGER PRIMARY KEY,bundle TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS node_claims(origin TEXT PRIMARY KEY,bundle TEXT NOT NULL,start BLOB NOT NULL,result BLOB,cancelled INTEGER NOT NULL DEFAULT 0);"
    )?;
    Ok(())
}
fn paired(store: &Store, id: EndpointId) -> Result<Card> {
    let json: String = store.conn.query_row(
        "SELECT card FROM paired_nodes WHERE id=?1",
        [id.to_string()],
        |r| r.get(0),
    )?;
    let card: Card = serde_json::from_str(&json)?;
    ensure!(
        card.id == id && card.version == 1 && card.revision == REVISION,
        "paired revision/identity mismatch"
    );
    Ok(card)
}

pub(crate) fn run(cfg: &RuntimeConfig, args: &Args) -> Result<()> {
    validate_storage(cfg)?;
    let store = Store::open(cfg)?;
    initialize(&store)?;
    match &args.action {
        Command::Enroll {
            role,
            bind,
            workspace,
        } => {
            ensure!(
                REVISION.len() == 40 && REVISION.bytes().all(|b| b.is_ascii_hexdigit()),
                "build from a pinned source revision"
            );
            ensure!(
                bind.port() != 0 && !bind.ip().is_unspecified() && !bind.ip().is_multicast(),
                "provide a concrete reachable IP and fixed UDP port"
            );
            ensure!(
                (*role == Role::Runner) == workspace.is_some(),
                "only a runner requires --workspace"
            );
            let workspace = workspace.as_ref().map(fs::canonicalize).transpose()?;
            ensure!(
                workspace.as_ref().is_none_or(|p| p.is_dir()),
                "workspace is not a directory"
            );
            let dir = directory(cfg);
            fs::create_dir_all(&dir)?;
            validate_storage(cfg)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
            }
            let bytes: Vec<u8> = store
                .conn
                .query_row("SELECT randomblob(32)", [], |r| r.get(0))?;
            let secret: [u8; 32] = bytes.as_slice().try_into()?;
            let card = Card {
                version: 1,
                role: *role,
                id: SecretKey::from(secret).public(),
                address: *bind,
                revision: REVISION.into(),
            };
            create_private(&dir.join("device-key"), &bytes)?;
            create_private(
                &dir.join("local.json"),
                &serde_json::to_vec(&Local {
                    card: card.clone(),
                    workspace,
                })?,
            )?;
            validate_storage(cfg)?;
            println!("{}", serde_json::to_string(&card)?);
        }
        Command::Pair { card } => {
            let own = local(cfg)?;
            let peer: Card = read_json(card)?;
            ensure!(
                peer.version == 1
                    && peer.revision == REVISION
                    && peer.role != own.card.role
                    && peer.id != own.card.id,
                "foreign role/revision/identity"
            );
            ensure!(
                peer.address.port() != 0 && !peer.address.ip().is_unspecified(),
                "invalid peer address"
            );
            if own.card.role == Role::Runner {
                let count: i64 = store.conn.query_row(
                    "SELECT count(*) FROM paired_nodes WHERE id!=?1",
                    [peer.id.to_string()],
                    |r| r.get(0),
                )?;
                ensure!(count == 0, "runner already pinned to another coordinator");
            }
            let json = serde_json::to_string(&peer)?;
            store.conn.execute(
                "INSERT OR IGNORE INTO paired_nodes(id,card) VALUES(?1,?2)",
                params![peer.id.to_string(), json],
            )?;
            ensure!(
                serde_json::to_string(&paired(&store, peer.id)?)? == json,
                "pairing conflict; existing admission preserved"
            );
            println!("Paired {}", display_name(&store, &peer.id.to_string())?);
        }
        Command::Card => println!("{}", serde_json::to_string(&local(cfg)?.card)?),
        Command::Status { probe } => {
            if *probe {
                let _instance = cfg.acquire_instance_lock()?;
                transport::runtime()?.block_on(async {
                    let endpoint=transport::coordinator(cfg).await?;
                    for id in choices(&store)? {
                        let card=paired(&store,id.parse()?)?;
                        let available=matches!(transport::call(&endpoint,&card,Request::Ping).await,Ok(Reply::Pong));
                        store.conn.execute("UPDATE paired_nodes SET last_contact=CASE WHEN ?2 THEN ?3 ELSE last_contact END,last_error=?4 WHERE id=?1",
                            params![id,available,crate::iso_now(&cfg.timezone),if available {Some("reachable at observation")} else {Some("unavailable at observation")}])?;
                    }
                    Ok::<_,anyhow::Error>(())
                })?;
            }
            println!("{}", status(&store, None, &cfg.timezone)?);
        }
        Command::Start => {
            let own = local(cfg)?;
            ensure!(
                own.card.role == Role::Runner,
                "only a runner starts a network listener; run starts the Telegram coordinator"
            );
            transport::runtime()?.block_on(transport::serve(cfg.clone()))?;
        }
        Command::Reconcile { run } => {
            transport::runtime()?.block_on(reconcile(cfg, &store, *run))?;
            println!(
                "Stored evidence checked. No task was run again. Use nodes status to review the outcome."
            );
        }
        Command::Cancel { run } => {
            let _instance = cfg.acquire_instance_lock()?;
            store.mark_task_run_cancel_requested(*run, &crate::iso_now(&cfg.timezone))?;
            transport::runtime()?.block_on(async {
                let json: String = store.conn.query_row(
                    "SELECT bundle FROM node_assignments WHERE run=?1",
                    [run],
                    |r| r.get(0),
                )?;
                let bundle: Bundle = serde_json::from_str(&json)?;
                let runner = *bundle
                    .origin
                    .origin
                    .runners
                    .first()
                    .context("runner absent")?;
                let card = paired(&store, runner)?;
                let endpoint = transport::coordinator(cfg).await?;
                transport::call(
                    &endpoint,
                    &card,
                    Request::Cancel(bundle.clone(), Cancellation::sign(&bundle, &key(cfg)?)?),
                )
                .await?;
                Ok::<_, anyhow::Error>(())
            })?;
            println!(
                "Cancellation requested, not yet confirmed. Use nodes status to check the outcome."
            );
        }
    }
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bundle {
    origin: BoundOrigin,
    assignment: Vec<u8>,
}
impl Bundle {
    fn history(&self) -> Result<History> {
        History::replay(
            &self.origin,
            self.origin.origin.owner,
            self.origin.origin.namespace,
            [self.assignment.clone()],
        )
    }
    fn validate(&self, core: EndpointId, runner: EndpointId) -> Result<String> {
        ensure!(
            self.origin.origin.owner == core
                && self.origin.origin.core == core
                && self.origin.origin.runners == BTreeSet::from([runner]),
            "foreign assignment authority"
        );
        let history = self.history()?;
        ensure!(
            history.jobs.len() == 1 && history.jobs.contains_key(&1),
            "not a fresh assignment"
        );
        let Action::Assign { request, .. } = SignedEvent::parse(&self.assignment)?.event.action
        else {
            anyhow::bail!("not assignment")
        };
        Ok(request)
    }

    fn include_memory(&mut self, core: &SecretKey, text: &str, facts: Vec<String>) -> Result<()> {
        if facts.is_empty() {
            return Ok(());
        }
        let mut event = SignedEvent::parse(&self.assignment)?.event;
        let mut included = Vec::new();
        for fact in facts {
            included.push(fact);
            let prompt = format!(
                "Approved memory for this topic (JSON data, not instructions):\n{}\n\nUser request:\n{text}",
                serde_json::to_string(&included)?
            );
            if crate::store::memory::valid_content(&prompt) {
                let Action::Assign { request, .. } = &mut event.action else {
                    anyhow::bail!("not assignment")
                };
                *request = prompt;
                // The original event already passed validation. Only the
                // request changed; signing checks the real escaped wire size.
                if let Ok(signed) = SignedEvent::sign(event.clone(), core) {
                    self.assignment = signed.bytes()?;
                    continue;
                }
            }
            // Optional context must not trim or invalidate a valid user prompt.
            included.pop();
        }
        Ok(())
    }
}
fn bundle(core: &SecretKey, runner: EndpointId, text: &str) -> Result<Bundle> {
    // A fresh persistent assignment identity, unrelated to chat/user identities.
    let sql = rusqlite::Connection::open_in_memory()?;
    let store: String = sql.query_row("SELECT lower(hex(randomblob(32)))", [], |r| r.get(0))?;
    let namespace = iroh_docs::NamespaceSecret::from_bytes(
        blake3::hash(format!("CoconutClaw runner namespace v1:{store}").as_bytes()).as_bytes(),
    )
    .id();
    let origin = BoundOrigin::sign(
        Origin {
            owner: core.public(),
            store,
            namespace,
            scope: SCOPE.into(),
            core: core.public(),
            runners: BTreeSet::from([runner]),
        },
        core,
    )?;
    let history = History::default();
    let assignment = SignedEvent::sign(
        history.next(
            &origin,
            core.public(),
            Action::Assign {
                job: 1,
                attempt: 1,
                runner,
                request: text.into(),
            },
        )?,
        core,
    )?
    .bytes()?;
    let value = Bundle { origin, assignment };
    value.validate(core.public(), runner)?;
    Ok(value)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancellation {
    origin: String,
    assignment: String,
    signature: Signature,
}
impl Cancellation {
    fn preimage(origin: &str, assignment: &str) -> Vec<u8> {
        format!("CoconutClaw runner cancel v1\0{origin}\0{assignment}").into_bytes()
    }
    fn sign(bundle: &Bundle, key: &SecretKey) -> Result<Self> {
        let origin = bundle.origin.hash()?;
        let assignment = blake3::hash(&bundle.assignment).to_string();
        Ok(Self {
            signature: key.sign(&Self::preimage(&origin, &assignment)),
            origin,
            assignment,
        })
    }
    fn verify(&self, bundle: &Bundle) -> Result<()> {
        ensure!(
            self.origin == bundle.origin.hash()?
                && self.assignment == blake3::hash(&bundle.assignment).to_string(),
            "foreign cancellation"
        );
        bundle.origin.origin.core.verify(
            &Self::preimage(&self.origin, &self.assignment),
            &self.signature,
        )?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Request {
    Begin(Bundle),
    Status(Bundle),
    Cancel(Bundle, Cancellation),
    Ping,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Reply {
    Evidence {
        start: Vec<u8>,
        result: Option<Vec<u8>>,
    },
    Pong,
    Refused,
}

struct Runner {
    cfg: RuntimeConfig,
    key: SecretKey,
    store: Store,
    active: HashMap<String, Arc<AtomicBool>>,
}
impl Runner {
    fn open(cfg: RuntimeConfig) -> Result<Self> {
        validate_storage(&cfg)?;
        let store = Store::open(&cfg)?;
        initialize(&store)?;
        let mut runner = Self {
            key: key(&cfg)?,
            cfg,
            store,
            active: HashMap::new(),
        };
        // A durable claim without this process's dispatch capability is Unknown.
        let rows: Vec<String> = runner
            .store
            .conn
            .prepare("SELECT bundle FROM node_claims WHERE result IS NULL")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for row in rows {
            let b: Bundle = serde_json::from_str(&row)?;
            runner.finish(
                &b,
                Outcome::Unknown,
                "runner restarted; outcome unknown; no automatic retry",
            )?;
        }
        Ok(runner)
    }
    fn evidence(&self, bundle: &Bundle) -> Result<Reply> {
        let (saved, start, result): (String, Vec<u8>, Option<Vec<u8>>) =
            self.store.conn.query_row(
                "SELECT bundle,start,result FROM node_claims WHERE origin=?1",
                [bundle.origin.hash()?],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
        ensure!(
            saved == serde_json::to_string(bundle)?,
            "assignment conflict"
        );
        Ok(Reply::Evidence { start, result })
    }
    fn admit(&mut self, peer: EndpointId, bundle: &Bundle) -> Result<Option<Arc<AtomicBool>>> {
        let card = paired(&self.store, peer)?;
        ensure!(card.role == Role::Coordinator, "not coordinator");
        bundle.validate(peer, self.key.public())?;
        let exists: bool = self.store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM node_claims WHERE origin=?1)",
            [bundle.origin.hash()?],
            |r| r.get(0),
        )?;
        if exists {
            self.evidence(bundle)?;
            return Ok(None);
        }
        ensure!(
            self.active.is_empty(),
            "runner busy; assignment not admitted"
        );
        let history = bundle.history()?;
        let start = SignedEvent::sign(
            history.next(
                &bundle.origin,
                self.key.public(),
                Action::Start { assignment: 1 },
            )?,
            &self.key,
        )?
        .bytes()?;
        self.store.conn.execute(
            "INSERT INTO node_claims(origin,bundle,start) VALUES(?1,?2,?3)",
            params![bundle.origin.hash()?, serde_json::to_string(bundle)?, start],
        )?;
        let flag = Arc::new(AtomicBool::new(false));
        self.active.insert(bundle.origin.hash()?, flag.clone());
        Ok(Some(flag))
    }
    fn finish(&mut self, bundle: &Bundle, mut outcome: Outcome, output: &str) -> Result<()> {
        let Reply::Evidence { start, result } = self.evidence(bundle)? else {
            unreachable!()
        };
        if result.is_some() {
            return Ok(());
        }
        let cancelled: bool = self.store.conn.query_row(
            "SELECT cancelled FROM node_claims WHERE origin=?1",
            [bundle.origin.hash()?],
            |r| r.get(0),
        )?;
        if cancelled && outcome != Outcome::Cancelled {
            outcome = Outcome::Unknown;
        }
        let history = History::replay(
            &bundle.origin,
            bundle.origin.origin.owner,
            bundle.origin.origin.namespace,
            [bundle.assignment.clone(), start],
        )?;
        let action = Action::Result {
            assignment: 1,
            outcome,
            output: output.into(),
        };
        let signed = SignedEvent::sign(
            history.next(&bundle.origin, self.key.public(), action)?,
            &self.key,
        )?
        .bytes()?;
        // Validate bounds and transitions before committing; oversized output is
        // Unknown, never silently truncated or retried.
        History::replay(
            &bundle.origin,
            bundle.origin.origin.owner,
            bundle.origin.origin.namespace,
            history.export().into_iter().chain([signed.clone()]),
        )?;
        self.store.conn.execute(
            "UPDATE node_claims SET result=?2 WHERE origin=?1 AND result IS NULL",
            params![bundle.origin.hash()?, signed],
        )?;
        self.active.remove(&bundle.origin.hash()?);
        Ok(())
    }
    fn cancel(&mut self, bundle: &Bundle, command: &Cancellation) -> Result<()> {
        command.verify(bundle)?;
        self.evidence(bundle)?;
        self.store.conn.execute(
            "UPDATE node_claims SET cancelled=1 WHERE origin=?1",
            [bundle.origin.hash()?],
        )?;
        if let Some(flag) = self.active.get(&bundle.origin.hash()?) {
            flag.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

pub(crate) fn freeze_route(store: &Store, run: i64, session: &str) -> Result<()> {
    initialize(store)?;
    store.conn.execute("INSERT OR IGNORE INTO node_routes(run,node) SELECT ?1,node FROM node_selection WHERE session=?2",params![run,session])?;
    Ok(())
}
pub(crate) fn selection(store: &Store, session: &str, id: Option<&str>) -> Result<()> {
    initialize(store)?;
    if let Some(id) = id {
        paired(store, id.parse()?)?;
        store.conn.execute("INSERT INTO node_selection VALUES(?1,?2) ON CONFLICT(session) DO UPDATE SET node=excluded.node",params![session,id])?;
    } else {
        store
            .conn
            .execute("DELETE FROM node_selection WHERE session=?1", [session])?;
    }
    Ok(())
}
pub(crate) fn choices(store: &Store) -> Result<Vec<String>> {
    initialize(store)?;
    Ok(store
        .conn
        .prepare("SELECT id FROM paired_nodes ORDER BY id LIMIT 32")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}
/// Cards currently contain a verified role, not a hostname or friendly name.
/// Never infer host identity from an IP address or the coordinator's machine.
pub(crate) fn display_name(store: &Store, id: &str) -> Result<String> {
    initialize(store)?;
    let card = id.parse().ok().and_then(|id| paired(store, id).ok());
    let role = match card.map(|card| card.role) {
        Some(Role::Runner) => "Runner",
        Some(Role::Coordinator) => "Coordinator",
        None => "Node",
    };
    let suffix = id.chars().rev().take(8).collect::<String>();
    Ok(format!(
        "{role} · {}",
        suffix.chars().rev().collect::<String>()
    ))
}

pub(crate) fn status(store: &Store, session: Option<&str>, timezone: &str) -> Result<String> {
    initialize(store)?;
    let mut lines = vec!["Nodes".into()];
    let rows: Vec<(String, Option<String>, Option<String>)> = store
        .conn
        .prepare("SELECT id,last_contact,last_error FROM paired_nodes ORDER BY id LIMIT 32")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if rows.is_empty() {
        lines.push("No remote nodes paired.".into());
    }
    for (id, contact, error) in rows {
        let check = match error.as_deref() {
            Some("reachable at observation") => "Reachable when last checked.",
            Some("unavailable at observation") => {
                "Not reachable at the last check (check time not recorded)."
            }
            Some(_) => {
                "Last exchange did not confirm an outcome. Inspect the run before trying again."
            }
            None if contact.is_some() => "Reached during the last recorded exchange.",
            None => "Not checked yet.",
        };
        lines.push(format!(
            "{}\n{check}\nLast reached: {}",
            display_name(store, &id)?,
            crate::wording::time(contact.as_deref(), timezone)
        ));
    }
    if let Some(session) = session {
        let selected: Option<String> = store
            .conn
            .query_row(
                "SELECT node FROM node_selection WHERE session=?1",
                [session],
                |r| r.get(0),
            )
            .optional()?;
        lines.push(format!(
            "New text tasks go to: {}",
            selected
                .as_deref()
                .map(|id| display_name(store, id))
                .transpose()?
                .unwrap_or_else(|| "this computer (local)".into())
        ));
        let assignments:Vec<(i64,String,String)>=store.conn.prepare("SELECT n.run,n.node,CASE WHEN instr(COALESCE(t.error_summary,''),'outcome unknown')>0 THEN 'unknown' ELSE t.status END FROM node_routes n JOIN task_runs t ON t.id=n.run WHERE t.session_id=?1 ORDER BY n.run DESC LIMIT 8")?
            .query_map([session],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?.collect::<rusqlite::Result<_>>()?;
        for (id, node, state) in assignments {
            lines.push(format!(
                "Run #{id}: {} / {}",
                display_name(store, &node)?,
                crate::wording::run_state(&state)
            ));
        }
        lines.push("Existing tasks keep their assigned node. Remote nodes accept text tasks only; if unreachable, tasks do not switch to this computer.".into());
    }
    lines.push("Saved checks, not live availability. For a new check, run coconutclaw nodes status --probe locally.".into());
    Ok(lines.join("\n\n"))
}

pub(crate) fn execute(
    cfg: &RuntimeConfig,
    store: &Store,
    run: i64,
    request: &crate::scheduler::TaskRequest,
    cancel: Arc<AtomicBool>,
) -> Result<bool> {
    initialize(store)?;
    let node: Option<String> = store
        .conn
        .query_row("SELECT node FROM node_routes WHERE run=?1", [run], |r| {
            r.get(0)
        })
        .optional()?;
    let Some(node) = node else { return Ok(false) };
    ensure!(
        request.media.is_none()
            && request.input.attachment_path.is_none()
            && request.input.attachment_type.is_none()
            && request.input.supplemental_context.is_none()
            && request.input.input_type == crate::InputType::Text
            && request.quoted.reply_text.is_none(),
        "remote text route does not accept attachments or quoted/private context"
    );
    let own = local(cfg)?;
    ensure!(own.card.role == Role::Coordinator, "not coordinator");
    let card = paired(store, node.parse()?)?;
    let core = key(cfg)?;
    let mut bundle = bundle(&core, card.id, &request.input.user_text)?;
    {
        // Order memory selection and the frozen assignment with respect to
        // correction/undo. Never hold the write transaction during transport.
        let tx = rusqlite::Transaction::new_unchecked(
            &store.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        let source = store
            .get_task_run(run)?
            .context("remote source run absent")?;
        ensure!(
            source.session_id == request.session.id()
                && source.prompt == request.input.user_text
                && source.channel == request.source.channel_name()
                && source.source_user_id == request.source_user_id
                && source.update_id == request.update_id,
            "remote source request changed"
        );
        if source.channel == "telegram"
            && !request.delegated
            && request.session.thread_id.is_some()
            && request.delivery.telegram_route().as_deref() == Some(source.session_id.as_str())
        {
            bundle.include_memory(
                &core,
                &source.prompt,
                store.remote_memory(&source.session_id, &source.prompt)?,
            )?;
        }
        store.conn.execute(
            "INSERT INTO node_assignments VALUES(?1,?2)",
            params![run, serde_json::to_string(&bundle)?],
        )?;
        store.reserve_runtime_run(run, &bundle.origin.hash()?, &bundle.assignment)?;
        tx.commit()?;
    }
    let result = transport::runtime()?.block_on(async {
        let endpoint = transport::coordinator(cfg).await?;
        async {
            transport::call(&endpoint, &card, Request::Begin(bundle.clone())).await?;
            loop {
                if cancel.load(Ordering::SeqCst)
                    || store
                        .get_task_run(run)?
                        .is_some_and(|r| r.cancel_requested_at.is_some())
                {
                    transport::call(
                        &endpoint,
                        &card,
                        Request::Cancel(bundle.clone(), Cancellation::sign(&bundle, &key(cfg)?)?),
                    )
                    .await?;
                }
                let reply =
                    transport::call(&endpoint, &card, Request::Status(bundle.clone())).await?;
                store.conn.execute(
                    "UPDATE paired_nodes SET last_contact=?2,last_error=NULL WHERE id=?1",
                    params![node, crate::iso_now(&cfg.timezone)],
                )?;
                if accept(store, run, &bundle, reply, &cfg.timezone)? {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await
    });
    store.conn.execute("UPDATE paired_nodes SET last_contact=CASE WHEN ?2 THEN ?3 ELSE last_contact END,last_error=?4 WHERE id=?1",
        params![node,result.is_ok(),crate::iso_now(&cfg.timezone),if result.is_err(){Some("unavailable; outcome unknown; use nodes reconcile")}else{None}])?;
    if result.is_err() {
        let tx = store.conn.unchecked_transaction()?;
        store.finish_task_run(
            run,
            TaskRunStatus::Failed,
            &crate::iso_now(&cfg.timezone),
            Some("remote outcome unknown; no automatic retry"),
            None,
        )?;
        store.conn.execute("UPDATE task_payloads SET output='Remote outcome unconfirmed (Unknown). The exchange with the assigned node was not confirmed. Open Tasks and Nodes, then check stored evidence locally with coconutclaw nodes reconcile RUN_ID before repeating the work. Nothing was retried or moved to this computer.' WHERE task_run_id=?1 AND output IS NULL",[run])?;
        tx.commit()?;
    }
    // Once reserved, failure is durable ambiguity, not the local provider-error
    // path (which would create a competing result inbox and delivery).
    Ok(true)
}
pub(crate) fn recover(store: &Store, now: &str) -> Result<()> {
    initialize(store)?;
    store.conn.execute(
        "UPDATE task_runs SET status='failed',finished_at=coalesce(finished_at,?1),
         error_summary='coordinator restarted; remote outcome unknown; no automatic retry'
         WHERE status IN ('waiting_remote','running','cancel_requested')
         AND id IN (SELECT run FROM node_assignments)
         AND NOT EXISTS(SELECT 1 FROM runtime_remote_results c WHERE c.task_run_id=task_runs.id AND c.state='pending')",[now])?;
    Ok(())
}
fn accept(store: &Store, run: i64, bundle: &Bundle, reply: Reply, timezone: &str) -> Result<bool> {
    let Reply::Evidence { start, result } = reply else {
        anyhow::bail!("runner refused evidence")
    };
    let Some(result) = result else {
        return Ok(false);
    };
    let history = History::replay(
        &bundle.origin,
        bundle.origin.origin.owner,
        bundle.origin.origin.namespace,
        [bundle.assignment.clone(), start.clone(), result.clone()],
    )?;
    let (outcome, output) = history
        .jobs
        .get(&1)
        .and_then(|j| j.result.as_ref())
        .context("result absent")?;
    let status = match outcome {
        Outcome::Completed => TaskRunStatus::Completed,
        Outcome::Cancelled => TaskRunStatus::Cancelled,
        _ => TaskRunStatus::Failed,
    };
    let tx = store.conn.unchecked_transaction()?;
    store.custody_runtime_result(
        run,
        &start,
        &result,
        (status, *outcome == Outcome::Unknown),
        output,
        &crate::iso_now(timezone),
    )?;
    tx.commit()?;
    store.apply_runtime_results(&crate::iso_now(timezone))?;
    Ok(true)
}
async fn reconcile(cfg: &RuntimeConfig, store: &Store, run: i64) -> Result<()> {
    let json: String = store.conn.query_row(
        "SELECT bundle FROM node_assignments WHERE run=?1",
        [run],
        |r| r.get(0),
    )?;
    let bundle: Bundle = serde_json::from_str(&json)?;
    let runner = *bundle
        .origin
        .origin
        .runners
        .first()
        .context("runner absent")?;
    let card = paired(store, runner)?;
    let _instance = cfg.acquire_instance_lock()?;
    let endpoint = transport::coordinator(cfg).await?;
    let result = async {
        let reply = transport::call(&endpoint, &card, Request::Status(bundle.clone())).await?;
        ensure!(
            accept(store, run, &bundle, reply, &cfg.timezone)?,
            "runner still active; no execution requested"
        );
        Ok(())
    }
    .await;
    endpoint.close().await;
    result
}
