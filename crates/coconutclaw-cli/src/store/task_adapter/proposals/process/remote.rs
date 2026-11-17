//! Concrete, dev-only remote route for the existing process commands.
//! Record bytes still move through ScopedPeer, not this control connection.
use super::*;
use coconutclaw_sync::synthetic::membership::{AcceptanceNetwork, Member, Policy, SignedPolicy};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection as IrohConnection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_docs::{AuthorId, Capability, NamespaceSecret};
use std::collections::{BTreeMap, BTreeSet};

const ALPN: &[u8] = b"/coconutclaw/disposable-runner/1";
const ROLE_MARKER: &str = "CoconutClaw disposable multihost acceptance v1\n";
const ENTRY: &str = "store::task_adapter::proposals::process::remote::multihost_role_entry";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Locator {
    pub addr: EndpointAddr,
    pub author: AuthorId,
    pub record: Record,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Ready {
    pub version: u8,
    pub run_id: String,
    pub role: String,
    pub hostname: String,
    pub machine_hash: String,
    pub os: String,
    pub arch: String,
    pub pid: u32,
    pub real_provider: bool,
    #[serde(default)]
    pub controlled_fixture: bool,
    pub owner: EndpointId,
    pub control: EndpointAddr,
    pub boundary: String,
    pub data: EndpointAddr,
    pub author: AuthorId,
    pub mirror: Option<(EndpointAddr, AuthorId)>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleConfig {
    root: PathBuf,
    role: String,
    run_id: String,
    network: AcceptanceNetwork,
    owner: Option<EndpointId>,
    core: Option<EndpointId>,
    provider: Option<provider::host::Config>,
    ready_socket: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Request {
    Initialize(Box<Bootstrap>),
    Process(Command),
    Import(Locator),
    Publish(Vec<u8>),
    Wait(u64),
    TransportProbe(Option<Locator>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    origin: BoundOrigin,
    policy: SignedPolicy,
    core: Ready,
    input: provider::host::Input,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    proposal: Locator,
    artifact: Option<Locator>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    version: u8,
    boundary: String,
    origin: String,
    request: Request,
}

#[derive(Serialize, Deserialize)]
enum Response {
    Process(Reply),
    Published(Box<Publication>),
    TransportProbe(Locator),
    Error(String),
}

pub(in crate::store::task_adapter) struct Client {
    endpoint: Endpoint,
    ready: Ready,
    origin: BoundOrigin,
    slots: Arc<Semaphore>,
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    let body = serde_json::to_vec(v)?;
    ensure!(
        !body.is_empty() && body.len() <= MAX_FRAME,
        "remote frame exceeds limit"
    );
    let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
    bytes.extend(body);
    Ok(bytes)
}

fn decode<T: for<'a> Deserialize<'a> + Serialize>(bytes: &[u8]) -> Result<T> {
    ensure!(
        bytes.len() > 4 && bytes.len() <= MAX_FRAME + 4,
        "invalid remote frame"
    );
    let size = u32::from_be_bytes(bytes[..4].try_into()?) as usize;
    ensure!(size == bytes.len() - 4, "remote frame length mismatch");
    let value = serde_json::from_slice(&bytes[4..])?;
    ensure!(encode(&value)? == bytes, "noncanonical remote frame");
    Ok(value)
}

async fn endpoint(network: &AcceptanceNetwork, key: SecretKey) -> Result<Endpoint> {
    Ok(Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .clear_ip_transports()
        .bind_addr(network.bind)?
        .clear_address_lookup()
        .relay_mode(
            network
                .relay
                .clone()
                .map(|u| iroh::RelayMode::Custom(iroh::RelayMap::from_iter([u])))
                .unwrap_or(iroh::RelayMode::Disabled),
        )
        .bind()
        .await?)
}

fn dial(addr: &EndpointAddr, network: &AcceptanceNetwork) -> EndpointAddr {
    // A declared relay is used explicitly, never public/default discovery.
    // Keep only that route, not unrelated advertised host interface addresses.
    if let Some(url) = &network.relay {
        EndpointAddr::new(addr.id).with_relay_url(url.clone())
    } else {
        addr.clone()
    }
}

impl Client {
    async fn probe_transport(&self, publisher: &mut ScopedPeer) -> Result<()> {
        let Response::TransportProbe(locator) = self.request(Request::TransportProbe(None)).await?
        else {
            anyhow::bail!("unexpected transport probe response")
        };
        ensure!(
            locator.addr.id == self.data_id()
                && locator.record.text == "acceptance-transport-runner",
            "wrong probe data endpoint"
        );
        // Prime the actual scoped-data UDP path from the gateway, not the Mac.
        // The observed LAN admits gateway-initiated UDP; control uses another endpoint.
        publisher
            .receive(
                self.ready.data.clone(),
                locator.author,
                &locator.record.key(),
            )
            .await?;
        ensure!(
            publisher.received_artifact(&locator.record)? == self.run_id().as_bytes(),
            "wrong transport probe bytes"
        );
        let record = publisher
            .publish("acceptance-transport-core", self.run_id().as_bytes())
            .await?;
        let reply = self
            .request(Request::TransportProbe(Some(Locator {
                addr: publisher.addr(),
                author: publisher.author().await?,
                record,
            })))
            .await?;
        ensure!(
            matches!(reply, Response::Process(Reply::Unit)),
            "wrong reverse probe response"
        );
        Ok(())
    }

    pub(in crate::store::task_adapter) fn data_id(&self) -> EndpointId {
        self.ready.data.id
    }
    pub(in crate::store::task_adapter) fn run_id(&self) -> &str {
        &self.ready.run_id
    }
    pub(in crate::store::task_adapter) fn hostname(&self) -> &str {
        &self.ready.hostname
    }
    pub(in crate::store::task_adapter) fn controlled_fixture(&self) -> bool {
        self.ready.controlled_fixture
    }
    async fn request(&self, request: Request) -> Result<Response> {
        let _slot = self.slots.acquire().await?;
        let conn = self
            .endpoint
            .connect(self.ready.control.clone(), ALPN)
            .await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&encode(&Message {
            version: 1,
            boundary: self.ready.boundary.clone(),
            origin: self.origin.hash()?,
            request,
        })?)
        .await?;
        send.finish()?;
        let response = decode(&recv.read_to_end(MAX_FRAME + 4).await?)?;
        conn.close(0u32.into(), b"one bounded task control");
        match response {
            Response::Error(error) => anyhow::bail!("remote runner refused: {error}"),
            response => Ok(response),
        }
    }

    pub(in crate::store::task_adapter::proposals) async fn call(
        &self,
        command: Command,
    ) -> Result<Reply> {
        match self.request(Request::Process(command)).await? {
            Response::Process(Reply::Error(error)) => anyhow::bail!("{error}"),
            Response::Process(reply) => Ok(reply),
            _ => anyhow::bail!("unexpected remote process response"),
        }
    }

    pub(in crate::store::task_adapter) async fn wait(&self, assignment: u64) -> Result<()> {
        match self.request(Request::Wait(assignment)).await? {
            Response::Process(Reply::Unit) => Ok(()),
            _ => anyhow::bail!("unexpected provider notification"),
        }
    }

    async fn initialize(
        &self,
        policy: SignedPolicy,
        core: Ready,
        input: provider::host::Input,
    ) -> Result<()> {
        match self
            .request(Request::Initialize(Box::new(Bootstrap {
                origin: self.origin.clone(),
                policy,
                core,
                input,
            })))
            .await?
        {
            Response::Process(Reply::Unit) => Ok(()),
            _ => anyhow::bail!("unexpected remote initialization"),
        }
    }

    pub(in crate::store::task_adapter) async fn import(
        &self,
        publisher: &mut ScopedPeer,
        locator: Locator,
    ) -> Result<()> {
        // A long native tool may outlive the gateway-initiated UDP path. Open
        // that path immediately before asking the runner to fetch a new record.
        // This precedes the sole import attempt; it is not a retry or lease renewal.
        self.probe_transport(publisher)
            .await
            .context("pre-import scoped data roundtrip")?;
        match self.request(Request::Import(locator)).await? {
            Response::Process(Reply::Unit) => Ok(()),
            _ => anyhow::bail!("unexpected remote receipt"),
        }
    }

    pub(in crate::store::task_adapter) async fn publish(
        &self,
        bytes: Vec<u8>,
    ) -> Result<(Locator, Option<Locator>)> {
        match self.request(Request::Publish(bytes)).await? {
            Response::Published(publication) => Ok((publication.proposal, publication.artifact)),
            _ => anyhow::bail!("unexpected remote publication"),
        }
    }
}

impl RunnerClient {
    pub(in crate::store::task_adapter) fn remote(&self) -> Option<&Arc<Client>> {
        match &self.route {
            Route::Remote(c) => Some(c),
            _ => None,
        }
    }
}

struct RemoteState {
    config: RoleConfig,
    ready: Ready,
    key: SecretKey,
    peer: Option<ScopedPeer>,
    state: Option<State>,
    core_data: Option<EndpointId>,
    notices: BTreeMap<u64, provider::Notice>,
}

impl RemoteState {
    async fn handle(&mut self, request: Request) -> Result<Response> {
        if let Request::Initialize(bootstrap) = request {
            let Bootstrap {
                origin,
                policy,
                core,
                input,
            } = *bootstrap;
            ensure!(
                origin.origin.owner == self.ready.owner
                    && Some(origin.origin.core) == self.config.core
                    && origin.origin.runners == BTreeSet::from([self.key.public()])
                    && origin.origin.namespace == namespace(&self.config).id()
                    && core.control.id == origin.origin.core
                    && core.owner == self.ready.owner
                    && core.run_id == self.ready.run_id
                    && core.role == "coordinator"
                    && input.nonce == self.ready.run_id
                    && input.a == 17
                    && input.b == 25,
                "remote bootstrap binding mismatch"
            );
            origin.verify(self.ready.owner, namespace(&self.config).id())?;
            if let Some(state) = &self.state {
                ensure!(
                    state.runner.origin.hash()? == origin.hash()?
                        && self.core_data == Some(core.data.id)
                        && std::fs::read(self.config.root.join("fixture-input.json"))?
                            == serde_json::to_vec(&input)?,
                    "reconnect bootstrap changed original binding"
                );
                self.peer
                    .as_mut()
                    .context("runner peer absent")?
                    .install_policy(&policy)?;
                return Ok(Response::Process(Reply::Unit));
            }
            self.peer
                .as_mut()
                .context("runner peer absent")?
                .install_policy(&policy)?;
            self.peer
                .as_mut()
                .context("runner peer absent")?
                .domain_history(&origin)?;
            let session = SessionKey::local(&format!("multihost:{}", self.config.run_id));
            let state = State::open(&self.config.root, origin, self.key.clone(), session.id())?;
            if let Some(cfg) = &self.config.provider {
                provider::host::validate(&self.config.root)?;
                ensure!(
                    cfg.executable.is_absolute(),
                    "native executable must be explicit"
                );
            }
            self.core_data = Some(core.data.id);
            self.state = Some(state);
            std::fs::write(
                self.config.root.join("fixture-input.json"),
                serde_json::to_vec(&input)?,
            )?;
            return Ok(Response::Process(Reply::Unit));
        }
        let state = self
            .state
            .as_mut()
            .context("remote process not initialized")?;
        // Every operation is fenced by the current durable scope/lease, except
        // Stop, which must remain available to clean up expired work.
        if !matches!(&request, Request::Process(Command::Stop)) {
            self.peer
                .as_mut()
                .context("runner peer absent")?
                .domain_history(&state.runner.origin)?;
        }
        match request {
            Request::TransportProbe(source) => {
                let peer = self.peer.as_mut().context("runner peer absent")?;
                if let Some(locator) = source {
                    ensure!(
                        Some(locator.addr.id) == self.core_data
                            && locator.record.text == "acceptance-transport-core"
                            && locator.author.to_string() == locator.record.origin,
                        "wrong reverse probe source"
                    );
                    peer.receive(
                        dial(&locator.addr, &self.config.network),
                        locator.author,
                        &locator.record.key(),
                    )
                    .await
                    .context("reverse scoped transport probe")?;
                    ensure!(
                        peer.received_artifact(&locator.record)? == self.ready.run_id.as_bytes(),
                        "wrong reverse probe bytes"
                    );
                    return Ok(Response::Process(Reply::Unit));
                }
                let record = peer
                    .publish("acceptance-transport-runner", self.ready.run_id.as_bytes())
                    .await?;
                Ok(Response::TransportProbe(Locator {
                    addr: peer.addr(),
                    author: peer.author().await?,
                    record,
                }))
            }
            Request::Import(locator) => {
                ensure!(
                    Some(locator.addr.id) == self.core_data
                        && locator.author.to_string() == locator.record.origin,
                    "record not from the pinned source publisher"
                );
                let peer = self.peer.as_mut().context("runner peer absent")?;
                if locator
                    .record
                    .text
                    .starts_with(crate::store::task_adapter::execution::CANCEL)
                {
                    peer.receive(
                        dial(&locator.addr, &self.config.network),
                        locator.author,
                        &locator.record.key(),
                    )
                    .await
                    .context("signed cancellation scoped fetch")?;
                    let history = peer.domain_history(&state.runner.origin)?;
                    ensure!(
                        crate::store::task_adapter::execution::verify_cancellation(
                            &state.runner.origin,
                            &history,
                            &locator.record.text,
                            1
                        )?,
                        "wrong cancellation assignment"
                    );
                } else {
                    peer.receive_domain(
                        &state.runner.origin,
                        dial(&locator.addr, &self.config.network),
                        locator.author,
                        &locator.record.key(),
                    )
                    .await?;
                }
                ensure!(
                    peer.received()?.contains(&locator.record),
                    "remote record receipt differs"
                );
                Ok(Response::Process(Reply::Unit))
            }
            Request::Process(command) => {
                if let Command::Cancel { assignment } = &command {
                    let peer = self.peer.as_mut().context("runner peer absent")?;
                    let history = peer.domain_history(&state.runner.origin)?;
                    let mut verified = false;
                    for record in peer.received()? {
                        if record
                            .text
                            .starts_with(crate::store::task_adapter::execution::CANCEL)
                        {
                            verified |= crate::store::task_adapter::execution::verify_cancellation(
                                &state.runner.origin,
                                &history,
                                &record.text,
                                *assignment,
                            )?;
                        }
                    }
                    ensure!(verified, "signed cancellation custody absent");
                }
                if let Command::Cancel { assignment } = &command
                    && *assignment == 1
                    && self
                        .config
                        .provider
                        .as_ref()
                        .is_some_and(|p| p.controlled_fixture)
                {
                    let workspace = self.config.root.join("workspace/1");
                    ensure!(workspace.is_dir(), "controlled workspace absent");
                    std::fs::write(workspace.join("fixture-cancel.json"), b"{}")?;
                }
                if let Command::Sign { history, .. } = &command {
                    ensure!(
                        *history
                            == self
                                .peer
                                .as_mut()
                                .context("runner peer absent")?
                                .domain_history(&state.runner.origin)?
                                .export(),
                        "signing challenge differs from actual scoped receipts"
                    );
                }
                if let Command::Begin { assignment, mode } = &command {
                    ensure!(
                        *assignment == 1
                            && *mode
                                == if self.config.provider.is_some() {
                                    Mode::HostCodex
                                } else {
                                    Mode::Cooperative
                                },
                        "only the explicitly provisioned single fixture is admitted"
                    );
                    let claimed: bool = state.store.conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM process_claim WHERE assignment=?1)",
                        [assignment],
                        |r| r.get(0),
                    )?;
                    ensure!(!claimed, "invocation already claimed; never retry");
                    if *mode == Mode::HostCodex {
                        let workspace = self.config.root.join(format!("workspace/{assignment}"));
                        std::fs::create_dir_all(workspace.parent().context("workspace parent")?)?;
                        std::fs::create_dir(&workspace)?;
                        std::fs::copy(
                            self.config.root.join("fixture-input.json"),
                            workspace.join("fixture-input.json"),
                        )?;
                        self.notices.insert(
                            *assignment,
                            provider::Notice::bind(&self.config.root, *assignment, true)?,
                        );
                    }
                }
                ensure!(
                    !matches!(command, Command::Arm { .. } | Command::Init { .. }),
                    "remote route has no crash injection or reinitialization command"
                );
                let stopping = matches!(command, Command::Stop);
                let reply = state.handle(command)?;
                if stopping {
                    state.providers.clear();
                }
                Ok(Response::Process(reply))
            }
            Request::Publish(bytes) => {
                let peer = self.peer.as_mut().context("runner peer absent")?;
                let history = peer.domain_history(&state.runner.origin)?;
                let signed = SignedProposal::verify(&bytes, &state.runner.origin, &history)?;
                ensure!(
                    state.runner.stored_proposal(signed.assignment)? == Some(bytes.clone()),
                    "proposal is not the runner's durable observation"
                );
                let record = peer.publish(PROPOSAL_RECORD, &bytes).await?;
                let proposal = Locator {
                    addr: peer.addr(),
                    author: peer.author().await?,
                    record,
                };
                let artifact = if self.config.provider.is_some()
                    && signed.outcome == Outcome::Completed
                {
                    let manifest: provider::host::Manifest = serde_json::from_str(&signed.output)?;
                    let path = self.config.root.join(format!(
                        "workspace/{}/fixture-output.json",
                        signed.assignment
                    ));
                    let stat = std::fs::symlink_metadata(&path)?;
                    ensure!(
                        stat.file_type().is_file() && stat.len() <= 4096,
                        "invalid artifact file"
                    );
                    let bytes = std::fs::read(path)?;
                    ensure!(
                        manifest.hash == blake3::hash(&bytes).to_string()
                            && manifest.size == bytes.len(),
                        "artifact changed after signed observation"
                    );
                    let record = peer.publish(provider::host::ARTIFACT, &bytes).await?;
                    Some(Locator {
                        addr: peer.addr(),
                        author: peer.author().await?,
                        record,
                    })
                } else {
                    None
                };
                Ok(Response::Published(Box::new(Publication {
                    proposal,
                    artifact,
                })))
            }
            Request::Wait(_) => anyhow::bail!("wait must not hold the control mutex"),
            Request::Initialize(_) => unreachable!(),
        }
    }
}

struct RpcCall {
    peer: EndpointId,
    message: Message,
    reply: oneshot::Sender<Response>,
    delivered: oneshot::Receiver<()>,
}

#[derive(Clone, Debug)]
struct Handler {
    sender: mpsc::Sender<RpcCall>,
    slots: Arc<Semaphore>,
    core: EndpointId,
}

impl Handler {
    async fn serve(&self, conn: &IrohConnection) -> Result<()> {
        let _slot = self.slots.acquire().await?;
        let (mut send, mut recv) = conn.accept_bi().await?;
        let message: Message = decode(&recv.read_to_end(MAX_FRAME + 4).await?)?;
        let (reply, receive) = oneshot::channel();
        let (delivered, delivery) = oneshot::channel();
        self.sender
            .send(RpcCall {
                peer: conn.remote_id(),
                message,
                reply,
                delivered: delivery,
            })
            .await
            .map_err(|_| anyhow::anyhow!("remote owner stopped"))?;
        let response = receive.await?;
        send.write_all(&encode(&response)?).await?;
        send.finish()?;
        let _ = send.stopped().await;
        let _ = delivered.send(());
        Ok(())
    }
}

impl RemoteState {
    async fn dispatch(&mut self, call: RpcCall) -> Result<bool> {
        let RpcCall {
            peer,
            message,
            reply,
            delivered,
        } = call;
        let state = self;
        ensure!(
            Some(peer) == state.config.core
                && message.version == 1
                && message.boundary == state.ready.boundary,
            "wrong remote role/boundary/version"
        );
        let expected_origin = match &message.request {
            Request::Initialize(bootstrap) => bootstrap.origin.hash()?,
            _ => state
                .state
                .as_ref()
                .context("runner not initialized")?
                .runner
                .origin
                .hash()?,
        };
        ensure!(message.origin == expected_origin, "wrong remote origin");
        let stop = matches!(&message.request, Request::Process(Command::Stop));
        if let Request::Wait(assignment) = message.request {
            let origin = state
                .state
                .as_ref()
                .context("runner not initialized")?
                .runner
                .origin
                .clone();
            state
                .peer
                .as_mut()
                .context("runner peer absent")?
                .domain_history(&origin)?;
            let notice = state
                .notices
                .remove(&assignment)
                .context("provider notification absent")?;
            // The single SQLite/peer owner is free to service cancellation.
            tokio::spawn(async move {
                let response = match notice.wait().await {
                    Ok(()) => Response::Process(Reply::Unit),
                    Err(error) => Response::Error(error.to_string()),
                };
                let _ = reply.send(response);
            });
            Ok(false)
        } else {
            let response = match state.handle(message.request).await {
                Ok(reply) => reply,
                Err(error) => Response::Error(format!("{error:#}")),
            };
            let successful_stop = stop && matches!(&response, Response::Process(Reply::Unit));
            let _ = reply.send(response);
            if successful_stop {
                let _ = delivered.await;
            }
            Ok(successful_stop)
        }
    }
}

impl ProtocolHandler for Handler {
    async fn accept(&self, conn: IrohConnection) -> Result<(), AcceptError> {
        if conn.remote_id() != self.core {
            conn.close(1u32.into(), b"only the pinned fixture core");
            return Ok(());
        }
        if self.serve(&conn).await.is_err() {
            conn.close(1u32.into(), b"invalid disposable runner request");
        }
        Ok(())
    }
}

pub(in crate::store::task_adapter) fn hostname() -> Result<String> {
    let out = OsCommand::new("/bin/hostname").output()?;
    ensure!(out.status.success(), "hostname unavailable");
    Ok(String::from_utf8(out.stdout)?.trim().to_owned())
}

fn machine_hash() -> Result<String> {
    #[cfg(target_os = "linux")]
    let bytes = std::fs::read("/etc/machine-id")?;
    #[cfg(target_os = "macos")]
    let bytes = {
        let out = OsCommand::new("/usr/sbin/ioreg")
            .args(["-rd1", "-c", "IOPlatformExpertDevice"])
            .output()
            .context("macOS ioreg host identity command unavailable")?;
        macos_identity(out.status.success(), &out.stdout)?.into_bytes()
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    anyhow::bail!("acceptance supports Linux/macOS only");
    Ok(blake3::hash(&bytes).to_string())
}

fn macos_identity(success: bool, stdout: &[u8]) -> Result<String> {
    ensure!(success, "macOS ioreg host identity command failed");
    ensure!(
        !stdout.is_empty() && stdout.len() <= 65536,
        "invalid macOS identity response size"
    );
    let text = std::str::from_utf8(stdout).context("invalid macOS identity response encoding")?;
    let mut identity = None;
    for line in text.lines() {
        let Some((key, value)) = line.trim().split_once('=') else {
            continue;
        };
        if key.trim() != "\"IOPlatformUUID\"" {
            continue;
        }
        ensure!(identity.is_none(), "ambiguous macOS hardware identity");
        let mut uuid: String =
            serde_json::from_str(value.trim()).context("invalid macOS hardware identity syntax")?;
        ensure!(
            uuid.len() == 36
                && uuid.bytes().enumerate().all(|(i, b)| {
                    if matches!(i, 8 | 13 | 18 | 23) {
                        b == b'-'
                    } else {
                        b.is_ascii_hexdigit()
                    }
                }),
            "malformed macOS hardware identity"
        );
        uuid.make_ascii_lowercase();
        ensure!(
            uuid.bytes().any(|b| b != b'0' && b != b'-')
                && uuid.bytes().any(|b| b != b'f' && b != b'-'),
            "placeholder macOS hardware identity"
        );
        identity = Some(uuid);
    }
    identity.context("macOS IOPlatformUUID absent")
}

fn namespace(config: &RoleConfig) -> NamespaceSecret {
    // This PUBLIC fixture namespace is not an authority credential. Both hosts
    // derive it; device signing keys never leave their host. Only owner-signed
    // endpoint/author membership enables the restricted record protocol.
    NamespaceSecret::from_bytes(
        blake3::hash(
            format!(
                "CoconutClaw public acceptance namespace v1:{}",
                config.run_id
            )
            .as_bytes(),
        )
        .as_bytes(),
    )
}

fn local_key(root: &Path, name: &str) -> Result<SecretKey> {
    let path = root.join(name);
    let mut bytes = [0; 32];
    if path.exists() {
        File::open(path)?.read_exact(&mut bytes)?;
    } else {
        File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let mut out = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)?;
        out.write_all(&bytes)?;
        out.sync_all()?;
    }
    Ok(SecretKey::from(bytes))
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let pending = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(pending, path)?;
    Ok(())
}

async fn peer(config: &RoleConfig, name: &str, owner: EndpointId) -> Result<ScopedPeer> {
    ScopedPeer::acceptance(
        &config.root.join(name),
        local_key(&config.root, &format!("{name}-key"))?,
        owner,
        Capability::Write(namespace(config)),
        config.network.clone(),
    )
    .await
}

fn check_root(config: &RoleConfig) -> Result<File> {
    ensure!(
        config.root.is_absolute()
            && config.run_id.len() >= 16
            && config.run_id.len() <= 128
            && config
                .run_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && matches!(config.role.as_str(), "runner" | "coordinator"),
        "invalid fixture role/root/id"
    );
    if !config.root.exists() {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir(&config.root)?;
        std::fs::set_permissions(&config.root, std::fs::Permissions::from_mode(0o700))?;
        std::fs::write(config.root.join("MULTIHOST"), ROLE_MARKER)?;
    }
    ensure!(
        std::fs::read_to_string(config.root.join("MULTIHOST"))? == ROLE_MARKER,
        "refuse unmarked existing directory"
    );
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.root.join("role.lock"))?;
    lock.try_lock().context("acceptance role already owned")?;
    Ok(lock)
}

async fn serve_role(config: RoleConfig) -> Result<()> {
    let _lock = check_root(&config)?;
    // Establish actual identity before opening peers/ports or generating keys.
    let machine_hash = machine_hash()?;
    let key = local_key(&config.root, "role-key")?;
    let owner = if config.role == "coordinator" {
        local_key(&config.root, "owner-key")?.public()
    } else {
        config.owner.context("runner needs pinned owner")?
    };
    let data = peer(&config, "data", owner).await?;
    let mirror = if config.role == "coordinator" {
        Some(peer(&config, "mirror", owner).await?)
    } else {
        None
    };
    let endpoint = endpoint(&config.network, key.clone()).await?;
    let ready = Ready {
        version: 1,
        run_id: config.run_id.clone(),
        role: config.role.clone(),
        hostname: hostname()?,
        machine_hash,
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        pid: std::process::id(),
        real_provider: config.provider.is_some(),
        controlled_fixture: config
            .provider
            .as_ref()
            .is_some_and(|p| p.controlled_fixture),
        owner,
        control: dial(&endpoint.addr(), &config.network),
        boundary: blake3::hash(
            &local_key(&config.root, &format!("boundary-{}", std::process::id()))?.to_bytes(),
        )
        .to_string(),
        data: dial(&data.addr(), &config.network),
        author: data.author().await?,
        mirror: match &mirror {
            Some(p) => Some((dial(&p.addr(), &config.network), p.author().await?)),
            None => None,
        },
    };
    if config.role == "runner" {
        if let Some(profile) = &config.provider {
            write_json(&config.root.join("host-provider.json"), profile)?;
            provider::host::validate(&config.root)?;
        }
        let mut state = RemoteState {
            config: config.clone(),
            ready: ready.clone(),
            key,
            peer: Some(data),
            state: None,
            core_data: None,
            notices: BTreeMap::new(),
        };
        let (sender, mut receive) = mpsc::channel(4);
        let router = Router::builder(endpoint)
            .accept(
                ALPN,
                Handler {
                    sender,
                    slots: Arc::new(Semaphore::new(4)),
                    core: config.core.context("runner requires pinned core")?,
                },
            )
            .spawn();
        announce(&config, &ready)?;
        while let Some(call) = receive.recv().await {
            // Invalid requests close their own stream, not the role or its stores.
            if state.dispatch(call).await.unwrap_or(false) {
                break;
            }
        }
        router.shutdown().await?;
        state.state.take();
        state
            .peer
            .take()
            .context("runner peer absent")?
            .shutdown()
            .await?;
        write_json(
            &config.root.join("stopped.json"),
            &serde_json::json!({"stopped":true,"pid":ready.pid}),
        )?;
    } else {
        coordinator(
            config,
            key,
            ready,
            endpoint,
            data,
            mirror.context("mirror absent")?,
        )
        .await?;
    }
    Ok(())
}

pub(in crate::store::task_adapter) struct LocalRequest {
    pub value: serde_json::Value,
    pub reply: oneshot::Sender<serde_json::Value>,
    pub delivered: oneshot::Receiver<()>,
}

async fn control_server(
    listener: tokio::net::UnixListener,
    sender: mpsc::Sender<LocalRequest>,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    loop {
        let (mut socket, _) = listener.accept().await?;
        let sender = sender.clone();
        tokio::spawn(async move {
            let result = async {
                let mut size = [0; 4];
                socket.read_exact(&mut size).await?;
                let size = u32::from_be_bytes(size) as usize;
                ensure!(
                    size > 0 && size <= 65536,
                    "local fixture command exceeds limit"
                );
                let mut bytes = vec![0; size];
                socket.read_exact(&mut bytes).await?;
                let value = serde_json::from_slice(&bytes)?;
                let (reply, receive) = oneshot::channel();
                let (delivered, delivery) = oneshot::channel();
                sender
                    .send(LocalRequest {
                        value,
                        reply,
                        delivered: delivery,
                    })
                    .await?;
                let bytes = serde_json::to_vec(&receive.await?)?;
                socket
                    .write_all(&(bytes.len() as u32).to_be_bytes())
                    .await?;
                socket.write_all(&bytes).await?;
                let _ = delivered.send(());
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if result.is_err() {
                let _ = socket.shutdown().await;
            }
        });
    }
}

async fn coordinator(
    config: RoleConfig,
    core: SecretKey,
    ready: Ready,
    endpoint: Endpoint,
    publisher: ScopedPeer,
    mirror: ScopedPeer,
) -> Result<()> {
    let (sender, mut receiver) = mpsc::channel::<LocalRequest>(4);
    let socket = config.root.join("control.sock");
    ensure!(
        !socket.exists(),
        "stale control socket: verify old role is stopped, remove only this owned socket"
    );
    let server = tokio::spawn(control_server(
        tokio::net::UnixListener::bind(&socket)?,
        sender,
    ));
    announce(&config, &ready)?;
    let first = receiver
        .recv()
        .await
        .context("coordinator control closed")?;
    ensure!(
        matches!(
            first.value["command"].as_str(),
            Some("check" | "run" | "run-cut" | "run-cancel" | "replay" | "replay-cancel")
        ),
        "first coordinator command must run/replay with runner_ready"
    );
    let runner: Ready = serde_json::from_value(first.value["runner_ready"].clone())?;
    let same_host = first.value["allow_same_host"] == true;
    ensure!(
        runner.version == 1
            && runner.role == "runner"
            && runner.run_id == ready.run_id
            && runner.owner == ready.owner
            && runner.control.id != core.public()
            && (same_host
                || (runner.machine_hash != ready.machine_hash
                    && runner.hostname != ready.hostname)),
        "runner identity/run/owner mismatch or not different hosts"
    );
    let owner = local_key(&config.root, "owner-key")?;
    let origin_path = config.root.join("origin.json");
    let origin = if origin_path.exists() {
        let origin: BoundOrigin = serde_json::from_slice(&std::fs::read(&origin_path)?)?;
        ensure!(
            origin.origin.core == core.public()
                && origin.origin.runners == BTreeSet::from([runner.control.id]),
            "stored origin differs from supplied remote role"
        );
        origin
    } else {
        let origin = BoundOrigin::sign(
            coconutclaw_sync::synthetic::membership::domain::Origin {
                owner: owner.public(),
                store: blake3::hash(config.run_id.as_bytes()).to_string(),
                namespace: namespace(&config).id(),
                scope: "synthetic/chat-a".into(),
                core: core.public(),
                runners: BTreeSet::from([runner.control.id]),
            },
            &owner,
        )?;
        write_json(&origin_path, &origin)?;
        origin
    };
    let policy_path = config.root.join("policy.json");
    let policy = if policy_path.exists() {
        serde_json::from_slice(&std::fs::read(&policy_path)?)?
    } else {
        let policy = SignedPolicy::sign(
            Policy {
                generation: 1,
                expires_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs()
                    + 86400,
                namespace: namespace(&config).id(),
                scope: origin.origin.scope.clone(),
                members: vec![
                    Member {
                        endpoint: ready.data.id,
                        authors: BTreeSet::from([ready.author]),
                    },
                    Member {
                        endpoint: ready.mirror.as_ref().context("mirror absent")?.0.id,
                        authors: BTreeSet::from([ready
                            .mirror
                            .as_ref()
                            .context("mirror absent")?
                            .1]),
                    },
                    Member {
                        endpoint: runner.data.id,
                        authors: BTreeSet::from([runner.author]),
                    },
                ],
            },
            &owner,
        )?;
        write_json(&policy_path, &policy)?;
        policy
    };
    let client = Arc::new(Client {
        endpoint: endpoint.clone(),
        ready: runner.clone(),
        origin: origin.clone(),
        slots: Arc::new(Semaphore::new(4)),
    });
    let input = provider::host::Input {
        nonce: config.run_id.clone(),
        a: 17,
        b: 25,
    };
    client
        .initialize(policy.clone(), ready.clone(), input.clone())
        .await?;
    let session = SessionKey::local(&format!("multihost:{}", config.run_id));
    let source = Store::open(&RuntimeConfig::test_config_at(config.root.join("source")))?;
    let target = Store::open(&RuntimeConfig::test_config_at(config.root.join("target")))?;
    let mut publisher = publisher;
    let mut mirror = mirror;
    publisher.install_policy(&policy)?;
    mirror.install_policy(&policy)?;
    client.probe_transport(&mut publisher).await?;
    let runner_client = RunnerClient {
        public: runner.control.id,
        origin: origin.hash()?,
        route: Route::Remote(client),
    };
    let mut driver = super::super::super::driver::RuntimeDriver::new(
        origin,
        session,
        (source, target),
        (publisher, mirror),
        (core, runner_client),
    )?;
    driver.reconcile().await?;
    let cut = first.value["command"] == "run-cut";
    if cut {
        driver.arm_remote_result_cut()?;
    }
    let result = if first.value["command"] == "check" {
        ensure!(
            driver.runner.stats().await?.is_empty(),
            "preflight requires unused fixture"
        );
        Ok(
            serde_json::json!({"ok":true,"transport_initialized":true,"scoped_data_roundtrip":true,"invocations":0,
            "multihost":!same_host,"fixture_only":same_host,"core":ready,"runner":runner}),
        )
    } else {
        run_fixture(
            &config,
            &ready,
            &runner,
            &mut driver,
            &mut receiver,
            first.value["command"].as_str().context("first command")?,
            same_host,
        )
        .await
    };
    let response = match &result {
        Ok(value) => value.clone(),
        Err(error) => {
            let failures: Vec<_> = driver
                .runner
                .provider_evidence()
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|e| {
                    e.failure.map(|failure| {
                        serde_json::json!({
                            "assignment":e.assignment,"failure":failure
                        })
                    })
                })
                .collect();
            serde_json::json!({"ok":false,"error":error.to_string(),"no_retry":true,
                "owned_cut":cut && error.to_string().starts_with("owned fault cut:"),
                "provider_failures":failures})
        }
    };
    write_json(&config.root.join("receipt.json"), &response)?;
    let owned_cut = response["owned_cut"] == true;
    let _ = first.reply.send(response);
    // Stay available for collection/status until explicit stop. No new task is admitted.
    let mut stopping = None;
    while let Some(request) = receiver.recv().await {
        let stop = request.value["command"] == "stop";
        let detach = request.value["command"] == "detach" && cut && owned_cut;
        if stop || detach {
            stopping = Some(request);
            break;
        }
        let reply = if request.value["command"] == "probe" {
            let bytes: Vec<u8> = serde_json::from_value(request.value["bytes"].clone())?;
            probe(&mut driver, bytes).await?
        } else if request.value["command"] == "status" {
            serde_json::json!({"receipt":serde_json::from_slice::<serde_json::Value>(
                    &std::fs::read(config.root.join("receipt.json"))?)?})
        } else {
            serde_json::json!({"ok":false,"error":"finite fixture already settled; stop/reopen for replay only"})
        };
        let _ = request.reply.send(reply);
    }
    let detached = stopping
        .as_ref()
        .is_some_and(|r| r.value["command"] == "detach");
    if detached {
        driver.detach_remote().await?;
    } else {
        driver.shutdown().await?;
    }
    endpoint.close().await;
    server.abort();
    let _ = server.await;
    std::fs::remove_file(socket)?;
    write_json(
        &config.root.join("stopped.json"),
        &serde_json::json!({"stopped":true,"pid":ready.pid,"runner_left_alive":detached}),
    )?;
    if let Some(request) = stopping {
        let _ = request.reply.send(serde_json::json!({"stopped":true}));
        let _ = request.delivered.await;
    }
    if detached { Ok(()) } else { result.map(|_| ()) }
}

async fn run_fixture(
    config: &RoleConfig,
    core: &Ready,
    remote: &Ready,
    driver: &mut super::super::super::driver::RuntimeDriver,
    control: &mut mpsc::Receiver<LocalRequest>,
    command: &str,
    same_host: bool,
) -> Result<serde_json::Value> {
    use super::super::super::driver::Step;
    let replay = matches!(command, "replay" | "replay-cancel");
    let cancelled = matches!(command, "run-cancel" | "replay-cancel");
    let input = provider::host::Input {
        nonce: core.run_id.clone(),
        a: 17,
        b: 25,
    };
    let existing: bool = driver.source.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM task_runs WHERE update_id='delegation:1:0')",
        [],
        |r| r.get(0),
    )?;
    let id = if existing {
        2
    } else {
        ensure!(!replay, "replay cannot allocate an assignment");
        super::super::super::tests::enqueue_text(
            &driver.source,
            &driver.session,
            0,
            if remote.controlled_fixture {
                provider::host::CONTROLLED_PROMPT
            } else {
                provider::host::PROMPT
            },
        )
    };
    let assignment = driver.queue(id).await?;
    ensure!(assignment == 1, "finite fixture must be assignment1");
    if !replay {
        let mode = if config.role == "coordinator" && remote.role == "runner" {
            // The remote role itself admits only its provisioned backend.
            // The request explicitly states real/native versus mock validation.
            if remote.real_provider {
                Mode::HostCodex
            } else {
                Mode::Cooperative
            }
        } else {
            anyhow::bail!("invalid fixture roles")
        };
        driver.admit_remote(assignment, mode).await?;
        ensure!(
            driver.step_remote_control(control).await? == Step::Applied(assignment),
            "result not applied"
        );
    }
    let before = driver.publisher.domain_history(&driver.origin)?.export();
    let proposal = driver
        .runner
        .stored_proposal(assignment)
        .await?
        .context("runner proposal absent")?;
    std::fs::write(config.root.join("proposal.json"), &proposal)?;
    ensure!(
        driver.accept_proposal(proposal.clone()).await? == Step::Duplicate(assignment),
        "duplicate not exact"
    );
    ensure!(
        driver.publisher.domain_history(&driver.origin)?.export() == before,
        "duplicate allocated history"
    );
    ensure!(
        driver
            .runner
            .begin(
                assignment,
                if remote.real_provider {
                    Mode::HostCodex
                } else {
                    Mode::Cooperative
                }
            )
            .await
            .is_err(),
        "settled assignment invoked again"
    );
    ensure!(
        driver.admit(assignment, |_| async {}).await.is_err(),
        "settled assignment re-admitted"
    );
    let run = driver
        .source
        .get_task_run(id)?
        .context("source task absent")?;
    let expected_status = if cancelled { "cancelled" } else { "completed" };
    ensure!(
        run.status == expected_status,
        "original Store did not settle as expected"
    );
    let stats = driver.runner.stats().await?;
    ensure!(
        stats.len() == 1 && stats[0].assignment == assignment && stats[0].status == expected_status,
        "remote invocation count/lifecycle mismatch"
    );
    let signed = driver.runner.saved().await?;
    ensure!(
        signed.len() == 2 && before.len() == 3,
        "runner/domain signature count mismatch"
    );
    write_json(
        &config.root.join("signed-events.json"),
        &before
            .iter()
            .map(|b| serde_json::from_slice::<serde_json::Value>(b))
            .collect::<std::result::Result<Vec<_>, _>>()?,
    )?;
    let artifact = if remote.real_provider && !cancelled {
        let (bytes, manifest): (Vec<u8>, String) = driver.source.conn.query_row(
            "SELECT bytes,manifest FROM acceptance_artifact WHERE assignment=?1",
            [assignment],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let manifest: provider::host::Manifest = serde_json::from_str(&manifest)?;
        provider::host::verify_artifact(
            &manifest,
            assignment,
            &input,
            &remote.hostname,
            &bytes,
            remote.controlled_fixture,
        )?;
        ensure!(
            manifest.runner_pid == stats[0].pid,
            "artifact invocation PID differs from durable claim"
        );
        let exported = config.root.join("fixture-output.json");
        if exported.exists() {
            ensure!(
                std::fs::read(exported)? == bytes,
                "exported artifact replay conflict"
            );
        } else {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(exported)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        Some(manifest)
    } else {
        None
    };
    let evidence = driver.runner.provider_evidence().await?;
    write_json(&config.root.join("provider-evidence.json"), &evidence)?;
    let mut refused = Vec::new();
    for field in ["origin", "source_run", "assignment_hash"] {
        let mut tampered: SignedProposal = serde_json::from_slice(&proposal)?;
        match field {
            "origin" => tampered.proposal.origin = "0".repeat(64),
            "source_run" => tampered.proposal.source_run += 1,
            _ => tampered.proposal.assignment_hash = "0".repeat(64),
        }
        let check = probe(driver, serde_json::to_vec(&tampered)?).await?;
        ensure!(check["rejected"] == true, "tampered proposal accepted");
        refused.push(field);
    }
    Ok(serde_json::json!({
        "ok":true,"multihost":!same_host,"fixture_only":same_host,"replay":replay,
        "core":core,"runner":remote,"assignment":assignment,"source_run":id,
        "source_status":run.status,"invocations":stats.len(),"exact_duplicate":true,
        "readmission_refused":true,"event_hashes":before.iter().map(|b|blake3::hash(b).to_string()).collect::<Vec<_>>(),
        "artifact":artifact,"tampered_fields_refused":refused,
        "outbox_delivery_not_executed":true
    }))
}

async fn probe(
    driver: &mut super::super::super::driver::RuntimeDriver,
    bytes: Vec<u8>,
) -> Result<serde_json::Value> {
    type PayloadSnapshot = (i64, Option<String>, i64, String);
    let payloads = |store: &Store| -> Result<Vec<PayloadSnapshot>> {
        Ok(store.conn.prepare(
            "SELECT task_run_id,output,delivered,delivery_state FROM task_payloads ORDER BY task_run_id"
        )?.query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?
            .collect::<rusqlite::Result<_>>()?)
    };
    let outbox = payloads(&driver.source)?;
    let before = driver.publisher.domain_history(&driver.origin)?.export();
    let tasks: i64 = driver
        .source
        .conn
        .query_row("SELECT count(*) FROM task_runs", [], |r| r.get(0))?;
    let invocations = driver.runner.stats().await?.len();
    let result = driver.accept_proposal(bytes).await;
    ensure!(
        driver.publisher.domain_history(&driver.origin)?.export() == before
            && driver
                .source
                .conn
                .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))?
                == tasks
            && driver.runner.stats().await?.len() == invocations,
        "probe changed admission/history/invocation state"
    );
    ensure!(
        payloads(&driver.source)? == outbox,
        "probe changed original outbox"
    );
    Ok(serde_json::json!({
        "rejected":result.is_err(),"exact_duplicate":matches!(result,Ok(super::super::super::driver::Step::Duplicate(_))),
        "error":result.err().map(|error|format!("{error:#}")),
        "history_tasks_invocations_unchanged":true,"outbox_unchanged":true
    }))
}

#[test]
#[ignore = "explicit portable dev role executable; never runs real Codex by default"]
fn multihost_role_entry() {
    assert!(
        std::env::args().any(|a| a == ENTRY),
        "launch the exact explicit role entry"
    );
    let config =
        std::env::var_os("COCONUTCLAW_MULTIHOST_CONFIG").expect("explicit role config required");
    let config: RoleConfig = serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let root = config.root.clone();
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(serve_role(config));
    if let Err(error) = &result {
        let _ = write_json(
            &root.join("failure.json"),
            &serde_json::json!({"ok":false,"error":error.to_string(),"no_retry":true}),
        );
    }
    result.unwrap();
}

fn announce(config: &RoleConfig, ready: &Ready) -> Result<()> {
    write_json(&config.root.join("ready.json"), ready)?;
    if let Some(socket) = &config.ready_socket {
        let mut stream = UnixStream::connect(socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        write_frame(&mut stream, &serde_json::to_vec(ready)?)?;
        // Keep the peer live until its reader has installed the timeout and
        // consumed Ready. macOS can reject SO_RCVTIMEO after peer disconnect.
        let mut ack = [0];
        stream.read_exact(&mut ack)?;
        ensure!(ack == [1], "invalid ready acknowledgment");
    }
    Ok(())
}

mod tests;
