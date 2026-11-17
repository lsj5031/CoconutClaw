//! The existing iroh control pattern: pinned QUIC, one canonical bounded frame
//! per stream. No docs/blob service, discovery or filesystem serving is enabled.
use super::*;
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use std::sync::OnceLock;

const MAX_FRAME: usize = 65536;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) fn runtime() -> Result<&'static tokio::runtime::Runtime> {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    // Concurrent initialization may create an unused runtime, never endpoints.
    let _ = RUNTIME.set(runtime);
    Ok(RUNTIME.get().expect("runtime initialized"))
}
pub(super) async fn endpoint(key: SecretKey, bind: SocketAddr) -> Result<Endpoint> {
    Ok(Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .clear_ip_transports()
        .bind_addr(bind)?
        .clear_address_lookup()
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await?)
}
pub(super) async fn coordinator(cfg: &RuntimeConfig) -> Result<Endpoint> {
    validate_storage(cfg)?;
    static ENDPOINTS: OnceLock<tokio::sync::Mutex<HashMap<PathBuf, Endpoint>>> = OnceLock::new();
    let mut endpoints = ENDPOINTS.get_or_init(Default::default).lock().await;
    if let Some(endpoint) = endpoints.get(&cfg.runtime_dir) {
        return Ok(endpoint.clone());
    }
    let own = local(cfg)?;
    let endpoint = endpoint(key(cfg)?, own.card.address).await?;
    endpoints.insert(cfg.runtime_dir.clone(), endpoint.clone());
    Ok(endpoint)
}
pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    crate::runner_wire::encode(value, MAX_FRAME)
}
pub(super) fn decode<T: Serialize + for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T> {
    crate::runner_wire::decode(bytes, MAX_FRAME)
}
pub(super) async fn call(endpoint: &Endpoint, card: &Card, request: Request) -> Result<Reply> {
    // Bound each control exchange, not the provider execution.
    tokio::time::timeout(IO_TIMEOUT, async {
        ensure!(
            card.revision == REVISION && card.role == Role::Runner,
            "wrong runner revision/role"
        );
        let connection = endpoint
            .connect(EndpointAddr::new(card.id).with_ip_addr(card.address), ALPN)
            .await?;
        let result = async {
            let (mut send, mut recv) = connection.open_bi().await?;
            send.write_all(&encode(&request)?).await?;
            send.finish()?;
            let reply: Reply = decode(&recv.read_to_end(MAX_FRAME + 4).await?)?;
            ensure!(!matches!(reply, Reply::Refused), "runner refused request");
            Ok(reply)
        }
        .await;
        connection.close(0u32.into(), b"control complete");
        result
    })
    .await
    .context("runner unavailable")?
}

pub(super) struct Handler {
    runner: Arc<Mutex<Runner>>,
    slots: Arc<tokio::sync::Semaphore>,
    workers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
impl std::fmt::Debug for Handler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RunnerHandler")
    }
}
impl ProtocolHandler for Handler {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        self.handle(connection)
            .await
            .map_err(|_| AcceptError::from_err(std::io::Error::other("runner control refused")))
    }
}
impl Handler {
    pub(super) fn new(runner: Arc<Mutex<Runner>>) -> Self {
        Self {
            runner,
            slots: Arc::new(tokio::sync::Semaphore::new(8)),
            workers: Default::default(),
        }
    }
    async fn handle(&self, connection: Connection) -> Result<()> {
        let _permit = self.slots.clone().try_acquire_owned()?;
        let peer = connection.remote_id();
        // Authenticate admission before allocating a frame or returning bytes.
        {
            let runner = self
                .runner
                .lock()
                .map_err(|_| anyhow::anyhow!("runner stopped"))?;
            validate_storage(&runner.cfg)?;
            ensure!(
                paired(&runner.store, peer)?.role == Role::Coordinator,
                "unpaired coordinator"
            );
        }
        tokio::time::timeout(IO_TIMEOUT, async {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let request: Request = decode(&recv.read_to_end(MAX_FRAME + 4).await?)?;
            // Errors intentionally carry no prompt, config, path or SDK output.
            let reply = self.dispatch(peer, request).unwrap_or(Reply::Refused);
            send.write_all(&encode(&reply)?).await?;
            send.finish()?;
            let _ = send.stopped().await;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
    pub(super) fn dispatch(&self, peer: EndpointId, request: Request) -> Result<Reply> {
        let mut runner = self
            .runner
            .lock()
            .map_err(|_| anyhow::anyhow!("runner stopped"))?;
        validate_storage(&runner.cfg)?;
        ensure!(
            paired(&runner.store, peer)?.role == Role::Coordinator,
            "unpaired coordinator"
        );
        match request {
            Request::Ping => Ok(Reply::Pong),
            Request::Status(bundle) => {
                bundle.validate(peer, runner.key.public())?;
                runner.evidence(&bundle)
            }
            Request::Cancel(bundle, command) => {
                bundle.validate(peer, runner.key.public())?;
                runner.cancel(&bundle, &command)?;
                runner.evidence(&bundle)
            }
            Request::Begin(bundle) => {
                if let Some(cancel) = runner.admit(peer, &bundle)? {
                    let mut cfg = runner.cfg.clone();
                    cfg.instance_dir = local(&cfg)?.workspace.context("runner workspace absent")?;
                    // Never grant remote text the coordinator's dangerous mode.
                    // Codex's native workspace sandbox is the execution boundary.
                    cfg.exec_policy = "safe".into();
                    let prompt = bundle.validate(peer, runner.key.public())?;
                    let owner = self.runner.clone();
                    let work = bundle.clone();
                    let work = tokio::task::spawn_blocking(move || {
                        let result = coconutclaw_provider::run_workspace(&cfg, &prompt, &cancel);
                        let (outcome, text) = match result {
                            Ok(_) if cancel.load(Ordering::SeqCst) => {
                                (Outcome::Cancelled, "cancelled".into())
                            }
                            Ok(result) if result.success => (Outcome::Completed, result.raw_output),
                            _ => (
                                Outcome::Unknown,
                                "provider outcome unknown; no automatic retry".into(),
                            ),
                        };
                        if let Ok(mut runner) = owner.lock()
                            && runner.finish(&work, outcome, &text).is_err()
                        {
                            let _ = runner.finish(
                                &work,
                                Outcome::Unknown,
                                "result exceeds signed transport bounds; outcome unknown",
                            );
                        }
                    });
                    let mut workers = self
                        .workers
                        .lock()
                        .map_err(|_| anyhow::anyhow!("runner stopped"))?;
                    workers.retain(|work| !work.is_finished());
                    workers.push(work);
                }
                runner.evidence(&bundle)
            }
        }
    }
}

pub(super) async fn serve(cfg: RuntimeConfig) -> Result<()> {
    let _instance = cfg.acquire_instance_lock()?;
    let own = local(&cfg)?;
    let runner = Arc::new(Mutex::new(Runner::open(cfg.clone())?));
    let endpoint = endpoint(key(&cfg)?, own.card.address).await?;
    let handler = Handler::new(runner.clone());
    let workers = handler.workers.clone();
    let router = Router::builder(endpoint).accept(ALPN, handler).spawn();
    let (stop, receive) = tokio::sync::oneshot::channel();
    let signal = Arc::new(Mutex::new(Some(stop)));
    ctrlc::set_handler(move || {
        if let Some(stop) = signal.lock().expect("signal").take() {
            let _ = stop.send(());
        }
    })?;
    println!("Runner ready {}", own.card.id);
    let _ = receive.await;
    {
        let runner = runner
            .lock()
            .map_err(|_| anyhow::anyhow!("runner stopped"))?;
        for flag in runner.active.values() {
            flag.store(true, Ordering::SeqCst);
        }
    }
    router.shutdown().await?;
    // Existing provider cleanup targets only currently owned live groups.
    coconutclaw_provider::kill_active_process_groups();
    let workers = std::mem::take(
        &mut *workers
            .lock()
            .map_err(|_| anyhow::anyhow!("runner stopped"))?,
    );
    for work in workers {
        work.await?;
    }
    Ok(())
}
