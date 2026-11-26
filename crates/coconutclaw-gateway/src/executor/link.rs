use std::io::Read;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use coconutclaw_gateway::executor_wire::{BODY_LIMIT, DEVICE, Status, request_bytes};
use ed25519_dalek::{Signer, SigningKey};
use reqwest::blocking::Client;
use reqwest::header::HeaderValue;
use reqwest::{Certificate, StatusCode, Url, redirect};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::journal::Journal;

#[derive(Debug)]
pub struct Unavailable;

impl std::fmt::Display for Unavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("gateway unavailable")
    }
}

impl std::error::Error for Unavailable {}

#[derive(Debug)]
pub struct PilotPaused(pub Duration);

impl std::fmt::Display for PilotPaused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("loopback gateway request budget exhausted")
    }
}

impl std::error::Error for PilotPaused {}

pub fn now_ms() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

pub struct Link {
    client: Client,
    url: Url,
    pub key: SigningKey,
    pub instance: String,
    incarnation: Option<HeaderValue>,
}

impl Link {
    pub fn new(
        origin: &str,
        certificate: &[u8],
        key: SigningKey,
        instance: String,
    ) -> Result<Self> {
        let mut url = Url::parse(origin).context("invalid loopback origin")?;
        ensure!(
            url.scheme() == "https"
                && url.host_str() == Some("127.0.0.1")
                && url.port().is_some()
                && url.path() == "/"
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "expected an explicit HTTPS 127.0.0.1 fixture origin"
        );
        url.set_path("/v1/agent");
        let client = Client::builder()
            .no_proxy()
            .redirect(redirect::Policy::none())
            .tls_built_in_root_certs(false)
            .add_root_certificate(Certificate::from_pem(certificate)?)
            .timeout(Duration::from_millis(750))
            .build()?;
        Ok(Self {
            client,
            url,
            key,
            instance,
            incarnation: None,
        })
    }

    #[cfg(target_os = "linux")]
    pub fn production(
        origin: &str,
        ca: Option<&[u8]>,
        key: SigningKey,
        instance: String,
        incarnation: &str,
    ) -> Result<Self> {
        let mut url = Url::parse(origin).map_err(|_| anyhow::anyhow!("invalid gateway origin"))?;
        ensure!(
            url.scheme() == "https"
                && url.host().is_some()
                && url.path() == "/"
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "expected a bare HTTPS gateway origin"
        );
        url.set_path("/v1/agent");
        let mut builder = Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(redirect::Policy::none())
            .timeout(Duration::from_millis(3500));
        if let Some(ca) = ca {
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(Certificate::from_pem(ca)?);
        }
        let mut incarnation = HeaderValue::from_str(incarnation)?;
        incarnation.set_sensitive(true);
        Ok(Self {
            client: builder.build()?,
            url,
            key,
            instance,
            incarnation: Some(incarnation),
        })
    }

    pub fn is_production(&self) -> bool {
        self.incarnation.is_some()
    }

    pub fn hello(&self, journal: &Journal) -> Result<()> {
        #[derive(Deserialize)]
        struct Hello {
            protocol: i64,
            instance: String,
            device: String,
            capabilities: Vec<String>,
            last_sequence: i64,
        }
        let hello: Hello = self.read(json!({"op":"hello"}))?;
        ensure!(
            hello.protocol == 1
                && hello.instance == self.instance
                && hello.device == DEVICE
                && hello.capabilities == ["text"]
                && (0..=i64::from(i32::MAX)).contains(&hello.last_sequence),
            "unexpected gateway identity or capability"
        );
        journal.sync_sequence(hello.last_sequence)
    }

    pub fn status(&self, attempt: i64) -> Result<Status> {
        let status: Status = self.read(json!({"op":"status", "attempt":attempt}))?;
        ensure!(status.attempt == attempt, "status bound to another attempt");
        Ok(status)
    }

    pub fn read<T: DeserializeOwned>(&self, command: Value) -> Result<T> {
        self.post(command, 0)?.context("gateway read conflicted")
    }

    pub fn post<T: DeserializeOwned>(&self, command: Value, sequence: i64) -> Result<Option<T>> {
        let timeout = if self.is_production() {
            Duration::from_millis(5000)
        } else if command["op"] == "start" {
            Duration::from_secs(6)
        } else {
            Duration::from_millis(750)
        };
        let body = serde_json::to_vec(&json!({
            "device":DEVICE, "at_ms":now_ms()?, "sequence":sequence, "command":command,
        }))?;
        ensure!(body.len() <= BODY_LIMIT, "gateway request exceeds body cap");
        let signature = self.key.sign(&request_bytes(&self.instance, &body));
        let mut request = self
            .client
            .post(self.url.clone())
            .timeout(timeout)
            .header("content-type", "application/json")
            .header("x-agent-signature", hex::encode(signature.to_bytes()))
            .body(body);
        if let Some(incarnation) = &self.incarnation {
            request = request.header("x-executor-incarnation", incarnation.clone());
        }
        let response = request.send().map_err(|_| Unavailable)?;
        match response.status() {
            StatusCode::OK | StatusCode::CONFLICT => {
                let status = response.status();
                let mut bytes = Vec::new();
                response
                    .take((BODY_LIMIT + 1) as u64)
                    .read_to_end(&mut bytes)
                    .map_err(|_| Unavailable)?;
                ensure!(bytes.len() <= BODY_LIMIT, "oversized gateway response");
                if status == StatusCode::CONFLICT {
                    let error: Value = serde_json::from_slice(&bytes)?;
                    if error["error"] == "replay" {
                        return Err(Unavailable.into());
                    }
                    ensure!(
                        error["error"] == "state_conflict",
                        "unexpected gateway conflict"
                    );
                    return Ok(None);
                }
                Ok(Some(serde_json::from_slice(&bytes).map_err(|_| {
                    anyhow::anyhow!("invalid gateway response")
                })?))
            }
            StatusCode::TOO_MANY_REQUESTS => {
                let seconds = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .filter(|value| (1..=86_400).contains(value))
                    .unwrap_or(60);
                Err(PilotPaused(Duration::from_secs(seconds)).into())
            }
            status if status.is_server_error() => Err(Unavailable.into()),
            status => bail!(
                "{} request rejected with HTTP {}",
                if self.is_production() {
                    "gateway"
                } else {
                    "fixture"
                },
                status.as_u16()
            ),
        }
    }
}
