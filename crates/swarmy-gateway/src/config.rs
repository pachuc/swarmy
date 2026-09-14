use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::Deserialize;
use swarmy_bus::{Config as BusConfig, SubjectToken};
use swarmy_llm::{
    Provider, ProviderStream, Request, Response, auth::FileCredentialStore,
    chatgpt::ChatGptProvider, fake::FakeProvider,
};
use tokio::io::AsyncWriteExt;

pub struct Config {
    pub cluster: String,
    pub directory: Vec<String>,
    pub nats: String,
    pub bus: BusConfig,
    pub class: SubjectToken,
    pub concurrency: usize,
    pub provider: Arc<dyn Provider>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let class = env::var("SWARMY_PROVIDER").context("SWARMY_PROVIDER is required")?;
        let provider: Arc<dyn Provider> = match class.as_str() {
            "fake" => Arc::new(FileFake::from_env()?),
            "chatgpt" => Arc::new(ChatGptProvider::new(Arc::new(FileCredentialStore::new(
                env::var("SWARMY_CHATGPT_AUTH").context("SWARMY_CHATGPT_AUTH is required")?,
            )))?),
            _ => bail!("unsupported SWARMY_PROVIDER: {class}"),
        };
        let concurrency = setting("SWARMY_GATEWAY_CONCURRENCY", 4_usize)?;
        let ack_wait = Duration::from_millis(setting("SWARMY_BUS_ACK_WAIT_MS", 30_000_u64)?);
        if concurrency == 0 || ack_wait < Duration::from_millis(30) {
            bail!("concurrency must be positive and ack wait at least 30 ms");
        }
        Ok(Self {
            cluster: env::var("SWARMY_FDB_CLUSTER_FILE")?,
            directory: env::var("SWARMY_STORE_DIRECTORY")
                .unwrap_or_else(|_| "swarmy".into())
                .split('/')
                .map(str::to_owned)
                .collect(),
            nats: env::var("SWARMY_NATS_URL")?,
            bus: BusConfig {
                prefix: env::var("SWARMY_BUS_PREFIX")
                    .ok()
                    .map(SubjectToken::new)
                    .transpose()?,
                ack_wait,
                max_deliver: setting("SWARMY_BUS_MAX_DELIVER", 5_i64)?,
            },
            class: SubjectToken::new(class)?,
            concurrency,
            provider,
        })
    }
}

fn setting<T: std::str::FromStr>(name: &str, default: T) -> Result<T> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|_| anyhow::anyhow!("invalid {name}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

#[derive(Deserialize)]
struct Script {
    #[serde(default)]
    latency_ms: u64,
    #[serde(default)]
    responses: BTreeMap<usize, Response>,
    #[serde(default)]
    fail: bool,
}

struct FileFake {
    provider: Arc<FakeProvider>,
    log: PathBuf,
    fail: bool,
    latency: Duration,
}

impl FileFake {
    fn from_env() -> Result<Self> {
        let script: Script =
            serde_json::from_slice(&std::fs::read(env::var("SWARMY_FAKE_SCRIPT")?)?)?;
        let mut provider = FakeProvider::default();
        provider.responses = script.responses;
        // Delay each emitted delta in the wrapper so tests can kill a partial stream.
        Ok(Self {
            provider: Arc::new(provider),
            log: env::var("SWARMY_FAKE_CALL_LOG")?.into(),
            fail: script.fail,
            latency: Duration::from_millis(script.latency_ms),
        })
    }
}

impl Provider for FileFake {
    fn request(&self, request: Request) -> ProviderStream {
        let provider = self.provider.clone();
        let log = self.log.clone();
        let fail = self.fail;
        let latency = self.latency;
        Box::pin(async_stream::try_stream! {
            let mut file = tokio::fs::OpenOptions::new().create(true).append(true).open(log).await?;
            file.write_all(b"call\n").await?;
            file.sync_data().await?;
            if fail {
                tokio::time::sleep(latency).await;
                Err(swarmy_llm::Error::Protocol("scripted provider failure".into()))?;
            }
            let mut stream = provider.request(request);
            while let Some(delta) = stream.next().await {
                tokio::time::sleep(latency).await;
                yield delta?;
            }
        })
    }
}
