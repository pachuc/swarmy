use anyhow::{Context, Result};
use jiff::Timestamp;
use std::sync::Arc;
use swarmy_api::{AppState, router};
use swarmy_bus::{Bus, Config, SubjectToken};
use swarmy_config::Settings;
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store, blob::ObjectBlobStore};

fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-api")?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run())
}
async fn run() -> Result<()> {
    let settings = Settings::load()?.settings;
    let token = std::env::var("SWARMY_API_TOKEN").unwrap_or(settings.api.token.clone());
    let listen = std::env::var("SWARMY_API_LISTEN").unwrap_or(settings.api.listen.clone());
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    let blobs = Arc::new(ObjectBlobStore::from_env()?);
    let store = Store::open(Some(&settings.fdb_cluster_file), Some(&directory), blobs).await?;
    let bus = Bus::connect(
        &settings.nats_url,
        Config {
            prefix: if settings.bus_prefix.is_empty() {
                None
            } else {
                Some(SubjectToken::new(settings.bus_prefix.clone())?)
            },
            ack_wait: std::time::Duration::from_millis(settings.bus_ack_wait_ms),
            max_deliver: settings.bus_max_deliver,
        },
    )
    .await?;
    let heartbeat_store = store.clone();
    let started = Timestamp::now();
    let instance_id = ulid::Ulid::generate().to_string();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tick.tick().await;
            let record = ServiceHeartbeat {
                role: ServiceRole::Api,
                instance_id: instance_id.clone(),
                version: env!("CARGO_PKG_VERSION").into(),
                host: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
                started_at: started,
                last_seen: Timestamp::now(),
                detail: ServiceDetail::None,
            };
            if let Err(error) = heartbeat_store.put_service_heartbeat(&record).await {
                tracing::warn!(%error, "api health heartbeat failed");
            }
        }
    });
    let mut state = AppState::new(store, bus, token, settings.catalog()?);
    state.resend_interval = std::time::Duration::from_millis(settings.scheduler_resend_interval_ms);
    state.default_image = settings.default_image.clone();
    state.default_selection = swarmy_core::ResolvedSelection {
        provider: settings.provider.clone(),
        model: settings.model.clone(),
        effort: settings.reasoning_effort.parse()?,
    };
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .context("bind API listener")?;
    tracing::info!(address = %listener.local_addr()?, "api ready");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
