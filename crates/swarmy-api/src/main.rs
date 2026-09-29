use anyhow::{Context, Result};
use jiff::Timestamp;
use std::sync::Arc;
use swarmy_api::{AppState, router};
use swarmy_bus::{Bus, Config, SubjectToken};
use swarmy_config::Settings;
use swarmy_store::{ServiceDetail, ServiceHeartbeat, ServiceRole, Store, blob::ObjectBlobStore};

fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-api")?;
    swarmy_config::init_tracing("info");
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run())
}
async fn run() -> Result<()> {
    let settings = Settings::load()?.settings;
    let token = std::env::var("SWARMY_API_TOKEN").unwrap_or(settings.api.token.clone());
    let listen = std::env::var("SWARMY_API_LISTEN").unwrap_or(settings.api.listen.clone());
    let (store, blobs) = Store::open_store(&settings).await?;
    let objects = blobs.object_store();
    let keyring = swarmy_config::Keyring::load().ok();
    let bus = Bus::connect(
        &settings.bus.nats_url,
        Config {
            prefix: if settings.bus.prefix.is_empty() {
                None
            } else {
                Some(SubjectToken::new(settings.bus.prefix.clone())?)
            },
            ack_wait: settings.bus.ack_wait,
            max_deliver: settings.bus.max_deliver_i64(),
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
    let mut state = AppState::new(store, bus, token, settings.catalog()?, objects);
    state.credential_keyring = keyring;
    state.gc = settings.gc;
    state.upload_dir = settings.state_dir.join("uploads");
    state.upload_max_bytes = settings.image.upload_max_bytes;
    swarmy_api::images::sweep_stale_uploads(&state.upload_dir);
    state.resend_interval = settings.scheduler.resend_interval;
    state.default_image = settings.selection.default_image.clone();
    state.fake_files = Some((settings.fake.script.clone(), settings.fake.call_log.clone()));
    state.default_selection = swarmy_core::ResolvedSelection {
        provider: settings.selection.provider.clone(),
        model: settings.selection.model.clone(),
        effort: settings.selection.effort,
    };
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .context("bind API listener")?;
    tracing::info!(address = %listener.local_addr()?, "api ready");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
