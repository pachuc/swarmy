use swarmy_api::{AppState, router};
use swarmy_bus::Bus;
use swarmy_config::Settings;
use swarmy_store::{ServiceDetail, ServiceRole, Store};

/// Startup failures: the binary only assembles the service, so every
/// error names the connection or socket that failed.
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Settings(#[from] swarmy_config::Error),
    #[error(transparent)]
    Store(#[from] swarmy_store::StoreError),
    #[error(transparent)]
    Bus(#[from] swarmy_bus::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("bind API listener {listen}: {source}")]
    Bind {
        listen: String,
        #[source]
        source: std::io::Error,
    },
}

type Result<T> = std::result::Result<T, Error>;

fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-api")?;
    swarmy_config::init_tracing();
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(run())
}

async fn run() -> Result<()> {
    let settings = Settings::load()?.settings;
    let token = std::env::var("SWARMY_API_TOKEN").unwrap_or(settings.api.token.clone());
    let listen = std::env::var("SWARMY_API_LISTEN").unwrap_or(settings.api.listen.clone());
    let opened = Store::open_store(&settings).await?;
    let store = opened.store;
    let objects = opened.blobs.object_store();
    let keyring = match swarmy_config::Keyring::load() {
        Ok(keyring) => Some(keyring),
        Err(error) => {
            tracing::warn!(%error, "keyring unavailable; doctor omits credentials");
            None
        }
    };
    let bus = Bus::connect(&settings.bus.nats_url, settings.bus.bus_config()?).await?;
    let heartbeat_store = store.clone();
    let started = jiff::Timestamp::now();
    let instance_id = ulid::Ulid::generate().to_string();
    tokio::spawn(async move {
        heartbeat_store
            .heartbeat_loop(
                ServiceRole::Api,
                instance_id,
                env!("CARGO_PKG_VERSION").into(),
                started,
                ServiceDetail::None,
                false,
            )
            .await;
    });
    let mut state = AppState::new(store, bus, token, settings.catalog()?, objects);
    state.credential_keyring = keyring;
    state.gc = settings.gc;
    state.upload_dir = settings.state_dir.join("uploads");
    state.upload_max_bytes = settings.image.upload_max_bytes;
    swarmy_api::images::sweep_stale_uploads(&state.upload_dir);
    state.resend_interval = settings.scheduler.resend_interval_ms;
    state.default_image = settings.selection.default_image.clone();
    state.fake_files = Some((settings.fake.script.clone(), settings.fake.call_log.clone()));
    state.default_selection = swarmy_core::ResolvedSelection {
        provider: settings.selection.provider.clone(),
        model: settings.selection.model.clone(),
        effort: settings.selection.effort,
    };
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|source| Error::Bind {
            listen: listen.clone(),
            source,
        })?;
    tracing::info!(address = %listener.local_addr()?, "api ready");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
