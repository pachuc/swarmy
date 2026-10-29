//! Primary-host service entry point.
use crate::{Holder, Reconciler, Tick};
use std::{ffi::OsString, time::Duration};
use swarmy_store::Store;

pub const LEASE_TTL: Duration = Duration::from_secs(15 * 60);
pub const RECONCILE_INTERVAL: Duration = Duration::from_secs(15);
/// # Errors
/// Returns startup failures.
pub fn run(args: Vec<OsString>) -> anyhow::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(serve(args))
}
async fn serve(args: Vec<OsString>) -> anyhow::Result<()> {
    swarmy_version::parse_from::<swarmy_version::ServiceArgs>("swarmy-provisioner", args)?;
    swarmy_config::init_tracing();
    let settings = swarmy_config::Settings::load()?.settings;
    let _network = swarmy_store::boot();
    let opened = Store::open_store(&settings).await?;
    let dirs = swarmy_config::paths::Dirs::from_process()?;
    let credentials = match swarmy_config::Keyring::load() {
        Ok(keyring) => Some(opened.store.credentials(keyring)),
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&error), "cluster keyring unavailable");
            None
        }
    };
    let reconciler = Reconciler::new(
        opened.store,
        credentials,
        opened.blobs.object_store(),
        dirs.data.join("provisioner"),
        dirs.cache,
    );
    let mut holder = Holder::new();
    let shutdown = swarmy_config::shutdown_signal();
    tokio::pin!(shutdown);
    let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
    tracing::info!("provisioner ready");
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            _ = interval.tick() => {},
        }
        match reconciler.tick(&mut holder).await {
            Ok(Tick::Standby) => tracing::debug!("provisioner lease held elsewhere"),
            Ok(Tick::Reconciled(outcomes)) => {
                for outcome in outcomes {
                    if outcome.before != outcome.after || outcome.limited {
                        tracing::info!(?outcome, "elastic group reconciled");
                    }
                }
            }
            Err(error) => {
                tracing::error!(error = %swarmy_core::error_chain(error.as_ref()), "provisioner tick failed")
            }
        }
    }
    if let Some(lease) = holder.lease() {
        swarmy_core::ignore_best_effort(
            reconciler.store().release_provisioner_lease(lease).await,
            "release provisioner lease",
        );
    }
    Ok(())
}
