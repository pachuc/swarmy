//! Primary-host service entry point.
use crate::{Holder, Outcome, Reconciler, Tick};
use std::{collections::BTreeMap, ffi::OsString, sync::Arc, time::Duration};
use swarmy_store::Store;

pub const LEASE_TTL: Duration = Duration::from_mins(15);
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
    )
    .with_nodes(
        Arc::new(crate::SshNodes::installed()?),
        swarmy_config::BucketSpec::from_settings(&settings.s3),
    );
    tracing::info!("provisioner ready");
    run_loop(&reconciler).await
}
async fn run_loop(reconciler: &Reconciler) -> anyhow::Result<()> {
    let mut holder = Holder::new();
    let mut previous_limited = BTreeMap::new();
    let shutdown = swarmy_config::shutdown_signal();
    tokio::pin!(shutdown);
    let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            _ = interval.tick() => {},
        }
        log_tick(reconciler.tick(&mut holder).await, &mut previous_limited);
    }
    if let Some(lease) = holder.lease() {
        swarmy_core::ignore_best_effort(
            reconciler.store().release_provisioner_lease(lease).await,
            "release provisioner lease",
        );
    }
    Ok(())
}
fn log_tick(result: anyhow::Result<Tick>, previous_limited: &mut BTreeMap<String, bool>) {
    match result {
        Ok(Tick::Standby) => tracing::debug!("provisioner lease held elsewhere"),
        Ok(Tick::Reconciled(outcomes)) => {
            for outcome in outcomes {
                log_outcome(&outcome, previous_limited);
            }
        }
        Err(error) => {
            tracing::error!(error = %swarmy_core::error_chain(error.as_ref()), "provisioner tick failed");
        }
    }
}

fn log_outcome(outcome: &Outcome, previous_limited: &mut BTreeMap<String, bool>) {
    let was_limited = previous_limited
        .insert(outcome.group.clone(), outcome.limited)
        .unwrap_or(outcome.limited);
    if outcome.before != outcome.after || was_limited != outcome.limited {
        tracing::info!(?outcome, "elastic group reconciled");
    } else if outcome.limited {
        tracing::debug!(?outcome, "elastic group creation limited");
    }
}
