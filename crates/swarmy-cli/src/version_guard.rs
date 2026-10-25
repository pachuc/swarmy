//! The interactive client requires the exact core build it was compiled with.
use swarmy_api_types::HealthResponse;

pub(crate) fn core_mismatch(health: &HealthResponse) -> Option<String> {
    (health.version != swarmy_version::VERSION || health.git_commit != swarmy_version::GIT_COMMIT).then(|| format!("API core {} ({}) does not match this client {}; run swarmy dev up (local) or swarmy remote upgrade NAME (remote) to align them", health.version, health.git_commit, swarmy_version::IDENTITY))
}
pub(crate) fn enforce(health: &HealthResponse, allow: bool) -> anyhow::Result<()> {
    if let Some(message) = core_mismatch(health) {
        if !allow {
            anyhow::bail!("{message}\nUse --allow-version-mismatch to override.");
        }
        eprintln!("warning: {message} (--allow-version-mismatch)");
    }
    Ok(())
}
