//! Checks that the API's core can serve this client. A different API major
//! version or package version is incompatible and refused; a core built from
//! another commit of the same version is only reported, because development
//! builds share one version and most commits change nothing on the wire.
use swarmy_api_types::HealthResponse;

/// How the API's core differs from this client.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CoreMismatch {
    /// The API major version or package version differs.
    Incompatible(String),
    /// Same version, built from another commit.
    Commit(String),
}

pub(crate) fn core_mismatch(health: &HealthResponse) -> Option<CoreMismatch> {
    let same_commit = health.git_commit == swarmy_version::GIT_COMMIT;
    // Servers older than the documented contract carry no api_version; those
    // still need an exact build match, as doctor's API check requires.
    let compatible = health.version == swarmy_version::VERSION
        && if health.api_version.is_empty() {
            same_commit
        } else {
            swarmy_api_types::same_major(&health.api_version, swarmy_api_types::API_VERSION)
        };
    if !compatible {
        return Some(CoreMismatch::Incompatible(format!(
            concat!(
                "API core {} ({}) with api {} is incompatible with this client {} (api {}); ",
                "run swarmy dev up (local) or swarmy remote upgrade NAME (remote) to align them"
            ),
            health.version,
            health.git_commit,
            health.api_version,
            swarmy_version::IDENTITY,
            swarmy_api_types::API_VERSION
        )));
    }
    (!same_commit).then(|| {
        CoreMismatch::Commit(format!(
            "API core {} ({}) was built from a different commit than this client {}",
            health.version,
            health.git_commit,
            swarmy_version::IDENTITY
        ))
    })
}

pub(crate) fn enforce(health: &HealthResponse, allow: bool) -> anyhow::Result<()> {
    match core_mismatch(health) {
        Some(CoreMismatch::Incompatible(message)) if !allow => {
            anyhow::bail!("{message}\nUse --allow-version-mismatch to override.");
        }
        Some(CoreMismatch::Incompatible(message)) => {
            eprintln!("warning: {message} (--allow-version-mismatch)");
        }
        Some(CoreMismatch::Commit(message)) => eprintln!("note: {message}"),
        None => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::{CoreMismatch, core_mismatch};

    fn health(version: &str, commit: &str, api: &str) -> swarmy_api_types::HealthResponse {
        swarmy_api_types::HealthResponse {
            version: version.into(),
            git_commit: commit.into(),
            api_version: api.into(),
            default_provider: String::new(),
            services: Vec::new(),
            node_count: 0,
        }
    }

    #[test]
    fn only_incompatible_cores_are_refused() {
        let api = swarmy_api_types::API_VERSION;
        let version = swarmy_version::VERSION;
        let commit = swarmy_version::GIT_COMMIT;
        assert_eq!(core_mismatch(&health(version, commit, api)), None);
        assert!(matches!(
            core_mismatch(&health(version, "0123456789ab", api)),
            Some(CoreMismatch::Commit(_))
        ));
        assert!(matches!(
            core_mismatch(&health("9.9.9", commit, api)),
            Some(CoreMismatch::Incompatible(_))
        ));
        assert!(matches!(
            core_mismatch(&health(version, commit, "99.0.0")),
            Some(CoreMismatch::Incompatible(_))
        ));
        assert!(matches!(
            core_mismatch(&health(version, "0123456789ab", "")),
            Some(CoreMismatch::Incompatible(_))
        ));
    }
}
