//! Settings built from the test environment instead of the host config file.

use std::collections::BTreeMap;

/// Build settings from the standard stack variables over defaults.
///
/// Root-adjacent suites (node, volume, image acceptance) run against the
/// same stack: cluster file, store directory, bus, object storage, and the
/// suite-provided API endpoint. One list instead of a repeated
/// eleven-variable literal in every test.
///
/// The API URL and token come from the root-suite harness, which starts an
/// API and exports `SWARMY_API_URL`/`SWARMY_API_TOKEN` (see
/// `scripts/node-suites/root-suites.sh`); tests that shell out to the CLI
/// inherit them through `settings.environment()`. Use
/// [`require_api_endpoint`] at those call sites so a missing endpoint fails
/// here with the suite pointer instead of the CLI's `no token configured`
/// error two layers down.
#[must_use]
pub fn stack_settings() -> swarmy_config::Settings {
    test_settings(&[
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_STORE_DIRECTORY",
        "SWARMY_NATS_URL",
        "SWARMY_S3_ENDPOINT",
        "SWARMY_S3_ACCESS_KEY",
        "SWARMY_S3_SECRET_KEY",
        "SWARMY_S3_BUCKET",
        "SWARMY_S3_PREFIX",
        "SWARMY_S3_REGION",
        "SWARMY_API_URL",
        "SWARMY_API_TOKEN",
    ])
}

/// Return the suite API endpoint from settings built by [`stack_settings`].
///
/// # Panics
/// Panics when the suite did not export `SWARMY_API_URL`/`SWARMY_API_TOKEN`
/// (the root-suite harness does; a bare local run does not). Fixture setup
/// has no recovery, and failing here names the missing piece instead of
/// surfacing the CLI's `no token configured` error after the spawn.
#[must_use]
pub fn require_api_endpoint(settings: &swarmy_config::Settings) -> (&str, &str) {
    let url = settings.api.url.as_deref().unwrap_or("");
    assert!(
        !url.is_empty() && !settings.api.token.is_empty(),
        "root tests that call the CLI need the suite API: run under scripts/node-suites/root-suites.sh, which exports SWARMY_API_URL and SWARMY_API_TOKEN",
    );
    (url, &settings.api.token)
}

/// Build settings from the named test-environment variables over defaults,
/// without reading any host configuration file.
///
/// Root-adjacent tests used `Settings::load()`, which merges the developer's
/// real configuration: a stray local config could change the store
/// directory, the image registry, or model selection under the test. Tests
/// must not depend on host state, so each call site names exactly the
/// variables its fixture needs (for example `SWARMY_FDB_CLUSTER_FILE` and
/// `SWARMY_STORE_DIRECTORY`) and everything else stays at compiled defaults.
/// Missing variables keep their default; call sites that must skip without
/// the stack keep their existing `require_stack` gate.
///
/// # Panics
/// Panics when a present variable fails validation; fixture setup has no
/// recovery.
#[must_use]
pub fn test_settings(names: &[&str]) -> swarmy_config::Settings {
    let mut settings = swarmy_config::Settings::default();
    let environment: BTreeMap<String, String> = names
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| ((*name).to_owned(), value))
        })
        .collect();
    settings
        .apply_environment(&environment)
        .expect("test variables must validate");
    settings
}
