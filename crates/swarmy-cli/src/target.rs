//! The API every client command talks to: one resolution, set once per process.
use anyhow::anyhow;
use std::sync::OnceLock;
use swarmy_client::api_client::{self, Api};

pub(crate) fn xdg_environment() -> std::collections::BTreeMap<String, String> {
    swarmy_config::paths::XDG_KEYS
        .into_iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
        .collect()
}

static FLAG: OnceLock<Option<String>> = OnceLock::new();

pub(crate) fn select(flag: Option<String>) -> anyhow::Result<()> {
    FLAG.set(flag)
        .map_err(|_| anyhow!("the target swarm was selected twice"))
}
pub(crate) fn flag() -> Option<&'static str> {
    FLAG.get().and_then(Option::as_deref)
}
pub(crate) fn resolve() -> Result<Api, swarmy_client::Error> {
    api_client::resolve(flag())
}
/// Connect to a named swarm independently of the global --swarm flag.
pub(crate) async fn connect_swarm(name: &str) -> anyhow::Result<(swarmy_client::Client, String)> {
    Ok(swarmy_cloud::tunnel::connect(Some(name)).await?)
}
pub(crate) async fn connect() -> anyhow::Result<(swarmy_client::Client, String)> {
    Ok(swarmy_cloud::tunnel::connect(flag()).await?)
}
pub(crate) async fn connect_with(
    explicit: Option<(&str, &str)>,
) -> anyhow::Result<(swarmy_client::Client, String)> {
    match explicit {
        Some((url, token)) => Ok(api_client::connect_to(url, token.to_owned())?),
        None => connect().await,
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    #[test]
    fn flag_starts_unselected() {
        assert_eq!(flag(), None);
    }
}
