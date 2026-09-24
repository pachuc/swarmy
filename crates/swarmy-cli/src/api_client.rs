//! Resolve one API endpoint for local or selected remote commands.
use anyhow::{Context, Result};
use swarmy_client::Client;

pub fn connect() -> Result<(Client, String)> {
    let settings = swarmy_config::Settings::load()?.settings;
    let endpoint = settings
        .api
        .url
        .clone()
        .unwrap_or_else(|| format!("http://{}", settings.api.listen));
    anyhow::ensure!(
        !settings.api.token.is_empty(),
        "no [api] token configured; run swarmy dev up"
    );
    let client = Client::new(&endpoint, settings.api.token)
        .with_context(|| format!("invalid API endpoint {endpoint}"))?;
    Ok((client, endpoint))
}

pub fn api_error(error: &swarmy_client::Error, endpoint: &str) -> anyhow::Error {
    anyhow::anyhow!("API at {endpoint}: {error}")
}

pub async fn call<T>(
    endpoint: &str,
    future: impl std::future::Future<Output = Result<T, swarmy_client::Error>>,
) -> Result<T> {
    tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .map_err(|_| anyhow::anyhow!("API at {endpoint}: request timed out"))?
        .map_err(|error| api_error(&error, endpoint))
}
