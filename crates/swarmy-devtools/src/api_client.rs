//! Resolve one API endpoint for local or selected remote commands.
use anyhow::{Context, Result};
use swarmy_client::Client;

pub fn endpoint() -> Result<String> {
    let settings = swarmy_config::Settings::load()?.settings;
    anyhow::ensure!(
        !settings.api.token.is_empty(),
        "no [api] token configured; run swarmy dev up"
    );
    Ok(settings
        .api
        .url
        .clone()
        .unwrap_or_else(|| format!("http://{}", settings.api.listen)))
}

pub fn connect() -> Result<(Client, String)> {
    let endpoint = endpoint()?;
    let token = swarmy_config::Settings::load()?.settings.api.token;
    let client = Client::new(&endpoint, token)
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
    call_with_timeout(endpoint, std::time::Duration::from_secs(10), future).await
}

/// Call the API with an explicit timeout. Image uploads use
/// [`swarmy_client::upload_timeout`], sized from the body on disk, because
/// the server chunks and stores the whole image before answering.
pub async fn call_with_timeout<T>(
    endpoint: &str,
    timeout: std::time::Duration,
    future: impl std::future::Future<Output = Result<T, swarmy_client::Error>>,
) -> Result<T> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| anyhow::anyhow!("API at {endpoint}: request timed out"))?
        .map_err(|error| api_error(&error, endpoint))
}

/// Upload a file with a timeout proportional to its size on disk.
pub async fn call_upload<T>(
    endpoint: &str,
    file: &std::path::Path,
    future: impl std::future::Future<Output = Result<T, swarmy_client::Error>>,
) -> Result<T> {
    let size = std::fs::metadata(file)
        .with_context(|| format!("reading upload size for {}", file.display()))?
        .len();
    call_with_timeout(endpoint, swarmy_client::upload_timeout(size), future).await
}
