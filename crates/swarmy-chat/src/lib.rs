#[cfg(feature = "terminal")]
pub mod client_chat;
pub mod client_conversation;
#[cfg(feature = "terminal")]
mod input;
mod api_client {
    use anyhow::{Context, Result};
    pub fn endpoint() -> Result<String> {
        let settings = swarmy_config::Settings::load()?.settings;
        anyhow::ensure!(
            !settings.api.token.is_empty(),
            "no [api] token configured; run swarmy dev up"
        );
        Ok(settings
            .api
            .url
            .unwrap_or_else(|| format!("http://{}", settings.api.listen)))
    }
    pub async fn call<T>(
        endpoint: &str,
        future: impl std::future::Future<Output = Result<T, swarmy_client::Error>>,
    ) -> Result<T> {
        tokio::time::timeout(std::time::Duration::from_secs(10), future)
            .await
            .with_context(|| format!("API at {endpoint}: request timed out"))?
            .with_context(|| format!("API at {endpoint}"))
    }
}
