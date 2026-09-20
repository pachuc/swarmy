//! Interactive logins and provider credential resolution.
mod azure;
mod legacy;
mod openrouter;
mod resolve;

pub use azure::AzureLogin;
pub use legacy::*;
pub use openrouter::OpenRouterLogin;
pub use resolve::{AuthStore, ResolvedAuth, Resolver, resolve};

use crate::Error;
use async_trait::async_trait;
use swarmy_core::{CredentialKind, CredentialRecord};

/// UI implementations keep terminal operations out of provider code.
#[async_trait]
pub trait LoginUi: Send + Sync {
    /// # Errors
    /// Returns errors when the interaction cannot be displayed.
    async fn notify_url(&self, url: &str) -> Result<(), Error>;
    /// # Errors
    /// Returns errors when the device instructions cannot be displayed.
    async fn notify_device_code(&self, url: &str, code: &str) -> Result<(), Error>;
    /// # Errors
    /// Returns input errors or cancellation.
    async fn prompt_secret(&self, prompt: &str) -> Result<String, Error>;
    /// # Errors
    /// Returns input errors, cancellation, or an invalid choice.
    async fn prompt_choice(&self, prompt: &str, choices: &[&str]) -> Result<usize, Error>;
}

#[async_trait]
pub trait Login: Send + Sync {
    fn provider(&self) -> &str;
    /// # Errors
    /// Returns interaction, provider, or malformed credential errors.
    async fn login(&self, ui: &dyn LoginUi) -> Result<CredentialKind, Error>;
    /// # Errors
    /// Returns provider errors or credentials requiring another login.
    async fn refresh(&self, record: &CredentialKind) -> Result<Option<CredentialKind>, Error>;
}

/// The permitted login registrations. Azure options are supplied by the CLI.
/// # Errors
/// Rejects providers without a permitted interactive login.
pub fn login_for(
    provider: &str,
    resource: Option<&str>,
    scope: Option<&str>,
) -> Result<Box<dyn Login>, Error> {
    match provider {
        "chatgpt" => Ok(Box::new(OAuthClient::new()?)),
        "openrouter" => Ok(Box::new(OpenRouterLogin::new()?)),
        "azure" => Ok(Box::new(AzureLogin::new(
            resource.unwrap_or_default(),
            scope,
        ))),
        _ => Err(Error::Credentials(
            "interactive login is only available for chatgpt, openrouter, and azure",
        )),
    }
}

#[async_trait]
impl Login for OAuthClient {
    fn provider(&self) -> &'static str {
        "chatgpt"
    }

    async fn login(&self, ui: &dyn LoginUi) -> Result<CredentialKind, Error> {
        let code = self.device_code().await?;
        ui.notify_device_code(&code.verification_url, &code.user_code)
            .await?;
        Ok(self.exchange_device_code(code).await?.to_record()?.kind)
    }

    async fn refresh(&self, kind: &CredentialKind) -> Result<Option<CredentialKind>, Error> {
        let record = CredentialRecord {
            kind: kind.clone(),
            updated_at: jiff::Timestamp::now(),
        };
        let credentials = Credentials::from_record(&record)?;
        Ok(Some(
            self.refresh_credentials(credentials)
                .await?
                .to_record()?
                .kind,
        ))
    }
}
