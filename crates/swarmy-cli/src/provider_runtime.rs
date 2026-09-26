use std::{path::PathBuf, sync::Arc};
use swarmy_core::CredentialRecord;
use swarmy_llm::{
    Error,
    auth::{AuthStore, Credentials, Login},
};

/// File credentials for providers that keep a local login file. Everything
/// else resolves from provider environment and ambient cloud chains through
/// the resolver. Cluster credentials stay on the control plane: the client
/// never opens the credential store directly.
struct FileAuthStore {
    path: PathBuf,
}

#[async_trait::async_trait]
impl AuthStore for FileAuthStore {
    async fn get(&self, provider: &str) -> Result<Option<CredentialRecord>, Error> {
        if provider != "chatgpt" {
            return Ok(None);
        }
        let bytes = match tokio::fs::read(&self.path).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::Protocol(error.to_string())),
            Ok(bytes) => bytes,
        };
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|error| Error::Protocol(error.to_string()))?;
        Ok(Some(Credentials::from_json(value)?.to_record()?))
    }
    async fn refresh(
        &self,
        provider: &str,
        _: &CredentialRecord,
        _: &dyn Login,
    ) -> Result<CredentialRecord, Error> {
        Err(Error::NeedsLogin(provider.into()))
    }
}

pub fn auth_store(settings: &swarmy_config::Settings) -> Arc<dyn AuthStore> {
    Arc::new(FileAuthStore {
        path: settings.credential_file.clone().into(),
    })
}
