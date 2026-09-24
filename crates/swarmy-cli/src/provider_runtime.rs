use std::{path::Path, sync::Arc, time::Duration};
use swarmy_core::CredentialRecord;
use swarmy_llm::{
    Error,
    auth::{AuthStore, Login},
};
use swarmy_store::{Store, blob::MemoryBlobStore};

struct EmptyStore;

#[async_trait::async_trait]
impl AuthStore for EmptyStore {
    async fn get(&self, _: &str) -> Result<Option<CredentialRecord>, Error> {
        Ok(None)
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

async fn open(settings: &swarmy_config::Settings) -> anyhow::Result<Store> {
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    Ok(Store::open(
        Some(&settings.fdb_cluster_file),
        Some(&directory),
        Arc::new(MemoryBlobStore::default()),
    )
    .await?)
}

pub async fn auth_store(settings: &swarmy_config::Settings) -> anyhow::Result<Arc<dyn AuthStore>> {
    // An uninitialized local checkout can still probe environment credentials.
    // A configured but unavailable store must never bypass a stored credential.
    if !Path::new(&settings.fdb_cluster_file).exists() {
        return Ok(Arc::new(EmptyStore));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        Ok(Arc::new(
            swarmy_gateway::credentials::ClusterCredentials::new(open(settings).await?).await?,
        ) as Arc<dyn AuthStore>)
    })
    .await
    .map_err(|_| anyhow::anyhow!("credential store timed out"))?
}
