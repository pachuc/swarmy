use std::{path::Path, sync::Arc, time::Duration};
use swarmy_core::{CredentialRecord, CredentialScope};
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

pub async fn report() -> anyhow::Result<Vec<crate::provider_report::ProviderRow>> {
    let settings = swarmy_config::Settings::load()?.settings;
    let mut rows = crate::provider_report::local(&settings.catalog()?, "absent");
    if !Path::new(&settings.fdb_cluster_file).exists() {
        return Ok(rows);
    }
    let store = open(&settings).await?;
    let keyring = swarmy_config::Keyring::load().ok();
    for row in &mut rows {
        row.store = "absent".into();
        if store
            .has_credential(CredentialScope::Cluster, &row.provider)
            .await?
        {
            row.credential = "store".into();
            row.store = "present".into();
            row.status = "unreadable; check keyring".into();
            if let Some(keyring) = &keyring
                && let Ok(Some(record)) = store
                    .credentials(keyring.clone())
                    .get_credential(CredentialScope::Cluster, &row.provider)
                    .await
            {
                row.status = serde_json::to_value(record.status(jiff::Timestamp::now()))?
                    .as_str()
                    .unwrap_or("unknown")
                    .into();
            }
        }
        row.gateway = "not served".into();
        if let Some(record) = store.gateway_provider(&row.provider).await? {
            row.gateway = if record.expires_at > jiff::Timestamp::now() {
                "served"
            } else {
                "expired"
            }
            .into();
            row.gateway_reason = record.reason;
        }
    }
    Ok(rows)
}
