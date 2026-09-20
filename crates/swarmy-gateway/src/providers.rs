use std::{collections::BTreeMap, sync::Arc};

use swarmy_config::Settings;
use swarmy_llm::{
    ClientAuth, Provider,
    auth::{ResolvedAuth, Resolver},
    catalog::{Api, Catalog, ProviderInfo},
};
use swarmy_store::Store;
use tokio::sync::Mutex;

use crate::{config::FileFake, credentials::ClusterCredentials};

type ClientKey = (String, String, [u8; 32]);

pub struct Providers {
    pub catalog: Catalog,
    pub served: Vec<String>,
    pub skipped: BTreeMap<String, String>,
    resolver: Result<Resolver, &'static str>,
    scripted: Result<Arc<dyn Provider>, &'static str>,
    clients: Mutex<BTreeMap<ClientKey, Arc<dyn Provider>>>,
}

/// Filter the catalog with the configured subset and resolver outcomes.
#[must_use]
pub fn provider_set(
    catalog: &Catalog,
    selected: Option<&[String]>,
    mut resolve: impl FnMut(&ProviderInfo) -> Result<(), String>,
) -> (Vec<String>, BTreeMap<String, String>) {
    let mut served = Vec::new();
    let mut skipped = BTreeMap::new();
    for provider in catalog.providers() {
        let result = if selected.is_some_and(|ids| !ids.contains(&provider.id)) {
            Err("excluded by providers setting".into())
        } else {
            resolve(provider)
        };
        match result {
            Ok(()) => served.push(provider.id.clone()),
            Err(reason) => {
                skipped.insert(provider.id.clone(), reason);
            }
        }
    }
    for id in selected.into_iter().flatten() {
        if catalog.provider(id).is_none() {
            skipped.insert(id.clone(), "unknown provider".into());
        }
    }
    (served, skipped)
}

impl Providers {
    /// Discover providers without constructing protocol clients or calling providers.
    /// Unavailable credentials and scripts are recorded in `skipped`.
    pub async fn discover(store: Store, settings: &Settings) -> Self {
        let catalog = settings.catalog();
        let resolver = match ClusterCredentials::new(store).await {
            Ok(credentials) => Resolver::new(Arc::new(credentials))
                .map_err(|_| "credential resolver cannot configure its HTTP client"),
            Err(error) => {
                tracing::warn!(%error, "cluster credential store unavailable");
                Err("cluster credential store unavailable; check keyring and database")
            }
        };
        let scripted = if std::path::Path::new(&settings.fake.script).is_file() {
            FileFake::from_settings(settings)
                .map(|provider| Arc::new(provider) as Arc<dyn Provider>)
                .map_err(|_| "fake script is unreadable or invalid")
        } else {
            Err("fake script is absent")
        };
        let mut result = Self {
            catalog,
            served: Vec::new(),
            skipped: BTreeMap::new(),
            resolver,
            scripted,
            clients: Mutex::new(BTreeMap::new()),
        };
        let mut resolutions = BTreeMap::new();
        for provider in result.catalog.providers() {
            if settings
                .providers
                .as_ref()
                .is_none_or(|ids| ids.contains(&provider.id))
            {
                resolutions.insert(
                    provider.id.clone(),
                    result
                        .auth(provider)
                        .await
                        .map(|_| ())
                        .map_err(|error| error.to_string()),
                );
            }
        }
        (result.served, result.skipped) =
            provider_set(&result.catalog, settings.providers.as_deref(), |provider| {
                resolutions
                    .remove(&provider.id)
                    .unwrap_or_else(|| Err("credential unavailable".into()))
            });
        result
    }

    async fn auth(&self, provider: &ProviderInfo) -> Result<ResolvedAuth, swarmy_llm::Error> {
        if provider.api == Api::Fake {
            return self
                .scripted
                .clone()
                .map(|client| ResolvedAuth {
                    auth: ClientAuth::Scripted(client),
                    version: [0; 32],
                })
                .map_err(swarmy_llm::Error::Credentials);
        }
        self.resolver
            .as_ref()
            .map_err(|reason| swarmy_llm::Error::Credentials(reason))?
            .resolve(&provider.id)
            .await
    }

    /// Resolve the current credential version before reusing a client.
    /// # Errors
    /// Returns credential failures or an unsupported protocol.
    pub async fn client(
        &self,
        provider: &str,
        model: &swarmy_llm::catalog::ModelInfo,
    ) -> Result<Arc<dyn Provider>, swarmy_llm::Error> {
        let info = self
            .catalog
            .provider(provider)
            .filter(|_| self.served.iter().any(|id| id == provider))
            .ok_or(swarmy_llm::Error::Credentials(
                "provider is not served by this gateway",
            ))?;
        let resolved = self.auth(info).await?;
        let key = (provider.to_owned(), model.id.clone(), resolved.version);
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }
        let client = swarmy_llm::client_for(info, model, resolved.auth)?;
        // Retire obsolete credential versions without retaining their secrets indefinitely.
        clients.retain(|(id, name, _), _| id != provider || name != &model.id);
        clients.insert(key, client.clone());
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersects_catalog_credentials_and_explicit_subset() {
        let catalog = Catalog::get();
        let resolver = |provider: &ProviderInfo| {
            if matches!(provider.id.as_str(), "openai" | "anthropic") {
                Ok(())
            } else {
                Err("missing key".into())
            }
        };
        let (served, skipped) = provider_set(catalog, None, resolver);
        assert_eq!(served, ["anthropic", "openai"]);
        assert_eq!(skipped["chatgpt"], "missing key");
        let (served, skipped) = provider_set(
            catalog,
            Some(&["openai".into(), "missing".into()]),
            resolver,
        );
        assert_eq!(served, ["openai"]);
        assert_eq!(skipped["anthropic"], "excluded by providers setting");
        assert_eq!(skipped["missing"], "unknown provider");
    }
}
