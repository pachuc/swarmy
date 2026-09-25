use std::{collections::BTreeMap, sync::Arc};

use swarmy_config::Settings;
use swarmy_core::CredentialScope;
use swarmy_llm::{
    ClientAuth, Provider,
    auth::{ResolvedAuth, Resolver},
    catalog::{Api, Catalog, ProviderInfo},
};
use swarmy_store::Store;
use tokio::sync::{Mutex, RwLock};

use crate::credentials::ClusterCredentials;

type ClientKey = (String, String, [u8; 32]);

pub struct Providers {
    pub catalog: Catalog,
    state: RwLock<ProviderState>,
    selected: Option<Vec<String>>,
    store: Store,
    resolver: Result<Resolver, &'static str>,
    scripted: Result<Arc<dyn Provider>, &'static str>,
    clients: Mutex<BTreeMap<ClientKey, Arc<dyn Provider>>>,
}

#[derive(Default)]
struct ProviderState {
    served: Vec<String>,
    skipped: BTreeMap<String, String>,
    fingerprints: BTreeMap<String, Option<[u8; 32]>>,
}

pub struct ProviderChanges {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub rotated: Vec<String>,
    pub served: Vec<String>,
    pub skipped: BTreeMap<String, String>,
}

fn changed_providers(
    previous: &BTreeMap<String, Option<[u8; 32]>>,
    current: &BTreeMap<String, Option<[u8; 32]>>,
) -> Vec<String> {
    current
        .iter()
        .filter(|(id, fingerprint)| previous.get(*id) != Some(*fingerprint))
        .map(|(id, _)| id.clone())
        .collect()
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
    /// Build the catalog and resolver without constructing protocol clients.
    /// # Errors
    /// Returns an invalid custom provider or model configuration.
    pub async fn discover(store: Store, settings: &Settings) -> Result<Self, swarmy_config::Error> {
        let catalog = settings.catalog()?;
        let resolver = match ClusterCredentials::new(store.clone()).await {
            Ok(credentials) => Resolver::new(Arc::new(credentials))
                .map_err(|_| "credential resolver cannot configure its HTTP client"),
            Err(error) => {
                tracing::warn!(%error, "cluster credential store unavailable");
                Err("cluster credential store unavailable; check keyring and database")
            }
        };
        let scripted = if std::path::Path::new(&settings.fake.script).is_file() {
            swarmy_llm::fake::FileFake::from_files(
                std::path::Path::new(&settings.fake.script),
                std::path::Path::new(&settings.fake.call_log),
            )
            .map(|provider| Arc::new(provider) as Arc<dyn Provider>)
            .map_err(|_| "fake script is unreadable or invalid")
        } else {
            Err("fake script is absent")
        };
        let result = Self {
            catalog,
            state: RwLock::new(ProviderState::default()),
            selected: settings.providers.clone(),
            store,
            resolver,
            scripted,
            clients: Mutex::new(BTreeMap::new()),
        };
        Ok(result)
    }

    /// Recheck only records whose encrypted value changed since the last tick.
    /// # Errors
    /// Returns database errors without changing the current served set.
    pub async fn refresh(&self) -> Result<ProviderChanges, swarmy_store::StoreError> {
        let mut fingerprints = BTreeMap::new();
        for provider in self.catalog.providers() {
            if self
                .selected
                .as_ref()
                .is_none_or(|ids| ids.contains(&provider.id))
            {
                let fingerprint = if provider.api == Api::Fake {
                    None
                } else {
                    self.store
                        .credential_fingerprint(CredentialScope::Cluster, &provider.id)
                        .await?
                };
                fingerprints.insert(provider.id.clone(), fingerprint);
            }
        }
        let (changed, old, mut skipped) = {
            let state = self.state.read().await;
            (
                changed_providers(&state.fingerprints, &fingerprints),
                state.served.clone(),
                state.skipped.clone(),
            )
        };
        let mut resolutions = BTreeMap::new();
        for id in &changed {
            let Some(provider) = self.catalog.provider(id) else {
                continue;
            };
            resolutions.insert(
                id.clone(),
                self.auth(provider)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
            );
        }
        let mut served = old.clone();
        for id in &changed {
            served.retain(|current| current != id);
            if let Some(result) = resolutions.remove(id) {
                match result {
                    Ok(()) => {
                        served.push(id.clone());
                        skipped.remove(id);
                    }
                    Err(reason) => {
                        skipped.insert(id.clone(), reason);
                    }
                }
            }
        }
        served.sort();
        for provider in self.catalog.providers() {
            if self
                .selected
                .as_ref()
                .is_some_and(|ids| !ids.contains(&provider.id))
            {
                skipped.insert(provider.id.clone(), "excluded by providers setting".into());
            }
        }
        for id in self.selected.iter().flatten() {
            if self.catalog.provider(id).is_none() {
                skipped.insert(id.clone(), "unknown provider".into());
            }
        }
        let added = served
            .iter()
            .filter(|id| !old.contains(id))
            .cloned()
            .collect();
        let removed = old
            .iter()
            .filter(|id| !served.contains(id))
            .cloned()
            .collect();
        let rotated = changed
            .iter()
            .filter(|id| old.contains(id) && served.contains(id))
            .cloned()
            .collect();
        self.clients
            .lock()
            .await
            .retain(|(id, _, _), _| !changed.contains(id));
        let mut state = self.state.write().await;
        state.served.clone_from(&served);
        state.skipped.clone_from(&skipped);
        state.fingerprints = fingerprints;
        Ok(ProviderChanges {
            added,
            removed,
            rotated,
            served,
            skipped,
        })
    }

    async fn auth(&self, provider: &ProviderInfo) -> Result<ResolvedAuth, swarmy_llm::Error> {
        if provider.api == Api::Fake {
            // The scripted client needs no credentials, but stored entries
            // still select the breaker record so one entry's rate limit does
            // not park the provider's other entries.
            let mut entry = None;
            if let Ok(resolver) = &self.resolver {
                entry = resolver.entry_label(&provider.id).await;
            }
            return self
                .scripted
                .clone()
                .map(|client| ResolvedAuth {
                    auth: ClientAuth::Scripted(client),
                    version: [0; 32],
                    entry,
                })
                .map_err(swarmy_llm::Error::Credentials);
        }
        self.resolver
            .as_ref()
            .map_err(|reason| swarmy_llm::Error::Credentials(reason))?
            .resolve(&provider.id)
            .await
    }

    /// Resolve the current credential version before reusing a client. The
    /// returned label identifies the stored entry behind the client, if any.
    /// # Errors
    /// Returns credential failures or an unsupported protocol.
    pub async fn client(
        &self,
        provider: &str,
        model: &swarmy_llm::catalog::ModelInfo,
    ) -> Result<(Arc<dyn Provider>, Option<String>), swarmy_llm::Error> {
        let served = self
            .state
            .read()
            .await
            .served
            .iter()
            .any(|id| id == provider);
        let info = self.catalog.provider(provider).filter(|_| served).ok_or(
            swarmy_llm::Error::Credentials("provider is not served by this gateway"),
        )?;
        let resolved = self.auth(info).await?;
        let key = (provider.to_owned(), model.id.clone(), resolved.version);
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&key) {
            return Ok((client.clone(), resolved.entry));
        }
        let client = swarmy_llm::client_for(info, model, resolved.auth)?;
        // Retire obsolete credential versions without retaining their secrets indefinitely.
        clients.retain(|(id, name, _), _| id != provider || name != &model.id);
        clients.insert(key, client.clone());
        Ok((client, resolved.entry))
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

    #[test]
    fn unchanged_fingerprints_skip_resolution_and_one_change_selects_one_provider() {
        let first = BTreeMap::from([
            ("openai".into(), Some([1; 32])),
            ("openrouter".into(), None),
        ]);
        assert!(changed_providers(&first, &first).is_empty());
        let second = BTreeMap::from([
            ("openai".into(), Some([1; 32])),
            ("openrouter".into(), Some([2; 32])),
        ]);
        assert_eq!(changed_providers(&first, &second), ["openrouter"]);
    }
}
