//! Resolve stored credentials before host environment and ambient cloud chains.
use std::sync::Arc;

use swarmy_core::{CredentialKind, CredentialRecord, CredentialStatus};

use super::CredentialStore;
use crate::{
    ClientAuth, Error,
    catalog::{Api, ProviderInfo},
};

/// The version is opaque and must never be logged. It changes with credential content.
pub struct ResolvedAuth {
    pub auth: ClientAuth,
    pub version: [u8; 32],
}

/// Resolve authentication using a caller-supplied store record and environment lookup.
/// A stored record owns the provider; invalid records never fall back to environment.
/// # Errors
/// Returns a credential error for absent, invalid, or expired credentials.
pub fn resolve(
    provider: &ProviderInfo,
    record: Option<&CredentialRecord>,
    environment: impl Fn(&str) -> Option<String>,
    chatgpt: Option<Arc<dyn CredentialStore>>,
) -> Result<ResolvedAuth, Error> {
    if let Some(record) = record {
        if record.status(jiff::Timestamp::now()) == CredentialStatus::NeedsLogin {
            return Err(Error::Credentials("stored credential needs login"));
        }
        let auth = if provider.id == "chatgpt" {
            ClientAuth::ChatGpt(
                chatgpt.ok_or(Error::Credentials("ChatGPT credential store unavailable"))?,
            )
        } else {
            match &record.kind {
                CredentialKind::ApiKey { key, .. } => ClientAuth::ApiKey(key.clone()),
                CredentialKind::OAuth { access, .. } => {
                    if provider.id == "anthropic" {
                        return Err(Error::Credentials("Anthropic requires an API key"));
                    }
                    if record.status(jiff::Timestamp::now()) != CredentialStatus::Ready {
                        return Err(Error::Credentials("stored credential expired"));
                    }
                    ClientAuth::Bearer(access.clone())
                }
            }
        };
        return Ok(ResolvedAuth {
            auth,
            version: *blake3::hash(&serde_json::to_vec(record)?).as_bytes(),
        });
    }
    for key in &provider.env_keys {
        // Cloud catalog entries also name project, region, and SDK-chain variables.
        // Those values are configuration, never an API key.
        if !key.ends_with("_API_KEY") && key != "AWS_BEARER_TOKEN_BEDROCK" {
            continue;
        }
        if let Some(value) = environment(key).filter(|value| !value.is_empty()) {
            let version = *blake3::hash(value.as_bytes()).as_bytes();
            let auth = if key == "AWS_BEARER_TOKEN_BEDROCK" {
                ClientAuth::Bearer(value)
            } else {
                ClientAuth::ApiKey(value)
            };
            return Ok(ResolvedAuth { auth, version });
        }
    }
    let auth = if provider.api == Api::Fake {
        ClientAuth::None
    } else if matches!(
        provider.id.as_str(),
        "amazon-bedrock" | "google-vertex" | "google-vertex-anthropic"
    ) {
        ClientAuth::Ambient
    } else if let Some(store) = chatgpt.filter(|_| provider.id == "chatgpt") {
        ClientAuth::ChatGpt(store)
    } else {
        return Err(Error::Credentials("no stored or environment credential"));
    };
    Ok(ResolvedAuth {
        auth,
        version: [0; 32],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;

    #[test]
    fn stored_credentials_own_provider_and_versions_follow_rotation() {
        let provider = Catalog::get().provider("openai").unwrap();
        let record = |key: &str| CredentialRecord {
            kind: CredentialKind::ApiKey {
                key: key.into(),
                extra: std::collections::BTreeMap::new(),
            },
            updated_at: jiff::Timestamp::now(),
        };
        let first = record("stored");
        let resolved =
            resolve(provider, Some(&first), |_| Some("environment".into()), None).unwrap();
        assert!(matches!(resolved.auth, ClientAuth::ApiKey(key) if key == "stored"));
        let second = resolve(provider, Some(&record("rotated")), |_| None, None).unwrap();
        assert_ne!(resolved.version, second.version);
        assert!(
            resolve(
                provider,
                Some(&record("")),
                |_| Some("environment".into()),
                None
            )
            .is_err()
        );
        assert!(
            matches!(resolve(provider, None, |_| Some("environment".into()), None).unwrap().auth, ClientAuth::ApiKey(key) if key == "environment")
        );
    }

    #[test]
    fn cloud_configuration_is_not_sent_as_an_api_key() {
        for id in ["amazon-bedrock", "google-vertex", "google-vertex-anthropic"] {
            let provider = Catalog::get().provider(id).unwrap();
            assert!(matches!(
                resolve(
                    provider,
                    None,
                    |key| (key != "AWS_BEARER_TOKEN_BEDROCK").then(|| "configuration".into()),
                    None
                )
                .unwrap()
                .auth,
                ClientAuth::Ambient
            ));
        }
        let provider = Catalog::get().provider("amazon-bedrock").unwrap();
        assert!(matches!(
            resolve(
                provider,
                None,
                |key| (key == "AWS_BEARER_TOKEN_BEDROCK").then(|| "token".into()),
                None
            )
            .unwrap()
            .auth,
            ClientAuth::Bearer(_)
        ));
    }
}
