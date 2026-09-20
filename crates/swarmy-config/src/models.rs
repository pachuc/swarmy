//! Project-specific additions and overrides for the embedded provider catalog.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use swarmy_core::ReasoningEffort;
use swarmy_llm::catalog::{
    Api, Catalog, Compat, Cost, Limit, ModelInfo, ProviderInfo, ReasoningOptions,
};

use crate::{Error, Settings};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomProvider {
    pub base_url: Option<String>,
    pub api: Option<Api>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomModel {
    pub provider: String,
    pub id: String,
    pub name: Option<String>,
    pub api: Option<Api>,
    pub base_url: Option<String>,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub reasoning: Option<Vec<ReasoningEffort>>,
    pub cost: Option<Cost>,
    pub compat: Option<Compat>,
}

impl Settings {
    /// Merge project metadata over the embedded snapshot. Omitted fields on
    /// existing models keep their snapshot values; later entries take precedence.
    /// Workers, gateways, and model selection should all use this catalog.
    ///
    /// # Errors
    /// Rejects unknown providers, incomplete new providers, and empty identifiers.
    pub fn catalog(&self) -> Result<Catalog, Error> {
        let mut providers: BTreeMap<_, _> = Catalog::get()
            .providers()
            .map(|provider| (provider.id.clone(), provider.clone()))
            .collect();
        for (id, custom) in &self.providers {
            if id.trim().is_empty() || id.contains('/') {
                return Err(Error::Catalog(format!("invalid provider id {id:?}")));
            }
            if let Some(provider) = providers.get_mut(id) {
                if let Some(api) = custom.api {
                    provider.api = api;
                }
                if let Some(url) = &custom.base_url {
                    provider.base_url.clone_from(url);
                }
            } else {
                let api = custom
                    .api
                    .ok_or_else(|| Error::Catalog(format!("new provider {id:?} requires api")))?;
                let base_url = custom
                    .base_url
                    .clone()
                    .filter(|url| !url.is_empty())
                    .ok_or_else(|| {
                        Error::Catalog(format!("new provider {id:?} requires base_url"))
                    })?;
                providers.insert(
                    id.clone(),
                    ProviderInfo {
                        id: id.clone(),
                        name: id.clone(),
                        api,
                        base_url,
                        env_keys: Vec::new(),
                        auth_kinds: vec!["api_key".into()],
                        models: BTreeMap::new(),
                    },
                );
            }
        }
        for custom in &self.models {
            if custom.id.trim().is_empty() {
                return Err(Error::Catalog("model id must not be empty".into()));
            }
            let provider = providers.get_mut(&custom.provider).ok_or_else(|| {
                Error::Catalog(format!(
                    "unknown provider {:?} for model {:?}; declare [providers.{}] with api and base_url",
                    custom.provider, custom.id, custom.provider
                ))
            })?;
            let model = provider
                .models
                .entry(custom.id.clone())
                .or_insert_with(|| ModelInfo {
                    id: custom.id.clone(),
                    name: custom.id.clone(),
                    family: None,
                    api: None,
                    base_url: None,
                    reasoning: None,
                    tool_call: true,
                    attachment: false,
                    input_modalities: vec!["text".into()],
                    limit: Limit {
                        context: 128_000,
                        output: Some(16_384),
                    },
                    cost: Cost::default(),
                    release_date: None,
                    status: None,
                    compat: Compat::default(),
                });
            custom.apply(model);
        }
        Ok(Catalog::from_providers(providers.into_values()))
    }
}

impl CustomModel {
    fn apply(&self, model: &mut ModelInfo) {
        if let Some(name) = &self.name {
            model.name.clone_from(name);
        }
        if let Some(api) = self.api {
            model.api = Some(api);
        }
        if let Some(url) = &self.base_url {
            model.base_url = Some(url.clone());
        }
        if let Some(context) = self.context_window {
            model.limit.context = context;
        }
        if let Some(output) = self.max_output_tokens {
            model.limit.output = Some(output);
        }
        if let Some(efforts) = &self.reasoning {
            model.reasoning = if efforts.is_empty() {
                None
            } else {
                Some(ReasoningOptions::Effort(efforts.clone()))
            };
        }
        if let Some(cost) = &self.cost {
            model.cost.clone_from(cost);
        }
        if let Some(compat) = &self.compat {
            model.compat.0.extend(compat.0.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(config: &str) -> Result<Settings, Error> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        std::fs::write(&path, config).unwrap();
        Settings::read(&path)
    }

    #[test]
    fn custom_provider_and_models_merge_over_snapshot() {
        let settings = read(
            r#"
[providers.private]
api = "OpenAiCompletions"
base_url = "http://localhost:8000/v1"
[providers.openai]
base_url = "https://proxy.example/v1"
[[models]]
provider = "private"
id = "team/model"
name = "Private model"
context_window = 64000
max_output_tokens = 4096
reasoning = ["none", "low", "high"]
cost = { input = 0.5, output = 1.5 }
compat = { max_tokens_field = "max_tokens", supports_developer_role = false }
[[models]]
provider = "openai"
id = "gpt-5.5"
name = "Project GPT"
context_window = 32000
compat = { supports_developer_role = false, private_flag = true }
"#,
        )
        .unwrap();
        let catalog = settings.catalog().unwrap();
        let provider = catalog.provider("private").unwrap();
        assert_eq!(provider.api, Api::OpenAiCompletions);
        assert_eq!(provider.base_url, "http://localhost:8000/v1");
        let model = catalog.model("private", "team/model").unwrap();
        assert_eq!(model.api, None);
        assert_eq!(model.name, "Private model");
        assert_eq!(
            model.limit,
            Limit {
                context: 64000,
                output: Some(4096)
            }
        );
        assert_eq!(
            model.cost,
            Cost {
                input: 0.5,
                output: 1.5,
                ..Cost::default()
            }
        );
        assert_eq!(model.compat.max_tokens_field(), Some("max_tokens"));
        assert_eq!(
            model.supported_efforts(),
            vec![
                ReasoningEffort::None,
                ReasoningEffort::Low,
                ReasoningEffort::High
            ]
        );
        assert_eq!(
            catalog.provider("openai").unwrap().base_url,
            "https://proxy.example/v1"
        );
        let original = Catalog::get().model("openai", "gpt-5.5").unwrap();
        let updated = catalog.model("openai", "gpt-5.5").unwrap();
        assert_eq!(updated.name, "Project GPT");
        assert_eq!(updated.limit.context, 32000);
        assert_eq!(updated.limit.output, original.limit.output);
        assert_eq!(updated.cost, original.cost);
        assert_eq!(updated.reasoning, original.reasoning);
        assert_eq!(updated.compat.supports_developer_role(), Some(false));
        for (key, value) in &original.compat.0 {
            if key != "supports_developer_role" {
                assert_eq!(updated.compat.0.get(key), Some(value));
            }
        }
        assert_eq!(
            catalog.model("anthropic", "claude-sonnet-4-6"),
            Catalog::get().model("anthropic", "claude-sonnet-4-6")
        );
        assert_ne!(original.name, updated.name);
        let round_trip = read(&settings.to_toml().unwrap())
            .unwrap()
            .catalog()
            .unwrap();
        assert_eq!(round_trip.provider("private"), Some(provider));
    }

    #[test]
    fn model_defaults_and_explicit_protocol_override() {
        let settings = read(
            r#"
[[models]]
provider = "openai"
id = "private"
[[models]]
provider = "openrouter"
id = "anthropic/claude-sonnet-4.6"
api = "OpenAiCompletions"
base_url = "https://private.example/v1"
reasoning = []
"#,
        )
        .unwrap();
        let catalog = settings.catalog().unwrap();
        let model = catalog.model("openai", "private").unwrap();
        assert_eq!(model.name, "private");
        assert_eq!(model.limit.context, 128_000);
        assert_eq!(model.limit.output, Some(16_384));
        assert!(model.tool_call);
        assert_eq!(model.cost, Cost::default());
        assert_eq!(model.supported_efforts(), vec![ReasoningEffort::None]);
        let model = catalog
            .model("openrouter", "anthropic/claude-sonnet-4.6")
            .unwrap();
        assert_eq!(model.api, Some(Api::OpenAiCompletions));
        assert_eq!(
            model.base_url.as_deref(),
            Some("https://private.example/v1")
        );
        assert_eq!(model.reasoning, None);
    }

    #[test]
    fn unknown_api_is_a_load_error_with_allowed_values() {
        for config in [
            "[providers.private]\napi = 'invalid'\nbase_url = 'http://localhost'",
            "[[models]]\nprovider = 'openai'\nid = 'private'\napi = 'invalid'",
        ] {
            let error = read(config).err().unwrap().to_string();
            assert!(error.contains("invalid"), "{error}");
            for api in [
                "AnthropicMessages",
                "OpenAiResponses",
                "OpenAiCodexResponses",
                "OpenAiCompletions",
                "GoogleGenerativeAi",
                "GoogleVertex",
                "BedrockConverse",
                "Fake",
            ] {
                assert!(error.contains(api), "{error}");
            }
        }
    }

    #[test]
    fn incomplete_provider_and_unknown_model_provider_fail_loading() {
        for (config, message) in [
            (
                "[providers.private]\nbase_url = 'http://localhost'",
                "requires api",
            ),
            (
                "[providers.private]\napi = 'OpenAiCompletions'",
                "requires base_url",
            ),
            (
                "[[models]]\nprovider = 'missing'\nid = 'model'",
                "unknown provider",
            ),
        ] {
            let error = read(config).err().unwrap().to_string();
            assert!(error.contains(message), "{error}");
        }
    }
}
