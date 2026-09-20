//! Generated provider metadata. Builds and catalog lookups never use the network.

use std::{collections::BTreeMap, sync::OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use swarmy_core::ReasoningEffort;

/// Wire protocol selected by a provider or an individual model override.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Api {
    AnthropicMessages,
    OpenAiResponses,
    OpenAiCodexResponses,
    OpenAiCompletions,
    GoogleGenerativeAi,
    GoogleVertex,
    BedrockConverse,
    Fake,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderInfo {
    pub id: String,
    pub name: String,
    pub api: Api,
    /// Empty for endpoints that depend on a cloud resource or region.
    pub base_url: String,
    pub env_keys: Vec<String>,
    pub auth_kinds: Vec<String>,
    pub models: BTreeMap<String, ModelInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub name: String,
    pub family: Option<String>,
    /// Mixed-protocol providers such as `OpenRouter` override their default here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<Api>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    pub reasoning: Option<ReasoningOptions>,
    pub tool_call: bool,
    pub attachment: bool,
    pub input_modalities: Vec<String>,
    pub limit: Limit,
    pub cost: Cost,
    pub release_date: Option<String>,
    pub status: Option<String>,
    #[serde(default)]
    pub compat: Compat,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningOptions {
    Effort(Vec<ReasoningEffort>),
    BudgetTokens { min: Option<u64>, max: Option<u64> },
    Toggle,
}

/// Dollars per million tokens. Cache prices default to zero when unreported.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Cost {
    pub input: f64,
    pub output: f64,
    #[serde(default)]
    pub cache_read: f64,
    #[serde(default)]
    pub cache_write: f64,
    #[serde(default)]
    pub tiers: Vec<CostTier>,
}

/// Prices used when total input tokens exceed this threshold, including caches.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CostTier {
    pub input_tokens_above: u64,
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limit {
    pub context: u64,
    /// None means the source does not report a completion limit.
    pub output: Option<u64>,
}

/// An open quirk table. Accessors distinguish missing flags from explicit false.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Compat(pub BTreeMap<String, Value>);

impl Compat {
    #[must_use]
    pub fn max_tokens_field(&self) -> Option<&str> {
        self.0.get("max_tokens_field").and_then(Value::as_str)
    }

    #[must_use]
    pub fn supports_developer_role(&self) -> Option<bool> {
        self.0
            .get("supports_developer_role")
            .and_then(Value::as_bool)
    }

    #[must_use]
    pub fn thinking_format(&self) -> Option<&str> {
        self.0.get("thinking_format").and_then(Value::as_str)
    }

    #[must_use]
    pub fn cache_control_format(&self) -> Option<&str> {
        self.0.get("cache_control_format").and_then(Value::as_str)
    }

    #[must_use]
    pub fn supports_strict_mode(&self) -> Option<bool> {
        self.0.get("supports_strict_mode").and_then(Value::as_bool)
    }

    #[must_use]
    pub fn force_adaptive_thinking(&self) -> Option<bool> {
        self.0
            .get("force_adaptive_thinking")
            .and_then(Value::as_bool)
    }

    #[must_use]
    pub fn supports_temperature(&self) -> Option<bool> {
        self.0.get("supports_temperature").and_then(Value::as_bool)
    }

    #[must_use]
    pub fn supports_long_cache_retention(&self) -> Option<bool> {
        self.0
            .get("supports_long_cache_retention")
            .and_then(Value::as_bool)
    }
}

const EFFORTS: [ReasoningEffort; 7] = [
    ReasoningEffort::None,
    ReasoningEffort::Minimal,
    ReasoningEffort::Low,
    ReasoningEffort::Medium,
    ReasoningEffort::High,
    ReasoningEffort::Xhigh,
    ReasoningEffort::Max,
];

impl ModelInfo {
    /// Efforts in ascending order. Budgets and toggles expose the ordinary
    /// scale; xhigh and max require an explicit entry in an effort list.
    #[must_use]
    pub fn supported_efforts(&self) -> Vec<ReasoningEffort> {
        match &self.reasoning {
            None => vec![ReasoningEffort::None],
            Some(ReasoningOptions::Effort(efforts)) => EFFORTS
                .into_iter()
                .filter(|effort| efforts.contains(effort))
                .collect(),
            Some(ReasoningOptions::BudgetTokens { .. } | ReasoningOptions::Toggle) => {
                EFFORTS[..5].to_vec()
            }
        }
    }

    /// Return the requested effort if supported, otherwise the next higher
    /// supported effort, then the next lower. The bool reports a change.
    #[must_use]
    pub fn clamp_effort(&self, requested: ReasoningEffort) -> (ReasoningEffort, bool) {
        let supported = self.supported_efforts();
        let index = EFFORTS
            .iter()
            .position(|&effort| effort == requested)
            .unwrap_or(0);
        let effort = EFFORTS[index..]
            .iter()
            .chain(EFFORTS[..index].iter().rev())
            .find(|effort| supported.contains(effort))
            .copied()
            .unwrap_or(ReasoningEffort::None);
        (effort, effort != requested)
    }
}

const PROVIDER_JSON: [&str; 12] = [
    include_str!("../catalog/anthropic.json"),
    include_str!("../catalog/openai.json"),
    include_str!("../catalog/chatgpt.json"),
    include_str!("../catalog/xai.json"),
    include_str!("../catalog/meta.json"),
    include_str!("../catalog/openrouter.json"),
    include_str!("../catalog/azure.json"),
    include_str!("../catalog/amazon-bedrock.json"),
    include_str!("../catalog/google.json"),
    include_str!("../catalog/google-vertex.json"),
    include_str!("../catalog/google-vertex-anthropic.json"),
    include_str!("../catalog/fake.json"),
];

#[derive(Debug)]
pub struct Catalog {
    providers: BTreeMap<String, ProviderInfo>,
}

impl Catalog {
    /// Build an owned catalog from provider metadata, ordered by provider id.
    #[must_use]
    pub fn from_providers(providers: impl IntoIterator<Item = ProviderInfo>) -> Self {
        Self {
            providers: providers
                .into_iter()
                .map(|provider| (provider.id.clone(), provider))
                .collect(),
        }
    }

    /// The immutable snapshot is parsed once for the lifetime of the process.
    ///
    /// # Panics
    /// Panics if a checked-in provider file is invalid. Tests validate every file.
    #[must_use]
    pub fn get() -> &'static Self {
        static CATALOG: OnceLock<Catalog> = OnceLock::new();
        CATALOG.get_or_init(|| Self {
            providers: PROVIDER_JSON
                .iter()
                .map(|json| {
                    let provider: ProviderInfo = serde_json::from_str(json)
                        .expect("embedded provider catalog must be valid");
                    (provider.id.clone(), provider)
                })
                .collect(),
        })
    }

    pub fn providers(&self) -> impl Iterator<Item = &ProviderInfo> {
        self.providers.values()
    }

    #[must_use]
    pub fn provider(&self, id: &str) -> Option<&ProviderInfo> {
        self.providers.get(id)
    }

    #[must_use]
    pub fn model(&self, provider: &str, id: &str) -> Option<&ModelInfo> {
        self.provider(provider)?.models.get(id)
    }

    /// Case-insensitive substring matches over `provider/model`, in stable order.
    #[must_use]
    pub fn find(&self, pattern: &str) -> Vec<(&ProviderInfo, &ModelInfo)> {
        let pattern = pattern.to_lowercase();
        self.providers()
            .flat_map(|provider| provider.models.values().map(move |model| (provider, model)))
            .filter(|(provider, model)| {
                format!("{}/{}", provider.id, model.id)
                    .to_lowercase()
                    .contains(&pattern)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_catalog_file_parses_and_is_embedded() {
        let catalog = Catalog::get();
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("catalog");
        let mut count = 0;
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            let json = std::fs::read_to_string(&path).unwrap();
            if path.file_name().unwrap() == "manifest.json" {
                let manifest: Value = serde_json::from_str(&json).unwrap();
                assert!(manifest["generated_at"].as_str().is_some());
                continue;
            }
            let provider: ProviderInfo = serde_json::from_str(&json).unwrap();
            assert_eq!(catalog.provider(&provider.id), Some(&provider));
            for (id, model) in &provider.models {
                assert_eq!(id, &model.id);
                assert!(model.tool_call);
                assert!(!model.supported_efforts().is_empty());
            }
            count += 1;
        }
        assert_eq!(count, catalog.providers().count());
        assert!(std::ptr::eq(catalog, Catalog::get()));
        assert!(catalog.provider("missing").is_none());
        assert!(catalog.model("anthropic", "missing").is_none());
    }

    #[test]
    fn catalog_efforts_and_quirks_match_known_models() {
        let catalog = Catalog::get();
        let sonnet = catalog.model("anthropic", "claude-sonnet-4-6").unwrap();
        assert_eq!(sonnet.compat.force_adaptive_thinking(), Some(true));
        assert!(sonnet.supported_efforts().contains(&ReasoningEffort::Max));
        let gpt = catalog.model("openai", "gpt-5.5").unwrap();
        assert_eq!(
            gpt.clamp_effort(ReasoningEffort::Max),
            (ReasoningEffort::Xhigh, true)
        );
        assert_eq!(
            gpt.clamp_effort(ReasoningEffort::None),
            (ReasoningEffort::None, false)
        );
        let ordinary = catalog
            .provider("openrouter")
            .unwrap()
            .models
            .values()
            .find(|model| model.reasoning.is_none())
            .unwrap();
        for requested in EFFORTS {
            assert_eq!(
                ordinary.clamp_effort(requested),
                (ReasoningEffort::None, requested != ReasoningEffort::None)
            );
        }
        let matches = catalog.find("SoNnEt");
        for id in ["anthropic", "openrouter"] {
            assert!(matches.iter().any(|(provider, _)| provider.id == id));
        }
        assert_eq!(
            catalog.find("OPENAI/GPT-5.5").len(),
            catalog.find("openai/gpt-5.5").len()
        );
    }

    #[test]
    fn effort_clamping_prefers_higher_then_lower_and_requires_explicit_extremes() {
        let mut model = Catalog::get().model("openai", "gpt-5.5").unwrap().clone();
        model.reasoning = Some(ReasoningOptions::Effort(vec![
            ReasoningEffort::High,
            ReasoningEffort::Low,
        ]));
        assert_eq!(
            model.supported_efforts(),
            vec![ReasoningEffort::Low, ReasoningEffort::High]
        );
        assert_eq!(
            model.clamp_effort(ReasoningEffort::Medium),
            (ReasoningEffort::High, true)
        );
        assert_eq!(
            model.clamp_effort(ReasoningEffort::Max),
            (ReasoningEffort::High, true)
        );
        for reasoning in [
            ReasoningOptions::Toggle,
            ReasoningOptions::BudgetTokens {
                min: Some(1024),
                max: None,
            },
        ] {
            model.reasoning = Some(reasoning);
            assert_eq!(model.supported_efforts(), EFFORTS[..5]);
            assert_eq!(
                model.clamp_effort(ReasoningEffort::Xhigh),
                (ReasoningEffort::High, true)
            );
        }
        model.reasoning = Some(ReasoningOptions::Effort(Vec::new()));
        assert_eq!(
            model.clamp_effort(ReasoningEffort::Max),
            (ReasoningEffort::None, true)
        );
    }

    #[test]
    fn compat_preserves_unknown_keys_and_rejects_wrong_accessor_types() {
        let value = serde_json::json!({"future_flag": {"nested": 1}, "supports_temperature": false, "supports_strict_mode": "true"});
        let compat: Compat = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(compat.supports_temperature(), Some(false));
        assert_eq!(compat.supports_strict_mode(), None);
        assert_eq!(compat.thinking_format(), None);
        assert_eq!(serde_json::to_value(compat).unwrap(), value);
    }

    #[test]
    fn max_effort_parses_and_round_trips_through_postcard() {
        let effort: ReasoningEffort = "max".parse().unwrap();
        assert_eq!(effort, ReasoningEffort::Max);
        assert_eq!(effort.to_string(), "max");
        let encoded = swarmy_core::encode(&effort).unwrap();
        assert_eq!(
            swarmy_core::decode::<ReasoningEffort>(&encoded).unwrap(),
            effort
        );
    }
}
