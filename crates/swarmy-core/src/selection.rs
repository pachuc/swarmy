use crate::ReasoningEffort;
use serde::{Deserialize, Serialize};

/// Unset fields inherit the next layer of inference defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceSelection {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSelection {
    pub provider: String,
    pub model: String,
    pub effort: ReasoningEffort,
}

impl InferenceSelection {
    #[must_use]
    pub fn resolve(&self, defaults: &ResolvedSelection) -> ResolvedSelection {
        ResolvedSelection {
            provider: self
                .provider
                .clone()
                .unwrap_or_else(|| defaults.provider.clone()),
            model: self.model.clone().unwrap_or_else(|| defaults.model.clone()),
            effort: self.effort.unwrap_or(defaults.effort),
        }
    }
}

/// Fields that an agent update explicitly returns to stack defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InferenceField {
    Provider,
    Model,
    Effort,
    Route,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resolves_each_layer_independently() {
        let defaults = ResolvedSelection {
            provider: "fake".into(),
            model: "base".into(),
            effort: ReasoningEffort::Medium,
        };
        assert_eq!(InferenceSelection::default().resolve(&defaults), defaults);
        let agent = InferenceSelection {
            provider: Some("openai".into()),
            model: Some("gpt-5.5".into()),
            effort: None,
        }
        .resolve(&defaults);
        let session = InferenceSelection {
            effort: Some(ReasoningEffort::Max),
            ..InferenceSelection::default()
        }
        .resolve(&agent);
        assert_eq!(
            session,
            ResolvedSelection {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                effort: ReasoningEffort::Max
            }
        );
    }
}
