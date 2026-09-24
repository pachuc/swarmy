use anyhow::{Result, ensure};
use swarmy_core::{InferenceField, InferenceSelection, ResolvedSelection};
use swarmy_llm::catalog::Catalog;

pub fn defaults(settings: &swarmy_config::Settings) -> Result<ResolvedSelection> {
    Ok(ResolvedSelection {
        provider: settings.provider.clone(),
        model: settings.model.clone(),
        effort: settings.reasoning_effort.parse()?,
    })
}

pub fn normalize(selection: InferenceSelection) -> Result<InferenceSelection> {
    Ok(swarmy_llm::selection::normalize(selection, Catalog::get())?)
}

pub fn validate(
    settings: &swarmy_config::Settings,
    selection: &InferenceSelection,
    defaults: &ResolvedSelection,
) -> Result<()> {
    Ok(swarmy_llm::selection::validate(
        &settings.catalog()?,
        selection,
        defaults,
    )?)
}

pub fn agent_selection(
    provider: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    update: bool,
) -> Result<(InferenceSelection, Vec<InferenceField>)> {
    let mut resets = Vec::new();
    let mut clear = |value: Option<String>, field| -> Result<Option<String>> {
        if value.as_deref() == Some("default") {
            ensure!(update, "default clears an override only with agent set");
            resets.push(field);
            Ok(None)
        } else {
            Ok(value)
        }
    };
    let provider = clear(provider, InferenceField::Provider)?;
    let model = clear(model, InferenceField::Model)?;
    let effort = clear(effort, InferenceField::Effort)?
        .map(|s| s.parse())
        .transpose()?;
    Ok((
        normalize(InferenceSelection {
            provider,
            model,
            effort,
        })?,
        resets,
    ))
}

pub async fn resolved_session(
    store: &swarmy_store::Store,
    session: &swarmy_core::SessionRecord,
) -> Result<ResolvedSelection> {
    let mut base = defaults(&swarmy_config::Settings::load()?.settings)?;
    if let swarmy_core::SessionKind::Named { agent_id } = session.kind
        && let Some(agent) = store.get_agent(agent_id).await?
    {
        base = agent.inference().resolve(&base);
    }
    Ok(session.inference.resolve(&base))
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::ReasoningEffort;

    #[test]
    fn shorthand_conflicts_and_native_slashes() {
        let select = |provider: Option<&str>, model: &str| {
            normalize(InferenceSelection {
                provider: provider.map(str::to_owned),
                model: Some(model.into()),
                effort: Some(ReasoningEffort::Max),
            })
        };
        let selection = select(None, "openai/gpt-5.5").unwrap();
        assert_eq!(selection.provider.as_deref(), Some("openai"));
        assert_eq!(selection.model.as_deref(), Some("gpt-5.5"));
        assert!(select(Some("anthropic"), "openai/gpt-5.5").is_err());
        let native = select(Some("openrouter"), "anthropic/claude-sonnet-4-6").unwrap();
        assert_eq!(native.provider.as_deref(), Some("openrouter"));
        assert_eq!(native.model.as_deref(), Some("anthropic/claude-sonnet-4.6"));
    }

    #[test]
    fn invalid_model_has_at_most_five_catalog_suggestions() {
        let defaults = ResolvedSelection {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            effort: ReasoningEffort::Medium,
        };
        let err = validate(
            &swarmy_config::Settings::default(),
            &InferenceSelection {
                model: Some("nonexistent".into()),
                ..Default::default()
            },
            &defaults,
        )
        .unwrap_err()
        .to_string();
        let suggestions = err.split("closest matches: ").nth(1).unwrap();
        assert_eq!(suggestions.split(", ").count(), 5);
        assert!(suggestions.contains("openai/"));
    }
}
