use anyhow::{Result, bail, ensure};
use swarmy_core::{InferenceField, InferenceSelection, ResolvedSelection};
use swarmy_llm::catalog::Catalog;

pub fn defaults(settings: &swarmy_config::Settings) -> Result<ResolvedSelection> {
    Ok(ResolvedSelection {
        provider: settings.provider.clone(),
        model: settings.model.clone(),
        effort: settings.reasoning_effort.parse()?,
    })
}

pub fn normalize(mut selection: InferenceSelection) -> Result<InferenceSelection> {
    if let Some(model) = &selection.model
        && let Some((provider, id)) = model.split_once('/')
    {
        // OpenRouter model ids have their own provider namespace.
        let native = selection.provider.as_deref() == Some("openrouter")
            && provider != "openrouter"
            || selection
                .provider
                .as_deref()
                .is_some_and(|p| Catalog::get().model(p, model).is_some());
        if !native {
            ensure!(
                selection.provider.as_deref().is_none_or(|p| p == provider),
                "--provider disagrees with --model prefix {provider}"
            );
            selection.provider = Some(provider.to_owned());
            selection.model = Some(id.to_owned());
        }
    }
    if selection.provider.as_deref() == Some("openrouter")
        && let Some(model) = &selection.model
        && Catalog::get().model("openrouter", model).is_none()
        && let Some(provider) = Catalog::get().provider("openrouter")
        // Anthropic's direct ids use dashes where OpenRouter uses version dots.
        && let Some(canonical) = provider.models.keys().find(|id| id.replace('.', "-") == *model)
    {
        selection.model = Some(canonical.clone());
    }
    Ok(selection)
}

pub fn validate(
    settings: &swarmy_config::Settings,
    selection: &InferenceSelection,
    defaults: &ResolvedSelection,
) -> Result<()> {
    let resolved = selection.resolve(defaults);
    let catalog = settings.catalog()?;
    if catalog.provider(&resolved.provider).is_some()
        && (selection.model.is_none()
            || catalog.model(&resolved.provider, &resolved.model).is_some()
            || (resolved.provider == "fake" && resolved.model == defaults.model))
    {
        return Ok(());
    }
    let pattern = selection.model.as_deref().unwrap_or(&resolved.provider);
    let mut matches = catalog.find(pattern);
    if matches.is_empty() {
        matches = catalog.find(&resolved.provider);
    }
    if matches.is_empty() {
        matches = catalog.find("");
    }
    matches.sort_by_cached_key(|(p, m)| {
        distance(
            &format!("{}/{}", p.id, m.id),
            &format!("{}/{}", resolved.provider, resolved.model),
        )
    });
    let suggestions = matches
        .iter()
        .take(5)
        .map(|(p, m)| format!("{}/{}", p.id, m.id))
        .collect::<Vec<_>>()
        .join(", ");
    bail!(
        "unknown provider/model {}/{}; closest matches: {suggestions}",
        resolved.provider,
        resolved.model
    )
}

fn distance(a: &str, b: &str) -> usize {
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, a) in a.bytes().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, b) in b.bytes().enumerate() {
            let old = row[j + 1];
            row[j + 1] = (row[j] + 1)
                .min(old + 1)
                .min(diagonal + usize::from(a != b));
            diagonal = old;
        }
    }
    row[b.len()]
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
