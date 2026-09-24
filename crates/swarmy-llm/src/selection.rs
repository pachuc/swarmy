//! Shared provider and model selection rules for CLI and API clients.
use crate::catalog::Catalog;
use swarmy_core::{InferenceSelection, ResolvedSelection};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SelectionError(pub String);

/// Normalize provider/model shorthand, preserving native `OpenRouter` IDs.
/// # Errors
/// Rejects conflicting provider and model prefixes.
pub fn normalize(
    mut selection: InferenceSelection,
    catalog: &Catalog,
) -> Result<InferenceSelection, SelectionError> {
    if let Some(model) = &selection.model
        && let Some((provider, id)) = model.split_once('/')
    {
        // OpenRouter model ids have their own provider namespace.
        let native = selection.provider.as_deref() == Some("openrouter")
            && provider != "openrouter"
            || selection
                .provider
                .as_deref()
                .is_some_and(|p| catalog.model(p, model).is_some());
        if !native {
            if selection.provider.as_deref().is_some_and(|p| p != provider) {
                return Err(SelectionError(format!(
                    "--provider disagrees with --model prefix {provider}"
                )));
            }
            selection.provider = Some(provider.to_owned());
            selection.model = Some(id.to_owned());
        }
    }
    if selection.provider.as_deref() == Some("openrouter")
        && let Some(model) = &selection.model
        && catalog.model("openrouter", model).is_none()
        && let Some(provider) = catalog.provider("openrouter")
        // Anthropic's direct ids use dashes where OpenRouter uses version dots.
        && let Some(canonical) = provider.models.keys().find(|id| id.replace('.', "-") == *model)
    {
        selection.model = Some(canonical.clone());
    }
    Ok(selection)
}

/// Validate a selection against the catalog and defaults.
/// # Errors
/// Reports an unknown provider/model and suggests nearby choices.
pub fn validate(
    catalog: &Catalog,
    selection: &InferenceSelection,
    defaults: &ResolvedSelection,
) -> Result<(), SelectionError> {
    let resolved = selection.resolve(defaults);
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
    Err(SelectionError(format!(
        "unknown provider/model {}/{}; closest matches: {suggestions}",
        resolved.provider, resolved.model
    )))
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
