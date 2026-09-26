//! Inference routes: named, ordered auth-entry failover chains.
//!
//! A route names the auth entries a session may use, in order. Each step
//! names a provider and an entry label, or `provider/*` for every entry of
//! that provider in creation order, with an optional model override for the
//! step. A one-step route never fails over, which is how an operator pins an
//! agent to one entry such as a subscription.
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Marker for "every entry of this provider" in a route step.
pub const ANY_ENTRY: &str = "*";

/// At most sixteen steps; longer chains hide configuration mistakes.
pub const MAX_ROUTE_STEPS: usize = 16;

/// One failover step: the provider, the entry label (or `*`), and an
/// optional model override applied while this step serves the turn.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteStep {
    pub provider: String,
    pub entry: String,
    #[serde(default)]
    pub model: Option<String>,
}

/// A named failover chain stored in the store and managed with
/// `swarmy auth routes`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteRecord {
    pub name: String,
    pub steps: Vec<RouteStep>,
    pub updated_at: Timestamp,
}

/// One expanded step ready for breaker checks: `label` is `None` only when
/// the provider holds no stored entries, so environment keys, ambient host
/// chains, and the fake provider share the provider's unlabeled record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpandedRouteStep {
    pub provider: String,
    pub label: Option<String>,
    pub model: Option<String>,
}

impl RouteStep {
    /// Whether this step selects every entry of its provider.
    #[must_use]
    pub fn is_any(&self) -> bool {
        self.entry == ANY_ENTRY
    }
}

/// Route names share the volume label rule so they survive shell, TOML, and
/// key encodings unchanged.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

/// Parse one `set` step in `PROVIDER/LABEL` or `PROVIDER/LABEL=MODEL` form.
/// The model may itself contain slashes, as `OpenRouter` ids do; the provider
/// is split at the first slash and the model at the first `=` after it.
/// Labels containing `/` or `=` cannot be addressed explicitly; they remain
/// reachable through `PROVIDER/*`.
///
/// # Errors
/// Returns a message for an empty provider, an empty label, or a step that
/// does not match the expected shape.
pub fn parse_step(text: &str) -> Result<RouteStep, String> {
    let (provider, rest) = text
        .split_once('/')
        .filter(|(provider, rest)| !provider.is_empty() && !rest.is_empty())
        .ok_or_else(|| format!("expected PROVIDER/LABEL[=MODEL], got {text:?}"))?;
    if !provider
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(format!("invalid provider id {provider:?}"));
    }
    let (entry, model) = match rest.split_once('=') {
        Some((entry, model)) if !entry.is_empty() && !model.is_empty() => {
            (entry, Some(model.to_owned()))
        }
        _ => (rest, None),
    };
    if entry.contains('/') || entry.contains('=') {
        return Err(format!(
            "entry labels with '/' or '=' use PROVIDER/*, got {text:?}"
        ));
    }
    Ok(RouteStep {
        provider: provider.to_owned(),
        entry: entry.to_owned(),
        model,
    })
}

/// Validate a route before storing it.
///
/// # Errors
/// Returns a message for a bad name, an empty step list, too many steps, or
/// a step whose provider id or entry label breaks the `set` syntax: labels
/// with `/` or `=` cannot be addressed explicitly and stay reachable through
/// `PROVIDER/*`, so storing them would wedge every turn on a skipped step.
pub fn validate(name: &str, steps: &[RouteStep]) -> Result<(), String> {
    if !valid_name(name) {
        return Err(
            "route names must contain 1-128 ASCII letters, digits, dots, dashes, or underscores"
                .into(),
        );
    }
    if steps.is_empty() {
        return Err("a route needs at least one step".into());
    }
    if steps.len() > MAX_ROUTE_STEPS {
        return Err(format!("a route holds at most {MAX_ROUTE_STEPS} steps"));
    }
    for step in steps {
        if step.provider.is_empty() || step.entry.is_empty() {
            return Err("route steps need a provider and an entry label".into());
        }
        if !step
            .provider
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(format!("invalid provider id {:?}", step.provider));
        }
        if step.entry != ANY_ENTRY && (step.entry.contains('/') || step.entry.contains('=')) {
            return Err(format!(
                "entry labels with '/' or '=' use PROVIDER/*, got {:?}",
                step.entry
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_specs_split_provider_label_and_model() {
        assert_eq!(
            parse_step("chatgpt/default").unwrap(),
            RouteStep {
                provider: "chatgpt".into(),
                entry: "default".into(),
                model: None,
            }
        );
        assert_eq!(
            parse_step("openai/work-key").unwrap(),
            RouteStep {
                provider: "openai".into(),
                entry: "work-key".into(),
                model: None,
            }
        );
        assert_eq!(
            parse_step("azure/*").unwrap(),
            RouteStep {
                provider: "azure".into(),
                entry: ANY_ENTRY.into(),
                model: None,
            }
        );
        assert_eq!(
            parse_step("azure/prod=gpt-5.5").unwrap(),
            RouteStep {
                provider: "azure".into(),
                entry: "prod".into(),
                model: Some("gpt-5.5".into()),
            }
        );
        // OpenRouter model ids contain slashes after the model separator.
        assert_eq!(
            parse_step("openrouter/main=anthropic/claude-sonnet-4.6").unwrap(),
            RouteStep {
                provider: "openrouter".into(),
                entry: "main".into(),
                model: Some("anthropic/claude-sonnet-4.6".into()),
            }
        );
        for bad in ["", "openai", "/label", "openai/", "open ai/x"] {
            assert!(parse_step(bad).is_err(), "rejects {bad:?}");
        }
        // A label with '=' inside the label portion cannot be addressed.
        assert!(parse_step("openai/a=b").is_ok());
        assert!(parse_step("openai/a/b=c").is_err());
    }

    #[test]
    fn routes_need_a_name_and_bounded_steps() {
        let step = RouteStep {
            provider: "openai".into(),
            entry: "default".into(),
            model: None,
        };
        assert!(validate("fallback", std::slice::from_ref(&step)).is_ok());
        assert!(validate("", std::slice::from_ref(&step)).is_err());
        assert!(validate("fallback", &[]).is_err());
        assert!(validate("fallback", &vec![step.clone(); MAX_ROUTE_STEPS + 1]).is_err());
        // Explicit labels must round-trip through the `set` syntax: labels
        // with '/' or '=' stay reachable through PROVIDER/* only.
        for entry in ["a/b", "a=b"] {
            assert!(
                validate(
                    "fallback",
                    std::slice::from_ref(&RouteStep {
                        provider: "openai".into(),
                        entry: entry.into(),
                        model: None,
                    })
                )
                .is_err(),
                "rejects {entry:?}"
            );
        }
        assert!(
            validate(
                "fallback",
                std::slice::from_ref(&RouteStep {
                    provider: "open ai".into(),
                    entry: "default".into(),
                    model: None,
                })
            )
            .is_err()
        );
        assert!(
            crate::decode::<RouteRecord>(
                &crate::encode(&RouteRecord {
                    name: "fallback".into(),
                    steps: vec![step],
                    updated_at: Timestamp::now(),
                })
                .unwrap()
            )
            .is_ok()
        );
    }
}
