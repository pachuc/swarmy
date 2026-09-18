use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
}

impl ReasoningEffort {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
        }
    }
}

impl std::fmt::Display for ReasoningEffort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("reasoning effort must be one of: none, minimal, low, medium, high, xhigh")]
pub struct InvalidReasoningEffort;

impl std::str::FromStr for ReasoningEffort {
    type Err = InvalidReasoningEffort;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "minimal" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" => Ok(Self::Xhigh),
            _ => Err(InvalidReasoningEffort),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effort_names_match_the_config_contract() {
        for value in ["none", "minimal", "low", "medium", "high", "xhigh"] {
            let effort: ReasoningEffort = value.parse().unwrap();
            assert_eq!(effort.to_string(), value);
            crate::encoding::tests::assert_round_trip(&effort);
        }
        for value in ["", "HIGH", " high", "high ", "ultra", "unknown"] {
            assert!(value.parse::<ReasoningEffort>().is_err());
        }
    }
}
