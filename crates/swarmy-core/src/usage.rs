use serde::{Deserialize, Serialize};

/// Input includes cache reads and writes; output includes reasoning.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
    #[serde(default, with = "crate::trailing")]
    pub cache_write_input_tokens: u64,
}

/// Durable billed totals. Saturating addition keeps diagnostics available on overflow.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageTotals {
    pub usage: TokenUsage,
    pub cost_micros: u64,
}

impl UsageTotals {
    pub fn add(&mut self, usage: &TokenUsage, cost_micros: u64) {
        self.cost_micros = self.cost_micros.saturating_add(cost_micros);
        self.usage.input_tokens = self.usage.input_tokens.saturating_add(usage.input_tokens);
        self.usage.cached_input_tokens = self
            .usage
            .cached_input_tokens
            .saturating_add(usage.cached_input_tokens);
        self.usage.cache_write_input_tokens = self
            .usage
            .cache_write_input_tokens
            .saturating_add(usage.cache_write_input_tokens);
        self.usage.output_tokens = self.usage.output_tokens.saturating_add(usage.output_tokens);
        self.usage.reasoning_output_tokens = self
            .usage
            .reasoning_output_tokens
            .saturating_add(usage.reasoning_output_tokens);
        self.usage.total_tokens = self.usage.total_tokens.saturating_add(usage.total_tokens);
    }

    /// Dollars rounded to four decimal places without losing integer precision.
    #[must_use]
    pub fn dollars(&self) -> String {
        let units = self.cost_micros / 100 + u64::from(self.cost_micros % 100 >= 50);
        format!("{}.{:04}", units / 10_000, units % 10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_usage_decodes_and_truncated_new_usage_is_rejected() {
        #[derive(Serialize)]
        struct LegacyUsage {
            input: u64,
            cached: u64,
            output: u64,
            reasoning: u64,
            total: u64,
        }
        let bytes = crate::encode(&LegacyUsage {
            input: 10,
            cached: 2,
            output: 5,
            reasoning: 1,
            total: 15,
        })
        .unwrap();
        let usage: TokenUsage = crate::decode(&bytes).unwrap();
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.cache_write_input_tokens, 0);
        let mut bytes = crate::encode(&usage).unwrap();
        bytes.pop();
        assert!(crate::decode::<TokenUsage>(&bytes).is_err());
    }
}
