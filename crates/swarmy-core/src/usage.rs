use serde::{Deserialize, Serialize};

/// Input includes cache reads and writes; output includes reasoning.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
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
    fn token_usage_has_fixed_bytes() {
        let usage = TokenUsage {
            input_tokens: 1,
            cached_input_tokens: 2,
            output_tokens: 3,
            reasoning_output_tokens: 4,
            total_tokens: 5,
            cache_write_input_tokens: 6,
        };
        assert_eq!(crate::encode(&usage).unwrap(), [1, 1, 2, 3, 4, 5, 6]);
        assert_eq!(
            crate::decode::<TokenUsage>(&[1, 1, 2, 3, 4, 5, 6]).unwrap(),
            usage
        );
        assert!(crate::decode::<TokenUsage>(&[1, 1, 2, 3, 4, 5]).is_err());
    }
}
