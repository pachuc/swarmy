//! Catalog prices are dollars per million tokens, equivalent to micros per token.
use swarmy_llm::{TokenUsage, catalog::Cost};

/// Compute the `OpenCode` formula, rounding only the final sum to whole micros.
#[must_use]
// Catalog prices are floating point. Saturating conversion intentionally caps
// extreme totals; token counts above 2^53 already exceed practical request sizes.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub fn cost_micros(cost: &Cost, usage: &TokenUsage) -> u64 {
    let (input, output, read, write) = cost
        .tiers
        .iter()
        .filter(|tier| usage.input_tokens > tier.input_tokens_above)
        .max_by_key(|tier| tier.input_tokens_above)
        .map_or(
            (cost.input, cost.output, cost.cache_read, cost.cache_write),
            |tier| (tier.input, tier.output, tier.cache_read, tier.cache_write),
        );
    let uncached = usage
        .input_tokens
        .saturating_sub(usage.cached_input_tokens)
        .saturating_sub(usage.cache_write_input_tokens);
    let text = usage
        .output_tokens
        .saturating_sub(usage.reasoning_output_tokens);
    (uncached as f64 * input
        + usage.cached_input_tokens as f64 * read
        + usage.cache_write_input_tokens as f64 * write
        + text as f64 * output
        + usage.reasoning_output_tokens as f64 * output)
        .round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_llm::catalog::Catalog;

    #[test]
    fn anthropic_cache_writes_and_reasoning() {
        let cost = &Catalog::get()
            .model("anthropic", "claude-sonnet-4-5")
            .unwrap()
            .cost;
        let usage = TokenUsage {
            input_tokens: 10_000,
            cached_input_tokens: 6_000,
            cache_write_input_tokens: 2_000,
            output_tokens: 1_000,
            reasoning_output_tokens: 400,
            ..Default::default()
        };
        // 2000*3 + 6000*0.3 + 2000*3.75 + (600+400)*15.
        assert_eq!(cost_micros(cost, &usage), 30_300);
    }

    #[test]
    fn openai_tier_uses_total_input_including_cache() {
        let cost = &Catalog::get().model("openai", "gpt-5.4").unwrap().cost;
        let mut usage = TokenUsage {
            input_tokens: 300_000,
            cached_input_tokens: 200_000,
            output_tokens: 2_000,
            reasoning_output_tokens: 500,
            ..Default::default()
        };
        assert_eq!(cost_micros(cost, &usage), 645_000);
        usage.input_tokens = 272_000;
        assert_eq!(cost_micros(cost, &usage), 260_000);
    }

    #[test]
    fn chatgpt_is_zero_cost() {
        let usage = TokenUsage {
            input_tokens: 100_000,
            output_tokens: 50_000,
            ..Default::default()
        };
        for model in Catalog::get().provider("chatgpt").unwrap().models.values() {
            assert_eq!(cost_micros(&model.cost, &usage), 0);
        }
    }
}
