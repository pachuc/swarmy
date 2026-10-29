//! Price copied from a machine group when an elastic machine is requested.
use serde::{Deserialize, Serialize};

/// Hourly machine price in millionths of one currency unit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HourlyPrice {
    /// Millionths of one currency unit per machine-hour.
    pub micros: u64,
    /// ISO 4217 code, three upper-case ASCII letters.
    pub currency: String,
}
impl HourlyPrice {
    /// Whether the currency is a three-letter upper-case ASCII code.
    #[must_use]
    pub fn currency_is_valid(&self) -> bool {
        self.currency.len() == 3 && self.currency.bytes().all(|b| b.is_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn currency_rule_requires_three_uppercase_ascii_letters() {
        for (currency, valid) in [
            ("EUR", true),
            ("USD", true),
            ("eur", false),
            ("EU", false),
            ("EURO", false),
            ("ÉUR", false),
        ] {
            assert_eq!(
                HourlyPrice {
                    micros: 1,
                    currency: currency.into()
                }
                .currency_is_valid(),
                valid
            );
        }
    }
}
