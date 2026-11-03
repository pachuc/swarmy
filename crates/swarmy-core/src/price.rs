//! Price copied from a machine group when an elastic machine is requested.
use serde::{Deserialize, Serialize};
use std::fmt;

/// Hourly machine price in millionths of one currency unit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HourlyPrice {
    /// Millionths of one currency unit per machine-hour.
    pub micros: u64,
    /// ISO 4217 code, three upper-case ASCII letters.
    pub currency: String,
}
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct InvalidPrice(&'static str);
impl HourlyPrice {
    /// Parse an exact decimal amount and ISO 4217 currency.
    /// # Errors
    /// Returns the violated decimal or currency rule.
    /// # Panics
    /// Never panics: the fractional length was checked before computing the scale.
    pub fn parse(amount: &str, currency: &str) -> Result<Self, InvalidPrice> {
        const FORMAT: &str =
            "price_per_hour must be a decimal like 0.75 with at most six decimal places";
        let (whole, fraction) = amount
            .split_once('.')
            .map_or((amount, None), |(a, b)| (a, Some(b)));
        if whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || fraction.is_some_and(|f| {
                f.is_empty() || f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit())
            })
        {
            return Err(InvalidPrice(FORMAT));
        }
        let units = whole
            .parse::<u64>()
            .map_err(|_| InvalidPrice("price_per_hour is too large"))?;
        let fractional = fraction.unwrap_or("");
        let fractional = if fractional.is_empty() {
            0
        } else {
            fractional
                .parse::<u64>()
                .map_err(|_| InvalidPrice(FORMAT))?
                * 10_u64.pow(u32::try_from(6 - fractional.len()).expect("fraction length checked"))
        };
        let micros = units
            .checked_mul(1_000_000)
            .and_then(|v| v.checked_add(fractional))
            .ok_or(InvalidPrice("price_per_hour is too large"))?;
        let result = Self {
            micros,
            currency: currency.into(),
        };
        if !result.currency_is_valid() {
            return Err(InvalidPrice(
                "currency must be three uppercase letters like EUR",
            ));
        }
        Ok(result)
    }
    /// Render the shortest decimal amount.
    #[must_use]
    pub fn amount(&self) -> String {
        let whole = self.micros / 1_000_000;
        let fractional = self.micros % 1_000_000;
        if fractional == 0 {
            return whole.to_string();
        }
        format!("{whole}.{fractional:06}")
            .trim_end_matches('0')
            .to_owned()
    }

    /// Whether the currency is a three-letter upper-case ASCII code.
    #[must_use]
    pub fn currency_is_valid(&self) -> bool {
        self.currency.len() == 3 && self.currency.bytes().all(|b| b.is_ascii_uppercase())
    }
}

impl fmt::Display for HourlyPrice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.amount(), self.currency)
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
    #[test]
    fn exact_decimal_prices() {
        for (input, normalized) in [
            ("0.75", "0.75"),
            ("1", "1"),
            ("12.345678", "12.345678"),
            ("0.000001", "0.000001"),
        ] {
            let price = HourlyPrice::parse(input, "EUR").unwrap();
            assert_eq!(price.amount(), normalized);
            assert_eq!(HourlyPrice::parse(&price.amount(), "EUR").unwrap(), price);
        }
        for input in ["", ".5", "1.", "-1", "1.2345678", "1e3"] {
            assert_eq!(
                HourlyPrice::parse(input, "EUR").unwrap_err().to_string(),
                "price_per_hour must be a decimal like 0.75 with at most six decimal places"
            );
        }
        for currency in ["eur", "EU"] {
            assert_eq!(
                HourlyPrice::parse("1", currency).unwrap_err().to_string(),
                "currency must be three uppercase letters like EUR"
            );
        }
        assert_eq!(
            HourlyPrice::parse("18446744073709551616", "EUR")
                .unwrap_err()
                .to_string(),
            "price_per_hour is too large"
        );
    }
}
