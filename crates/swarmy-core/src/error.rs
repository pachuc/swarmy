//! Rendering an error with its full cause chain.
//!
//! A variant with `#[source]` must not also print the source in its message;
//! the chain here supplies the causes. Services log this rendering in an
//! `error` field wherever they keep an error instead of returning it, so a
//! corruption report says what actually broke instead of only showing the
//! top-level message.

/// Render an error followed by its `source()` chain, as anyhow's `{:#}`
/// would: each cause is appended after `": "`.
///
/// `anyhow::Error` does not implement `std::error::Error`; dereference it
/// first (`error_chain(&*error)`), which renders its whole context chain.
#[must_use]
pub fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut next = error.source();
    while let Some(source) = next {
        out.push_str(": ");
        out.push_str(&source.to_string());
        next = source.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::error_chain;

    #[derive(Debug, thiserror::Error)]
    #[error("outer failed")]
    struct Outer {
        #[source]
        source: std::io::Error,
    }

    #[test]
    fn renders_every_cause_in_order() {
        let error = Outer {
            source: std::io::Error::other("entropy pool is empty"),
        };
        assert_eq!(error_chain(&error), "outer failed: entropy pool is empty");
        let lone = std::io::Error::other("no causes here");
        assert_eq!(error_chain(&lone), "no causes here");
    }
}
