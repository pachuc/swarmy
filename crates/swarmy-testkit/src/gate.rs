//! Stack gate for tests that need the dev stack.
//!
//! Each helper does the whole skip decision so call sites stay one line:
//! the value comes back as `Some` when present, the test logs a skip and
//! gets `None` when a developer runs it without the stack, and a missing
//! required setting panics when `CI` is set so a broken stack setup fails
//! the suite instead of silently passing it.

/// Read a required dev-stack setting.
///
/// A value that is present but not valid UTF-8 panics everywhere: that is a
/// broken environment, not a missing stack.
///
/// # Panics
/// Panics when the setting is missing under `CI`, or when it is present but
/// not valid UTF-8.
#[must_use = "check the returned option: missing stack settings skip the test locally"]
pub fn require_stack(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => missing_stack(name),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{name} is set but is not valid UTF-8")
        }
    }
}

/// Read an optional fixture setting such as a root-built test image.
/// Missing values skip even under `CI` because kernel-only suites are not
/// provisioned on hosted runners.
#[must_use = "check the returned option: missing optional settings skip the test"]
pub fn optional_env(name: &str) -> Option<String> {
    if let Ok(value) = std::env::var(name) {
        Some(value)
    } else {
        eprintln!("skipping integration test: optional {name} is unavailable");
        None
    }
}

/// Check an opt-in flag that must equal `1`, such as `SWARMY_API_FAKE_BENCH`.
/// Disabled flags skip even under `CI`; the hint must say how to opt in.
#[must_use = "check the returned option: disabled opt-in flags skip the test"]
pub fn opt_in_env(name: &str, hint: &str) -> Option<String> {
    if let Ok("1") = std::env::var(name).as_deref() {
        Some("1".to_owned())
    } else {
        eprintln!("skipping opt-in integration test: {hint}");
        None
    }
}

fn missing_stack<T>(name: &str) -> Option<T> {
    assert!(
        std::env::var_os("CI").is_none(),
        "CI requires {name} for integration tests"
    );
    eprintln!("skipping integration test: {name} is unavailable");
    None
}

