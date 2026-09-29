//! Shared environment checks for integration tests that need the dev stack.

/// Read a stack setting, preserving local skips while failing broken CI setup.
pub fn stack_env(name: &str) -> Result<String, std::env::VarError> {
    let value = std::env::var(name);
    assert!(
        std::env::var_os("CI").is_none() || value.is_ok(),
        "CI requires {name} for integration tests"
    );
    value
}

/// Check a stack setting without converting it to UTF-8.
pub fn stack_env_os(name: &str) -> Option<std::ffi::OsString> {
    let value = std::env::var_os(name);
    assert!(
        std::env::var_os("CI").is_none() || value.is_some(),
        "CI requires {name} for integration tests"
    );
    value
}
