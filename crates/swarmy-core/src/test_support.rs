//! Shared environment checks for integration tests that need the dev stack.

/// Read a stack setting, preserving local skips while failing broken CI setup.
#[must_use]
pub fn stack_env(name: &str) -> Result<String, std::env::VarError> {
    let value = std::env::var(name);
    if std::env::var_os("CI").is_some() && value.is_err() {
        panic!("CI requires {name} for integration tests");
    }
    value
}

/// Check a stack setting without converting it to UTF-8.
#[must_use]
pub fn stack_env_os(name: &str) -> Option<std::ffi::OsString> {
    let value = std::env::var_os(name);
    if std::env::var_os("CI").is_some() && value.is_none() {
        panic!("CI requires {name} for integration tests");
    }
    value
}
