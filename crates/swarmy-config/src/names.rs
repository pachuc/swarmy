//! Validated identifiers used in swarm and service paths.
use crate::Error;

#[must_use]
pub const fn default_sandboxes() -> u32 {
    64
}

/// Validate service logins before using them in paths or shell output.
///
/// # Errors
///
/// Rejects empty names and shell metacharacters.
pub fn validate_service_user(user: &str) -> Result<(), Error> {
    if !user.is_empty()
        && user.len() <= 64
        && user
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        Ok(())
    } else {
        Err(Error::Invalid(
            "service user must contain 1-64 letters, digits, hyphens, or underscores",
        ))
    }
}

/// Validate names before using them as filenames or shell output.
/// # Errors
/// Rejects empty names, path traversal, and shell metacharacters.
pub fn validate_name(name: &str) -> Result<(), Error> {
    if !is_valid_name(name) {
        return Err(Error::Invalid(
            "names must contain 1-64 letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

pub(crate) fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Whether a name is safe for an OpenTofu machine group.
#[must_use]
pub fn is_opentofu_name(name: &str) -> bool {
    name.len() <= 20
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}
