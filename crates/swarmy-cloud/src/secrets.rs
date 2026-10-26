//! Read secrets from restricted local inputs.
use crate::Result;
use std::{io::Read, os::unix::fs::PermissionsExt, path::Path};

/// Read one secret line from a file only the owner can read. The value never
/// appears in an error message.
///
/// # Errors
///
/// Rejects missing files, group- or world-accessible permissions, and empty
/// or multi-line contents.
pub fn read_secret_file(path: &Path) -> Result<String> {
    let mode = std::fs::metadata(path)?.permissions().mode();
    // No group or other permission bits: the low six mode bits must be zero.
    // The mask states the requirement directly; the lint's arithmetic form
    // would restate the same check less readably next to this comment.
    #[expect(
        clippy::verbose_bit_mask,
        reason = "mask matches the permission requirement below"
    )]
    crate::Error::ensure(
        mode & 0o077 == 0,
        format!(
            "secret file {} must not be readable by group or others (chmod 600)",
            path.display()
        ),
    )?;
    let secret = std::fs::read_to_string(path)?;
    let secret = secret.strip_suffix('\n').unwrap_or(&secret);
    let secret = secret.strip_suffix('\r').unwrap_or(secret);
    crate::Error::ensure(
        !secret.is_empty() && !secret.contains('\n'),
        "secret file must hold exactly one non-empty line",
    )?;
    Ok(secret.to_owned())
}

/// Read one secret line from stdin without echoing or logging it.
///
/// # Errors
///
/// Reports I/O failures and empty input.
pub fn read_secret_stdin() -> Result<String> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let secret = input.strip_suffix('\n').unwrap_or(&input);
    let secret = secret.strip_suffix('\r').unwrap_or(secret);
    crate::Error::ensure(!secret.is_empty(), "stdin secret must not be empty")?;
    Ok(secret.to_owned())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn secret_file_requires_owner_only_and_one_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, "value\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            read_secret_file(&path)
                .unwrap_err()
                .to_string()
                .contains("chmod 600")
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_secret_file(&path).unwrap(), "value");
        std::fs::write(&path, "one\ntwo\n").unwrap();
        assert!(read_secret_file(&path).is_err());
    }
}
