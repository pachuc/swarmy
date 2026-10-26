//! Atomic private file operations shared by configuration writers.
use crate::Error;
use std::{
    fs::{self, File, Permissions},
    io::Write,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::Path,
};

/// Replace a file atomically, with mode 0600 and a synced parent.
/// # Errors
/// Returns filesystem errors.
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    persist(path, bytes, true)
}
/// Create a private file without replacing an existing one.
/// # Errors
/// Returns `Error::Io` with `AlreadyExists` when the destination exists.
pub fn create_private(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    persist(path, bytes, false)
}
fn persist(path: &Path, bytes: &[u8], replace: bool) -> Result<(), Error> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary
        .as_file()
        .set_permissions(Permissions::from_mode(0o600))?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    if replace {
        temporary.persist(path).map_err(|e| e.error)?;
    } else {
        temporary.persist_noclobber(path).map_err(|e| e.error)?;
    }
    File::open(parent)?.sync_all()?;
    Ok(())
}
/// Create nested private directories and reset the leaf's mode to 0700.
/// # Errors
/// Returns filesystem errors.
pub fn create_private_dir(path: &Path) -> Result<(), Error> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    fs::set_permissions(path, Permissions::from_mode(0o700))?;
    Ok(())
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn private_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        write_private(&path, b"first").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(
            matches!(create_private(&path,b"wrong"),Err(Error::Io(e)) if e.kind()==std::io::ErrorKind::AlreadyExists)
        );
        fs::set_permissions(&path, Permissions::from_mode(0o644)).unwrap();
        write_private(&path, b"second").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        let nested = dir.path().join("a/b");
        create_private_dir(&nested).unwrap();
        fs::set_permissions(&nested, Permissions::from_mode(0o755)).unwrap();
        create_private_dir(&nested).unwrap();
        assert_eq!(
            fs::metadata(&nested).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
}
