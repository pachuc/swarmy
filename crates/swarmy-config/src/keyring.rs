//! Cluster data key, kept separately from the encrypted database.
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use rand::TryRngCore;

use crate::Error;

#[derive(Clone)]
pub struct Keyring([u8; 32]);

impl Keyring {
    #[must_use]
    pub const fn from_bytes(key: [u8; 32]) -> Self {
        Self(key)
    }

    #[must_use]
    pub const fn key(&self) -> &[u8; 32] {
        &self.0
    }

    /// # Errors
    /// Fails if neither an override nor a home directory is available.
    pub fn path() -> Result<PathBuf, Error> {
        std::env::var_os("SWARMY_KEYRING")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".swarmy/keyring"))
            })
            .ok_or(Error::Keyring("set HOME or SWARMY_KEYRING"))
    }

    /// # Errors
    /// Rejects absent, malformed, or insufficiently protected key files.
    pub fn load() -> Result<Self, Error> {
        Self::read(&Self::path()?)
    }

    /// # Errors
    /// Rejects absent, malformed, or insufficiently protected key files.
    pub fn read(path: &Path) -> Result<Self, Error> {
        let mut file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > 128 {
            return Err(Error::Keyring("expected a base64 32-byte key file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o777 != 0o600 {
                return Err(Error::Keyring("keyring must have mode 600"));
            }
        }
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        let bytes = STANDARD
            .decode(text.trim())
            .map_err(|_| Error::Keyring("invalid base64 keyring"))?;
        Ok(Self(bytes.try_into().map_err(|_| {
            Error::Keyring("keyring must contain 32 bytes")
        })?))
    }

    /// Atomically create a fresh key. Never replace an existing cluster key.
    /// # Errors
    /// Returns an error on I/O, randomness failure, or an existing destination.
    pub fn generate() -> Result<Self, Error> {
        Self::generate_at(&Self::path()?)
    }

    /// # Errors
    /// Returns an error on I/O, randomness failure, or an existing destination.
    pub fn generate_at(path: &Path) -> Result<Self, Error> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut directory = fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory.create(parent)?;
        let mut key = [0; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut key)
            .map_err(|_| Error::Keyring("randomness unavailable"))?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            temporary
                .as_file()
                .set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        writeln!(temporary, "{}", STANDARD.encode(key))?;
        temporary.as_file().sync_all()?;
        temporary.persist_noclobber(path).map_err(|e| e.error)?;
        File::open(parent)?.sync_all()?;
        Ok(Self(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_malformed_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keyring");
        Keyring::generate_at(&path).unwrap();
        for text in [
            "not base64!".to_owned(),
            STANDARD.encode([1; 31]),
            STANDARD.encode([1; 33]),
        ] {
            fs::write(&path, text).unwrap();
            assert!(matches!(Keyring::read(&path), Err(Error::Keyring(_))));
        }
    }

    #[test]
    fn generate_read_and_do_not_replace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keyring");
        let key = Keyring::generate_at(&path).unwrap();
        assert_eq!(key.key(), Keyring::read(&path).unwrap().key());
        assert!(Keyring::generate_at(&path).is_err());
        assert_eq!(key.key(), Keyring::read(&path).unwrap().key());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            assert!(matches!(Keyring::read(&path), Err(Error::Keyring(_))));
        }
    }
}
