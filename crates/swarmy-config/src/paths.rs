//! XDG base directories for swarmy.
use crate::{Error, process_environment};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
/// Environment keys used to locate the XDG base directories.
pub const XDG_KEYS: [&str; 5] = [
    "HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_CACHE_HOME",
];
/// Swarmy's four base directories, each already ending in `swarmy`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dirs {
    pub config: PathBuf,
    pub data: PathBuf,
    pub state: PathBuf,
    pub cache: PathBuf,
}
impl Dirs {
    /// # Errors
    /// Returns `Directories` when neither an absolute XDG value nor absolute HOME exists.
    pub fn from_env(environment: &BTreeMap<String, String>) -> Result<Self, Error> {
        const MISSING: &str =
            "set HOME to an absolute path (or every XDG_*_HOME variable to absolute paths)";
        Ok(Self {
            config: config(environment).ok_or(Error::Directories(MISSING))?,
            data: data(environment).ok_or(Error::Directories(MISSING))?,
            state: state(environment).ok_or(Error::Directories(MISSING))?,
            cache: cache(environment).ok_or(Error::Directories(MISSING))?,
        })
    }
    /// # Errors
    /// Returns `Environment` for non-UTF-8 values, or errors from `from_env`.
    pub fn from_process() -> Result<Self, Error> {
        Self::from_env(&process_environment(|key| XDG_KEYS.contains(&key))?)
    }
    #[must_use]
    pub fn specs(&self) -> PathBuf {
        self.config.join("swarms")
    }
    #[must_use]
    pub fn records(&self) -> PathBuf {
        self.data.join("swarms")
    }
    #[must_use]
    pub fn current(&self) -> PathBuf {
        self.state.join("current")
    }
}
fn base(env: &BTreeMap<String, String>, variable: &str, suffix: &str) -> Option<PathBuf> {
    env.get(variable)
        .filter(|v| !v.is_empty() && Path::new(v).is_absolute())
        .map(PathBuf::from)
        .or_else(|| {
            env.get("HOME")
                .filter(|v| !v.is_empty() && Path::new(v).is_absolute())
                .map(|v| Path::new(v).join(suffix))
        })
        .map(|p| p.join("swarmy"))
}
#[must_use]
pub fn config(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    base(env, "XDG_CONFIG_HOME", ".config")
}
#[must_use]
pub fn data(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    base(env, "XDG_DATA_HOME", ".local/share")
}
#[must_use]
pub fn state(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    base(env, "XDG_STATE_HOME", ".local/state")
}
#[must_use]
pub fn cache(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    base(env, "XDG_CACHE_HOME", ".cache")
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn xdg_and_home() {
        let mut env = BTreeMap::new();
        env.insert("HOME".into(), "/home/test".into());
        let dirs = Dirs::from_env(&env).unwrap();
        assert_eq!(dirs.specs(), Path::new("/home/test/.config/swarmy/swarms"));
        assert_eq!(
            dirs.records(),
            Path::new("/home/test/.local/share/swarmy/swarms")
        );
        assert_eq!(
            dirs.current(),
            Path::new("/home/test/.local/state/swarmy/current")
        );
        env.insert("XDG_CONFIG_HOME".into(), "relative".into());
        assert_eq!(Dirs::from_env(&env).unwrap(), dirs);
        env.remove("HOME");
        assert!(matches!(Dirs::from_env(&env), Err(Error::Directories(_))));
        for key in [
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_STATE_HOME",
            "XDG_CACHE_HOME",
        ] {
            env.insert(key.into(), format!("/tmp/{key}"));
        }
        assert_eq!(
            Dirs::from_env(&env).unwrap().cache,
            Path::new("/tmp/XDG_CACHE_HOME/swarmy")
        );
    }
    #[test]
    fn ignores_relative_or_empty_values() {
        let mut env = BTreeMap::from([("HOME".to_owned(), "/home/test".to_owned())]);
        for (i, key) in [
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_STATE_HOME",
            "XDG_CACHE_HOME",
        ]
        .into_iter()
        .enumerate()
        {
            env.insert(
                key.into(),
                if i % 2 == 0 {
                    "relative/dir".into()
                } else {
                    String::new()
                },
            );
        }
        assert_eq!(
            Dirs::from_env(&env).unwrap(),
            Dirs {
                config: "/home/test/.config/swarmy".into(),
                data: "/home/test/.local/share/swarmy".into(),
                state: "/home/test/.local/state/swarmy".into(),
                cache: "/home/test/.cache/swarmy".into(),
            }
        );
        for home in ["relative", ""] {
            env.insert("HOME".into(), home.into());
            assert!(matches!(Dirs::from_env(&env), Err(Error::Directories(_))));
        }
    }
}
