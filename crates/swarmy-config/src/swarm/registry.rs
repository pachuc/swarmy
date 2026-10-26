//! Private on-disk registry of swarm specs and records.
use super::{SwarmRecord, SwarmSpec, validate_swarm_name};
use crate::{Error, create_private_dir, paths::Dirs, write_private};
use std::{
    collections::BTreeSet,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};
pub struct Swarm {
    pub spec: SwarmSpec,
    pub record: Option<SwarmRecord>,
}
pub struct Registry {
    dirs: Dirs,
}
fn missing<T>(result: std::io::Result<T>) -> Result<Option<T>, Error> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn file_error(path: &Path, source: Error) -> Error {
    Error::File {
        path: path.to_path_buf(),
        source: Box::new(source),
    }
}
fn read_text(path: &Path) -> Result<Option<String>, Error> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(file_error(path, e.into())),
    }
}
impl Registry {
    #[must_use]
    pub const fn new(dirs: Dirs) -> Self {
        Self { dirs }
    }
    #[must_use]
    pub const fn dirs(&self) -> &Dirs {
        &self.dirs
    }
    /// # Errors
    /// Returns `Spec` on an invalid name.
    pub fn spec_path(&self, name: &str) -> Result<PathBuf, Error> {
        validate_swarm_name(name)?;
        Ok(self.dirs.specs().join(format!("{name}.toml")))
    }
    /// # Errors
    /// Returns `Spec` on an invalid name.
    pub fn swarm_dir(&self, name: &str) -> Result<PathBuf, Error> {
        validate_swarm_name(name)?;
        Ok(self.dirs.records().join(name))
    }
    /// # Errors
    /// Returns `Spec` on an invalid name.
    pub fn record_path(&self, name: &str) -> Result<PathBuf, Error> {
        Ok(self.swarm_dir(name)?.join("record.toml"))
    }
    /// # Errors
    /// Returns filesystem errors.
    pub fn list(&self) -> Result<Vec<String>, Error> {
        let mut names = BTreeSet::new();
        for (directory, specs) in [(self.dirs.specs(), true), (self.dirs.records(), false)] {
            if let Some(entries) = missing(fs::read_dir(directory))? {
                for entry in entries {
                    let entry = entry?;
                    let path = entry.path();
                    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                        continue;
                    };
                    if specs {
                        if path.is_file() && path.extension().is_some_and(|e| e == "toml") {
                            if let Some(stem) = path
                                .file_stem()
                                .and_then(|s| s.to_str())
                                .filter(|s| validate_swarm_name(s).is_ok())
                            {
                                names.insert(stem.to_owned());
                            }
                        }
                    } else if path.is_dir() && validate_swarm_name(name).is_ok() {
                        names.insert(name.to_owned());
                    }
                }
            }
        }
        Ok(names.into_iter().collect())
    }
    /// # Errors
    /// Returns read, parse, or name mismatch errors.
    pub fn read_spec(&self, name: &str) -> Result<Option<SwarmSpec>, Error> {
        let path = self.spec_path(name)?;
        let Some(text) = read_text(&path)? else {
            return Ok(None);
        };
        let spec = SwarmSpec::parse(&text).map_err(|e| file_error(&path, e))?;
        if spec.name != name {
            return Err(file_error(
                &path,
                Error::Registry(format!(
                    "{} names swarm \"{}\" but belongs to \"{}\"",
                    path.display(),
                    spec.name,
                    name
                )),
            ));
        }
        Ok(Some(spec))
    }
    /// # Errors
    /// Returns read, parse, or name mismatch errors.
    pub fn read_record(&self, name: &str) -> Result<Option<SwarmRecord>, Error> {
        let path = self.record_path(name)?;
        let Some(text) = read_text(&path)? else {
            return Ok(None);
        };
        let record = SwarmRecord::parse(&text).map_err(|e| file_error(&path, e))?;
        if record.name != name {
            return Err(file_error(
                &path,
                Error::Registry(format!(
                    "{} names swarm \"{}\" but belongs to \"{}\"",
                    path.display(),
                    record.name,
                    name
                )),
            ));
        }
        Ok(Some(record))
    }
    /// # Errors
    /// Returns `UnknownSwarm` without a spec, or registry read errors.
    pub fn read(&self, name: &str) -> Result<Swarm, Error> {
        let spec = self
            .read_spec(name)?
            .ok_or_else(|| Error::UnknownSwarm(name.to_owned()))?;
        Ok(Swarm {
            spec,
            record: self.read_record(name)?,
        })
    }
    /// # Errors
    /// Returns validation, encode, or filesystem errors.
    pub fn write_spec(&self, spec: &SwarmSpec) -> Result<(), Error> {
        let text = spec.to_toml()?;
        let path = self.spec_path(&spec.name)?;
        create_private_dir(&self.dirs.specs())?;
        write_private(&path, text.as_bytes())
    }
    /// # Errors
    /// Returns validation, encode, or filesystem errors.
    pub fn write_record(&self, record: &SwarmRecord) -> Result<(), Error> {
        let text = record.to_toml()?;
        let dir = self.swarm_dir(&record.name)?;
        create_private_dir(&dir)?;
        write_private(&dir.join("record.toml"), text.as_bytes())
    }
    /// # Errors
    /// Returns `UnknownSwarm` or filesystem errors.
    pub fn remove(&self, name: &str) -> Result<(), Error> {
        let spec = self.spec_path(name)?;
        let dir = self.swarm_dir(name)?;
        if !spec.exists() && !dir.exists() {
            return Err(Error::UnknownSwarm(name.to_owned()));
        }
        if let Err(e) = fs::remove_file(spec) {
            if e.kind() != ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        if let Err(e) = fs::remove_dir_all(dir) {
            if e.kind() != ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        if self.current()?.as_deref() == Some(name) {
            self.clear_current()?;
        }
        Ok(())
    }
    /// # Errors
    /// Returns read or invalid content errors.
    pub fn current(&self) -> Result<Option<String>, Error> {
        let path = self.dirs.current();
        let Some(text) = read_text(&path)? else {
            return Ok(None);
        };
        let name = text.trim_end_matches('\n');
        if name.is_empty() {
            return Ok(None);
        };
        if validate_swarm_name(name).is_err() {
            return Err(file_error(
                &path,
                Error::Registry(format!("{} does not hold a swarm name", path.display())),
            ));
        }
        Ok(Some(name.to_owned()))
    }
    /// # Errors
    /// Returns `UnknownSwarm` or filesystem errors.
    pub fn set_current(&self, name: &str) -> Result<(), Error> {
        self.read(name)?;
        create_private_dir(&self.dirs.state)?;
        write_private(&self.dirs.current(), format!("{name}\n").as_bytes())
    }
    /// # Errors
    /// Returns filesystem errors.
    pub fn clear_current(&self) -> Result<(), Error> {
        if let Err(e) = fs::remove_file(self.dirs.current()) {
            if e.kind() != ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        Ok(())
    }
}
