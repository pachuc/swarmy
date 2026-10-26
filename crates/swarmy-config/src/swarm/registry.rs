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
fn registry_entry_name(path: &Path, specs: bool) -> Option<String> {
    let name = if specs {
        if !path.is_file() || path.extension().is_none_or(|e| e != "toml") {
            return None;
        }
        path.file_stem()?.to_str()?
    } else {
        if !path.is_dir() {
            return None;
        }
        path.file_name()?.to_str()?
    };
    validate_swarm_name(name).is_ok().then(|| name.to_owned())
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
                    let path = entry?.path();
                    if let Some(name) = registry_entry_name(&path, specs) {
                        names.insert(name);
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
        if let Err(e) = fs::remove_file(spec)
            && e.kind() != ErrorKind::NotFound
        {
            return Err(e.into());
        }
        if let Err(e) = fs::remove_dir_all(dir)
            && e.kind() != ErrorKind::NotFound
        {
            return Err(e.into());
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
        }
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
        if let Err(e) = fs::remove_file(self.dirs.current())
            && e.kind() != ErrorKind::NotFound
        {
            return Err(e.into());
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use crate::swarm::{GroupSpec, Lifecycle, Module, Placement, SwarmApi};
    use std::os::unix::fs::PermissionsExt;
    fn setup() -> (tempfile::TempDir, Registry) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let dirs = Dirs {
            config: root.join("config"),
            data: root.join("data"),
            state: root.join("state"),
            cache: root.join("cache"),
        };
        (temp, Registry::new(dirs))
    }
    fn spec(name: &str) -> SwarmSpec {
        SwarmSpec {
            name: name.into(),
            lifecycle: Lifecycle::Persistent,
            groups: [(
                "box".into(),
                GroupSpec {
                    module: Module::Existing,
                    size: None,
                    location: None,
                    count: None,
                    hosts: vec![crate::swarm::HostSpec {
                        address: "203.0.113.10".into(),
                        ssh_user: "root".into(),
                        ssh_key: "/keys/box".into(),
                    }],
                },
            )]
            .into(),
            placement: Placement {
                stack: "box".into(),
                services: "box".into(),
                nodes: vec!["box".into()],
            },
        }
    }
    #[test]
    fn disk_round_trip_and_modes() {
        let (_tmp, r) = setup();
        assert!(r.list().unwrap().is_empty());
        assert!(!r.dirs().config.exists());
        let spec = spec("a");
        r.write_spec(&spec).unwrap();
        r.write_spec(&spec).unwrap();
        assert_eq!(r.read_spec("a").unwrap(), Some(spec));
        let record = SwarmRecord {
            name: "a".into(),
            created_at: "2026-10-26T00:00:00Z".parse().unwrap(),
            api: SwarmApi::default(),
            hosts: std::collections::BTreeMap::default(),
            tofu_state: None,
        };
        r.write_record(&record).unwrap();
        r.write_record(&record).unwrap();
        assert_eq!(r.read_record("a").unwrap(), Some(record));
        for path in [r.spec_path("a").unwrap(), r.record_path("a").unwrap()] {
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        for path in [
            r.dirs().specs(),
            r.dirs().records(),
            r.swarm_dir("a").unwrap(),
        ] {
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        assert_eq!(fs::read_dir(r.dirs().specs()).unwrap().count(), 1);
        assert_eq!(fs::read_dir(r.swarm_dir("a").unwrap()).unwrap().count(), 1);
    }
    #[test]
    fn list_remove_and_current() {
        let (_tmp, r) = setup();
        assert!(r.current().unwrap().is_none());
        assert!(matches!(r.set_current("nope"), Err(Error::UnknownSwarm(_))));
        for name in ["z", "a"] {
            r.write_spec(&spec(name)).unwrap();
        }
        create_private_dir(&r.swarm_dir("m").unwrap()).unwrap();
        fs::write(r.dirs().specs().join("notes.txt"), "").unwrap();
        fs::write(r.dirs().specs().join("bad name.toml"), "").unwrap();
        fs::write(r.dirs().records().join("stray"), "").unwrap();
        assert_eq!(r.list().unwrap(), ["a", "m", "z"]);
        assert!(matches!(r.read("missing"), Err(Error::UnknownSwarm(_))));
        r.set_current("a").unwrap();
        assert_eq!(r.current().unwrap(), Some("a".into()));
        assert_eq!(
            fs::metadata(r.dirs().current())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        r.remove("z").unwrap();
        assert_eq!(r.current().unwrap(), Some("a".into()));
        r.remove("a").unwrap();
        assert_eq!(r.current().unwrap(), None);
        r.clear_current().unwrap();
        assert!(matches!(r.remove("unknown"), Err(Error::UnknownSwarm(_))));
        fs::write(r.dirs().current(), "../x").unwrap();
        assert!(r.current().is_err());
    }
    #[test]
    fn file_errors_name_mismatch() {
        let (_tmp, r) = setup();
        r.write_spec(&spec("a")).unwrap();
        fs::write(r.spec_path("a").unwrap(), spec("b").to_toml().unwrap()).unwrap();
        assert!(
            matches!(r.read_spec("a"),Err(Error::File{source,..}) if matches!(*source,Error::Registry(_)))
        );
    }
}
