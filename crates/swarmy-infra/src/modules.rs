//! The machine-group OpenTofu modules this client carries, compiled in from
//! `infra/modules`. Each entry is a complete root configuration: its `*.tf`
//! files and its provider lock file. Tests stay in the repository.
use crate::ModuleFile;

/// One carried module: the name a swarm spec's `module = "..."` uses, and
/// its root files sorted by path.
pub struct Module {
    pub name: &'static str,
    pub files: &'static [ModuleFile<'static>],
}

macro_rules! module_file {
    ($module:literal, $file:literal) => {
        ModuleFile {
            path: $file,
            contents: include_str!(concat!("../../../infra/modules/", $module, "/", $file))
                .as_bytes(),
        }
    };
}

macro_rules! module {
    ($name:literal) => {
        Module {
            name: $name,
            files: &[
                module_file!($name, ".terraform.lock.hcl"),
                module_file!($name, "main.tf"),
                module_file!($name, "outputs.tf"),
                module_file!($name, "variables.tf"),
                module_file!($name, "versions.tf"),
            ],
        }
    };
}

/// Every carried module, sorted by name.
pub static MODULES: &[Module] = &[module!("aws"), module!("hetzner-cloud")];

/// The carried module with this name.
#[must_use]
pub fn find(name: &str) -> Option<&'static Module> {
    MODULES.iter().find(|module| module.name == name)
}

/// Names of every carried module, sorted.
#[must_use]
pub fn names() -> Vec<&'static str> {
    MODULES.iter().map(|module| module.name).collect()
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::{MODULES, find, names};
    use std::{
        collections::BTreeMap,
        fs,
        path::{Component, Path},
    };

    #[test]
    fn table_matches_the_module_directories() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../infra/modules");
        let mut disk = BTreeMap::<String, BTreeMap<String, Vec<u8>>>::new();
        for entry in fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_dir() {
                continue;
            }
            let mut files = BTreeMap::new();
            for file in fs::read_dir(entry.path()).unwrap() {
                let file = file.unwrap();
                let path = file.path();
                if file.file_type().unwrap().is_file()
                    && (path.extension().is_some_and(|ext| ext == "tf")
                        || path
                            .file_name()
                            .is_some_and(|name| name == ".terraform.lock.hcl"))
                {
                    files.insert(
                        file.file_name().to_string_lossy().into_owned(),
                        fs::read(path).unwrap(),
                    );
                }
            }
            disk.insert(entry.file_name().to_string_lossy().into_owned(), files);
        }
        let carried = MODULES
            .iter()
            .map(|module| {
                let files = module
                    .files
                    .iter()
                    .map(|file| (file.path.to_owned(), file.contents.to_vec()))
                    .collect();
                (module.name.to_owned(), files)
            })
            .collect::<BTreeMap<String, BTreeMap<String, Vec<u8>>>>();
        assert_eq!(disk, carried);
        assert!(MODULES.windows(2).all(|pair| pair[0].name < pair[1].name));
        for module in MODULES {
            assert!(
                module
                    .files
                    .windows(2)
                    .all(|pair| pair[0].path < pair[1].path)
            );
            assert!(
                module
                    .files
                    .iter()
                    .any(|file| file.path == ".terraform.lock.hcl")
            );
        }
    }

    #[test]
    fn find_knows_only_carried_modules() {
        assert_eq!(names(), ["aws", "hetzner-cloud"]);
        assert!(find("aws").is_some());
        assert!(find("gcp").is_none());
    }

    #[test]
    fn carried_files_are_plain_relative_paths() {
        for module in MODULES {
            for file in module.files {
                let mut components = Path::new(file.path).components();
                assert!(matches!(components.next(), Some(Component::Normal(_))));
                assert!(components.next().is_none());
                assert_ne!(file.path, crate::VARIABLES_FILE);
            }
        }
    }
}
