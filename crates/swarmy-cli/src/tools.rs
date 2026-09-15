use std::{ffi::OsString, os::unix::fs::PermissionsExt, path::PathBuf};

/// Keep the caller's choices first, then search the locations installed by our script.
pub fn search_path() -> Result<OsString, std::env::JoinPathsError> {
    let mut paths: Vec<_> =
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()).collect();
    if let Some(lib) = option_env!("SWARMY_FDB_LIB_DIR")
        && let Some(prefix) = std::path::Path::new(lib).parent()
    {
        paths.push(prefix.join("bin"));
    }
    if let Some(home) = std::env::var_os("HOME") {
        paths.push(PathBuf::from(home).join(".local/bin"));
    }
    // FoundationDB Debian packages put fdbserver here.
    paths.push(PathBuf::from("/usr/sbin"));
    std::env::join_paths(paths)
}

pub fn find(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&search_path().ok()?)
        .map(|dir| dir.join(name))
        .find(|path| {
            path.metadata().is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
}
