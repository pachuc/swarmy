use std::{env, path::Path};

// Cargo does not propagate rustc-link-arg from libraries to their consumers.
// Every binary package uses this script too, so installed executables retain it.
fn main() {
    println!("cargo:rerun-if-env-changed=SWARMY_FDB_LIB_DIR");
    println!("cargo:rerun-if-changed=../swarmy-store/build.rs");
    let target = env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target != "linux" && target != "macos" {
        return;
    }
    if let Ok(directory) = env::var("SWARMY_FDB_LIB_DIR") {
        let path = Path::new(&directory);
        assert!(
            path.is_absolute() && path.is_dir(),
            "SWARMY_FDB_LIB_DIR must be an existing absolute directory"
        );
        assert!(
            !directory.contains([',', ':', '\n', '\r']),
            "SWARMY_FDB_LIB_DIR contains a linker separator"
        );
        link(&directory);
        println!("cargo:rustc-env=SWARMY_FDB_LIB_DIR={directory}");
    }
    for directory in ["/usr/lib", "/usr/local/lib", "/usr/lib/x86_64-linux-gnu"] {
        if Path::new(directory).is_dir() {
            link(directory);
        }
    }
}

fn link(directory: &str) {
    println!("cargo:rustc-link-search=native={directory}");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{directory}");
}
