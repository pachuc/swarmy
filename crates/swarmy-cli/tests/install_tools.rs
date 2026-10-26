#![deny(clippy::disallowed_methods)]
//! `swarmy install-tools` runs the embedded installer without a checkout.
const CLI: &str = env!("CARGO_BIN_EXE_swarmy");

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn install_tools_passes_the_prefix_to_the_embedded_installer() {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(CLI)
        .current_dir(dir.path())
        .env("HOME", dir.path())
        .args(["install-tools", "--prefix", "bad:prefix"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("linker separators"), "{stderr}");
    assert!(
        stderr.contains("backing tool installation failed"),
        "{stderr}"
    );
}

#[test]
fn install_tools_rejects_json() {
    let output = std::process::Command::new(CLI)
        .args(["--json", "install-tools"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--json is not supported"));
}
