#![deny(clippy::disallowed_methods)]
//! Execute on a suite node with root, NBD, and a prebuilt desktop image.
use std::process::Command;

#[test]
fn root_desktop_agent_uses_browser_and_screen() {
    let name = "root_desktop_agent_uses_browser_and_screen";
    if !swarmy_testkit::require_root_host(name, &["runc", "pasta"]) {
        return;
    }
    let Some(image) = swarmy_testkit::optional_env("SWARMY_TEST_DESKTOP_IMAGE") else {
        return;
    };
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_NATS_URL",
        "SWARMY_S3_ENDPOINT",
    ] {
        if swarmy_testkit::require_stack(variable).is_none() {
            return;
        }
    }
    let executable = std::path::Path::new(env!("CARGO_BIN_EXE_swarmy-chaos"));
    let mut run = Command::new(executable);
    run.args([
        "--no-start-stack",
        "--scenario",
        "desktop",
        "--sessions",
        "1",
        "--schedulers",
        "1",
        "--workers",
        "1",
        "--gateways",
        "1",
        "--kills",
        "0",
    ])
    .arg("--bin-dir")
    .arg(executable.parent().unwrap())
    .arg("--image")
    .arg(image);
    if let Some(output) = std::env::var_os("SWARMY_TEST_DESKTOP_OUTPUT") {
        run.arg("--desktop-output").arg(output);
    }
    assert!(run.status().unwrap().success(), "desktop acceptance failed");
}
