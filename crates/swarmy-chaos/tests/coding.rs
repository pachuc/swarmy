//! Build as an ordinary user, then execute this test binary with sudo -E.
use std::process::Command;

#[test]
fn root_coding_recovery() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping coding acceptance: run the built test with sudo");
        return;
    }
    for variable in [
        "SWARMY_TEST_IMAGE",
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_NATS_URL",
        "SWARMY_S3_ENDPOINT",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping coding acceptance: {variable} is unset");
            return;
        }
    }
    let executable = std::path::Path::new(env!("CARGO_BIN_EXE_swarmy-chaos"));
    let status = Command::new(executable)
        .args([
            "--no-start-stack",
            "--coding",
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
        .arg(std::env::var_os("SWARMY_TEST_IMAGE").unwrap())
        .status()
        .unwrap();
    assert!(status.success(), "coding recovery acceptance failed");
}
