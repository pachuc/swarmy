//! Run through scripts/test-bash.sh so compilation happens without root.
use std::process::Command;

#[test]
fn root_bash_disk_and_failure_acceptance() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping bash acceptance: run the built test with sudo");
        return;
    }
    let Ok(image) = std::env::var("SWARMY_TEST_IMAGE") else {
        eprintln!("skipping bash acceptance: SWARMY_TEST_IMAGE is unset");
        return;
    };
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_NATS_URL",
        "SWARMY_S3_ENDPOINT",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping bash acceptance: {variable} is unset");
            return;
        }
    }
    let executable = std::path::Path::new(env!("CARGO_BIN_EXE_swarmy-chaos"));
    let binaries = executable.parent().unwrap();
    for scenario in [
        vec!["--sessions", "1", "--steps", "2", "--kills", "0"],
        vec![
            "--persistent",
            "--sessions",
            "2",
            "--gateways",
            "1",
            "--kills",
            "0",
        ],
        vec![
            "--sessions",
            "1",
            "--steps",
            "3",
            "--kills",
            "0",
            "--kill-node-mid-command",
        ],
        vec![
            "--sessions",
            "2",
            "--steps",
            "3",
            "--kills",
            "12",
            "--seed",
            "42",
        ],
    ] {
        eprintln!("bash acceptance: {scenario:?}");
        let status = Command::new(executable)
            .arg("--no-start-stack")
            .arg("--bin-dir")
            .arg(binaries)
            .arg("--image")
            .arg(&image)
            .args(&scenario)
            .args(["--session-timeout-secs", "240"])
            .status()
            .unwrap();
        assert!(status.success(), "bash acceptance failed: {scenario:?}");
    }
}
