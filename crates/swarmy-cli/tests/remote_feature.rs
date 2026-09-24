#[cfg(not(feature = "remote"))]
#[test]
fn remote_command_reports_missing_feature() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .args(["remote", "up", "x"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(
        stderr.trim(),
        "Error: swarmy was built without remote support"
    );
}
