#[test]
fn file_helper_behaviour_without_root() {
    let result = std::process::Command::new("python3")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/files_test.py"))
        .output()
        .expect("Python 3 is required for the file helper tests");
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
