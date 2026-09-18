#[test]
fn sandbox_python_helpers() {
    let result = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/tool_helpers.py"
        ))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("python3 is required to test sandbox helpers");
    assert!(
        result.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
