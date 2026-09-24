use std::os::unix::fs::PermissionsExt;
use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn credential_helpers_fetch_on_use_and_never_cache() {
    let directory = tempfile::tempdir().unwrap();
    let script = include_str!("../../../images/common/agent-setup.sh")
        .split("<<'PY'\n")
        .nth(1)
        .unwrap()
        .split("\nPY\n")
        .next()
        .unwrap()
        .replace(
            "/run/swarmy/github.sock",
            directory.path().join("github.sock").to_str().unwrap(),
        )
        .replace(
            "/usr/bin/gh",
            directory.path().join("real-gh").to_str().unwrap(),
        )
        .replace(
            "/run/swarmy-gh",
            directory.path().join("config").to_str().unwrap(),
        );
    let helper = directory.path().join("helper");
    std::fs::write(&helper, &script).unwrap();
    std::fs::write(directory.path().join("gh"), script).unwrap();
    // The stand-in verifies environment-only delivery, including the tmpfs config path.
    std::fs::write(directory.path().join("real-gh"), "#!/usr/bin/python3\nimport os\nassert os.environ['GH_TOKEN'] == 'rotated'\nassert os.environ['GH_ENTERPRISE_TOKEN'] == 'rotated'\nassert os.environ['GH_CONFIG_DIR'].endswith('/config')\n").unwrap();
    std::fs::set_permissions(
        directory.path().join("real-gh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let listener =
        std::os::unix::net::UnixListener::bind(directory.path().join("github.sock")).unwrap();
    let server = std::thread::spawn(move || {
        use std::io::Read;
        for response in [
            r#"{"token":"initial"}"#,
            r#"{"token":"rotated"}"#,
            r#"{"error":"unavailable"}"#,
        ] {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 13];
            socket.read_exact(&mut request).unwrap();
            assert_eq!(&request, b"github-token\n");
            writeln!(socket, "{response}").unwrap();
        }
    });
    for (action, host, expected) in [
        ("store", "github.com", ""),
        ("erase", "github.com", ""),
        ("get", "example.com", ""),
        (
            "get",
            "github.com",
            "username=x-access-token\npassword=initial\n\n",
        ),
    ] {
        let mut child = Command::new("python3")
            .arg(&helper)
            .arg(action)
            .env_remove("GH_HOST")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("protocol=https\nhost={host}\n\n").as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, expected.as_bytes());
    }
    let output = Command::new("python3")
        .arg(directory.path().join("gh"))
        .args(["api", "user"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new("python3")
        .arg(directory.path().join("gh"))
        .args(["api", "user"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(output.stderr, b"GitHub credential unavailable\n");
    server.join().unwrap();
    assert!(!directory.path().join("config").exists());
}
