#![deny(clippy::disallowed_methods)]
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    process::{Command, Output, Stdio},
};
const TOKEN: &str = "hcloud-test-token-5f2b9c";
const S3_ACCESS: &str = "s3-access-7c1d";
const S3_SECRET: &str = "s3-secret-9e4a";
const AWS_ACCESS: &str = "aws-akid-3b8f";
const AWS_SECRET: &str = "aws-secret-6d2e";
const SCW_ACCESS: &str = "SCWEXAMPLEACCESS0001";
const SCW_SECRET: &str = "scw-secret-4f1a";
const SCW_PROJECT: &str = "99999999-aaaa-bbbb-cccc-dddddddddddd";
fn run(root: &std::path::Path, args: &[&str], input: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_DATA_HOME", root.join("data"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("RUST_LOG", "trace")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert_no_secret(&output);
    output
}
fn assert_no_secret(output: &Output) {
    for secret in [
        TOKEN,
        S3_ACCESS,
        S3_SECRET,
        AWS_ACCESS,
        AWS_SECRET,
        SCW_ACCESS,
        SCW_SECRET,
        SCW_PROJECT,
    ] {
        for bytes in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(bytes).contains(secret));
        }
    }
}
fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}
fn path(root: &std::path::Path) -> std::path::PathBuf {
    root.join("data/swarmy/cloud-credentials.toml")
}
#[test]
fn login_from_stdin_then_ls() {
    let tmp = tempfile::tempdir().unwrap();
    for dir in ["home", "config", "data", "state", "cache"] {
        fs::create_dir(tmp.path().join(dir)).unwrap();
    }
    assert!(
        run(
            tmp.path(),
            &["cloud", "login", "hetzner-cloud"],
            &format!("{TOKEN}\n")
        )
        .status
        .success()
    );
    let file = path(tmp.path());
    assert_eq!(
        fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(file.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let saved = swarmy_config::cloud::CloudCredentials::load(&file).unwrap();
    assert!(matches!(
        saved.get(swarmy_config::cloud::CloudProvider::HetznerCloud),
        Some(swarmy_config::cloud::CloudCredential::HetznerCloud(c)) if c.token == TOKEN
    ));
    let out = run(tmp.path(), &["cloud", "ls"], "");
    assert!(out.status.success());
    assert!(text(&out.stdout).contains("hetzner-cloud token"));
    assert!(text(&out.stdout).contains("aws           not set"));
    let out = run(tmp.path(), &["--json", "cloud", "ls"], "");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json.as_array().unwrap().len(), 4);
    assert_eq!(json[0]["set"], true);
    assert_eq!(json[2]["set"], false);
    for dir in ["home", "config", "state", "cache"] {
        assert_eq!(fs::read_dir(tmp.path().join(dir)).unwrap().count(), 0);
    }
}
#[test]
fn s3_and_aws_options() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(
        run(
            tmp.path(),
            &["cloud", "login", "hetzner-s3"],
            &format!("{S3_ACCESS}\n{S3_SECRET}\n")
        )
        .status
        .success()
    );
    assert!(
        run(
            tmp.path(),
            &["cloud", "login", "aws", "--region", "eu-central-1"],
            &format!("{AWS_ACCESS}\n{AWS_SECRET}\n")
        )
        .status
        .success()
    );
    let out = run(tmp.path(), &["cloud", "ls"], "");
    let listing = text(&out.stdout);
    assert!(listing.contains("access_key, secret_key"));
    assert!(listing.contains("access_key_id, secret_access_key, region"));
    assert!(!listing.contains("eu-central-1"));
}
#[test]
fn scaleway_login_reads_three_lines() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(
        tmp.path(),
        &["cloud", "login", "scaleway"],
        &format!("{SCW_ACCESS}\n{SCW_SECRET}\n{SCW_PROJECT}\n"),
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let listing = text(&run(tmp.path(), &["cloud", "ls"], "").stdout);
    assert!(listing.contains("access_key, secret_key, project_id"));
    assert!(
        fs::read_to_string(path(tmp.path()))
            .unwrap()
            .contains("[scaleway]")
    );
}
#[test]
fn wrong_flags_and_input_are_refused() {
    for (args, input, error) in [
        (
            vec!["cloud", "login", "scaleway"],
            "a\nb\n",
            "expected 3 lines",
        ),
        (
            vec!["cloud", "login", "scaleway"],
            "a\nb\nnot-a-uuid\n",
            "project_id must be a UUID",
        ),
        (
            vec!["cloud", "login", "hetzner-cloud", "--region", "x"],
            TOKEN,
            "--region applies only to aws",
        ),
        (
            vec!["cloud", "login", "hetzner-s3"],
            S3_ACCESS,
            "expected 2 lines",
        ),
        (
            vec!["cloud", "login", "hetzner-cloud"],
            "",
            "expected 1 lines",
        ),
        (
            vec!["cloud", "login", "hetzner-cloud", "--token", "x"],
            "",
            "--token",
        ),
        (
            vec!["cloud", "login", "digitalocean"],
            "",
            "hetzner-cloud, hetzner-s3, aws, scaleway",
        ),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let out = run(tmp.path(), &args, input);
        assert!(!out.status.success());
        assert!(text(&out.stderr).contains(error), "{}", text(&out.stderr));
        assert!(!path(tmp.path()).exists());
    }
}
#[test]
fn logout_removes_only_that_provider() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(
        run(tmp.path(), &["cloud", "login", "hetzner-cloud"], TOKEN)
            .status
            .success()
    );
    assert!(
        run(
            tmp.path(),
            &["cloud", "login", "aws"],
            &format!("{AWS_ACCESS}\n{AWS_SECRET}")
        )
        .status
        .success()
    );
    let out = run(tmp.path(), &["cloud", "logout", "aws"], "");
    assert!(text(&out.stdout).contains("removed aws credentials"));
    let contents = fs::read_to_string(path(tmp.path())).unwrap();
    assert!(contents.contains(TOKEN));
    assert!(!contents.contains(AWS_ACCESS));
    assert!(!contents.contains(AWS_SECRET));
    assert_eq!(
        fs::metadata(path(tmp.path())).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let out = run(tmp.path(), &["--json", "cloud", "logout", "aws"], "");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&out.stdout).unwrap()["removed"],
        false
    );
    let fresh = tempfile::tempdir().unwrap();
    assert!(
        run(fresh.path(), &["cloud", "logout", "hetzner-s3"], "")
            .status
            .success()
    );
    assert!(!path(fresh.path()).exists());
}
#[test]
fn loose_file_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(
        run(tmp.path(), &["cloud", "login", "hetzner-cloud"], TOKEN)
            .status
            .success()
    );
    fs::set_permissions(path(tmp.path()), fs::Permissions::from_mode(0o644)).unwrap();
    let out = run(tmp.path(), &["cloud", "ls"], "");
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("chmod 600"));
}

#[test]
fn malformed_secret_line_is_not_echoed() {
    let tmp = tempfile::tempdir().unwrap();
    let file = path(tmp.path());
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    fs::write(&file, format!("[hetzner-cloud]\ntokn = \"{TOKEN}\"\n")).unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    let output = run(tmp.path(), &["cloud", "ls"], "");
    assert!(!output.status.success());
    assert!(text(&output.stderr).contains("not valid cloud credentials TOML near line 2"));
}
