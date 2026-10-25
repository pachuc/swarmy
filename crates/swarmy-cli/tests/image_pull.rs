#![cfg(target_os = "linux")]
#![deny(clippy::disallowed_methods)]
#[path = "support/fake_api.rs"]
mod fake_api;
use std::{
    fs,
    process::{Command, Output},
};
use swarmy_testkit::release::{
    FAKE_IMAGE_RECIPE, host_target, image_archive, serve_files, sha256sums,
};

const CLI: &str = env!("CARGO_BIN_EXE_swarmy");
fn run(home: &std::path::Path, args: &[&str], release: Option<&str>) -> Output {
    let mut command = Command::new(CLI);
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("SWARMY_") {
            command.env_remove(key);
        }
    }
    if let Some(release) = release {
        command.env("SWARMY_RELEASE_BASE_URL", release);
    }
    command
        .args(args)
        .current_dir(home)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .output()
        .unwrap()
}
fn setup(
    registered: bool,
) -> (
    tempfile::TempDir,
    String,
    std::sync::Arc<std::sync::Mutex<Vec<fake_api::Request>>>,
) {
    let home = tempfile::tempdir().unwrap();
    let (address, requests) = fake_api::serve(registered);
    fs::create_dir_all(home.path().join(".swarmy")).unwrap();
    fs::write(
        home.path().join(".swarmy/config.toml"),
        format!("[api]\nurl = \"http://{address}\"\ntoken = \"x\"\n"),
    )
    .unwrap();
    let tag = swarmy_version::RELEASE_TAG;
    let top = format!("swarmy-image-base-ubuntu-{}", host_target());
    let asset = format!("{top}.tar.gz");
    let archive = image_archive(
        &top,
        16 * 1024 * 1024,
        &[(0, &[1; 4096]), (8 * 1024 * 1024, &[2; 4096])],
        FAKE_IMAGE_RECIPE,
    );
    let sums = sha256sums(&[(&asset, &archive)]);
    let url = serve_files(vec![
        (format!("/{tag}/{asset}"), archive),
        (format!("/{tag}/SHA256SUMS"), sums.into_bytes()),
    ]);
    (home, format!("http://{url}"), requests)
}
#[test]
fn image_pull_verifies_uploads_sparse_and_sets_default() {
    let (home, url, requests) = setup(false);
    let output = run(
        home.path(),
        &["image", "pull", "base-ubuntu", "--set-default"],
        Some(&url),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = requests.lock().unwrap();
    let posts: Vec<_> = requests.iter().filter(|r| r.method == "POST").collect();
    assert_eq!(posts.len(), 1);
    let post = posts[0];
    for query in [
        "name=base-ubuntu",
        "size=16777216",
        "scratch=%2Ftmp",
        "memory_mib=768",
        "display=false",
    ] {
        assert!(
            post.path_and_query.contains(query),
            "{}",
            post.path_and_query
        );
    }
    assert!(
        post.path_and_query
            .contains(&format!("tag={}", swarmy_version::RELEASE_TAG))
    );
    assert!((8192 + 32..16 * 1024 * 1024).contains(&post.body_len));
    assert_eq!(post.first_offset, Some(0));
    let settings = swarmy_config::Settings::read(&home.path().join(".swarmy/config.toml")).unwrap();
    assert_eq!(
        settings.selection.default_image.as_deref(),
        Some(format!("base-ubuntu:{}", swarmy_version::RELEASE_TAG).as_str())
    );
    assert!(
        !home
            .path()
            .join(format!(
                ".swarmy/images/{}/swarmy-image-base-ubuntu-{}",
                swarmy_version::RELEASE_TAG,
                host_target()
            ))
            .exists()
    );
}
#[test]
fn image_pull_refuses_a_corrupt_asset() {
    let (home, _, requests) = setup(false);
    let tag = swarmy_version::RELEASE_TAG;
    let asset = format!("swarmy-image-base-ubuntu-{}.tar.gz", host_target());
    let sums = sha256sums(&[(&asset, b"other" as &[u8])]);
    let address = serve_files(vec![
        (format!("/{tag}/{asset}"), b"corrupt".to_vec()),
        (format!("/{tag}/SHA256SUMS"), sums.into_bytes()),
    ]);
    let output = run(
        home.path(),
        &["image", "pull"],
        Some(&format!("http://{address}")),
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("checksum mismatch"));
    assert!(!requests.lock().unwrap().iter().any(|r| r.method == "POST"));
}
#[test]
fn image_pull_skips_a_registered_image() {
    let (home, url, requests) = setup(true);
    let output = run(home.path(), &["image", "pull"], Some(&url));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("already registered"));
    assert!(!requests.lock().unwrap().iter().any(|r| r.method == "POST"));
}
#[test]
fn image_pull_needs_a_release() {
    let (home, _, _) = setup(false);
    let output = run(home.path(), &["image", "pull"], None);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("swarmy image build images/base-ubuntu")
    );
}
#[test]
fn image_build_no_register_requires_output() {
    let (home, _, _) = setup(false);
    let output = run(
        home.path(),
        &["image", "build", "images/base-ubuntu", "--no-register"],
        None,
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("--output"));
}
