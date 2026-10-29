#![deny(clippy::disallowed_methods)]
use swarmy_infra::{
    Error, download,
    opentofu::{self, Platform, Source},
};
use swarmy_testkit::release::{serve_files, sha256sums, tofu_release};
#[tokio::test]
async fn installs_and_reuses() {
    let host = Platform::host().unwrap();
    let files = tofu_release("1.13.2", host.suffix(), "#!/bin/sh\necho fake\n");
    let digest = sha256sums(&[("asset", &files[0].1)])
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    let addr = serve_files(files.clone());
    let source = Source::new(&format!("http://{addr}"), "1.13.2").unwrap();
    let cache = tempfile::tempdir().unwrap();
    let path = opentofu::ensure(cache.path(), &source, host).await.unwrap();
    assert_eq!(path, cache.path().join("tofu/1.13.2/tofu"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o111,
            0
        );
    }
    assert_eq!(
        std::fs::read_to_string(path.parent().unwrap().join(".sha256")).unwrap(),
        digest
    );
    let addr = serve_files(files);
    let source = Source::new(&format!("http://{addr}"), "1.13.2").unwrap();
    assert_eq!(
        opentofu::ensure(cache.path(), &source, host).await.unwrap(),
        path
    );
    let addr = serve_files(tofu_release(
        "1.13.2",
        host.suffix(),
        "#!/bin/sh\necho changed\n",
    ));
    let source = Source::new(&format!("http://{addr}"), "1.13.2").unwrap();
    opentofu::ensure(cache.path(), &source, host).await.unwrap();
    assert!(std::fs::read_to_string(path).unwrap().contains("changed"));
}
#[tokio::test]
async fn checksum_mismatch_refused() {
    let host = Platform::host().unwrap();
    let mut files = tofu_release("1.13.2", host.suffix(), "#!/bin/sh\n");
    files[0].1 = b"corrupt".to_vec();
    let addr = serve_files(files);
    let cache = tempfile::tempdir().unwrap();
    let error = opentofu::ensure(
        cache.path(),
        &Source::new(&format!("http://{addr}"), "1.13.2").unwrap(),
        host,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        Error::Download(download::Error::ChecksumMismatch { .. })
    ));
    let source = std::error::Error::source(&error).unwrap().to_string();
    assert!(
        source.contains("checksum mismatch")
            && source.contains(&format!("tofu_1.13.2_{}.tar.gz", host.suffix()))
    );
    assert!(!cache.path().join("tofu/1.13.2").exists());
    assert!(
        std::fs::read_dir(cache.path().join("tofu"))
            .unwrap()
            .next()
            .is_none()
    );
}
#[tokio::test]
async fn missing_release_and_entry() {
    let host = Platform::host().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let addr = serve_files(vec![]);
    assert!(matches!(
        opentofu::ensure(
            cache.path(),
            &Source::new(&format!("http://{addr}"), "1.13.2").unwrap(),
            host
        )
        .await,
        Err(Error::ReleaseNotFound { .. })
    ));
    let addr = serve_files(vec![(
        "/v1.13.2/tofu_1.13.2_SHA256SUMS".into(),
        b"empty".to_vec(),
    )]);
    assert!(matches!(
        opentofu::ensure(
            cache.path(),
            &Source::new(&format!("http://{addr}"), "1.13.2").unwrap(),
            host
        )
        .await,
        Err(Error::MissingChecksum { .. })
    ));
}
#[tokio::test]
async fn pins() {
    let host = Platform::host().unwrap();
    let files = tofu_release("1.13.2", host.suffix(), "#!/bin/sh\n");
    let digest = sha256sums(&[("asset", &files[0].1)])
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    let addr = serve_files(files);
    let cache = tempfile::tempdir().unwrap();
    let source = Source::new(&format!("http://{addr}"), "1.13.2")
        .unwrap()
        .pin(host, &"0".repeat(64));
    assert!(matches!(
        opentofu::ensure(cache.path(), &source, host).await,
        Err(Error::PinMismatch { .. })
    ));
    assert!(!cache.path().join("tofu/1.13.2").exists());
    let source = Source::new(&format!("http://{addr}"), "1.13.2")
        .unwrap()
        .pin(host, &digest);
    let path = opentofu::ensure(cache.path(), &source, host).await.unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let closed = listener.local_addr().unwrap();
    drop(listener);
    let offline = Source::new(&format!("http://{closed}"), "1.13.2")
        .unwrap()
        .pin(host, &digest);
    assert_eq!(
        opentofu::ensure(cache.path(), &offline, host)
            .await
            .unwrap(),
        path
    );
}

#[tokio::test]
async fn explicit_override_resolves_without_download() {
    use swarmy_infra::Tofu;
    let cache = tempfile::tempdir().unwrap();
    let missing = cache.path().join("missing-tofu");
    assert!(
        matches!(Tofu::resolve_with(Some(missing.as_os_str()), cache.path()).await, Err(Error::TofuBinMissing(path)) if path == missing)
    );
    let binary = cache.path().join("tofu");
    std::fs::write(&binary, b"fake").unwrap();
    assert_eq!(
        Tofu::resolve_with(Some(binary.as_os_str()), cache.path())
            .await
            .unwrap()
            .binary(),
        binary
    );
}
