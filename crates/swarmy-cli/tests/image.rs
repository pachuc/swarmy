use std::process::{Child, Command, Stdio};

#[test]
fn image_commands_are_advertised_and_validate_arguments() {
    for binary in [env!("CARGO_BIN_EXE_swarmy")] {
        let output = Command::new(binary)
            .args(["image", "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let help = String::from_utf8(output.stdout).unwrap();
        for command in ["build", "ls", "show"] {
            assert!(help.contains(command));
        }
        assert!(
            !Command::new(binary)
                .args(["image", "build", "images/base-ubuntu"])
                .output()
                .unwrap()
                .status
                .success()
        );
        let output = Command::new(binary)
            .args(["--json", "image", "show", "missing-tag"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("expected NAME:TAG"));
    }
}

struct Mount<'a>(&'a std::path::Path);
impl Drop for Mount<'_> {
    fn drop(&mut self) {
        assert!(
            Command::new("umount")
                .arg(self.0)
                .status()
                .unwrap()
                .success()
        );
    }
}

/// A `swarmy-api` service process serving the dev stack on a loopback port.
///
/// `image build` goes through the API, but the root-suite environment only
/// provides `FoundationDB`, NATS, and the object store from
/// `scripts/dev-stack.sh`, with no API service. The acceptance tests start
/// their own service against the dev stack and point the `swarmy` binary at
/// it, so they need nothing beyond the dev stack and sudo. The service
/// shares the ambient store directory and object store settings with
/// `swarmyd vol`, so volumes created from uploaded images resolve.
struct ApiService {
    child: Child,
    url: String,
    token: String,
}

impl Drop for ApiService {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Resolve the profile directory holding the test `swarmy` binary, so
/// `CARGO_TARGET_DIR`, `--target`, and custom `--profile` layouts resolve
/// without assuming `debug` or `release`.
fn profile_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_swarmy"))
        .parent()
        .expect("profile directory")
        .to_owned()
}

/// Run `cargo build -p <package> --bin <binary>` once when the sibling
/// binary beside the test profile directory is missing. A warm target
/// directory makes this a no-op, and the profile directory selects the cargo
/// profile while a `<target>/<triple>/<profile>` layout selects `--target`.
fn ensure_sibling(package: &str, binary: &str) -> std::path::PathBuf {
    let profile = profile_dir();
    let path = profile.join(format!("{binary}{}", std::env::consts::EXE_SUFFIX));
    if path.exists() {
        return path;
    }
    // `cargo test -p swarmy-cli` builds the `swarmy` binary but not its
    // siblings, so build the missing one here.
    let mut build = Command::new("cargo");
    build
        .args(["build", "-p", package, "--bin", binary])
        .current_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("workspace root")
                .parent()
                .expect("workspace root"),
        );
    // Cargo does not export the triple for a CLI `--target` build, so read
    // it from the test binary's path instead.
    if let Some(triple) = profile
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| name.contains('-'))
    {
        build.arg("--target").arg(triple);
    }
    match profile
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("debug")
    {
        "debug" => {}
        "release" => {
            build.arg("--release");
        }
        name => {
            build.arg("--profile").arg(name);
        }
    }
    assert!(build.status().unwrap().success());
    assert!(
        path.exists(),
        "build succeeded but {} is missing",
        path.display()
    );
    path
}

async fn start_api_service() -> ApiService {
    let api = ensure_sibling("swarmy-api", "swarmy-api");
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let token = ulid::Ulid::generate().to_string();
    let mut child = Command::new(&api)
        .env("SWARMY_API_LISTEN", format!("127.0.0.1:{port}"))
        .env("SWARMY_API_TOKEN", &token)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // The service opens the store and bus before binding, so an open port
    // means it is ready for uploads.
    let started = std::time::Instant::now();
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("swarmy-api exited during startup: {status}");
        }
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            break;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(60),
            "swarmy-api did not listen on 127.0.0.1:{port} within 60s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    ApiService {
        child,
        url: format!("http://127.0.0.1:{port}"),
        token,
    }
}

fn image_json(api: &ApiService, arguments: &[&str]) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .arg("--json")
        .arg("image")
        .args(arguments)
        .env("SWARMY_API_URL", &api.url)
        .env("SWARMY_API_TOKEN", &api.token)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// Run the sibling `swarmyd` binary beside the test `swarmy` binary. Volume
/// management moved to the node daemon, so the retag property is observed
/// through `swarmyd vol` rather than the store.
fn swarmyd_json(arguments: &[&str]) -> serde_json::Value {
    let swarmyd = ensure_sibling("swarmyd", "swarmyd");
    let output = Command::new(&swarmyd)
        .arg("vol")
        .arg("--json")
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn needs_backing_services() -> bool {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping image acceptance test: not running as root");
        return false;
    }
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_NATS_URL",
        "SWARMY_S3_ENDPOINT",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping image acceptance test: {variable} is unset");
            return false;
        }
    }
    true
}

#[tokio::test]
async fn root_base_ubuntu_acceptance() {
    if !needs_backing_services() {
        return;
    }
    let api = start_api_service().await;
    let tag = format!("test-{}", ulid::Ulid::generate());
    let recipe = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../images/base-ubuntu");
    let directory = tempfile::tempdir().unwrap();
    let raw = directory.path().join("base.ext4");
    let started = std::time::Instant::now();
    let first = image_json(
        &api,
        &[
            "build",
            recipe.to_str().unwrap(),
            "--tag",
            &tag,
            "--output",
            raw.to_str().unwrap(),
        ],
    );
    eprintln!("first Ubuntu build ({:?}): {first}", started.elapsed());
    assert_eq!(first["size"], 8_u64 * 1024 * 1024 * 1024);
    assert_eq!(first["chunks_total"], 32768);
    assert!(first["chunks_stored"].as_u64().unwrap() > 0);
    let reference = format!("base-ubuntu:{tag}");
    check_registration(&api, &reference, &tag, &first);
    // The server mints a fresh manifest id on every upload, so the retag
    // property is read from the volume record: a volume created on the first
    // manifest keeps that manifest after the rebuild.
    let created = swarmyd_json(&["create", &reference]);
    let volume_id = created["volume_id"].as_str().unwrap().to_owned();
    assert_eq!(created["manifest_id"], first["manifest_id"]);
    check_chroot(directory.path(), &raw);
    let started = std::time::Instant::now();
    let second_raw = directory.path().join("second.ext4");
    let second = image_json(
        &api,
        &[
            "build",
            recipe.to_str().unwrap(),
            "--tag",
            &tag,
            "--output",
            second_raw.to_str().unwrap(),
        ],
    );
    eprintln!("second Ubuntu build ({:?}): {second}", started.elapsed());
    if first["header"]["root_hash"] != second["header"]["root_hash"] {
        eprintln!(
            "nonidentical images retained for diagnosis at {}",
            directory.keep().display()
        );
        panic!("repeated image builds have different root hashes");
    }
    assert_eq!(
        second["chunks_uploaded"], 0,
        "identical image uploaded new chunks"
    );
    assert_eq!(
        image_json(&api, &["show", &reference])["manifest_id"],
        second["manifest_id"]
    );
    // Retagging must leave the already-created volume on its original
    // manifest, read from the volume record rather than the upload response.
    let shown_volume = swarmyd_json(&["show", &volume_id]);
    assert_eq!(
        shown_volume["record"]["head_manifest"],
        first["manifest_id"]
    );
}

fn check_registration(api: &ApiService, reference: &str, tag: &str, first: &serde_json::Value) {
    let shown = image_json(api, &["show", reference]);
    assert_eq!(shown["header"], first["header"]);
    assert_eq!(shown["manifest_id"], first["manifest_id"]);
    let listed = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .args(["--json", "image", "ls"])
        .env("SWARMY_API_URL", &api.url)
        .env("SWARMY_API_TOKEN", &api.token)
        .output()
        .unwrap();
    assert!(listed.status.success());
    assert!(
        String::from_utf8(listed.stdout)
            .unwrap()
            .lines()
            .any(|line| {
                let record: serde_json::Value = serde_json::from_str(line).unwrap();
                record["name"] == "base-ubuntu"
                    && record["tag"] == tag
                    && record["manifest_id"] == first["manifest_id"]
            })
    );
}

fn check_chroot(directory: &std::path::Path, raw: &std::path::Path) {
    let mounted = directory.join("mounted");
    std::fs::create_dir(&mounted).unwrap();
    assert!(
        Command::new("mount")
            .args(["-o", "loop,ro,noload"])
            .arg(raw)
            .arg(&mounted)
            .status()
            .unwrap()
            .success()
    );
    {
        let _mount = Mount(&mounted);
        let output = Command::new("chroot")
            .arg(&mounted)
            .args([
                "/bin/bash",
                "-ec",
                "git --version; gh --version; rg --version; fd --version; jq --version; node --version; npm --version; python3 --version; pip --version; cc --version; curl --version; pkg-config --version; test -d /home/agent/work",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!("chroot: {}", String::from_utf8_lossy(&output.stdout));
    }
}

#[tokio::test]
async fn root_custom_recipe_registers_requested_name() {
    if !needs_backing_services() {
        return;
    }
    let api = start_api_service().await;
    let dir = tempfile::tempdir().unwrap();
    let recipe = dir.path().join("custom-recipe");
    std::fs::create_dir_all(recipe.join("rootfs")).unwrap();
    std::fs::write(recipe.join("recipe.toml"), "disk_size = 16777216\nsource_date_epoch = 1714003200\n[source]\nkind = 'directory'\npath = 'rootfs'\n").unwrap();
    let tag = format!("override-{}", ulid::Ulid::generate());
    let built = image_json(
        &api,
        &[
            "build",
            recipe.to_str().unwrap(),
            "--tag",
            &tag,
            "--name",
            "base-ubuntu",
        ],
    );
    assert_eq!(built["name"], "base-ubuntu");
    check_registration(&api, &format!("base-ubuntu:{tag}"), &tag, &built);
}
