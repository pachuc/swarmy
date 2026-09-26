use std::process::Command;

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

fn image_json(arguments: &[&str]) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .arg("--json")
        .arg("image")
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

/// Run the sibling `swarmyd` binary beside the test `swarmy` binary. Volume
/// management moved to the node daemon, so the retag property is observed
/// through `swarmyd vol` rather than the store.
fn swarmyd_json(arguments: &[&str]) -> serde_json::Value {
    let swarmy = std::path::PathBuf::from(env!("CARGO_BIN_EXE_swarmy"));
    let profile = swarmy.parent().expect("profile directory");
    let swarmyd = profile.join(format!("swarmyd{}", std::env::consts::EXE_SUFFIX));
    if !swarmyd.exists() {
        // `cargo test -p swarmy-cli` does not build the daemon binary, so
        // build it once; a warm target directory makes this a no-op. The
        // profile directory comes from `CARGO_BIN_EXE_swarmy`, so custom
        // `--profile`, `CARGO_TARGET_DIR`, and `--target` layouts resolve the
        // same way as the `cli_bin` helper in `swarmy-api`.
        let mut build = Command::new("cargo");
        build
            .args(["build", "-p", "swarmyd", "--bin", "swarmyd"])
            .current_dir(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("workspace root")
                    .parent()
                    .expect("workspace root"),
            );
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
    }
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

#[tokio::test]
async fn root_base_ubuntu_acceptance() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping Ubuntu acceptance test: not running as root");
        return;
    }
    for variable in ["SWARMY_FDB_CLUSTER_FILE", "SWARMY_S3_ENDPOINT"] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping Ubuntu acceptance test: {variable} is unset");
            return;
        }
    }
    let tag = format!("test-{}", ulid::Ulid::generate());
    let recipe = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../images/base-ubuntu");
    let directory = tempfile::tempdir().unwrap();
    let raw = directory.path().join("base.ext4");
    let started = std::time::Instant::now();
    let first = image_json(&[
        "build",
        recipe.to_str().unwrap(),
        "--tag",
        &tag,
        "--output",
        raw.to_str().unwrap(),
    ]);
    eprintln!("first Ubuntu build ({:?}): {first}", started.elapsed());
    assert_eq!(first["size"], 8_u64 * 1024 * 1024 * 1024);
    assert_eq!(first["chunks_total"], 32768);
    assert!(first["chunks_stored"].as_u64().unwrap() > 0);
    let reference = format!("base-ubuntu:{tag}");
    check_registration(&reference, &tag, &first);
    // The server mints a fresh manifest id on every upload, so the retag
    // property is read from the volume record: a volume created on the first
    // manifest keeps that manifest after the rebuild.
    let created = swarmyd_json(&["create", &reference]);
    let volume_id = created["volume_id"].as_str().unwrap().to_owned();
    assert_eq!(created["manifest_id"], first["manifest_id"]);
    check_chroot(directory.path(), &raw);
    let started = std::time::Instant::now();
    let second_raw = directory.path().join("second.ext4");
    let second = image_json(&[
        "build",
        recipe.to_str().unwrap(),
        "--tag",
        &tag,
        "--output",
        second_raw.to_str().unwrap(),
    ]);
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
        image_json(&["show", &reference])["manifest_id"],
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

fn check_registration(reference: &str, tag: &str, first: &serde_json::Value) {
    let shown = image_json(&["show", reference]);
    assert_eq!(shown["header"], first["header"]);
    assert_eq!(shown["manifest_id"], first["manifest_id"]);
    let listed = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .args(["--json", "image", "ls"])
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

#[test]
fn root_custom_recipe_registers_requested_name() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n"
        || std::env::var_os("SWARMY_FDB_CLUSTER_FILE").is_none()
        || std::env::var_os("SWARMY_S3_ENDPOINT").is_none()
    {
        eprintln!("skipping image name test: needs root and backing services");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let recipe = dir.path().join("custom-recipe");
    std::fs::create_dir_all(recipe.join("rootfs")).unwrap();
    std::fs::write(recipe.join("recipe.toml"), "disk_size = 16777216\nsource_date_epoch = 1714003200\n[source]\nkind = 'directory'\npath = 'rootfs'\n").unwrap();
    let tag = format!("override-{}", ulid::Ulid::generate());
    let built = image_json(&[
        "build",
        recipe.to_str().unwrap(),
        "--tag",
        &tag,
        "--name",
        "base-ubuntu",
    ]);
    assert_eq!(built["name"], "base-ubuntu");
    check_registration(&format!("base-ubuntu:{tag}"), &tag, &built);
}
