//! Small local release fixtures, with no backing stack dependencies.
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    path::Path,
    process::Command,
};

/// Serve GET paths from a detached loopback listener until process exit.
/// # Panics
/// Panics if the fixture cannot bind its listener.
#[must_use]
pub fn serve_files(files: Vec<(String, Vec<u8>)>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener binds");
    let addr = listener.local_addr().expect("fixture listener address");
    std::thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(mut connection) = connection else {
                break;
            };
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while request.len() < 65536 && !request.ends_with(b"\r\n\r\n") {
                let Ok(size) = connection.read(&mut buffer) else {
                    break;
                };
                if size == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..size]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let line = String::from_utf8_lossy(&request);
            let path = line
                .split_whitespace()
                .nth(1)
                .unwrap_or("")
                .split('?')
                .next()
                .unwrap_or("");
            let found = files.iter().find(|(name, _)| name == path);
            let (status, body): (&str, &[u8]) =
                found.map_or(("404 Not Found", b""), |(_, bytes)| ("200 OK", bytes));
            let header = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if connection.write_all(header.as_bytes()).is_err() {
                continue;
            }
            drop(connection.write_all(body));
        }
    });
    addr
}

/// Package top/name entries using the system tar.
/// # Panics
/// Panics if temporary files or tar fail.
#[must_use]
pub fn archive(top: &str, files: &[(&str, &[u8])]) -> Vec<u8> {
    let temp = tempfile::tempdir().expect("fixture directory");
    let dir = temp.path().join(top);
    std::fs::create_dir(&dir).expect("archive directory");
    write_members(&dir, files);
    let output = Command::new("tar")
        .arg("-czf")
        .arg("-")
        .arg("-C")
        .arg(temp.path())
        .arg(top)
        .output()
        .expect("tar runs");
    assert!(output.status.success(), "tar succeeds");
    output.stdout
}
fn write_members(dir: &Path, files: &[(&str, &[u8])]) {
    for (name, bytes) in files {
        let path = dir.join(name);
        std::fs::write(&path, bytes).expect("archive member");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("member mode");
        }
    }
}
/// Package archive members at the root.
/// # Panics
/// Panics if temporary files or tar fail.
#[must_use]
pub fn flat_archive(files: &[(&str, &[u8])]) -> Vec<u8> {
    let temp = tempfile::tempdir().expect("fixture directory");
    write_members(temp.path(), files);
    let output = Command::new("tar")
        .args(["-czf", "-", "-C"])
        .arg(temp.path())
        .args(files.iter().map(|(name, _)| name))
        .output()
        .expect("tar runs");
    assert!(output.status.success(), "tar succeeds");
    output.stdout
}
/// Serve a flat OpenTofu release fixture.
/// # Panics
/// Panics if temporary files or tar fail.
#[must_use]
pub fn tofu_release(version: &str, platform: &str, tofu_script: &str) -> Vec<(String, Vec<u8>)> {
    let asset = format!("tofu_{version}_{platform}.tar.gz");
    let archive = flat_archive(&[("tofu", tofu_script.as_bytes()), ("LICENSE", b"fixture")]);
    let sums = sha256sums(&[(&asset, &archive)]);
    vec![
        (format!("/v{version}/{asset}"), archive),
        (
            format!("/v{version}/tofu_{version}_SHA256SUMS"),
            sums.into_bytes(),
        ),
    ]
}
#[must_use]
pub fn sha256sums(assets: &[(&str, &[u8])]) -> String {
    use std::fmt::Write as _;
    let mut sums = String::new();
    for (name, bytes) in assets {
        writeln!(sums, "{:x}  {name}", Sha256::digest(bytes))
            .expect("writing to String cannot fail");
    }
    sums
}
#[must_use]
pub fn fake_release(tag: &str, target: &str, core_script: &str) -> Vec<(String, Vec<u8>)> {
    let core_name = format!("swarmy-core-{target}");
    let client_name = format!("swarmy-{target}");
    let core = archive(
        &core_name,
        &[
            ("swarmy-core", core_script.as_bytes()),
            ("libfdb_c.so", b"dummy"),
        ],
    );
    let client = archive(&client_name, &[("swarmy", b"#!/bin/sh\n")]);
    let core_asset = format!("{core_name}.tar.gz");
    let client_asset = format!("{client_name}.tar.gz");
    let sums = sha256sums(&[(&core_asset, &core), (&client_asset, &client)]);
    vec![
        (format!("/{tag}/{core_asset}"), core),
        (format!("/{tag}/{client_asset}"), client),
        (format!("/{tag}/SHA256SUMS"), sums.into_bytes()),
    ]
}
#[must_use]
pub fn host_target() -> String {
    format!("{}-unknown-linux-gnu", std::env::consts::ARCH)
}

/// A minimal base-image recipe with a 16 MiB disk and directory source.
pub const FAKE_IMAGE_RECIPE: &str = "disk_size = 16777216\nsource_date_epoch = 1714003200\n\n[sandbox]\nscratch = [\"/tmp\"]\nmemory_mib = 768\n\n[source]\nkind = \"directory\"\npath = \"rootfs\"\n";

/// Package a sparse disk and its recipe using GNU tar's sparse format.
/// # Panics
/// Panics when fixture files cannot be created or tar fails.
#[must_use]
pub fn image_archive(top: &str, size: u64, data: &[(u64, &[u8])], recipe: &str) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    let temp = tempfile::tempdir().expect("fixture directory");
    let dir = temp.path().join(top);
    std::fs::create_dir(&dir).expect("image archive directory");
    let disk = std::fs::File::create(dir.join("disk.ext4")).expect("sparse disk");
    disk.set_len(size).expect("disk size");
    for (offset, bytes) in data {
        disk.write_all_at(bytes, *offset).expect("disk extent");
    }
    std::fs::write(dir.join("recipe.toml"), recipe).expect("image recipe");
    let output = Command::new("tar")
        .args(["--sparse", "-czf", "-"])
        .arg("-C")
        .arg(temp.path())
        .arg(top)
        .output()
        .expect("tar runs");
    assert!(output.status.success(), "tar succeeds");
    output.stdout
}
