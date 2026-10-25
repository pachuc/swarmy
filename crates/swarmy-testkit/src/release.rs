//! Small local release fixtures, with no backing stack dependencies.
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    process::Command,
};

/// Serve GET paths from a detached loopback listener until process exit.
/// # Panics
/// Panics if the fixture cannot bind its listener.
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
            connection
                .write_all(header.as_bytes())
                .expect("fixture response header");
            connection.write_all(body).expect("fixture response body");
        }
    });
    addr
}

/// Package top/name entries using the system tar.
/// # Panics
/// Panics if temporary files or tar fail.
pub fn archive(top: &str, files: &[(&str, &[u8])]) -> Vec<u8> {
    let temp = tempfile::tempdir().expect("fixture directory");
    let dir = temp.path().join(top);
    std::fs::create_dir(&dir).expect("archive directory");
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
#[must_use]
pub fn sha256sums(assets: &[(&str, &[u8])]) -> String {
    assets
        .iter()
        .map(|(name, bytes)| format!("{:x}  {name}\n", Sha256::digest(bytes)))
        .collect()
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
