use super::*;
use std::io::{BufRead, Write};

struct Fake {
    child: Child,
    output: std::io::BufReader<std::process::ChildStdout>,
    port: u16,
    files: tempfile::TempDir,
}
impl Fake {
    fn new(first: &str, second: &str) -> Self {
        let files = tempfile::tempdir().unwrap();
        assert!(
            Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=localhost",
                    "-addext",
                    "subjectAltName=DNS:localhost",
                    "-keyout",
                ])
                .arg(files.path().join("key.pem"))
                .arg("-out")
                .arg(files.path().join("cert.pem"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success()
        );
        let mut child = Command::new("python3")
            .args(["-c", include_str!("github_fake.py")])
            .arg(files.path().join("cert.pem"))
            .arg(files.path().join("key.pem"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        writeln!(
            child.stdin.take().unwrap(),
            "{}",
            serde_json::json!({"first": first, "second": second})
        )
        .unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        output.read_line(&mut line).unwrap();
        Self {
            child,
            output,
            port: line.trim().parse().unwrap(),
            files,
        }
    }
    fn authorized(&mut self) {
        for _ in 0..2 {
            let mut line = String::new();
            self.output.read_line(&mut line).unwrap();
            assert_eq!(line, "authorized\n");
        }
    }
}
impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_github_credentials_never_enter_disk_or_snapshot() {
    if Command::new("id").arg("-u").output().unwrap().stdout != b"0\n" {
        eprintln!("skipping GitHub acceptance: run the built executable with sudo");
        return;
    }
    for variable in [
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_S3_ENDPOINT",
        "SWARMY_NATS_URL",
    ] {
        if std::env::var_os(variable).is_none() {
            eprintln!("skipping GitHub acceptance: {variable} is unset");
            return;
        }
    }
    boot_network();
    let mut settings = swarmy_config::Settings::load().unwrap().settings;
    let images = store(&settings).await;
    let base = base_image(&settings, &images).await;
    settings.store_directory = format!("swarmy-github-test-{}", ulid::Ulid::generate());
    let store = store(&settings).await;
    store
        .put_manifest(base, &images.get_manifest(base).await.unwrap().unwrap())
        .await
        .unwrap();
    store
        .put_image("credentials", &ImageTag("test".into()), base)
        .await
        .unwrap();
    let first = format!("test_first_{}", ulid::Ulid::generate());
    let second = format!("test_second_{}", ulid::Ulid::generate());
    let agent = store
        .create_agent_with_github_token(
            "github",
            "credentials:test",
            "",
            Some(&first),
            jiff::Timestamp::now(),
        )
        .await
        .unwrap();
    let volume = VolumeId::from_ulid(agent.agent_id.as_ulid());
    store.create_volume(volume, base).await.unwrap();
    let mut node = Node::new(settings);
    node.start();
    node.ready(&store, jiff::Timestamp::UNIX_EPOCH).await;
    let sandbox = match node
        .request(Request::Create {
            spec: SandboxSpec {
                agent_id: agent.agent_id,
            },
            disk: BlockDevice { volume_id: volume },
        })
        .await
    {
        Response::Sandbox(sandbox) => sandbox,
        response => panic!("create: {response:?}"),
    };
    exercise(&node, &store, &sandbox, volume, &first, &second).await;
    node.destroy(sandbox).await;
    node.stop().await;
    eprintln!(
        "GitHub acceptance passed: tools, git and gh authorization, rotation, refusal after clear, disk and all snapshot chunks contain neither token"
    );
}

async fn exercise(
    node: &Node,
    store: &Store,
    sandbox: &Sandbox,
    volume: VolumeId,
    first: &str,
    second: &str,
) {
    let mut fake = Fake::new(first, second);
    let bundle = node
        .root
        .path()
        .join(format!(".swarmy/node/bundles/{}", sandbox.agent_id));
    std::fs::copy(
        fake.files.path().join("cert.pem"),
        bundle.join("guest/test-ca.pem"),
    )
    .unwrap();
    let versions = "git --version && gh --version && rg --version && fd --version && jq --version && node --version && npm --version && python3 --version && pip --version && cc --version && pkg-config --version && test -d /home/agent/work";
    let (result, _, stderr) = node.exec(sandbox, versions, 30_000).await;
    assert_eq!(result.exit_code, 0, "{}", String::from_utf8_lossy(&stderr));
    for path in ["first", "second"] {
        if path == "second" {
            store
                .set_agent_github_token(sandbox.agent_id, Some(second))
                .await
                .unwrap();
        }
        let command = format!(
            "export GH_HOST=localhost:{port} SSL_CERT_FILE=/run/swarmy/test-ca.pem GIT_SSL_CAINFO=/run/swarmy/test-ca.pem GIT_TERMINAL_PROMPT=0; git ls-remote https://localhost:{port}/{path} && gh api https://localhost:{port}/{path}",
            port = fake.port
        );
        let (result, stdout, stderr) = node.exec(sandbox, &command, 30_000).await;
        assert_eq!(result.exit_code, 0, "{}", String::from_utf8_lossy(&stderr));
        assert!(String::from_utf8_lossy(&stdout).contains("true"));
        fake.authorized();
    }
    assert_eq!(
        node.exec(
            sandbox,
            "gh config set prompt disabled && test -f /run/swarmy-gh/config.yml",
            10_000
        )
        .await
        .0
        .exit_code,
        0
    );
    assert!(!bundle.join("rootfs/run/swarmy-gh/config.yml").exists());
    store
        .set_agent_github_token(sandbox.agent_id, None)
        .await
        .unwrap();
    assert_ne!(
        node.exec(sandbox, "gh api user", 10_000).await.0.exit_code,
        0
    );
    let config = swarmy_volume::server::ServerConfig {
        directory: node.root.path().join(".swarmy/volumes"),
        node: node.id,
        store: store.clone(),
        objects: node.settings.object_store().unwrap(),
    };
    let snapshot = swarmy_volume::server::checkpoint(&config, volume, Some(bundle.join("rootfs")))
        .await
        .unwrap();
    scan_files(
        &bundle.join("rootfs"),
        &[first.as_bytes(), second.as_bytes()],
    );
    scan_chunks(
        store,
        &node.settings,
        snapshot,
        &[first.as_bytes(), second.as_bytes()],
    )
    .await;
}

fn scan_files(path: &Path, tokens: &[&[u8]]) {
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            scan_files(&entry.path(), tokens);
        } else if kind.is_file() {
            let bytes = std::fs::read(entry.path()).unwrap();
            for token in tokens {
                assert!(
                    !bytes.windows(token.len()).any(|window| window == *token),
                    "credential in {}",
                    entry.path().display()
                );
            }
        }
    }
}

async fn scan_chunks(
    store: &Store,
    settings: &swarmy_config::Settings,
    id: ManifestId,
    tokens: &[&[u8]],
) {
    let objects = settings.object_store().unwrap();
    let header = store.get_manifest(id).await.unwrap().unwrap();
    let blocks = header.size / u64::from(swarmy_core::CHUNK_SIZE);
    let manifest = swarmy_volume::Manifest::load(objects.as_ref(), header)
        .await
        .unwrap();
    let chunks = swarmy_volume::ChunkStore::new(objects.clone());
    let mut seen = std::collections::HashSet::new();
    for block in 0..blocks {
        let hash = manifest.chunk_hash(objects.as_ref(), block).await.unwrap();
        if hash == swarmy_core::ContentHash::ZERO || !seen.insert(hash) {
            continue;
        }
        let bytes = chunks.get_chunk(hash).await.unwrap();
        for token in tokens {
            assert!(
                !bytes.windows(token.len()).any(|window| window == *token),
                "credential in snapshot chunk"
            );
        }
    }
}
