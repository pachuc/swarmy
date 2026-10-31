#![deny(clippy::disallowed_methods)]
use std::{sync::Arc, time::Duration};
use swarmy_core::{NodeCapacity, NodeId, NodeRole, NodeToken};
use swarmy_node_protocol::{ClientError, NodeAdvert, NodeClient, RegisterRequest};
use swarmy_store::{ServiceRole, blob::MemoryBlobStore};
use ulid::Ulid;
#[tokio::test]
async fn node_api_role_serves_and_honours_revocation() {
    let Some(stack) = swarmy_testkit::Stack::load("node-api-role") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let node = NodeId::from_ulid(Ulid::generate());
    let command = |action| {
        std::process::Command::new(swarmy_testkit::bin("swarmy-core"))
            .args(["node-credential", action, &node.to_string()])
            .env("SWARMY_FDB_CLUSTER_FILE", &stack.cluster)
            .env("SWARMY_STORE_DIRECTORY", &stack.prefix)
            .output()
            .unwrap()
    };
    let issued = command("issue");
    assert!(
        issued.status.success(),
        "issue exited with {}",
        issued.status
    );
    let token = NodeToken::parse(std::str::from_utf8(&issued.stdout).unwrap()).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = tokio::process::Command::new(swarmy_testkit::bin("swarmy-core"))
        .arg("node-api")
        .env("SWARMY_FDB_CLUSTER_FILE", &stack.cluster)
        .env("SWARMY_STORE_DIRECTORY", &stack.prefix)
        .env("SWARMY_NODE_API_LISTEN", format!("127.0.0.1:{port}"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut service = swarmy_testkit::ChildGuard::new(child);
    swarmy_testkit::eventually("node API listens", Duration::from_secs(60), async || {
        assert!(!service.has_exited());
        std::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .is_ok()
            .then_some(())
    })
    .await;
    let client = NodeClient::new(&format!("http://127.0.0.1:{port}"), &token).unwrap();
    let request = RegisterRequest {
        node: NodeAdvert {
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1 << 30,
                sandboxes: 1,
            },
            gpu: None,
        },
    };
    client.register(&request).await.unwrap();
    assert!(client.placements().await.unwrap().placements.is_empty());
    swarmy_testkit::eventually("node API heartbeat", Duration::from_secs(30), async || {
        store
            .list_services()
            .await
            .unwrap()
            .iter()
            .any(|row| row.heartbeat.role == ServiceRole::NodeApi && row.alive)
            .then_some(())
    })
    .await;
    assert!(command("revoke").status.success());
    swarmy_testkit::eventually(
        "node credential revocation",
        Duration::from_secs(30),
        async || {
            matches!(
                client.register(&request).await,
                Err(ClientError::Unauthorized)
            )
            .then_some(())
        },
    )
    .await;
}
