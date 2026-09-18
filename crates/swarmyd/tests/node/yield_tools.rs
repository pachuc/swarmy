use super::{Node, persistent::invoke};
use serde_json::json;
use std::time::{Duration, Instant};
use swarmy_bus::Bus;
use swarmy_core::AgentId;
use swarmy_store::Store;

pub(super) async fn run(node: &Node, store: &Store, bus: &Bus) {
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    // Boot before measuring the yield so image hydration is outside the timer.
    invoke(node, store, bus, agent, "bash", json!({"command":"true"})).await;
    let start = Instant::now();
    let yielded = invoke(
        node,
        store,
        bus,
        agent,
        "bash",
        json!({"command":"echo ready; sleep 300", "yield_seconds":1}),
    )
    .await;
    assert!(start.elapsed() < Duration::from_secs(5));
    assert_eq!(yielded["backgrounded"], true);
    assert_eq!(yielded["timed_out"], false);
    assert_eq!(yielded["stdout"], "ready\n");
    let id = &yielded["process_id"];
    let listed = invoke(node, store, bus, agent, "process_list", json!({})).await;
    assert!(
        listed
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["process_id"] == *id && row["status"] == "running")
    );
    let log = invoke(
        node,
        store,
        bus,
        agent,
        "process_log",
        json!({"process_id":id}),
    )
    .await;
    assert_eq!(log["output"], "ready\n");
    invoke(
        node,
        store,
        bus,
        agent,
        "process_stop",
        json!({"process_id":id}),
    )
    .await;
    let timed = invoke(
        node,
        store,
        bus,
        agent,
        "bash",
        json!({"command":"sleep 300", "timeout_ms":100}),
    )
    .await;
    assert_eq!(timed["backgrounded"], true);
    assert_eq!(timed["timed_out"], true);
    invoke(
        node,
        store,
        bus,
        agent,
        "process_stop",
        json!({"process_id":timed["process_id"]}),
    )
    .await;
    spill(node, store, bus, agent).await;
    stdin(node, store, bus, agent).await;
    web_fetch(node, store, bus, agent).await;
    eprintln!(
        "bash acceptance passed: live yielded process, list/log/stop, timeout backgrounding, head/tail spill, interactive stdin, and sandbox web_fetch"
    );
}

async fn spill(node: &Node, store: &Store, bus: &Bus, agent: AgentId) {
    let result = invoke(
        node,
        store,
        bus,
        agent,
        "bash",
        json!({"command":"printf HEAD; head -c 70000 /dev/zero | tr '\\0' x; printf TAIL"}),
    )
    .await;
    let output = result["stdout"].as_str().unwrap();
    assert!(output.starts_with("HEAD"));
    assert!(output.ends_with("TAIL"));
    assert!(output.len() <= 32768);
    assert!(output.contains("bytes elided; full output at"));
    let path = result["log_path"].as_str().unwrap();
    assert!(path.starts_with("/home/agent/.swarmy/output/"));
    let bytes = std::fs::read(
        node.root
            .path()
            .join(format!(".swarmy/node/bundles/{agent}/rootfs{path}")),
    )
    .unwrap();
    assert_eq!(bytes.len(), 70008);
    assert_eq!(&bytes[..4], b"HEAD");
    assert_eq!(&bytes[70004..], b"TAIL");
    assert!(bytes[4..70004].iter().all(|byte| *byte == b'x'));
}

async fn stdin(node: &Node, store: &Store, bus: &Bus, agent: AgentId) {
    let result = invoke(
        node,
        store,
        bus,
        agent,
        "bash",
        json!({"command":"read -r line; printf 'received:%s' \"$line\"", "yield_seconds":0}),
    )
    .await;
    let id = &result["process_id"];
    let written = invoke(
        node,
        store,
        bus,
        agent,
        "write_stdin",
        json!({"process_id":id, "text":"hello\n"}),
    )
    .await;
    assert_eq!(written["bytes_written"], 6);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let log = invoke(
                node,
                store,
                bus,
                agent,
                "process_log",
                json!({"process_id":id}),
            )
            .await;
            if log["status"] == "exited" {
                assert_eq!(log["output"], "received:hello");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

async fn web_fetch(node: &Node, store: &Store, bus: &Bus, agent: AgentId) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let command = format!(
        "mkdir -p /tmp/web-test; printf '<h1>Sandbox page</h1><p>hello &amp; goodbye</p>' > /tmp/web-test/index.html; exec python3 -m http.server {port} --bind 127.0.0.1 --directory /tmp/web-test"
    );
    let process = invoke(
        node,
        store,
        bus,
        agent,
        "bash",
        json!({"command":command, "yield_seconds":1}),
    )
    .await;
    let result = invoke(
        node,
        store,
        bus,
        agent,
        "web_fetch",
        json!({"url":format!("http://127.0.0.1:{port}/")}),
    )
    .await;
    assert_eq!(result["output"], "Sandbox page\nhello & goodbye");
    invoke(
        node,
        store,
        bus,
        agent,
        "process_stop",
        json!({"process_id":process["process_id"]}),
    )
    .await;
}
