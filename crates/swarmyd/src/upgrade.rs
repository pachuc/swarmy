//! Query the running node through its existing control protocol before replacing it.
//! This runs in the newly built binary while the old daemon still owns the runtime.
use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use swarmy_core::{AgentId, ExecOutput, ExecRequest, Sandbox};
use swarmy_node_protocol::{NodeClient, PlacementOccupancy};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

async fn process_list(socket: &Path, agent: AgentId, epoch: u64) -> Result<bool> {
    process_list_with_timeout(socket, agent, epoch, Duration::from_secs(20)).await
}

async fn process_list_with_timeout(
    socket: &Path,
    agent: AgentId,
    epoch: u64,
    frame_timeout: Duration,
) -> Result<bool> {
    let mut stream = UnixStream::connect(socket)
        .await
        .context("connect running swarmyd control socket")?;
    let request = crate::Request::Exec {
        sandbox: Sandbox { agent_id: agent },
        request: ExecRequest {
            args: vec![
                "/usr/bin/python3".into(),
                "-c".into(),
                include_str!("processes.py").into(),
                "process_list".into(),
                epoch.to_string(),
                String::new(),
                String::new(),
                r#"{"call_id":""}"#.into(),
            ],
            stdin: Vec::new(),
            timeout_ms: 10_000,
        },
    };
    let mut line = serde_json::to_vec(&request)?;
    line.push(b'\n');
    stream.write_all(&line).await?;
    let mut reader = BufReader::new(stream);
    let mut output = Vec::new();
    loop {
        line.clear();
        let read =
            match tokio::time::timeout(frame_timeout, reader.read_until(b'\n', &mut line)).await {
                Ok(read) => read?,
                Err(_) => return Ok(true), // A foreground command can hold the exec lock.
            };
        ensure!(
            read > 0,
            "swarmyd closed the process listing before returning a result"
        );
        match serde_json::from_slice::<crate::Response>(&line)? {
            crate::Response::Output(ExecOutput::Stdout(bytes)) => {
                output.extend(bytes);
                ensure!(
                    output.len() <= 5 * 1024 * 1024,
                    "process listing exceeded 5 MiB"
                );
            }
            crate::Response::Output(ExecOutput::Stderr(_)) => {}
            crate::Response::Exited(exit) => {
                ensure!(
                    exit.exit_code == 0 && !exit.timed_out,
                    "process listing failed for {agent}"
                );
                let records: Vec<serde_json::Value> = serde_json::from_slice(&output)?;
                return Ok(records.iter().any(|record| record["status"] == "running"));
            }
            crate::Response::Error(message)
                if message == "sandbox is missing or already exists" =>
            {
                // A stored placement can outlive an idle-evicted sandbox. It has no process to drain.
                return Ok(false);
            }
            crate::Response::Error(message) => {
                bail!("process listing failed for {agent}: {message}")
            }
            other => bail!("unexpected node process listing response: {other:?}"),
        }
    }
}

/// Whether a placement still holds work for an idle agent. A stopped node has
/// no control socket: every placement it still holds was not released, and
/// counts without a listing. A listing that fails (a sandbox evicted
/// mid-query, a helper that exits non-zero, a socket that refuses) says
/// nothing about idleness, so it also counts, with a warning.
async fn placement_busy(socket: &Path, agent: AgentId, epoch: u64, warn: &impl Fn(&str)) -> bool {
    if !socket.exists() {
        return true;
    }
    match process_list(socket, agent, epoch).await {
        Ok(running) => running,
        Err(error) => {
            warn(&format!(
                "process listing for {agent} failed, treating it as busy: {error:#}"
            ));
            true
        }
    }
}

pub(crate) async fn run(
    loaded: &swarmy_config::Loaded,
    warn: impl Fn(&str),
) -> Result<Vec<String>> {
    let settings = &loaded.settings;
    let client = NodeClient::from_token_file(&settings.node.api_url, &settings.node.token_file)?;
    let socket = loaded.root.join(".swarmy/node/control.sock");
    busy_placements(&client, &socket, warn).await
}

async fn busy_placements(
    client: &NodeClient,
    socket: &Path,
    warn: impl Fn(&str),
) -> Result<Vec<String>> {
    let answer = client.placements().await?;
    let mut busy = Vec::new();
    for PlacementOccupancy {
        placement,
        occupied,
    } in answer.placements
    {
        if occupied || placement_busy(socket, placement.agent_id, placement.epoch, &warn).await {
            busy.push(placement.agent_id.to_string());
        }
    }
    Ok(busy)
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::ExecResult;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn existing_node_exec_protocol_reports_a_running_process() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let agent = AgentId::from_ulid(ulid::Ulid::generate());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let crate::Request::Exec { request, .. } = serde_json::from_str(&line).unwrap() else {
                panic!("expected the old node exec request")
            };
            assert_eq!(request.args[3], "process_list");
            assert_eq!(request.args[4], "7");
            for response in [
                crate::Response::Output(ExecOutput::Stdout(br#"[{"status":"running"}]"#.to_vec())),
                crate::Response::Exited(ExecResult {
                    exit_code: 0,
                    timed_out: false,
                }),
            ] {
                let mut frame = serde_json::to_vec(&response).unwrap();
                frame.push(b'\n');
                write.write_all(&frame).await.unwrap();
            }
        });
        assert!(process_list(&path, agent, 7).await.unwrap());
        server.await.unwrap();
    }
}

#[cfg(test)]
mod response_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::ExecResult;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn a_stopped_node_counts_its_placements_without_a_listing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.sock");
        let warnings = std::sync::Mutex::new(Vec::new());
        let warn = |message: &str| warnings.lock().unwrap().push(message.to_owned());
        let agent = AgentId::from_ulid(ulid::Ulid::generate());
        assert!(placement_busy(&path, agent, 7, &warn).await);
        assert!(warnings.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_refusing_socket_counts_as_busy_with_a_warning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.sock");
        let listener = UnixListener::bind(&path).unwrap();
        drop(listener);
        let warnings = std::sync::Mutex::new(Vec::new());
        let warn = |message: &str| warnings.lock().unwrap().push(message.to_owned());
        let agent = AgentId::from_ulid(ulid::Ulid::generate());
        assert!(placement_busy(&path, agent, 7, &warn).await);
        let warnings = warnings.lock().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("treating it as busy"));
    }

    async fn response(value: crate::Response) -> Result<bool> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut line = String::new();
            BufReader::new(read).read_line(&mut line).await.unwrap();
            let mut bytes = serde_json::to_vec(&value).unwrap();
            bytes.push(b'\n');
            write.write_all(&bytes).await.unwrap();
        });
        let result = process_list(&path, AgentId::from_ulid(ulid::Ulid::generate()), 7).await;
        server.await.unwrap();
        result
    }

    #[tokio::test]
    async fn missing_sandbox_is_idle_but_failed_listing_is_not() {
        assert!(
            !response(crate::Response::Error(
                "sandbox is missing or already exists".into()
            ))
            .await
            .unwrap()
        );
        assert!(
            response(crate::Response::Exited(ExecResult {
                exit_code: 1,
                timed_out: false
            }))
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn held_exec_lock_is_busy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, _write) = stream.into_split();
            let mut line = String::new();
            BufReader::new(read).read_line(&mut line).await.unwrap();
            // Hold the server past the client's 10 ms timeout: the timeout
            // firing is the assertion, so the delay is the point.
            #[expect(
                clippy::disallowed_methods,
                reason = "the slow server response is what the client timeout assertion needs"
            )]
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        assert!(
            process_list_with_timeout(
                &path,
                AgentId::from_ulid(ulid::Ulid::generate()),
                7,
                Duration::from_millis(10)
            )
            .await
            .unwrap()
        );
        server.await.unwrap();
    }
}

#[cfg(test)]
mod drain_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::sync::Arc;
    use swarmy_core::{AgentCallStatus, SessionId};
    use swarmy_node_api::{NodeApiPolicy, NodeApiState, router, test_support::TestApi};
    use swarmy_store::blob::MemoryBlobStore;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn drain_counts_occupied_and_listed_placements() {
        let Some(stack) = swarmy_testkit::Stack::load("na10-drain") else {
            return;
        };
        let (store, mut guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
        let node = swarmy_node_api::test_support::node(&store, 2).await.node_id;
        let (_, first) = swarmy_node_api::test_support::hosted_session(&store).await;
        let (_, second) = swarmy_node_api::test_support::hosted_session(&store).await;
        let expires = store.now().checked_add(Duration::from_secs(60)).unwrap();
        let occupied = store.place(first, node, expires).await.unwrap();
        store.place(second, node, expires).await.unwrap();
        store.claim_placement(&occupied).await.unwrap();
        store
            .put_agent_call_status(&AgentCallStatus {
                agent_id: first,
                node_id: node,
                epoch: occupied.epoch,
                holder_session_id: Some(SessionId::from_ulid(ulid::Ulid::generate())),
                queued_calls: 0,
                observed_at: store.now(),
                expires_at: expires,
            })
            .await
            .unwrap();
        let bus = swarmy_bus::Bus::connect(
            &stack.nats_url,
            swarmy_bus::Config {
                prefix: Some(swarmy_bus::SubjectToken::new(&stack.prefix).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        bus.setup(&[]).await.unwrap();
        let api = TestApi::start(router(NodeApiState::new(
            store.clone(),
            bus,
            NodeApiPolicy {
                placement_lease: Duration::from_secs(60),
                snapshot_retention: std::num::NonZeroUsize::new(10).unwrap(),
                resend_interval: Duration::from_secs(5),
            },
        )))
        .await;
        let token = store.issue_node_credential(node).await.unwrap();
        let client = NodeClient::new(api.url(), &token).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("node.sock");
        let mut busy = busy_placements(&client, &socket, |_| {}).await.unwrap();
        busy.sort();
        let mut expected = vec![first.to_string(), second.to_string()];
        expected.sort();
        assert_eq!(busy, expected);
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (_read, mut write) = stream.into_split();
            let mut frame = serde_json::to_vec(&crate::Response::Error(
                "sandbox is missing or already exists".into(),
            ))
            .unwrap();
            frame.push(b'\n');
            write.write_all(&frame).await.unwrap();
        });
        assert_eq!(
            busy_placements(&client, &socket, |_| {}).await.unwrap(),
            vec![first.to_string()]
        );
        server.await.unwrap();
        guard.cleanup().await;
    }
}
