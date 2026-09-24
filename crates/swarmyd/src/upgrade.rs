//! Query the running node through its existing control protocol before replacing it.
//! This runs in the newly built binary while the old daemon still owns the runtime.
use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use swarmy_core::{AgentId, ExecOutput, ExecRequest, NodeId, Sandbox};
use swarmy_store::{Store, blob::MemoryBlobStore};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

async fn process_list(socket: &Path, agent: AgentId, epoch: u64) -> Result<bool> {
    let mut stream = UnixStream::connect(socket)
        .await
        .context("connect running swarmyd control socket")?;
    let request = swarmyd::Request::Exec {
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
        ensure!(
            tokio::time::timeout(Duration::from_secs(20), reader.read_until(b'\n', &mut line))
                .await??
                > 0,
            "swarmyd closed the process listing before returning a result"
        );
        match serde_json::from_slice::<swarmyd::Response>(&line)? {
            swarmyd::Response::Output(ExecOutput::Stdout(bytes)) => {
                output.extend(bytes);
                ensure!(
                    output.len() <= 5 * 1024 * 1024,
                    "process listing exceeded 5 MiB"
                );
            }
            swarmyd::Response::Output(ExecOutput::Stderr(_)) => {}
            swarmyd::Response::Exited(exit) => {
                ensure!(
                    exit.exit_code == 0 && !exit.timed_out,
                    "process listing failed for {agent}"
                );
                let records: Vec<serde_json::Value> = serde_json::from_slice(&output)?;
                return Ok(records.iter().any(|record| record["status"] == "running"));
            }
            swarmyd::Response::Error(message)
                if message == "sandbox is missing or already exists" =>
            {
                // A stored placement can outlive an idle-evicted sandbox. It has no process to drain.
                return Ok(false);
            }
            swarmyd::Response::Error(message) => {
                bail!("process listing failed for {agent}: {message}")
            }
            other => bail!("unexpected node process listing response: {other:?}"),
        }
    }
}

pub async fn run(loaded: &swarmy_config::Loaded) -> Result<()> {
    let node: NodeId = loaded.node_id()?;
    let settings = &loaded.settings;
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    let store = Store::open(
        Some(&settings.fdb_cluster_file),
        Some(&directory),
        Arc::new(MemoryBlobStore::default()),
    )
    .await?;
    let socket = loaded.root.join(".swarmy/node/control.sock");
    let mut after = None;
    let mut busy = Vec::new();
    loop {
        let page = store
            .list_by_node(node, after, swarmy_store::MAX_SCAN_LIMIT)
            .await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|placement| placement.agent_id);
        for placement in page {
            let occupied = store
                .agent_call_status(placement.agent_id)
                .await?
                .is_some_and(|status| {
                    status.holder_session_id.is_some() || status.queued_calls > 0
                });
            if occupied || process_list(&socket, placement.agent_id, placement.epoch).await? {
                busy.push(placement.agent_id.to_string());
            }
        }
    }
    println!("{}", serde_json::to_string(&busy)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::ExecResult;
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
            let swarmyd::Request::Exec { request, .. } = serde_json::from_str(&line).unwrap()
            else {
                panic!("expected the old node exec request")
            };
            assert_eq!(request.args[3], "process_list");
            assert_eq!(request.args[4], "7");
            for response in [
                swarmyd::Response::Output(ExecOutput::Stdout(
                    br#"[{"status":"running"}]"#.to_vec(),
                )),
                swarmyd::Response::Exited(ExecResult {
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
