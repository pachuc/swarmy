use anyhow::Result;
use std::{sync::Arc, time::Duration};
use swarmy_sandbox::{RuncRuntime, SandboxRuntime};
use swarmyd::{Request, Response};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::mpsc,
};

pub async fn handle(
    socket: UnixStream,
    runtime: Arc<RuncRuntime>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let (read, mut write) = socket.into_split();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(read.take(65536)).read_line(&mut line),
    )
    .await??;
    let request: Request = serde_json::from_str(&line)?;
    let result = match request {
        Request::Create { spec, disk } => runtime.create(spec, disk).await.map(Response::Sandbox),
        Request::Pause(sandbox) => runtime.pause(&sandbox).await.map(Response::Paused),
        Request::Resume(handle) => runtime.resume(handle).await.map(Response::Sandbox),
        Request::Destroy(sandbox) => runtime.destroy(sandbox).await.map(|()| Response::Destroyed),
        Request::Capabilities => Ok(Response::Capabilities(runtime.capabilities())),
        Request::Exec { sandbox, request } => {
            let (send, mut receive) = mpsc::channel(16);
            let exec = runtime.exec(&sandbox, request, send);
            tokio::pin!(exec);
            let result = loop {
                tokio::select! {
                    result = &mut exec => break result,
                    _ = shutdown.changed() => return Ok(()),
                    Some(output) = receive.recv() => frame(&mut write, &Response::Output(output)).await?,
                }
            };
            // The process can finish before the last buffered output is sent.
            while let Some(output) = receive.recv().await {
                frame(&mut write, &Response::Output(output)).await?;
            }
            result.map(Response::Exited)
        }
    };
    let response = result.unwrap_or_else(|error| Response::Error(error.to_string()));
    frame(&mut write, &response).await
}

async fn frame(write: &mut (impl AsyncWrite + Unpin), response: &Response) -> Result<()> {
    let mut bytes = serde_json::to_vec(response)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(5), write.write_all(&bytes)).await??;
    Ok(())
}
