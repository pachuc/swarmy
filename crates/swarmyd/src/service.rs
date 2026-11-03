use crate::{
    Request, Response,
    keeper::{Answered, Keeper},
};
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use swarmy_core::{VolumeId, WriterOpening};
use swarmy_sandbox::RuncRuntime;
use swarmy_volume::WriterGrant;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::mpsc,
};

pub(crate) async fn handle(
    socket: UnixStream,
    runtime: Arc<RuncRuntime>,
    keeper: Keeper,
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
        Request::Create { spec, disk, writer } => {
            let grant = passed_grant(&keeper, disk.volume_id, writer);
            runtime
                .create(spec, disk, grant, None)
                .await
                .map(Response::Sandbox)
        }
        Request::Pause(sandbox) => runtime.pause(&sandbox).await.map(Response::Paused),
        Request::Resume { handle, writer } => {
            let grant = passed_grant(&keeper, handle.disk.volume_id, writer);
            runtime.resume(handle, grant).await.map(Response::Sandbox)
        }
        Request::Destroy(sandbox) => runtime.destroy(sandbox).await.map(|()| Response::Destroyed),
        Request::Checkpoint(sandbox) => runtime
            .checkpoint(&sandbox)
            .await
            .map(Response::Checkpointed),
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

/// The keeper renews a lease handed over the control socket from now on.
fn passed_grant(keeper: &Keeper, volume: VolumeId, writer: WriterOpening) -> WriterGrant {
    // The control socket has no server answer, so its first deadline uses the local clock.
    let answered = Answered {
        sent: tokio::time::Instant::now(),
        server_now: jiff::Timestamp::now(),
    };
    WriterGrant {
        volume,
        lease: keeper.hold_writer(volume, writer.lease, answered),
        head: writer.head,
        header: writer.header,
        renewal: None,
    }
}
