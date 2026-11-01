use crate::keeper::{Answered, Keeper};
use anyhow::{Result, anyhow};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use swarmy_bus::{Bus, WorkMessage, WorkQueue};
use swarmy_core::{BlockDevice, ImageProbe};
use swarmy_node_protocol::{
    ClientError, DEFAULT_DEADLINE, ImageProbeFinishRequest, NodeClient, NodeRefusal,
};
use swarmy_sandbox::{ExecRequest, RuncRuntime};
use swarmy_volume::WriterGrant;
use tokio::sync::mpsc;

pub(crate) fn spawn(
    bus: Bus,
    client: NodeClient,
    keeper: Keeper,
    runtime: Arc<RuncRuntime>,
    ack_wait: Duration,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(serve(bus, client, keeper, runtime, ack_wait))
}

async fn serve(
    bus: Bus,
    client: NodeClient,
    keeper: Keeper,
    runtime: Arc<RuncRuntime>,
    ack_wait: Duration,
) -> Result<()> {
    let queue = WorkQueue::ImageProbes;
    bus.setup(std::slice::from_ref(&queue)).await?;
    let mut messages = bus.consume::<ImageProbe>(&queue).await?;
    loop {
        let message = match messages.next().await {
            None => return Err(anyhow!("image probe subscription closed")),
            Some(Err(error)) => {
                tracing::warn!(error = %swarmy_core::error_chain(&error), "image probe delivery failed");
                continue;
            }
            Some(Ok(message)) => message,
        };
        let outcome = message
            .keep_alive_while(
                ack_wait / 3,
                probe(&client, &keeper, &runtime, &message.value),
            )
            .await;
        settle(&message, outcome).await;
    }
}

/// Acknowledge completed probes and release failed ones for redelivery.
async fn settle(message: &WorkMessage<ImageProbe>, outcome: Result<()>) {
    match outcome {
        Ok(()) => {
            if let Err(error) = message.acknowledge().await {
                tracing::warn!(error = %swarmy_core::error_chain(&error), "image probe acknowledgement failed");
            }
        }
        Err(error) => {
            tracing::warn!(error = %swarmy_core::error_chain(&*error), "image probe failed");
            if let Err(error) = message
                .negative_acknowledge(Some(Duration::from_secs(30)))
                .await
            {
                tracing::warn!(error = %swarmy_core::error_chain(&error), "image probe release failed");
            }
        }
    }
}

async fn probe(
    client: &NodeClient,
    keeper: &Keeper,
    runtime: &RuncRuntime,
    image: &ImageProbe,
) -> Result<()> {
    let started = Instant::now();
    let sent = tokio::time::Instant::now();
    let opened = match client
        .open_image_probe(image, sent + DEFAULT_DEADLINE)
        .await
    {
        Ok(opened) => opened,
        Err(ClientError::Refused(NodeRefusal::AlreadyDone)) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let answered = Answered {
        sent,
        server_now: opened.server_now,
    };
    let grant = WriterGrant {
        volume: opened.volume,
        head: opened.head,
        header: opened.header,
        lease: keeper.hold_writer(opened.volume, opened.writer, answered),
        renewal: None,
    };
    let sandbox = runtime
        .create(
            opened.spec,
            BlockDevice {
                volume_id: opened.volume,
            },
            grant,
            None,
        )
        .await?;
    let captured = async {
        let (output, mut discard) = mpsc::channel(16);
        let drain = async { while discard.recv().await.is_some() {} };
        let request = ExecRequest {
            args: vec!["/bin/bash".into(), "-c".into(), "true".into()],
            timeout_ms: 60_000,
            stdin: Vec::new(),
        };
        let (ran, ()) = tokio::join!(runtime.exec(&sandbox, request, output), drain);
        ran?;
        crate::hosting::store_boot_set(runtime, opened.volume).await
    }
    .await;
    let destroyed = runtime.destroy(sandbox).await;
    let captured = captured?;
    destroyed?;
    let key = captured.as_ref().map(|(key, _)| key.clone());
    client
        .finish_image_probe(
            &ImageProbeFinishRequest {
                manifest: image.manifest,
                key: key.clone(),
            },
            tokio::time::Instant::now() + DEFAULT_DEADLINE,
        )
        .await?;
    let chunks = captured.as_ref().map_or(0, |(_, set)| set.chunks.len());
    tracing::info!(image = %format!("{}:{}", image.name, image.tag.0), chunks, key = key.as_deref().unwrap_or(""), elapsed_ms = started.elapsed().as_millis(), "image probe finished");
    Ok(())
}
