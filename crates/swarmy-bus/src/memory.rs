use crate::{Bus, Error, nats};
use futures_util::StreamExt;
use std::future::Future;
use swarmy_core::{MemoryRequest, NodeId, decode, encode};

impl Bus {
    /// Read memory through the node holding the agent's computer.
    /// # Errors
    /// Returns transport, timeout, or encoding failures.
    pub async fn request_memory(
        &self,
        node: NodeId,
        request: &MemoryRequest,
    ) -> Result<Result<String, String>, Error> {
        self.request_context(node, request, "memory").await
    }

    /// Read repository instructions through the current placement holder.
    /// # Errors
    /// Returns transport, timeout, or encoding failures.
    pub async fn request_instructions(
        &self,
        node: NodeId,
        request: &MemoryRequest,
    ) -> Result<Result<String, String>, Error> {
        self.request_context(node, request, "instructions").await
    }

    async fn request_context(
        &self,
        node: NodeId,
        request: &MemoryRequest,
        kind: &str,
    ) -> Result<Result<String, String>, Error> {
        let reply = self
            .client
            .send_request(
                self.config.subject(&format!("node.{kind}.{node}")),
                async_nats::Request::new()
                    .payload(encode(request)?.into())
                    .timeout(Some(std::time::Duration::from_secs(15))),
            )
            .await
            .map_err(nats)?;
        Ok(decode(&reply.payload)?)
    }

    /// Serve read-only memory requests on this node.
    /// # Errors
    /// Returns subscription or transport failures.
    pub async fn serve_memory<F, Fut>(&self, node: NodeId, handler: F) -> Result<(), Error>
    where
        F: Fn(MemoryRequest) -> Fut,
        Fut: Future<Output = Result<String, String>>,
    {
        self.serve_context(node, "memory", handler).await
    }

    /// Serve read-only repository instruction requests.
    /// # Errors
    /// Returns subscription or transport failures.
    pub async fn serve_instructions<F, Fut>(&self, node: NodeId, handler: F) -> Result<(), Error>
    where
        F: Fn(MemoryRequest) -> Fut,
        Fut: Future<Output = Result<String, String>>,
    {
        self.serve_context(node, "instructions", handler).await
    }

    async fn serve_context<F, Fut>(&self, node: NodeId, kind: &str, handler: F) -> Result<(), Error>
    where
        F: Fn(MemoryRequest) -> Fut,
        Fut: Future<Output = Result<String, String>>,
    {
        let mut requests = self
            .client
            .subscribe(self.config.subject(&format!("node.{kind}.{node}")))
            .await
            .map_err(nats)?;
        while let Some(message) = requests.next().await {
            let Some(reply) = message.reply else { continue };
            let request = match decode(&message.payload) {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!(%error, "invalid memory request");
                    continue;
                }
            };
            self.client
                .publish(reply, encode(&handler(request).await)?.into())
                .await
                .map_err(nats)?;
        }
        Err(Error::Nats("memory subscription closed".into()))
    }
}
