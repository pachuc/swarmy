//! Compatibility constructor for subscription-backed Responses inference.
use std::sync::Arc;

use crate::{
    Error, Provider, ProviderStream, Request,
    api::responses::ResponsesProvider,
    auth::{CredentialStore, OAuthClient},
};
use swarmy_core::SessionId;

pub const BACKEND_BASE: &str = "https://chatgpt.com/backend-api/codex";

#[derive(Clone)]
pub struct ChatGptProvider(ResponsesProvider);

impl ChatGptProvider {
    /// # Errors
    /// Returns an error if the HTTP clients cannot be configured.
    pub fn new(store: Arc<dyn CredentialStore>) -> Result<Self, Error> {
        Self::with_endpoints(store, BACKEND_BASE, OAuthClient::new()?)
    }

    /// Override endpoints for a local protocol test server.
    /// # Errors
    /// Returns an error if the HTTP client cannot be configured.
    pub fn with_endpoints(
        store: Arc<dyn CredentialStore>,
        base: &str,
        oauth: OAuthClient,
    ) -> Result<Self, Error> {
        Ok(Self(ResponsesProvider::codex(store, base, oauth)?))
    }
}

impl Provider for ChatGptProvider {
    fn request(&self, request: Request) -> ProviderStream {
        self.0.request(request)
    }

    fn request_for_session(&self, request: Request, session_id: SessionId) -> ProviderStream {
        self.0.request_for_session(request, session_id)
    }
}
