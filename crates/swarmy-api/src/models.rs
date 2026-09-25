//! Model probes through the control plane's stored credentials. An operator
//! who ran `swarmy auth set` keeps credentials on the control plane, so the
//! client asks the API to probe instead of resolving local files.
use super::{ApiResult, AppState, credential_store, error, storage};
use axum::{Json, extract::State, http::StatusCode};
use futures::StreamExt as _;
use std::{sync::Arc, time::Instant};
use swarmy_api_types as api;
use swarmy_core::{CredentialScope, Message, MessageId, MessageRole, Part};
use swarmy_llm::{
    Delta, GenerationSettings, Request, Response,
    auth::{AuthStore, Login},
};

/// One cluster credential entry behind the resolver. Environment and ambient
/// chains stay available underneath, exactly as on the CLI.
struct ClusterAuthStore {
    provider: String,
    record: Option<swarmy_core::CredentialRecord>,
}

#[async_trait::async_trait]
impl AuthStore for ClusterAuthStore {
    async fn get(
        &self,
        provider: &str,
    ) -> Result<Option<swarmy_core::CredentialRecord>, swarmy_llm::Error> {
        if provider == self.provider {
            return Ok(self.record.clone());
        }
        Ok(None)
    }
    async fn refresh(
        &self,
        provider: &str,
        _: &swarmy_core::CredentialRecord,
        _: &dyn Login,
    ) -> Result<swarmy_core::CredentialRecord, swarmy_llm::Error> {
        Err(swarmy_llm::Error::NeedsLogin(provider.into()))
    }
}

fn provider_failure(message: String) -> (StatusCode, Json<api::ApiError>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(api::ApiError {
            code: "provider_failure".into(),
            message,
            provider_text: None,
        }),
    )
}

pub async fn probe(
    State(state): State<AppState>,
    Json(body): Json<api::ProbeModel>,
) -> ApiResult<api::ProbeResult> {
    let started = Instant::now();
    if body.provider.is_empty() || body.model.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_request"));
    }
    let provider = state
        .catalog
        .provider(&body.provider)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "model_not_found"))?;
    if provider.api == swarmy_llm::catalog::Api::Fake {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_request"));
    }
    let model = state
        .catalog
        .model(&body.provider, &body.model)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "model_not_found"))?;
    let requested = body
        .effort
        .clone()
        .map(|effort| {
            serde_json::to_value(effort)
                .ok()
                .and_then(|value| serde_json::from_value(value).ok())
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_request"))
        })
        .transpose()?
        .unwrap_or(swarmy_core::ReasoningEffort::None);
    let (effort, _) = model.clamp_effort(requested);
    let auth = resolve_auth(&state, &body).await?;
    let answer = run_probe(provider, model, auth, effort).await?;
    let effort_value = serde_json::to_value(effort)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
    let cost_micros = swarmy_llm::cost::cost_micros(&model.cost, &answer.usage);
    Ok(Json(api::ProbeResult {
        provider: body.provider.clone(),
        model: model.id.clone(),
        usage: serde_json::to_value(&answer.usage)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?,
        cost_micros,
        effort: effort_value,
        elapsed_seconds: started.elapsed().as_secs_f64(),
    }))
}

/// Resolve the labelled cluster entry through the same resolver the CLI
/// uses, so environment and ambient chains stay available underneath.
async fn resolve_auth(
    state: &AppState,
    body: &api::ProbeModel,
) -> Result<swarmy_llm::ClientAuth, (StatusCode, Json<api::ApiError>)> {
    let label = body.label.clone().unwrap_or_else(|| "default".into());
    let record = credential_store(state)?
        .get_entry(CredentialScope::Cluster, &body.provider, &label)
        .await
        .map_err(storage)?;
    let resolver = swarmy_llm::auth::Resolver::new(Arc::new(ClusterAuthStore {
        provider: body.provider.clone(),
        record,
    }))
    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    Ok(swarmy_llm::auth::resolve(&body.provider, &resolver)
        .await
        .map_err(|resolve_error| {
            error(
                StatusCode::BAD_REQUEST,
                &format!("credential_unavailable: {resolve_error}"),
            )
        })?
        .auth)
}

/// Send one short completion and require a usable answer, mirroring the
/// local `models probe` acceptance check.
async fn run_probe(
    provider: &swarmy_llm::catalog::ProviderInfo,
    model: &swarmy_llm::catalog::ModelInfo,
    auth: swarmy_llm::ClientAuth,
    effort: swarmy_core::ReasoningEffort,
) -> Result<Response, (StatusCode, Json<api::ApiError>)> {
    let client = swarmy_llm::client_for(provider, model, auth)
        .map_err(|failure| provider_failure(failure.to_string()))?;
    let request = Request {
        system_prompt: "Follow the user's instructions precisely.".into(),
        messages: vec![Message {
            id: MessageId::from_ulid(ulid::Ulid::generate()),
            role: MessageRole::User,
            parts: vec![Part::Text {
                text: "Reply with the single word ready.".into(),
            }],
        }],
        tools: Vec::new(),
        settings: GenerationSettings {
            model: model.id.clone(),
            reasoning_effort: Some(effort),
            ..GenerationSettings::default()
        },
    };
    let mut response = None;
    let mut stream = client.request(request);
    while let Some(delta) = stream.next().await {
        let delta = delta.map_err(|failure| provider_failure(failure.to_string()))?;
        if let Delta::Completed(completed) = delta {
            response = Some(completed);
            break;
        }
    }
    let answer = response.ok_or_else(|| provider_failure("provider stream ended".into()))?;
    if answer.stop_reason != swarmy_llm::StopReason::EndTurn
        || !answer
            .parts
            .iter()
            .any(|part| matches!(part, Part::Text { text } if !text.trim().is_empty()))
    {
        return Err(provider_failure("provider returned no answer".into()));
    }
    Ok(answer)
}
