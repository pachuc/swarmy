//! Model probes through the control plane's stored credentials. An operator
//! who ran `swarmy auth set` keeps credentials on the control plane, so the
//! client asks the API to probe instead of resolving local files.
use super::{ApiResult, AppState, credential_store, error, failure, storage};
use axum::{Json, extract::State, http::StatusCode};
use futures::StreamExt as _;
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use swarmy_api_types as api;
use swarmy_core::{
    CredentialScope, Message, MessageId, MessageRole, Part, ToolResult, UsageTotals,
};
use swarmy_llm::{
    Delta, GenerationSettings, Request, Response, StopReason, ToolDefinition,
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

fn provider_failure(provider_text: String) -> (StatusCode, Json<api::ApiError>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(api::ApiError {
            code: "provider_failure".into(),
            message: "provider probe failed".into(),
            provider_text: Some(provider_text),
        }),
    )
}

/// The server gives up before the client's 300 second wait so a hung
/// provider surfaces as a probe error rather than a client timeout.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(240);

fn provider_timeout() -> (StatusCode, Json<api::ApiError>) {
    (
        StatusCode::GATEWAY_TIMEOUT,
        Json(api::ApiError {
            code: "provider_timeout".into(),
            message: "provider probe timed out".into(),
            provider_text: None,
        }),
    )
}

pub(crate) async fn probe(
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
    let model = state
        .catalog
        .model(&body.provider, &body.model)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "model_not_found"))?;
    let requested = body.effort.map_or(
        swarmy_core::ReasoningEffort::None,
        swarmy_core::ReasoningEffort::from,
    );
    let (effort, _) = model.clamp_effort(requested);
    let auth = if provider.api == swarmy_llm::catalog::Api::Fake {
        let (script, call_log) = state
            .fake_files
            .as_ref()
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_request"))?;
        swarmy_llm::ClientAuth::Scripted(Arc::new(
            swarmy_llm::fake::FileFake::from_files(script, call_log)
                .map_err(|failure| provider_failure(failure.to_string()))?,
        ))
    } else {
        resolve_auth(&state, &body).await?
    };
    let totals = match tokio::time::timeout(
        PROBE_TIMEOUT,
        run_probe(provider, model, auth, effort, body.tools),
    )
    .await
    {
        Ok(answer) => answer?,
        Err(_) => return Err(provider_timeout()),
    };
    let effort_value = serde_json::to_value(effort)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
    Ok(Json(api::ProbeResult {
        provider: body.provider.clone(),
        model: model.id.clone(),
        usage: serde_json::to_value(&totals.usage)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?,
        cost_micros: totals.cost_micros,
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
    .map_err(|cause| failure(StatusCode::INTERNAL_SERVER_ERROR, "storage_error", cause))?;
    Ok(swarmy_llm::auth::resolve(&body.provider, &resolver)
        .await
        .map_err(|resolve_error| {
            (
                StatusCode::BAD_REQUEST,
                Json(api::ApiError {
                    code: "credential_unavailable".into(),
                    message: resolve_error.to_string(),
                    provider_text: None,
                }),
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
    tools: bool,
) -> Result<UsageTotals, (StatusCode, Json<api::ApiError>)> {
    let client = swarmy_llm::client_for(provider, model, auth)
        .map_err(|failure| provider_failure(failure.to_string()))?;
    let mut request = Request {
        no_cache: false,
        system_prompt: "Follow the user's instructions precisely.".into(),
        messages: vec![Message {
            id: MessageId::from_ulid(ulid::Ulid::generate()),
            role: MessageRole::User,
            parts: vec![Part::Text {
                text: if tools {
                    "Call get_time exactly once, then reply with the single word ready."
                } else {
                    "Reply with the single word ready."
                }
                .into(),
            }],
        }],
        tools: if tools {
            vec![ToolDefinition {
                name: "get_time".into(),
                description: "Return the current UTC time.".into(),
                parameters: serde_json::json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
            }]
        } else {
            Vec::new()
        },
        settings: GenerationSettings {
            model: model.id.clone(),
            reasoning_effort: Some(effort),
            ..GenerationSettings::default()
        },
    };
    let first = completion(client.as_ref(), request.clone()).await?;
    let mut totals = UsageTotals::default();
    totals.add(
        &first.usage,
        swarmy_llm::cost::cost_micros(&model.cost, &first.usage),
    );
    let answer = if tools {
        if first.stop_reason != StopReason::ToolCalls {
            return Err(provider_failure("provider did not request get_time".into()));
        }
        let calls: Vec<_> = first
            .parts
            .iter()
            .filter_map(|part| match part {
                Part::ToolCall {
                    call_id,
                    tool,
                    input,
                } => Some((call_id, tool, input)),
                _ => None,
            })
            .collect();
        if calls.len() != 1 || calls[0].1 != "get_time" || calls[0].2 != &serde_json::json!({}) {
            return Err(provider_failure(
                "provider must call get_time exactly once".into(),
            ));
        }
        let result = Part::ToolResult {
            call_id: calls[0].0.clone(),
            result: ToolResult::Completed {
                output: jiff::Timestamp::now().to_string(),
                title: "Current UTC time".into(),
                metadata: BTreeMap::new(),
            },
        };
        request.messages.push(Message {
            id: MessageId::from_ulid(ulid::Ulid::generate()),
            role: MessageRole::Assistant,
            parts: first.parts,
        });
        request.messages.push(Message {
            id: MessageId::from_ulid(ulid::Ulid::generate()),
            role: MessageRole::Tool,
            parts: vec![result],
        });
        let second = completion(client.as_ref(), request).await?;
        totals.add(
            &second.usage,
            swarmy_llm::cost::cost_micros(&model.cost, &second.usage),
        );
        second
    } else {
        first
    };
    if answer.stop_reason != StopReason::EndTurn
        || !answer
            .parts
            .iter()
            .any(|part| matches!(part, Part::Text { text } if !text.trim().is_empty()))
    {
        return Err(provider_failure("provider returned no answer".into()));
    }
    Ok(totals)
}

async fn completion(
    client: &dyn swarmy_llm::Provider,
    request: Request,
) -> Result<Response, (StatusCode, Json<api::ApiError>)> {
    let mut stream = client.request(request);
    while let Some(delta) = stream.next().await {
        let delta = delta.map_err(|failure| provider_failure(failure.to_string()))?;
        if let Delta::Completed(completed) = delta {
            return Ok(completed);
        }
    }
    Err(provider_failure("provider stream ended".into()))
}
