//! Public JSON contract for the versioned control plane API.
//! This crate intentionally has no dependency on storage or transport.

use serde::{Deserialize, Serialize};
mod metrics;
pub use metrics::{
    AgentMetrics, ComputerMetric, InferenceMetric, LatencyPercentiles, StageTiming, ToolMetric,
    TurnMetrics,
};
use utoipa::{OpenApi, ToSchema};

/// The API document version served under `/v1`. The leading major version
/// selects the route prefix, so every `1.x` document is served from `/v1`.
pub const API_VERSION: &str = "1.0.0";

fn major_part(version: &str) -> Option<u64> {
    version
        .strip_prefix(['v', 'V'])
        .unwrap_or(version)
        .split(['.', '-', '+'])
        .next()?
        .parse()
        .ok()
}

/// Whether two API versions share a major version. Clients accept any server
/// with the same major version; unparseable versions never match.
#[must_use]
pub fn same_major(left: &str, right: &str) -> bool {
    match (major_part(left), major_part(right)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// A log namespace. The tagged representation reserves channels without changing session cursors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum LogId {
    Session(String),
    Channel(String),
}

/// A sequence is local to one log, starts at one, and increases without gaps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Cursor {
    pub log_id: LogId,
    pub sequence: u64,
}

/// One connection may replay and follow several logs at once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Subscription {
    pub cursors: Vec<Cursor>,
    pub token_deltas: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LiveTokenDelta {
    pub turn_id: String,
    pub position: u64,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Running,
    Finished,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    Ephemeral,
    Named,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Idle,
    Runnable,
    Leased,
    WaitingInference,
    WaitingTools,
    Sleeping,
    Completed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

/// An ISO 8601 timestamp is used for `wake_at`; reasons are human-readable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WaitingReason {
    pub wake_at: Option<String>,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ImageRef {
    pub name: String,
    pub tag: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub description: String,
    pub image: ImageRef,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
    pub system_prompt: Option<String>,
    pub created_at: String,
    pub main_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Session {
    pub id: String,
    pub agent_id: Option<String>,
    pub kind: SessionKind,
    pub state: SessionState,
    pub log_id: LogId,
    pub head_sequence: u64,
    pub created_at: String,
    pub computer_deleted: bool,
    pub waiting: Option<WaitingReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<ReasoningEffort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Turn {
    pub id: String,
    pub session_id: String,
    pub status: TurnStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    Tool,
    System,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Message {
    pub id: String,
    pub session_id: String,
    pub role: MessageRole,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Image {
    pub id: String,
    pub name: String,
    pub tag: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Model {
    pub id: String,
    pub provider_id: String,
    pub context_window: u64,
    #[serde(flatten)]
    #[serde(default)]
    pub catalog: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Provider {
    pub id: String,
    pub name: String,
    pub status: String,
    #[serde(flatten)]
    #[serde(default)]
    pub catalog: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    Subscription,
    ApiKey,
    Cloud,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStatus {
    Ready,
    Expired,
    NeedsLogin,
}

/// Credential metadata contains no secret material.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Credential {
    pub provider: String,
    pub kind: CredentialKind,
    pub label: String,
    pub status: CredentialStatus,
    pub updated_at: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub last_used_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Sandbox,
    Volume,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeCapacity {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub sandboxes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Node {
    pub id: String,
    pub roles: Vec<NodeRole>,
    pub capacity: NodeCapacity,
    pub alive: bool,
    pub last_seen: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ServiceHealth {
    pub role: String,
    pub instance_id: String,
    pub version: String,
    pub alive: bool,
    pub last_seen: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<String>,
}

/// The unauthenticated health projection. `api_version` carries the document
/// version so clients can accept any server with the same major version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    pub version: String,
    pub git_commit: String,
    pub api_version: String,
    /// The provider new conversations use when the request names none.
    pub default_provider: String,
    pub services: Vec<ServiceHealth>,
    pub node_count: u64,
}

/// Protected diagnostic snapshot used by doctor; credential metadata contains no secrets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DoctorSnapshot {
    pub services: Vec<DoctorService>,
    pub images: Vec<String>,
    pub default_image: Option<String>,
    pub credentials: Option<Vec<Credential>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DoctorService {
    pub role: String,
    pub instance_id: String,
    pub version: String,
    pub alive: bool,
    pub providers: Vec<String>,
    pub capacity: Option<NodeCapacity>,
}

/// Every client mutation has a key that survives retries of the same intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateAgent {
    pub idempotency_key: String,
    pub name: String,
    pub description: String,
    pub image: ImageRef,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateAgent {
    pub idempotency_key: String,
    pub description: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSession {
    pub idempotency_key: String,
    pub agent_id: Option<String>,
    /// Open a side conversation rather than the named agent's main session.
    #[serde(default)]
    pub new: bool,
    pub image: Option<ImageRef>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
    #[serde(default)]
    pub route: Option<String>,
}
/// `close` terminates the session; other fields override inference selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateSession {
    pub idempotency_key: String,
    pub close: bool,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
    #[serde(default)]
    pub route: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateTurn {
    pub idempotency_key: String,
    pub session_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateMessage {
    pub idempotency_key: String,
    pub session_id: String,
    pub role: MessageRole,
    pub text: String,
}
/// Append a user message at the observed log head.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AppendMessage {
    pub idempotency_key: String,
    pub expected_head: u64,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AppendedMessage {
    pub sequence: u64,
    pub turn_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterruptSession {
    pub idempotency_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CloseSession {
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InterruptStatus {
    Requested,
    Finished,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterruptOutcome {
    pub result: InterruptStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SessionClosed {
    pub closed: bool,
}

/// Register an already built image; image builds are not mutations of this resource.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateImage {
    pub idempotency_key: String,
    pub name: String,
    pub tag: String,
}
/// Setting the same provider again replaces its credential without exposing its secret on reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateCredential {
    pub idempotency_key: String,
    pub provider: String,
    pub kind: CredentialKind,
    pub label: String,
    pub secret: String,
}

/// Body for agent and credential deletions. The key scopes the replayed result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeleteRequest {
    pub idempotency_key: String,
}

/// Deleting a labelled credential reports the removal without returning secrets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CredentialDeleted {
    pub deleted: bool,
}

/// One route step: the provider, the entry label (`*` for every entry of
/// the provider in creation order), and an optional model override.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RouteStep {
    pub provider: String,
    pub entry: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// A named inference failover chain over auth entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Route {
    pub name: String,
    pub steps: Vec<RouteStep>,
    pub updated_at: String,
}

/// Setting a route replaces its steps in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SetRoute {
    pub idempotency_key: String,
    pub steps: Vec<RouteStep>,
}

/// Deleting a route reports the removal; assigned sessions fall back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RouteDeleted {
    pub deleted: bool,
}

/// Session route override for one conversation. `None` clears the override.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SetSessionRoute {
    pub idempotency_key: String,
    pub route: Option<String>,
}

/// Operator-configured quota for an entry without published quotas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SetEntryQuota {
    pub idempotency_key: String,
    pub limit: u64,
    pub window_seconds: u64,
}

/// Quota view for one entry, either observed or configured. Requests and
/// tokens are separate observed dimensions; `free` carries requests remaining
/// and `tokens_remaining` carries tokens remaining. Configured `used` counts
/// whole hourly buckets overlapping the window (hour granularity).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EntryQuotaView {
    pub source: String,
    pub used: u64,
    pub free: Option<u64>,
    pub limit: Option<u64>,
    pub window_seconds: Option<u64>,
    pub observed_at: Option<String>,
    pub remaining: std::collections::BTreeMap<String, u64>,
    pub requests_remaining: Option<u64>,
    pub tokens_remaining: Option<u64>,
}

/// A durable event has a cursor even when delivered on a multiplexed connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Event {
    pub log_id: LogId,
    pub sequence: u64,
    pub payload: EventPayload,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum EventPayload {
    MessageAppended {
        message: Message,
    },
    TurnStarted {
        turn: Turn,
    },
    TurnFinished {
        turn: Turn,
    },
    ToolCall {
        turn_id: String,
        call_id: String,
        name: String,
        arguments: serde_json::Value,
    },
    ToolResult {
        turn_id: String,
        call_id: String,
        result: serde_json::Value,
    },
    InferenceError {
        turn_id: String,
        error: ApiError,
    },
    Idle {
        session_id: String,
    },
    TokenDelta {
        turn_id: String,
        position: u64,
        text: String,
    },
    ServiceStatusChanged {
        health: ServiceHealth,
    },
    NodeStatusChanged {
        node: Node,
    },
    /// Stored events without a dedicated public projection retain their original data.
    StoreRecord {
        record: serde_json::Value,
    },
}

/// `provider_text` preserves the original provider error without rewriting it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub provider_text: Option<String>,
}

/// CLI projections retain store record fields for compatibility with existing scripts.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliSession {
    pub session_id: String,
    pub resolved_inference: serde_json::Value,
    pub archived: bool,
    pub main: bool,
    pub agent_name: Option<String>,
    #[serde(flatten)]
    pub record: std::collections::BTreeMap<String, serde_json::Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliSessionDetail {
    pub session: serde_json::Value,
    pub resolved: serde_json::Value,
    pub usage: serde_json::Value,
    pub cost_dollars: String,
    pub scratch: serde_json::Value,
    pub requirements: serde_json::Value,
    pub placement: serde_json::Value,
    pub address: Option<String>,
    pub wait: serde_json::Value,
    pub events: Vec<serde_json::Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliAgent {
    pub agent_id: String,
    pub name: String,
    #[serde(flatten)]
    pub record: std::collections::BTreeMap<String, serde_json::Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliImage {
    pub name: String,
    pub tag: String,
    pub manifest_id: String,
    pub header: serde_json::Value,
    pub scratch: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliCredential {
    pub provider: String,
    pub kind: String,
    pub label: String,
    pub status: String,
    pub updated_at: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub last_used_at: Option<String>,
    pub expires_at: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliSaved {
    pub saved: bool,
}
/// CLI projection of a named inference route.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliRoute {
    pub name: String,
    pub steps: Vec<RouteStep>,
    pub updated_at: String,
}
/// CLI input for replacing a route's steps in order.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliRouteInput {
    pub idempotency_key: String,
    pub name: String,
    pub steps: Vec<RouteStep>,
}
/// CLI deletion marker for a named route.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliRouteDeleted {
    pub deleted: bool,
}
/// Input for CLI agent creation or settings updates.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliAgentChoice {
    pub idempotency_key: String,
    pub name: Option<String>,
    pub image: Option<String>,
    pub description: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub system_prompt: Option<String>,
    pub memory: Option<u64>,
    pub gpu: Option<String>,
    pub github_token: Option<String>,
    pub clear_github_token: Option<bool>,
    pub resets: Option<Vec<String>>,
    #[serde(default)]
    pub route: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CliCredentialInput {
    pub idempotency_key: String,
    pub provider: String,
    #[serde(default)]
    pub label: Option<String>,
    /// Encrypted by the API, never returned by credential endpoints.
    pub record: serde_json::Value,
}

/// CLI compatibility routes. These signatures are mirrored by the server router.
pub mod cli_paths {
    use super::{
        ApiError, CliAgent, CliAgentChoice, CliCredential, CliCredentialInput, CliImage, CliRoute,
        CliRouteDeleted, CliRouteInput, CliSaved, CliSession, CliSessionDetail, DoctorSnapshot,
        SetSessionRoute,
    };
    #[utoipa::path(get, path = "/v1/cli/doctor",
        responses((status = 200, body = DoctorSnapshot), (status = 503, body = ApiError)))]
    pub fn cli_doctor() {}
    #[utoipa::path(get, path = "/v1/cli/sessions",
        responses((status = 200, body = Vec<CliSession>), (status = 400, body = ApiError)))]
    pub fn cli_sessions() {}
    #[utoipa::path(get, path = "/v1/cli/sessions/{id}",
        responses((status = 200, body = CliSessionDetail), (status = 400, body = ApiError)))]
    pub fn cli_session() {}
    #[utoipa::path(get, path = "/v1/cli/agents",
        responses((status = 200, body = Vec<CliAgent>), (status = 400, body = ApiError)))]
    pub fn cli_agents() {}
    #[utoipa::path(post, path = "/v1/cli/agents",
    request_body = CliAgentChoice,
        responses((status = 200, body = CliAgent), (status = 400, body = ApiError)))]
    pub fn cli_create_agent() {}
    #[utoipa::path(get, path = "/v1/cli/agents/{name}",
        responses((status = 200, body = CliAgent), (status = 400, body = ApiError)))]
    pub fn cli_agent() {}
    #[utoipa::path(patch, path = "/v1/cli/agents/{name}/settings",
    request_body = CliAgentChoice,
        responses((status = 200, body = CliAgent), (status = 400, body = ApiError)))]
    pub fn cli_update_agent() {}
    #[utoipa::path(get, path = "/v1/cli/images/{name}/{tag}",
        responses((status = 200, body = CliImage), (status = 400, body = ApiError)))]
    pub fn cli_image() {}
    #[utoipa::path(get, path = "/v1/cli/credentials",
        responses((status = 200, body = Vec<CliCredential>), (status = 400, body = ApiError)))]
    pub fn cli_credentials() {}
    #[utoipa::path(post, path = "/v1/cli/credentials",
    request_body = CliCredentialInput,
        responses((status = 200, body = CliSaved), (status = 400, body = ApiError)))]
    pub fn cli_set_credential() {}
    #[utoipa::path(get, path = "/v1/cli/credentials/{provider}",
        responses((status = 200, body = CliCredential), (status = 400, body = ApiError)))]
    pub fn cli_credential() {}
    #[utoipa::path(get, path = "/v1/cli/routes",
        responses((status = 200, body = Vec<CliRoute>), (status = 400, body = ApiError)))]
    pub fn cli_routes() {}
    #[utoipa::path(get, path = "/v1/cli/routes/{name}",
        responses((status = 200, body = CliRoute), (status = 400, body = ApiError)))]
    pub fn cli_route() {}
    #[utoipa::path(post, path = "/v1/cli/routes",
    request_body = CliRouteInput,
        responses((status = 200, body = CliSaved), (status = 400, body = ApiError)))]
    pub fn cli_set_route() {}
    #[utoipa::path(delete, path = "/v1/cli/routes/{name}",
        responses((status = 200, body = CliRouteDeleted), (status = 400, body = ApiError)))]
    pub fn cli_remove_route() {}
    #[utoipa::path(patch, path = "/v1/cli/sessions/{id}/route",
    request_body = SetSessionRoute,
        responses((status = 200, body = CliSessionDetail), (status = 400, body = ApiError)))]
    pub fn cli_set_session_route() {}
}

/// Versioned resource routes. These signatures are mirrored by the server router.
pub mod api_paths {
    use super::{
        Agent, AgentMetrics, ApiError, AppendMessage, AppendedMessage, CloseSession, CreateAgent,
        CreateCredential, CreateSession, Credential, CredentialDeleted, DeleteRequest,
        EntryQuotaView, Event, HealthResponse, Image, InterruptOutcome, InterruptSession, Model,
        Provider, Route, RouteDeleted, Session, SessionClosed, SetEntryQuota, SetRoute,
        SetSessionRoute, Subscription, TurnMetrics, UpdateAgent,
    };
    #[utoipa::path(get, path = "/v1/health",
        responses((status = 200, body = HealthResponse)))]
    pub fn health() {}
    #[utoipa::path(get, path = "/v1/openapi.json",
        responses((status = 200, description = "The OpenAPI document for this server version")))]
    pub fn openapi() {}
    #[utoipa::path(get, path = "/v1/docs",
        responses((status = 200, description = "Rendered API reference for this server version")))]
    pub fn docs() {}
    #[utoipa::path(get, path = "/v1/agents",
        params(
            ("after" = Option<String>, Query, description = "Return agents after this id"),
            ("limit" = Option<usize>, Query, description = "Maximum agents to return"),
        ),
        responses((status = 200, body = Vec<Agent>), (status = 401, body = ApiError)))]
    pub fn list_agents() {}
    #[utoipa::path(post, path = "/v1/agents",
        request_body = CreateAgent,
        responses((status = 200, body = Agent), (status = 400, body = ApiError)))]
    pub fn create_agent() {}
    #[utoipa::path(get, path = "/v1/agents/{id}",
        params(("id" = String, Path, description = "Agent id or name")),
        responses((status = 200, body = Agent), (status = 404, body = ApiError)))]
    pub fn show_agent() {}
    #[utoipa::path(patch, path = "/v1/agents/{id}",
        params(("id" = String, Path, description = "Agent id or name")),
        request_body = UpdateAgent,
        responses((status = 200, body = Agent), (status = 404, body = ApiError)))]
    pub fn update_agent() {}
    #[utoipa::path(delete, path = "/v1/agents/{id}",
        params(("id" = String, Path, description = "Agent id or name")),
        request_body = DeleteRequest,
        responses((status = 200, description = "Deletion marker")))]
    pub fn delete_agent() {}
    #[utoipa::path(get, path = "/v1/sessions",
        params(
            ("after" = Option<String>, Query, description = "Return sessions after this id"),
            ("limit" = Option<usize>, Query, description = "Maximum sessions to return"),
        ),
        responses((status = 200, body = Vec<Session>), (status = 401, body = ApiError)))]
    pub fn list_sessions() {}
    #[utoipa::path(post, path = "/v1/sessions",
        request_body = CreateSession,
        responses((status = 200, body = Session), (status = 400, body = ApiError)))]
    pub fn create_session() {}
    #[utoipa::path(get, path = "/v1/sessions/{id}",
        params(("id" = String, Path, description = "Session id")),
        responses((status = 200, body = Session), (status = 404, body = ApiError)))]
    pub fn show_session() {}
    #[utoipa::path(delete, path = "/v1/sessions/{id}",
        params(("id" = String, Path, description = "Session id")),
        request_body = CloseSession,
        responses((status = 200, body = SessionClosed), (status = 404, body = ApiError)))]
    pub fn close_session() {}
    #[utoipa::path(patch, path = "/v1/sessions/{id}/route",
        params(("id" = String, Path, description = "Session id")),
        request_body = SetSessionRoute,
        responses((status = 200, body = Session), (status = 404, body = ApiError)))]
    pub fn set_session_route() {}
    #[utoipa::path(get, path = "/v1/routes",
        responses((status = 200, body = Vec<Route>), (status = 401, body = ApiError)))]
    pub fn list_routes() {}
    #[utoipa::path(post, path = "/v1/routes/{name}",
        params(("name" = String, Path, description = "Route name")),
        request_body = SetRoute,
        responses((status = 200, body = Route), (status = 400, body = ApiError)))]
    pub fn set_route() {}
    #[utoipa::path(get, path = "/v1/routes/{name}",
        params(("name" = String, Path, description = "Route name")),
        responses((status = 200, body = Route), (status = 404, body = ApiError)))]
    pub fn show_route() {}
    #[utoipa::path(delete, path = "/v1/routes/{name}",
        params(("name" = String, Path, description = "Route name")),
        request_body = DeleteRequest,
        responses((status = 200, body = RouteDeleted), (status = 404, body = ApiError)))]
    pub fn delete_route() {}
    #[utoipa::path(post, path = "/v1/sessions/{id}/messages",
        params(("id" = String, Path, description = "Session id")),
        request_body = AppendMessage,
        responses((status = 200, body = AppendedMessage), (status = 404, body = ApiError)))]
    pub fn append_message() {}
    #[utoipa::path(post, path = "/v1/sessions/{id}/interrupt",
        params(("id" = String, Path, description = "Session id")),
        request_body = InterruptSession,
        responses((status = 200, body = InterruptOutcome), (status = 404, body = ApiError)))]
    pub fn interrupt_session() {}
    #[utoipa::path(get, path = "/v1/sessions/{id}/wait-idle",
        params(
            ("id" = String, Path, description = "Session id"),
            ("after" = Option<u64>, Query, description = "Wait for a head beyond this sequence"),
            ("timeout_ms" = Option<u64>, Query, description = "Maximum wait in milliseconds"),
        ),
        responses((status = 200, body = Session), (status = 404, body = ApiError)))]
    pub fn wait_idle() {}
    #[utoipa::path(get, path = "/v1/sessions/{id}/events",
        params(
            ("id" = String, Path, description = "Session id"),
            ("after" = Option<u64>, Query, description = "Replay events after this sequence"),
            ("limit" = Option<usize>, Query, description = "Maximum events to return"),
        ),
        responses((status = 200, body = Vec<Event>), (status = 404, body = ApiError)))]
    pub fn session_events() {}
    #[utoipa::path(get, path = "/v1/events",
        params(("subscription" = Option<String>, Query,
            description = "URL-encoded Subscription JSON; a Last-Event-ID header overrides it")),
        responses((status = 200, description = "Server-sent event stream of Event frames",
            body = Event), (status = 400, body = ApiError)))]
    pub fn subscribe() {}
    #[utoipa::path(put, path = "/v1/events/{connection_id}/subscription",
        params(("connection_id" = String, Path, description = "Stream connection id")),
        request_body = Subscription,
        responses((status = 204, description = "Subscription replaced"),
            (status = 404, body = ApiError)))]
    pub fn update_subscription() {}
    #[utoipa::path(get, path = "/v1/images",
        params(
            ("after" = Option<String>, Query, description = "Return images after this name and tag"),
            ("limit" = Option<usize>, Query, description = "Maximum images to return"),
        ),
        responses((status = 200, body = Vec<Image>), (status = 401, body = ApiError)))]
    pub fn list_images() {}
    #[utoipa::path(get, path = "/v1/images/{name}/{tag}",
        params(
            ("name" = String, Path, description = "Image name"),
            ("tag" = String, Path, description = "Image tag"),
        ),
        responses((status = 200, body = Image), (status = 404, body = ApiError)))]
    pub fn show_image() {}
    #[utoipa::path(get, path = "/v1/models",
        params(
            ("q" = Option<String>, Query, description = "Free-text model search"),
            ("provider" = Option<String>, Query, description = "Restrict to one provider"),
            ("reasoning" = Option<bool>, Query, description = "Only models with reasoning effort"),
        ),
        responses((status = 200, body = Vec<Model>), (status = 401, body = ApiError)))]
    pub fn list_models() {}
    #[utoipa::path(get, path = "/v1/models/search",
        params(("q" = String, Query, description = "Free-text model search")),
        responses((status = 200, body = Vec<Model>), (status = 401, body = ApiError)))]
    pub fn search_models() {}
    #[utoipa::path(get, path = "/v1/models/{provider}/{model}",
        params(
            ("provider" = String, Path, description = "Provider id"),
            ("model" = String, Path, description = "Model id"),
        ),
        responses((status = 200, body = Model), (status = 404, body = ApiError)))]
    pub fn show_model() {}
    #[utoipa::path(get, path = "/v1/providers",
        responses((status = 200, body = Vec<Provider>), (status = 401, body = ApiError)))]
    pub fn list_providers() {}
    #[utoipa::path(get, path = "/v1/credentials",
        responses((status = 200, body = Vec<Credential>), (status = 401, body = ApiError)))]
    pub fn list_credentials() {}
    #[utoipa::path(post, path = "/v1/credentials",
        request_body = CreateCredential,
        responses((status = 200, body = Credential), (status = 400, body = ApiError)))]
    pub fn set_credential() {}
    #[utoipa::path(get, path = "/v1/credentials/{provider}",
        params(("provider" = String, Path, description = "Provider id")),
        responses((status = 200, body = Credential), (status = 404, body = ApiError)))]
    pub fn check_credential() {}
    #[utoipa::path(delete, path = "/v1/credentials/{provider}",
        params(("provider" = String, Path, description = "Provider id")),
        request_body = DeleteRequest,
        responses((status = 200, description = "Deletion marker")))]
    pub fn remove_credential() {}
    #[utoipa::path(get, path = "/v1/sessions/{id}/metrics",
        params(
            ("id" = String, Path, description = "Session id"),
            ("after" = Option<String>, Query, description = "Return turns after this turn id"),
            ("limit" = Option<usize>, Query, description = "Maximum turns to return"),
        ),
        responses((status = 200, body = Vec<TurnMetrics>), (status = 404, body = ApiError)))]
    pub fn session_metrics() {}
    #[utoipa::path(get, path = "/v1/agents/{id}/metrics",
        params(
            ("id" = String, Path, description = "Agent id or name"),
            ("limit" = Option<usize>, Query, description = "Maximum recent turns to roll up"),
            ("since" = Option<String>, Query, description = "Roll up turns after this turn id"),
        ),
        responses((status = 200, body = AgentMetrics), (status = 404, body = ApiError)))]
    pub fn agent_metrics() {}
    #[utoipa::path(get, path = "/v1/credentials/{provider}/{label}",
        params(
            ("provider" = String, Path, description = "Provider id"),
            ("label" = String, Path, description = "Credential label"),
        ),
        responses((status = 200, body = Credential), (status = 404, body = ApiError)))]
    pub fn check_credential_entry() {}
    #[utoipa::path(delete, path = "/v1/credentials/{provider}/{label}",
        params(
            ("provider" = String, Path, description = "Provider id"),
            ("label" = String, Path, description = "Credential label"),
        ),
        request_body = DeleteRequest,
        responses((status = 200, body = CredentialDeleted), (status = 404, body = ApiError)))]
    pub fn remove_credential_entry() {}
    #[utoipa::path(get, path = "/v1/credentials/{provider}/{label}/quota",
        params(
            ("provider" = String, Path, description = "Provider id"),
            ("label" = String, Path, description = "Credential label"),
        ),
        responses((status = 200, body = EntryQuotaView), (status = 404, body = ApiError)))]
    pub fn entry_quota() {}
    #[utoipa::path(post, path = "/v1/credentials/{provider}/{label}/quota",
        params(
            ("provider" = String, Path, description = "Provider id"),
            ("label" = String, Path, description = "Credential label"),
        ),
        request_body = SetEntryQuota,
        responses((status = 200, body = EntryQuotaView), (status = 400, body = ApiError), (status = 404, body = ApiError)))]
    pub fn set_entry_quota() {}
}

/// The schema document is generated from the same types clients and servers serialize.
// Keep `info(version)` in sync with `API_VERSION` above.
#[derive(OpenApi)]
#[openapi(
    info(title = "Swarmy API", version = "1.0.0", description = "Version 1 control plane. Additive-only within /v1: new routes and fields may appear, nothing is removed or retyped, and deprecations carry an x-sunset date. See docs/api.md."),
    servers((url = "/v1", description = "Version 1 control plane")),
    paths(
        api_paths::health, api_paths::openapi, api_paths::docs,
        api_paths::list_agents, api_paths::create_agent, api_paths::show_agent,
        api_paths::update_agent, api_paths::delete_agent,
        api_paths::list_sessions, api_paths::create_session, api_paths::show_session,
        api_paths::close_session, api_paths::append_message, api_paths::interrupt_session,
        api_paths::wait_idle, api_paths::session_events, api_paths::session_metrics,
        api_paths::agent_metrics,
        api_paths::subscribe, api_paths::update_subscription,
        api_paths::list_images, api_paths::show_image,
        api_paths::list_models, api_paths::search_models, api_paths::show_model,
        api_paths::list_providers,
        api_paths::list_credentials, api_paths::set_credential,
        api_paths::check_credential, api_paths::remove_credential,
        api_paths::check_credential_entry, api_paths::remove_credential_entry,
        api_paths::entry_quota, api_paths::set_entry_quota,
        cli_paths::cli_doctor, cli_paths::cli_sessions, cli_paths::cli_session, cli_paths::cli_agents,
        cli_paths::cli_create_agent, cli_paths::cli_agent, cli_paths::cli_update_agent,
        cli_paths::cli_image, cli_paths::cli_credentials, cli_paths::cli_set_credential,
        cli_paths::cli_credential
    ),
    components(schemas(
    LogId, Cursor, Subscription, TurnStatus, SessionKind, SessionState, ReasoningEffort,
    WaitingReason, ImageRef, Agent, Session, Turn, MessageRole, Message, Image, Model,
    Provider, CredentialKind, CredentialStatus, Credential, NodeRole, NodeCapacity,
    Node, ServiceHealth, HealthResponse, DoctorSnapshot, DoctorService, StageTiming, InferenceMetric,
    ToolMetric, ComputerMetric, TurnMetrics, LatencyPercentiles, AgentMetrics, CreateAgent,
    UpdateAgent, DeleteRequest,
    CreateSession, UpdateSession,
    CreateTurn, CreateMessage, AppendMessage, AppendedMessage, InterruptSession, CloseSession,
    InterruptStatus, InterruptOutcome, SessionClosed,
    CreateImage, CreateCredential, CredentialDeleted, SetEntryQuota, EntryQuotaView, Event, EventPayload, ApiError, CliSession, CliSessionDetail,
    CliAgent, CliImage, CliCredential, CliSaved, CliAgentChoice, CliCredentialInput
)))]
pub struct ApiDocument;

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: Serialize + for<'a> Deserialize<'a> + PartialEq + std::fmt::Debug>(
        input: serde_json::Value,
    ) {
        let value: T = serde_json::from_value(input).unwrap();
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(serde_json::from_str::<T>(&json).unwrap(), value);
    }

    macro_rules! check {
        ($ty:ty, $($value:tt)+) => {
            round_trip::<$ty>(serde_json::json!($($value)+))
        };
    }

    #[test]
    fn resource_json_contract() {
        check!(LogId, {"kind":"session","id":"s"});
        check!(LogId, {"kind":"channel","id":"c"});
        check!(Cursor, {"log_id":{"kind":"session","id":"s"},"sequence":0});
        check!(Subscription, {"cursors":[],"token_deltas":false});
        for status in ["running", "finished", "failed"] {
            check!(TurnStatus, status);
        }
        for kind in ["ephemeral", "named"] {
            check!(SessionKind, kind);
        }
        for state in [
            "idle",
            "runnable",
            "leased",
            "waiting_inference",
            "waiting_tools",
            "sleeping",
            "completed",
        ] {
            check!(SessionState, state);
        }
        for effort in ["none", "minimal", "low", "medium", "high", "xhigh", "max"] {
            check!(ReasoningEffort, effort);
        }
        check!(WaitingReason, {"wake_at":"2026-09-23T12:00:00Z","reasons":["provider rate limit"]});
        check!(ImageRef, {"name":"base","tag":"dev"});
        check!(Agent, {"id":"a","name":"worker","description":"coding agent","image":{"name":"base","tag":"dev"},"provider":"openai","model":"gpt","effort":"high","system_prompt":null,"created_at":"2026-09-23T12:00:00Z","main_session_id":"s"});
        check!(Session, {"id":"s","agent_id":"a","kind":"named","state":"sleeping","log_id":{"kind":"session","id":"s"},"head_sequence":2,"created_at":"2026-09-23T12:00:00Z","computer_deleted":false,"waiting":{"wake_at":null,"reasons":["timer"]}});
        check!(Session, {"id":"archived","agent_id":"a","kind":"named","state":"completed","log_id":{"kind":"session","id":"archived"},"head_sequence":7,"created_at":"2026-09-23T12:00:00Z","computer_deleted":false,"waiting":null,"provider":"fake","model":"scripted","effort":"medium","next_session":"successor"});
        check!(Turn, {"id":"t","session_id":"s","status":"running","started_at":"2026-09-23T12:00:00Z","finished_at":null});
        for role in ["user", "assistant", "tool", "system"] {
            check!(MessageRole, role);
        }
        check!(Message, {"id":"m","session_id":"s","role":"user","text":"hello"});
        check!(Image, {"id":"i","name":"base","tag":"dev"});
        check!(Model, {"id":"m","provider_id":"p","context_window":100});
        check!(Provider, {"id":"p","name":"provider","status":"available"});
        for kind in ["subscription", "api_key", "cloud"] {
            check!(CredentialKind, kind);
        }
        for status in ["ready", "expired", "needs_login"] {
            check!(CredentialStatus, status);
        }
        check!(Credential, {"provider":"openai","kind":"api_key","label":"primary","status":"ready","updated_at":"2026-09-23T12:00:00Z"});
        for role in ["sandbox", "volume"] {
            check!(NodeRole, role);
        }
        check!(NodeCapacity, {"cpu_millis":1000,"memory_bytes":4096,"disk_bytes":8192,"sandboxes":2});
        check!(Node, {"id":"n","roles":["sandbox"],"capacity":{"cpu_millis":1000,"memory_bytes":4096,"disk_bytes":8192,"sandboxes":2},"alive":true,"last_seen":"2026-09-23T12:00:00Z"});
        check!(ServiceHealth, {"role":"gateway","instance_id":"g1","version":"0.1.0","alive":true,"last_seen":"2026-09-23T12:00:00Z"});
        check!(HealthResponse, {"version":"0.1.0","git_commit":"abc","api_version":"1.0.0","default_provider":"openai","services":[],"node_count":0});
        check!(ApiError, {"code":"provider_error","message":"failed","provider_text":"original"});
    }

    #[test]
    fn same_major_accepts_any_minor_within_major() {
        assert!(same_major("1.0.0", "1.0.0"));
        assert!(same_major("1.4.0", "1.0.0"));
        assert!(same_major("v1", API_VERSION));
        assert!(!same_major("2.0.0", API_VERSION));
        assert!(!same_major("", API_VERSION));
        assert!(!same_major("unknown", API_VERSION));
    }

    #[test]
    fn openapi_info_version_tracks_api_version() {
        assert_eq!(ApiDocument::openapi().info.version, API_VERSION);
    }

    #[test]
    fn mutation_json_contract() {
        check!(CreateAgent, {"idempotency_key":"k","name":"a","description":"d","image":{"name":"base","tag":"dev"},"provider":null,"model":null,"effort":null,"system_prompt":null});
        check!(UpdateAgent, {"idempotency_key":"k","description":null,"provider":"openai","model":null,"effort":"high","system_prompt":null});
        check!(DeleteRequest, {"idempotency_key":"k"});
        check!(CreateSession, {"idempotency_key":"k","agent_id":null,"image":{"name":"base","tag":"dev"},"provider":null,"model":null,"effort":null});
        check!(UpdateSession, {"idempotency_key":"k","close":true,"provider":null,"model":null,"effort":null});
        check!(CreateTurn, {"idempotency_key":"k","session_id":"s"});
        check!(CreateMessage, {"idempotency_key":"k","session_id":"s","role":"user","text":"hi"});
        check!(CreateImage, {"idempotency_key":"k","name":"base","tag":"dev"});
        check!(CreateCredential, {"idempotency_key":"k","provider":"openai","kind":"api_key","label":"primary","secret":"input-only"});
        check!(CredentialDeleted, {"deleted":true});
        check!(SetEntryQuota, {"idempotency_key":"k","limit":1000,"window_seconds":18000});
        check!(EntryQuotaView, {"source":"configured","used":3,"free":997,"limit":1000,"window_seconds":18000,"observed_at":null,"remaining":{},"requests_remaining":null,"tokens_remaining":null});
    }

    #[test]
    fn event_json_contract() {
        let message = serde_json::json!({"id":"m","session_id":"s","role":"user","text":"hi"});
        let turn = serde_json::json!({"id":"t","session_id":"s","status":"running","started_at":"2026-09-23T12:00:00Z","finished_at":null});
        let node = serde_json::json!({"id":"n","roles":["sandbox"],"capacity":{"cpu_millis":1000,"memory_bytes":4096,"disk_bytes":8192,"sandboxes":2},"alive":true,"last_seen":"2026-09-23T12:00:00Z"});
        let health = serde_json::json!({"role":"gateway","instance_id":"g1","version":"0.1.0","alive":true,"last_seen":"2026-09-23T12:00:00Z"});
        let payloads = [
            serde_json::json!({"type":"message_appended","data":{"message":message}}),
            serde_json::json!({"type":"turn_started","data":{"turn":turn}}),
            serde_json::json!({"type":"turn_finished","data":{"turn":turn}}),
            serde_json::json!({"type":"tool_call","data":{"turn_id":"t","call_id":"c","name":"bash","arguments":{"command":"ls"}}}),
            serde_json::json!({"type":"tool_result","data":{"turn_id":"t","call_id":"c","result":{"output":"ok"}}}),
            serde_json::json!({"type":"inference_error","data":{"turn_id":"t","error":{"code":"provider_error","message":"failed","provider_text":"original"}}}),
            serde_json::json!({"type":"idle","data":{"session_id":"s"}}),
            serde_json::json!({"type":"token_delta","data":{"turn_id":"t","position":0,"text":"a"}}),
            serde_json::json!({"type":"service_status_changed","data":{"health":health}}),
            serde_json::json!({"type":"node_status_changed","data":{"node":node}}),
        ];
        for payload in payloads {
            round_trip::<EventPayload>(payload.clone());
            check!(Event, {"log_id":{"kind":"session","id":"s"},"sequence":1,"payload":payload});
        }
    }

    #[test]
    fn openapi_is_current() {
        let generated = serde_json::to_string_pretty(&ApiDocument::openapi()).unwrap() + "\n";
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/openapi.json");
        if std::env::var_os("SWARMY_UPDATE_OPENAPI").is_some() {
            std::fs::write(path, &generated).unwrap();
        } else {
            assert_eq!(
                std::fs::read_to_string(path).unwrap(),
                generated,
                "regenerate with SWARMY_UPDATE_OPENAPI=1 cargo test -p swarmy-api-types openapi_is_current"
            );
        }
    }
}
