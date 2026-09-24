//! Public JSON contract for the versioned control plane API.
//! This crate intentionally has no dependency on storage or transport.

use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Provider {
    pub id: String,
    pub name: String,
    pub status: String,
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
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateAgent {
    pub idempotency_key: String,
    pub description: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
    pub system_prompt: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSession {
    pub idempotency_key: String,
    pub agent_id: Option<String>,
    pub image: Option<ImageRef>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
}
/// `close` terminates the session; other fields override inference selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateSession {
    pub idempotency_key: String,
    pub close: bool,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<ReasoningEffort>,
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

/// The schema document is generated from the same types clients and servers serialize.
#[derive(OpenApi)]
#[openapi(
    info(title = "Swarmy API", version = "1.0.0"),
    servers((url = "/v1", description = "Version 1 control plane")),
    components(schemas(
    LogId, Cursor, Subscription, TurnStatus, SessionKind, SessionState, ReasoningEffort,
    WaitingReason, ImageRef, Agent, Session, Turn, MessageRole, Message, Image, Model,
    Provider, CredentialKind, CredentialStatus, Credential, NodeRole, NodeCapacity,
    Node, ServiceHealth, CreateAgent, UpdateAgent, CreateSession, UpdateSession,
    CreateTurn, CreateMessage, CreateImage, CreateCredential, Event, EventPayload, ApiError
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
        check!(ApiError, {"code":"provider_error","message":"failed","provider_text":"original"});
    }

    #[test]
    fn mutation_json_contract() {
        check!(CreateAgent, {"idempotency_key":"k","name":"a","description":"d","image":{"name":"base","tag":"dev"},"provider":null,"model":null,"effort":null,"system_prompt":null});
        check!(UpdateAgent, {"idempotency_key":"k","description":null,"provider":"openai","model":null,"effort":"high","system_prompt":null});
        check!(CreateSession, {"idempotency_key":"k","agent_id":null,"image":{"name":"base","tag":"dev"},"provider":null,"model":null,"effort":null});
        check!(UpdateSession, {"idempotency_key":"k","close":true,"provider":null,"model":null,"effort":null});
        check!(CreateTurn, {"idempotency_key":"k","session_id":"s"});
        check!(CreateMessage, {"idempotency_key":"k","session_id":"s","role":"user","text":"hi"});
        check!(CreateImage, {"idempotency_key":"k","name":"base","tag":"dev"});
        check!(CreateCredential, {"idempotency_key":"k","provider":"openai","kind":"api_key","label":"primary","secret":"input-only"});
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
            serde_json::json!({"type":"token_delta","data":{"turn_id":"t","text":"a"}}),
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
