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
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub image_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub prompt: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Session {
    pub id: String,
    pub agent_id: Option<String>,
    pub log_id: LogId,
    pub latest_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Turn {
    pub id: String,
    pub session_id: String,
    pub status: TurnStatus,
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
    pub status: HealthStatus,
}

/// Only metadata is returned; no credential response includes secret material.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Credential {
    pub id: String,
    pub provider_id: String,
    pub kind: String,
    pub valid: bool,
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Node {
    pub id: String,
    pub status: HealthStatus,
    pub capacity: u32,
    pub occupied: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ServiceHealth {
    pub service: String,
    pub status: HealthStatus,
    pub detail: Option<String>,
}

/// Idempotency keys are unique to a mutation intent, not to a retry attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateAgent {
    pub idempotency_key: String,
    pub name: String,
    pub image_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub prompt: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateAgent {
    pub idempotency_key: String,
    pub prompt: Option<String>,
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSession {
    pub idempotency_key: String,
    pub agent_id: Option<String>,
    pub image_id: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateSession {
    pub idempotency_key: String,
    pub agent_id: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateTurn {
    pub idempotency_key: String,
    pub session_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateTurn {
    pub idempotency_key: String,
    pub status: TurnStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateMessage {
    pub idempotency_key: String,
    pub session_id: String,
    pub role: MessageRole,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateMessage {
    pub idempotency_key: String,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateImage {
    pub idempotency_key: String,
    pub name: String,
    pub tag: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateImage {
    pub idempotency_key: String,
    pub tag: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateModel {
    pub idempotency_key: String,
    pub provider_id: String,
    pub context_window: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateModel {
    pub idempotency_key: String,
    pub context_window: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateProvider {
    pub idempotency_key: String,
    pub name: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateProvider {
    pub idempotency_key: String,
    pub status: HealthStatus,
}
/// Secret material is accepted only on input, and is never echoed in a response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateCredential {
    pub idempotency_key: String,
    pub provider_id: String,
    pub kind: String,
    pub secret: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateCredential {
    pub idempotency_key: String,
    pub secret: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateNode {
    pub idempotency_key: String,
    pub capacity: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateNode {
    pub idempotency_key: String,
    pub status: HealthStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateServiceHealth {
    pub idempotency_key: String,
    pub service: String,
    pub status: HealthStatus,
    pub detail: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateServiceHealth {
    pub idempotency_key: String,
    pub status: HealthStatus,
    pub detail: Option<String>,
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
#[openapi(components(schemas(
    LogId,
    Cursor,
    Subscription,
    TurnStatus,
    HealthStatus,
    Agent,
    Session,
    Turn,
    MessageRole,
    Message,
    Image,
    Model,
    Provider,
    Credential,
    Node,
    ServiceHealth,
    CreateAgent,
    UpdateAgent,
    CreateSession,
    UpdateSession,
    CreateTurn,
    UpdateTurn,
    CreateMessage,
    UpdateMessage,
    CreateImage,
    UpdateImage,
    CreateModel,
    UpdateModel,
    CreateProvider,
    UpdateProvider,
    CreateCredential,
    UpdateCredential,
    CreateNode,
    UpdateNode,
    CreateServiceHealth,
    UpdateServiceHealth,
    Event,
    EventPayload,
    ApiError
)))]
pub struct ApiDocument;

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: Serialize + for<'a> Deserialize<'a> + PartialEq + std::fmt::Debug>(value: T) {
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(serde_json::from_str::<T>(&json).unwrap(), value);
    }

    #[test]
    fn json_contract() {
        let log = LogId::Session("s".into());
        let health = HealthStatus::Healthy;
        let role = MessageRole::User;
        let status = TurnStatus::Running;
        let message = Message {
            id: "m".into(),
            session_id: "s".into(),
            role: role.clone(),
            text: "hi".into(),
        };
        let turn = Turn {
            id: "t".into(),
            session_id: "s".into(),
            status: status.clone(),
        };
        let node = Node {
            id: "n".into(),
            status: health.clone(),
            capacity: 2,
            occupied: 1,
        };
        let service = ServiceHealth {
            service: "gateway".into(),
            status: health.clone(),
            detail: None,
        };
        let error = ApiError {
            code: "provider_error".into(),
            message: "failed".into(),
            provider_text: Some("original".into()),
        };
        round_trip(log.clone());
        round_trip(LogId::Channel("c".into()));
        round_trip(Cursor {
            log_id: log.clone(),
            sequence: 0,
        });
        round_trip(Subscription {
            cursors: vec![Cursor {
                log_id: log.clone(),
                sequence: 1,
            }],
            token_deltas: false,
        });
        round_trip(status.clone());
        round_trip(TurnStatus::Finished);
        round_trip(TurnStatus::Failed);
        round_trip(health.clone());
        round_trip(HealthStatus::Degraded);
        round_trip(HealthStatus::Unavailable);
        round_trip(role.clone());
        round_trip(MessageRole::Assistant);
        round_trip(MessageRole::Tool);
        round_trip(MessageRole::System);
        round_trip(Agent {
            id: "a".into(),
            name: "agent".into(),
            image_id: "i".into(),
            provider_id: "p".into(),
            model_id: "m".into(),
            prompt: "prompt".into(),
        });
        round_trip(Session {
            id: "s".into(),
            agent_id: Some("a".into()),
            log_id: log.clone(),
            latest_sequence: 1,
        });
        round_trip(turn.clone());
        round_trip(message.clone());
        round_trip(Image {
            id: "i".into(),
            name: "base".into(),
            tag: "v1".into(),
        });
        round_trip(Model {
            id: "m".into(),
            provider_id: "p".into(),
            context_window: 100,
        });
        round_trip(Provider {
            id: "p".into(),
            name: "provider".into(),
            status: health.clone(),
        });
        round_trip(Credential {
            id: "k".into(),
            provider_id: "p".into(),
            kind: "api_key".into(),
            valid: true,
            expires_at: None,
        });
        round_trip(node.clone());
        round_trip(service.clone());
        round_trip(error.clone());
        round_trip(CreateAgent {
            idempotency_key: "k".into(),
            name: "a".into(),
            image_id: "i".into(),
            provider_id: "p".into(),
            model_id: "m".into(),
            prompt: "p".into(),
        });
        round_trip(UpdateAgent {
            idempotency_key: "k".into(),
            prompt: None,
            provider_id: None,
            model_id: None,
        });
        round_trip(CreateSession {
            idempotency_key: "k".into(),
            agent_id: None,
            image_id: Some("i".into()),
        });
        round_trip(UpdateSession {
            idempotency_key: "k".into(),
            agent_id: None,
        });
        round_trip(CreateTurn {
            idempotency_key: "k".into(),
            session_id: "s".into(),
        });
        round_trip(UpdateTurn {
            idempotency_key: "k".into(),
            status,
        });
        round_trip(CreateMessage {
            idempotency_key: "k".into(),
            session_id: "s".into(),
            role,
            text: "hi".into(),
        });
        round_trip(UpdateMessage {
            idempotency_key: "k".into(),
            text: "hi".into(),
        });
        round_trip(CreateImage {
            idempotency_key: "k".into(),
            name: "base".into(),
            tag: "v1".into(),
        });
        round_trip(UpdateImage {
            idempotency_key: "k".into(),
            tag: "v2".into(),
        });
        round_trip(CreateModel {
            idempotency_key: "k".into(),
            provider_id: "p".into(),
            context_window: 100,
        });
        round_trip(UpdateModel {
            idempotency_key: "k".into(),
            context_window: 100,
        });
        round_trip(CreateProvider {
            idempotency_key: "k".into(),
            name: "p".into(),
        });
        round_trip(UpdateProvider {
            idempotency_key: "k".into(),
            status: health.clone(),
        });
        round_trip(CreateCredential {
            idempotency_key: "k".into(),
            provider_id: "p".into(),
            kind: "api_key".into(),
            secret: "input-only".into(),
        });
        round_trip(UpdateCredential {
            idempotency_key: "k".into(),
            secret: None,
        });
        round_trip(CreateNode {
            idempotency_key: "k".into(),
            capacity: 2,
        });
        round_trip(UpdateNode {
            idempotency_key: "k".into(),
            status: health.clone(),
        });
        round_trip(CreateServiceHealth {
            idempotency_key: "k".into(),
            service: "gateway".into(),
            status: health.clone(),
            detail: None,
        });
        round_trip(UpdateServiceHealth {
            idempotency_key: "k".into(),
            status: health,
            detail: None,
        });
        for payload in [
            EventPayload::MessageAppended { message },
            EventPayload::TurnStarted { turn: turn.clone() },
            EventPayload::TurnFinished { turn },
            EventPayload::ToolCall {
                turn_id: "t".into(),
                call_id: "c".into(),
                name: "bash".into(),
                arguments: serde_json::json!({"command":"ls"}),
            },
            EventPayload::ToolResult {
                turn_id: "t".into(),
                call_id: "c".into(),
                result: serde_json::json!({"output":"ok"}),
            },
            EventPayload::InferenceError {
                turn_id: "t".into(),
                error,
            },
            EventPayload::Idle {
                session_id: "s".into(),
            },
            EventPayload::TokenDelta {
                turn_id: "t".into(),
                text: "a".into(),
            },
            EventPayload::ServiceStatusChanged { health: service },
            EventPayload::NodeStatusChanged { node },
        ] {
            round_trip(payload.clone());
            round_trip(Event {
                log_id: log.clone(),
                sequence: 1,
                payload,
            });
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
