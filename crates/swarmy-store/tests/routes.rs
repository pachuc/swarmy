//! Route resolution, expansion, and session step movement.
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
    time::Duration,
};

use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use swarmy_config::Keyring;
use swarmy_core::{
    AgentId, AgentSettings, CredentialKind, CredentialRecord, CredentialScope, InferenceSelection,
    Lease, LeaseOwnerId, RouteStep, SessionId,
};
use swarmy_store::{CredentialKey, Store, StoreError, blob::MemoryBlobStore};

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct Fixture {
    store: Store,
    keyring: Keyring,
}

impl Fixture {
    fn new() -> Option<Self> {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping route integration: SWARMY_FDB_CLUSTER_FILE unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let db = Arc::new(Database::new(Some(&cluster)).unwrap());
        let root = Subspace::all().subspace(&("route-tests", ulid::Ulid::generate().to_string()));
        let store = Store::with_subspace(db, root, Arc::new(MemoryBlobStore::default()));
        Some(Self {
            store,
            keyring: Keyring::from_bytes([13; 32]),
        })
    }

    async fn entry(&self, provider: &str, label: &str) {
        self.store
            .credentials(self.keyring.clone())
            .put_entry(
                CredentialScope::Cluster,
                provider,
                label,
                &CredentialRecord {
                    kind: CredentialKind::ApiKey {
                        key: format!("{label}-key"),
                        extra: BTreeMap::new(),
                    },
                    updated_at: Timestamp::now(),
                },
            )
            .await
            .unwrap();
    }

    fn step(provider: &str, entry: &str) -> RouteStep {
        RouteStep {
            provider: provider.into(),
            entry: entry.into(),
            model: None,
        }
    }

    async fn snapshot(
        &self,
        agent: AgentId,
        session_route: Option<&str>,
        session_provider: Option<&str>,
    ) -> swarmy_store::RouteSnapshot {
        self.store
            .route_snapshot(
                agent,
                session_route,
                session_provider,
                None,
                "fake",
                Timestamp::now(),
            )
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn route_crud_validates_names_steps_and_providers() {
    let Some(f) = Fixture::new() else {
        return;
    };
    assert!(matches!(
        f.store
            .put_route("", &[Fixture::step("openai", "default")])
            .await,
        Err(StoreError::InvalidRoute(_))
    ));
    assert!(matches!(
        f.store.put_route("empty", &[]).await,
        Err(StoreError::InvalidRoute(_))
    ));
    assert!(f.store.get_route("missing").await.unwrap().is_none());
    assert!(!f.store.delete_route("missing").await.unwrap());
    f.store
        .put_route("fallback", &[Fixture::step("openai", "default")])
        .await
        .unwrap();
    let record = f.store.get_route("fallback").await.unwrap().unwrap();
    assert_eq!(record.name, "fallback");
    assert_eq!(record.steps.len(), 1);
    let names: Vec<_> = f
        .store
        .list_routes()
        .await
        .unwrap()
        .into_iter()
        .map(|route| route.name)
        .collect();
    assert_eq!(names, ["fallback"]);
    assert!(f.store.delete_route("fallback").await.unwrap());
    assert!(f.store.get_route("fallback").await.unwrap().is_none());
}

#[tokio::test]
async fn resolution_prefers_session_agent_default_then_implicit() {
    let Some(f) = Fixture::new() else {
        return;
    };
    for (provider, label) in [("openai", "a"), ("openai", "b"), ("azure", "c")] {
        f.entry(provider, label).await;
    }
    f.store
        .put_route("session-route", &[Fixture::step("azure", "c")])
        .await
        .unwrap();
    f.store
        .put_route("agent-route", &[Fixture::step("openai", "b")])
        .await
        .unwrap();
    f.store
        .put_route("default-route", &[Fixture::step("openai", "a")])
        .await
        .unwrap();
    let agent = AgentId::from_ulid(ulid::Ulid::generate());

    // No assignment: the implicit route of the selected provider's entries.
    let snapshot = f.snapshot(agent, None, Some("openai")).await;
    assert_eq!(snapshot.name, None);
    assert_eq!(
        snapshot
            .steps
            .iter()
            .map(|step| (step.provider.clone(), step.label.clone()))
            .collect::<Vec<_>>(),
        [
            ("openai".to_owned(), Some("a".to_owned())),
            ("openai".to_owned(), Some("b".to_owned())),
        ]
    );
    // Swarm default, then agent, then session override win in order.
    let snapshot = f
        .store
        .route_snapshot(
            agent,
            None,
            Some("openai"),
            Some("default-route"),
            "fake",
            Timestamp::now(),
        )
        .await
        .unwrap();
    assert_eq!(snapshot.name.as_deref(), Some("default-route"));
    // An agent id without a record behaves like no agent assignment.
    let snapshot = f
        .store
        .route_snapshot(
            agent,
            Some("session-route"),
            Some("openai"),
            Some("default-route"),
            "fake",
            Timestamp::now(),
        )
        .await
        .unwrap();
    assert_eq!(snapshot.name.as_deref(), Some("session-route"));
    assert_eq!(
        snapshot.steps[0].provider, "azure",
        "session override beats agent and default"
    );
}

#[tokio::test]
async fn wildcard_expands_in_creation_order_and_missing_routes_fall_back() {
    let Some(f) = Fixture::new() else {
        return;
    };
    // Creation order is second, first, third: labels sort differently.
    for label in ["second", "first", "third"] {
        f.entry("openai", label).await;
    }
    f.store
        .put_route(
            "pooled",
            &[RouteStep {
                provider: "openai".into(),
                entry: "*".into(),
                model: Some("gpt-5.5".into()),
            }],
        )
        .await
        .unwrap();
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    let snapshot = f.snapshot(agent, Some("pooled"), Some("openai")).await;
    assert_eq!(snapshot.name.as_deref(), Some("pooled"));
    assert_eq!(
        snapshot
            .steps
            .iter()
            .map(|step| step.label.clone())
            .collect::<Vec<_>>(),
        [
            Some("second".to_owned()),
            Some("first".to_owned()),
            Some("third".to_owned())
        ]
    );
    assert!(
        snapshot
            .steps
            .iter()
            .all(|step| step.model.as_deref() == Some("gpt-5.5")),
        "wildcard steps keep the step model override"
    );
    // A deleted route falls back to the implicit chain instead of wedging turns.
    f.store.delete_route("pooled").await.unwrap();
    let snapshot = f.snapshot(agent, Some("pooled"), Some("openai")).await;
    assert_eq!(snapshot.name, None);
    assert_eq!(snapshot.steps.len(), 3);
}

#[tokio::test]
async fn open_breakers_skip_and_exhaustion_reports_earliest() {
    let Some(f) = Fixture::new() else {
        return;
    };
    for label in ["primary", "backup"] {
        f.entry("openai", label).await;
    }
    f.store
        .put_route(
            "fallback",
            &[
                Fixture::step("openai", "primary"),
                Fixture::step("openai", "backup"),
            ],
        )
        .await
        .unwrap();
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    let snapshot = f.snapshot(agent, Some("fallback"), Some("openai")).await;
    assert_eq!(snapshot.pick(0), Some(0));
    // A one-step-old failure opens only its own entry's breaker.
    let primary = CredentialKey::entry("openai", "primary");
    let until = Timestamp::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    f.store
        .entry_failure(&primary, until, "openai/primary: quota reached")
        .await
        .unwrap();
    let snapshot = f.snapshot(agent, Some("fallback"), Some("openai")).await;
    assert_eq!(snapshot.pick(0), Some(1), "open steps are skipped");
    assert_eq!(snapshot.pick(1), Some(1));
    assert!(snapshot.pick(2).is_none(), "past-the-end has no pick");
    let backup = CredentialKey::entry("openai", "backup");
    assert!(
        f.store
            .entry_open_until(&backup, Timestamp::now())
            .await
            .unwrap()
            .is_none(),
        "failover never touches the next entry's breaker"
    );
    let soon = Timestamp::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
    f.store
        .entry_failure(&backup, soon, "openai/backup: quota reached")
        .await
        .unwrap();
    let snapshot = f.snapshot(agent, Some("fallback"), Some("openai")).await;
    assert_eq!(snapshot.pick(0), None);
    let (at, reason) = snapshot.earliest().unwrap();
    assert_eq!(
        at, soon,
        "exhaustion waits for the earliest retry among steps"
    );
    assert!(
        reason.contains("openai/backup"),
        "reasons name the entry: {reason}"
    );
}

fn route_pair() -> Vec<RouteStep> {
    vec![
        Fixture::step("openai", "primary"),
        Fixture::step("openai", "backup"),
    ]
}

async fn routed_session(f: &Fixture) -> SessionId {
    // Metadata-only image for sessions that never materialize a computer.
    let manifest = swarmy_core::ManifestId::from_ulid(ulid::Ulid::from_parts(9, 9));
    f.store
        .put_manifest(
            manifest,
            &swarmy_core::ManifestHeader {
                size: u64::from(swarmy_core::CHUNK_SIZE),
                chunk_size: swarmy_core::CHUNK_SIZE,
                root_hash: swarmy_core::ContentHash::ZERO,
            },
        )
        .await
        .unwrap();
    f.store
        .put_image("fixture", &swarmy_core::ImageTag("test".into()), manifest)
        .await
        .unwrap();
    let id = SessionId::from_ulid(ulid::Ulid::generate());
    f.store
        .create_session_with_route(
            id,
            None,
            Some("fixture:test"),
            Timestamp::now(),
            &InferenceSelection::default(),
            Some("fallback"),
        )
        .await
        .unwrap();
    id
}

async fn leased(f: &Fixture, id: SessionId) -> Lease {
    f.store.wake_session(id, Timestamp::now()).await.unwrap();
    let owner = LeaseOwnerId::from_ulid(ulid::Ulid::generate());
    f.store
        .claim_lease(
            id,
            owner,
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn session_route_assignment_validates_and_round_trips() {
    let Some(f) = Fixture::new() else {
        return;
    };
    for label in ["primary", "backup"] {
        f.entry("openai", label).await;
    }
    f.store.put_route("fallback", &route_pair()).await.unwrap();
    f.store
        .put_route("pinned", &[Fixture::step("openai", "primary")])
        .await
        .unwrap();
    // A missing route fails session creation; an image error would too, so
    // create the session on the real route and check the missing name on update.
    assert!(matches!(
        f.store
            .create_session_with_route(
                SessionId::from_ulid(ulid::Ulid::generate()),
                None,
                Some("fixture:test"),
                Timestamp::now(),
                &InferenceSelection::default(),
                Some("missing"),
            )
            .await,
        Err(StoreError::RouteMissing)
    ));
    let id = routed_session(&f).await;
    let record = f.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(record.route.as_deref(), Some("fallback"));
    assert_eq!(record.route_step, 0);
    // The override changes the next turn's chain and restarts its position.
    f.store.set_session_route(id, Some("pinned")).await.unwrap();
    let record = f.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(record.route.as_deref(), Some("pinned"));
    f.store.set_session_route(id, None).await.unwrap();
    let record = f.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(record.route, None);
    assert!(matches!(
        f.store.set_session_route(id, Some("missing")).await,
        Err(StoreError::RouteMissing)
    ));
}

#[tokio::test]
async fn session_step_moves_past_failures_and_parks_exhausted() {
    let Some(f) = Fixture::new() else {
        return;
    };
    for label in ["primary", "backup"] {
        f.entry("openai", label).await;
    }
    f.store.put_route("fallback", &route_pair()).await.unwrap();
    let id = routed_session(&f).await;
    // Lease-checked step movement: advance past the failed step, then park
    // the exhausted chain until the earliest retry with a reset position.
    let lease = leased(&f, id).await;
    let now = Timestamp::now();
    f.store
        .set_session_route_step(
            id,
            &lease,
            1,
            &["openai/primary: quota reached".into()],
            now,
        )
        .await
        .unwrap();
    let record = f.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(record.route_step, 1);
    let reasons = f.store.inference_wait(id).await.unwrap().unwrap().reasons;
    assert!(
        reasons
            .iter()
            .any(|reason| reason.contains("openai/primary")),
        "{reasons:?}"
    );
    let retry_at = now.checked_add(Duration::from_secs(60)).unwrap();
    assert!(
        f.store
            .park_exhausted_route(
                id,
                &lease,
                &swarmy_store::InferenceFailureWait {
                    seq: 3,
                    reason: "openai/backup: quota reached",
                    wake_at: retry_at,
                },
                retry_at,
                now,
                Duration::from_secs(3600),
            )
            .await
            .unwrap()
    );
    let record = f.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(record.route_step, 0, "exhaustion restarts the chain");
    assert_eq!(record.state, swarmy_core::SessionState::Sleeping);
    let wait = f.store.inference_wait(id).await.unwrap().unwrap();
    assert_eq!(wait.wake_at, retry_at);
    assert!(
        wait.reasons
            .iter()
            .any(|reason| reason.contains("openai/backup")),
        "{:?}",
        wait.reasons
    );
    // A stale lease cannot move the chain.
    let stale = Lease {
        owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
        expires_at: now.checked_add(Duration::from_secs(30)).unwrap(),
        seq: 0,
    };
    assert!(matches!(
        f.store
            .set_session_route_step(id, &stale, 1, &[], now)
            .await,
        Err(StoreError::LeaseMismatch)
    ));
    // Success restarts the chain for the next turn.
    f.store.clear_inference_wait(id).await.unwrap();
    assert!(f.store.inference_wait(id).await.unwrap().is_none());
}

#[tokio::test]
async fn agent_and_session_assignment_validate_routes() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.store
        .put_route("fallback", &[Fixture::step("openai", "default")])
        .await
        .unwrap();
    assert!(matches!(
        f.store
            .set_agent(
                AgentId::from_ulid(ulid::Ulid::generate()),
                &AgentSettings {
                    route: Some("missing".into()),
                    ..Default::default()
                },
            )
            .await,
        Err(StoreError::AgentMissing | StoreError::RouteMissing)
    ));
    let id = SessionId::from_ulid(ulid::Ulid::generate());
    assert!(matches!(
        f.store.set_session_route(id, Some("missing")).await,
        Err(StoreError::RouteMissing)
    ));
}
