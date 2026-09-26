//! Usage series and entry quota routes read from the metering rollups.
use std::sync::{Arc, OnceLock};

use jiff::Timestamp;
use swarmy_api::{AppState, router};
use swarmy_client::Client;
use swarmy_core::{
    AgentId, Event, InflightRecord, LeaseOwnerId, Message, MessageId, MessageRole, Part, RequestId,
    SessionId,
};
use swarmy_store::{MeteringDimension, Store, UsageGroupBy, blob::MemoryBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct Fixture {
    store: Store,
    client: Client,
    server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new() -> Option<Self> {
        let cluster = std::env::var("SWARMY_FDB_CLUSTER_FILE").ok()?;
        let nats = std::env::var("SWARMY_NATS_URL").ok()?;
        NETWORK.get_or_init(swarmy_store::boot);
        let store = Store::open(
            Some(&cluster),
            Some(&["usage-test".into(), Ulid::generate().to_string()]),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap();
        let bus = swarmy_bus::Bus::connect(&nats, swarmy_bus::Config::default())
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut state = AppState::new(
            store.clone(),
            bus,
            "test-token".into(),
            swarmy_llm::catalog::Catalog::get().clone(),
            Arc::new(object_store::memory::InMemory::new()),
        );
        state.credential_keyring = Some(swarmy_config::Keyring::from_bytes([7; 32]));
        let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
        Some(Self {
            store,
            client: Client::new(&base, "test-token").unwrap(),
            server,
        })
    }
}

fn usage(input: u64, output: u64, cost: u64) -> (swarmy_core::TokenUsage, u64) {
    (
        swarmy_core::TokenUsage {
            input_tokens: input,
            cached_input_tokens: 1,
            output_tokens: output,
            reasoning_output_tokens: 0,
            total_tokens: input + output + 1,
            cache_write_input_tokens: 0,
        },
        cost,
    )
}

struct CompletionSeed<'a> {
    provider: &'a str,
    model: &'a str,
    entry: &'a str,
    kind: &'a str,
    at: Timestamp,
    cost: u64,
}

async fn complete(store: &Store, id: SessionId, seed: &CompletionSeed<'_>) {
    let lease = store
        .claim_lease(
            id,
            LeaseOwnerId::from_ulid(Ulid::generate()),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let head = store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let step = head + 1;
    store
        .submit_inference(
            head,
            &lease,
            &InflightRecord {
                session_id: id,
                seq: step,
                provider: seed.provider.into(),
                key_id: String::new(),
            },
            &"input",
        )
        .await
        .unwrap();
    let request = RequestId::for_step(id, step);
    let claim = swarmy_store::InferenceClaim {
        session_id: id,
        request_id: request,
        owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        expires_at: Timestamp::now()
            .checked_add(std::time::Duration::from_secs(60))
            .unwrap(),
    };
    store
        .start_inference(&claim, Timestamp::now())
        .await
        .unwrap();
    let (tokens, _) = usage(10, 20, seed.cost);
    let event = Event::InferenceCompleted {
        seq: 0,
        request_id: request,
        message: Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts: vec![Part::Text {
                text: "done".into(),
            }],
        },
        provider: seed.provider.into(),
        model: seed.model.into(),
        effort_used: None,
        usage: tokens,
        cost_micros: seed.cost,
        effort_requested: None,
        effort_clamped: false,
        entry: None,
        route: None,
        route_step: None,
    };
    assert!(
        store
            .complete_inference(
                &swarmy_store::InferenceCompletion {
                    claim,
                    expected_head: step,
                    event,
                    now: seed.at,
                    entry: Some(seed.entry.into()),
                    entry_kind: Some(seed.kind.into()),
                    quota_remaining: std::collections::BTreeMap::new(),
                    quota_resets: std::collections::BTreeMap::new(),
                },
                &"answer",
            )
            .await
            .unwrap()
    );
}

async fn seed(store: &Store) -> (SessionId, SessionId, AgentId) {
    use swarmy_core::{CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId};
    let manifest = ManifestId::from_ulid(Ulid::generate());
    store
        .put_manifest(
            manifest,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash::ZERO,
            },
        )
        .await
        .unwrap();
    store
        .put_image("fixture", &ImageTag("test".into()), manifest)
        .await
        .unwrap();
    let first = SessionId::from_ulid(Ulid::generate());
    let second = SessionId::from_ulid(Ulid::generate());
    for id in [first, second] {
        store
            .create_session_with_inference(
                id,
                None,
                Some("fixture:test"),
                Timestamp::now(),
                &swarmy_core::InferenceSelection::default(),
            )
            .await
            .unwrap();
        // Leases claim only runnable sessions; completions return them to
        // runnable, so one wake covers the whole seed sequence.
        store.wake_session(id, Timestamp::now()).await.unwrap();
    }
    let agent = store.fetch_session(first).await.unwrap().unwrap().agent_id;
    // Spread completions across days, weeks, and months on both sessions.
    let days = [0, 1, 8, 32, 65];
    for (index, day) in days.iter().enumerate() {
        let at = Timestamp::from_second(1_700_000_000 + i64::try_from(index).unwrap()).unwrap();
        let at = at.checked_add(jiff::Span::new().hours(day * 24)).unwrap();
        let id = if index % 2 == 0 { first } else { second };
        let provider = if index % 2 == 0 { "openai" } else { "xai" };
        let entry = if index % 2 == 0 {
            "primary"
        } else {
            "secondary"
        };
        complete(
            store,
            id,
            &CompletionSeed {
                provider,
                model: "gpt-5",
                entry,
                kind: "api-key",
                at,
                cost: 100 * u64::try_from(index + 1).unwrap(),
            },
        )
        .await;
    }
    (first, second, agent)
}

fn stamp(second: i64) -> String {
    Timestamp::from_second(second).unwrap().to_string()
}

#[tokio::test]
async fn usage_series_matches_store_views_for_every_dimension_and_group() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let (first, second, agent) = seed(&fixture.store).await;
    let from = stamp(1_700_000_000 - 3_600);
    let to = stamp(1_700_000_000 + 70 * 86_400);
    let from_ts = Timestamp::from_second(1_700_000_000 - 3_600).unwrap();
    let to_ts = Timestamp::from_second(1_700_000_000 + 70 * 86_400).unwrap();
    let keys = [
        ("session", first.to_string()),
        ("session", second.to_string()),
        ("agent", agent.to_string()),
        ("provider", "openai".into()),
        ("provider", "xai".into()),
        ("entry", "openai/primary".into()),
        ("entry", "xai/secondary".into()),
        ("kind", "api-key".into()),
        ("model", "gpt-5".into()),
    ];
    for group in ["day", "week", "month", "year"] {
        let group_by = match group {
            "day" => UsageGroupBy::Day,
            "week" => UsageGroupBy::Week,
            "month" => UsageGroupBy::Month,
            _ => UsageGroupBy::Year,
        };
        for (by, key) in &keys {
            let dimension = MeteringDimension::parse(by).unwrap();
            let expected = fixture
                .store
                .usage(dimension, key, from_ts, to_ts, group_by)
                .await
                .unwrap();
            let response = fixture
                .client
                .usage(by, Some(key), &from, &to, group)
                .await
                .unwrap();
            assert_eq!(response.groups.len(), expected.len(), "{by}/{key}/{group}");
            for (group, want) in response.groups.iter().zip(&expected) {
                assert_eq!(group.start, want.start.to_string());
                assert_eq!(group.end, want.end.to_string());
                assert_eq!(group.totals.input_tokens, want.totals.usage.input_tokens);
                assert_eq!(group.totals.cost_micros, want.totals.cost_micros);
                assert_eq!(group.totals.completions, want.completions);
                assert_eq!(group.totals.cost_dollars, want.totals.dollars());
            }
            let mut total = swarmy_core::UsageTotals::default();
            let mut completions = 0;
            for want in &expected {
                total.add(&want.totals.usage, want.totals.cost_micros);
                completions += want.completions;
            }
            assert_eq!(response.total.cost_micros, total.cost_micros);
            assert_eq!(response.total.completions, completions);
        }
        // Without a key the route aggregates the whole dimension.
        for by in ["agent", "entry", "model"] {
            let dimension = MeteringDimension::parse(by).unwrap();
            let expected = fixture
                .store
                .usage_aggregate(dimension, from_ts, to_ts, group_by)
                .await
                .unwrap();
            let response = fixture
                .client
                .usage(by, None, &from, &to, group)
                .await
                .unwrap();
            assert_eq!(response.groups.len(), expected.len(), "{by}/{group}");
            assert_eq!(
                response.total.completions,
                expected.iter().map(|group| group.completions).sum::<u64>()
            );
        }
    }
}

#[tokio::test]
async fn usage_rejects_bad_filters() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let from = stamp(1_700_000_000 - 3_600);
    let to = stamp(1_700_000_000 + 86_400);
    for (by, key, from, to, group) in [
        ("team", None, from.as_str(), to.as_str(), "day"),
        ("agent", None, from.as_str(), to.as_str(), "hour"),
        ("agent", Some(""), from.as_str(), to.as_str(), "day"),
        ("agent", None, to.as_str(), from.as_str(), "day"),
        ("agent", None, "yesterday", to.as_str(), "day"),
        // A 401-day span exceeds the 400-day cap.
        (
            "agent",
            None,
            "2023-01-01T00:00:00Z",
            "2024-02-06T00:00:00Z",
            "day",
        ),
    ] {
        let error = fixture
            .client
            .usage(by, key, from, to, group)
            .await
            .unwrap_err();
        assert!(
            matches!(error, swarmy_client::Error::Api { status, .. }
                if status == reqwest::StatusCode::BAD_REQUEST),
            "{by:?} {key:?}"
        );
    }
    // The span cap names its limit.
    let error = fixture
        .client
        .usage(
            "agent",
            None,
            "2023-01-01T00:00:00Z",
            "2024-02-06T00:00:00Z",
            "day",
        )
        .await
        .unwrap_err();
    let swarmy_client::Error::Api { body, .. } = error else {
        panic!("expected API error");
    };
    assert_eq!(body.code, "span_too_large");
}

#[tokio::test]
async fn usage_echoes_by_and_entry_quota_round_trips() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let (first, _, _) = seed(&fixture.store).await;
    let from = stamp(1_700_000_000 - 3_600);
    let to = stamp(1_700_000_000 + 86_400);
    let to_ts: Timestamp = to.parse().unwrap();
    // The response echoes `by` as sent, so `kind` stays `kind`.
    let echoed = fixture
        .client
        .usage("kind", Some("api-key"), &from, &to, "day")
        .await
        .unwrap();
    assert_eq!(echoed.by, "kind");
    // The hour containing `to` is included: a completion thirty minutes
    // before the bound still lands in the series.
    let half_hour_before_to = Timestamp::from_second(to_ts.as_second() - 1_800).unwrap();
    complete(
        &fixture.store,
        first,
        &CompletionSeed {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            at: half_hour_before_to,
            cost: 7,
        },
    )
    .await;
    let covering = fixture
        .client
        .usage(
            "session",
            Some(&first.to_string()),
            &stamp(to_ts.as_second() - 3_600),
            &to,
            "day",
        )
        .await
        .unwrap();
    assert_eq!(
        covering
            .groups
            .iter()
            .map(|group| group.totals.completions)
            .sum::<u64>(),
        1
    );
    // The `kind` alias reads the same buckets as `entry_kind`.
    let kind = fixture
        .client
        .usage("kind", Some("api-key"), &from, &to, "day")
        .await
        .unwrap();
    let entry_kind = fixture
        .client
        .usage("entry_kind", Some("api-key"), &from, &to, "day")
        .await
        .unwrap();
    assert_eq!(kind.groups, entry_kind.groups);
    // One entry carries observed quotas, another a configured limit.
    let keyring = swarmy_config::Keyring::from_bytes([7; 32]);
    let credentials = fixture.store.credentials(keyring);
    credentials
        .put_entry(
            swarmy_core::CredentialScope::Cluster,
            "openai",
            "main",
            &swarmy_core::CredentialRecord {
                kind: swarmy_core::CredentialKind::ApiKey {
                    key: "test".into(),
                    extra: std::collections::BTreeMap::new(),
                },
                updated_at: Timestamp::now(),
            },
        )
        .await
        .unwrap();
    fixture
        .store
        .observe_entry_quota(
            "openai",
            "main",
            &std::collections::BTreeMap::from([("requests".into(), 97_u64)]),
            &std::collections::BTreeMap::from([("requests".into(), 60_u64)]),
        )
        .await
        .unwrap();
    let view = fixture.client.entry_quota("openai", "main").await.unwrap();
    let stored = fixture.store.entry_quota("openai", "main").await.unwrap();
    assert_eq!(view.source, "observed");
    assert_eq!(view.requests_remaining, Some(97));
    assert_eq!(view.free, stored.free);
    assert_eq!(view.window_seconds, stored.window_seconds);
    let missing = fixture
        .client
        .entry_quota("openai", "missing")
        .await
        .unwrap_err();
    assert!(matches!(missing, swarmy_client::Error::Api { status, .. }
            if status == reqwest::StatusCode::NOT_FOUND));
}
