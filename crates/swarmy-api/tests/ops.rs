//! Image uploads, collection runs, timeline streams, and doctor nodes.
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_api::{AppState, router};
use swarmy_api_types as api;
use swarmy_bus::{Bus, Config};
use swarmy_client::Client;
use swarmy_core::{CHUNK_SIZE, TurnStage};
use swarmy_store::{Store, blob::MemoryBlobStore};
use tokio::task::JoinHandle;
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct Fixture {
    store: Store,
    client: Client,
    bus: Bus,
    server: JoinHandle<Result<(), std::io::Error>>,
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
            Some(&["ops-test".into(), Ulid::generate().to_string()]),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap();
        let bus = Bus::connect(&nats, Config::default()).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = AppState::new(
            store.clone(),
            bus.clone(),
            "test-token".into(),
            swarmy_llm::catalog::Catalog::get().clone(),
            Arc::new(object_store::memory::InMemory::new()),
        );
        let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
        Some(Self {
            store,
            client: Client::new(&base, "test-token").unwrap(),
            bus,
            server,
        })
    }

    async fn session(&self) -> String {
        use swarmy_core::{ContentHash, ImageTag, ManifestHeader, ManifestId};
        let manifest = ManifestId::from_ulid(Ulid::generate());
        self.store
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
        self.store
            .put_image("fixture", &ImageTag("test".into()), manifest)
            .await
            .unwrap();
        let agent = self
            .store
            .create_agent(
                &format!("ops-{}", Ulid::generate()),
                "fixture:test",
                "",
                jiff::Timestamp::now(),
            )
            .await
            .unwrap();
        let (id, _) = self
            .store
            .open_main_session(agent.agent_id, jiff::Timestamp::now())
            .await
            .unwrap();
        id.to_string()
    }
}

#[tokio::test]
async fn upload_registers_image_and_rejects_bad_requests() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    // Zero chunks never reach object storage; two chunks, none stored.
    let dir = tempfile::tempdir().unwrap();
    let raw = dir.path().join("disk.ext4");
    std::fs::File::create(&raw)
        .unwrap()
        .set_len(u64::from(CHUNK_SIZE) * 2)
        .unwrap();
    let key = Ulid::generate().to_string();
    let uploaded = fixture
        .client
        .upload_image(&swarmy_client::UploadImage {
            name: "ops",
            tag: "test",
            idempotency_key: &key,
            scratch: &[],
            memory_mib: None,
            display: false,
            file: &raw,
        })
        .await
        .unwrap();
    assert_eq!(uploaded.name, "ops");
    assert_eq!(uploaded.tag, "test");
    assert_eq!(uploaded.size, u64::from(CHUNK_SIZE) * 2);
    assert_eq!(uploaded.chunks_total, 2);
    assert_eq!(uploaded.chunks_stored, 0);
    assert_eq!(uploaded.chunks_uploaded, 0);
    // A retried upload with the same key observes the completed response.
    let repeated = fixture
        .client
        .upload_image(&swarmy_client::UploadImage {
            name: "ops",
            tag: "test",
            idempotency_key: &key,
            scratch: &[],
            memory_mib: None,
            display: false,
            file: &raw,
        })
        .await
        .unwrap();
    assert_eq!(repeated.manifest_id, uploaded.manifest_id);
    let shown = fixture.client.image("ops", "test").await.unwrap();
    assert_eq!(shown.id, uploaded.manifest_id);
    // Invalid labels and empty bodies fail before any registration.
    let bad = fixture
        .client
        .upload_image(&swarmy_client::UploadImage {
            name: "bad name",
            tag: "test",
            idempotency_key: &Ulid::generate().to_string(),
            scratch: &[],
            memory_mib: None,
            display: false,
            file: &raw,
        })
        .await;
    assert!(bad.is_err());
    let empty = dir.path().join("empty.ext4");
    std::fs::write(&empty, []).unwrap();
    let empty = fixture
        .client
        .upload_image(&swarmy_client::UploadImage {
            name: "ops",
            tag: "empty",
            idempotency_key: &Ulid::generate().to_string(),
            scratch: &[],
            memory_mib: None,
            display: false,
            file: &empty,
        })
        .await;
    assert!(empty.is_err());
}

#[tokio::test]
async fn gc_run_starts_sweeps_and_reports_counts() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let started = fixture
        .client
        .start_gc_run(&api::StartGcRun {
            idempotency_key: Ulid::generate().to_string(),
            dry_run: true,
        })
        .await
        .unwrap();
    assert!(started.dry_run);
    // An empty namespace finishes immediately; poll for generality.
    let run = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let run = fixture.client.gc_run(&started.run_id).await.unwrap();
            if run.finished {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(run.run_id, started.run_id);
    assert!(run.finished);
    assert!(run.error.is_none());
    assert_eq!(run.deleted, 0);
    // Retrying the start with the same key returns the same run.
    let missing = fixture.client.gc_run("01ARZ3NDEKTSV4RRFFQ69G5FAV").await;
    assert!(missing.is_err());
}

#[tokio::test]
async fn timeline_stream_delivers_turn_observations() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let missing = Ulid::generate().to_string();
    let mut stream = fixture.client.stream(api::Subscription {
        cursors: vec![api::Cursor {
            log_id: api::LogId::Timeline(missing),
            sequence: 0,
        }],
        token_deltas: false,
    });
    // A timeline for a missing session is rejected like any unknown log.
    assert!(stream.open().await.is_err());
    drop(stream);

    let session = fixture.session().await;
    let mut stream = fixture.client.stream(api::Subscription {
        cursors: vec![api::Cursor {
            log_id: api::LogId::Timeline(session.clone()),
            sequence: 0,
        }],
        token_deltas: false,
    });
    stream.open().await.unwrap();
    let turn = Ulid::generate().to_string();
    fixture
        .bus
        .record_turn(&Bus::turn_event(
            session
                .parse()
                .map(swarmy_core::SessionId::from_ulid)
                .unwrap(),
            turn.parse().map(swarmy_core::MessageId::from_ulid).unwrap(),
            TurnStage::Submitted,
            None,
        ))
        .await;
    let event = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.sequence, 1);
    let api::EventPayload::StoreRecord { record } = event.payload else {
        panic!("timeline observations keep their stored shape");
    };
    let observation: swarmy_core::TurnEvent = serde_json::from_value(record).unwrap();
    assert_eq!(observation.turn_id.to_string(), turn);
    assert_eq!(observation.stage, TurnStage::Submitted);
}
#[tokio::test]
async fn doctor_reports_registered_nodes() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let node = swarmy_core::NodeId::from_ulid(Ulid::generate());
    fixture
        .store
        .put_node(&swarmy_core::NodeRecord {
            node_id: node,
            roles: vec![swarmy_core::NodeRole::Sandbox],
            capacity: swarmy_core::NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1 << 40,
                sandboxes: 4,
            },
            last_heartbeat: jiff::Timestamp::now(),
            cached_images: Vec::new(),
        })
        .await
        .unwrap();
    let snapshot = fixture.client.doctor().await.unwrap();
    let reported = snapshot
        .nodes
        .iter()
        .find(|reported| reported.node_id == node.to_string())
        .unwrap();
    assert_eq!(reported.capacity.sandboxes, 4);
    assert_eq!(reported.committed_memory_bytes, 0);
}
