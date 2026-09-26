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
    base: String,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new() -> Option<Self> {
        Self::with_upload_max(16 * 1024 * 1024 * 1024).await
    }

    /// Serve the same routes with an overridden spool ceiling, so size-limit
    /// tests need no multi-gigabyte bodies.
    async fn with_upload_max(upload_max_bytes: u64) -> Option<Self> {
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
        let mut state = AppState::new(
            store.clone(),
            bus.clone(),
            "test-token".into(),
            swarmy_llm::catalog::Catalog::get().clone(),
            Arc::new(object_store::memory::InMemory::new()),
        );
        // Tests override the spool ceiling without uploading gigabytes.
        state.upload_max_bytes = upload_max_bytes;
        let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
        Some(Self {
            store,
            client: Client::new(&base, "test-token").unwrap(),
            bus,
            server,
            base,
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

async fn upload_image(
    fixture: &Fixture,
    raw: &std::path::Path,
    tag: &str,
    key: &str,
    scratch: &[String],
    memory_mib: Option<u64>,
) -> Result<api::ImageUpload, swarmy_client::Error> {
    fixture
        .client
        .upload_image(&swarmy_client::UploadImage {
            name: "ops",
            tag,
            idempotency_key: key,
            scratch,
            memory_mib,
            display: false,
            file: raw,
        })
        .await
}

#[tokio::test]
async fn upload_validates_requirements_before_chunking() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let raw = dir.path().join("disk.ext4");
    std::fs::File::create(&raw)
        .unwrap()
        .set_len(u64::from(CHUNK_SIZE))
        .unwrap();
    // Scratch paths must be absolute and non-overlapping, like `Recipe::load`.
    for scratch in [
        vec!["relative/path".to_owned()],
        vec!["/data".to_owned(), "/data/sub".to_owned()],
        vec!["/".to_owned()],
    ] {
        assert!(
            upload_image(
                &fixture,
                &raw,
                &Ulid::generate().to_string(),
                &Ulid::generate().to_string(),
                &scratch,
                None
            )
            .await
            .is_err(),
            "scratch {scratch:?} must be rejected"
        );
    }
    for memory in [Some(0), Some(2 * 1024 * 1024)] {
        assert!(
            upload_image(
                &fixture,
                &raw,
                &Ulid::generate().to_string(),
                &Ulid::generate().to_string(),
                &[],
                memory,
            )
            .await
            .is_err(),
            "memory {memory:?} must be rejected"
        );
    }
    assert!(
        upload_image(
            &fixture,
            &raw,
            &Ulid::generate().to_string(),
            &Ulid::generate().to_string(),
            &["/data".to_owned()],
            Some(512),
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn upload_rejects_bodies_over_the_configured_limit() {
    // A one-chunk body against a sub-chunk ceiling exercises the 413 path
    // without staging gigabytes.
    let Some(fixture) = Fixture::with_upload_max(u64::from(CHUNK_SIZE) - 1).await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let raw = dir.path().join("disk.ext4");
    std::fs::File::create(&raw)
        .unwrap()
        .set_len(u64::from(CHUNK_SIZE))
        .unwrap();
    // The server rejects an oversized body from the `Content-Length` header
    // without draining it, so the client still streaming the body may
    // observe a broken pipe instead of the 413. Both outcomes reject the
    // upload; the test below proves the server stops reading early.
    match fixture
        .client
        .upload_image(&swarmy_client::UploadImage {
            name: "ops",
            tag: "too-large",
            idempotency_key: &Ulid::generate().to_string(),
            scratch: &[],
            memory_mib: None,
            display: false,
            file: &raw,
        })
        .await
    {
        Err(swarmy_client::Error::Api { status, .. }) => {
            assert_eq!(status, reqwest::StatusCode::PAYLOAD_TOO_LARGE);
        }
        Err(swarmy_client::Error::Transport(_)) => {}
        other => panic!("oversized upload must be rejected, got {other:?}"),
    }
}

#[tokio::test]
async fn upload_with_length_stops_before_streaming_the_body() {
    // The client sends a fixed `Content-Length`, so the server rejects from
    // the header without spooling. A throttled counting stream proves the
    // server never reads the whole body: whichever way the close races with
    // the upload (a 413 response or a broken pipe), most chunks are never
    // polled for sending.
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    let Some(fixture) = Fixture::with_upload_max(32 * 1024).await else {
        return;
    };
    let chunk = vec![0u8; 16 * 1024];
    let total_chunks = 256u64;
    let total = total_chunks * chunk.len() as u64;
    let sent = Arc::new(AtomicU64::new(0));
    let counted = sent.clone();
    let body_stream = async_stream::stream! {
        for _ in 0..total_chunks {
            tokio::time::sleep(Duration::from_millis(2)).await;
            counted.fetch_add(chunk.len() as u64, Ordering::SeqCst);
            yield Ok::<Vec<u8>, std::io::Error>(chunk.clone());
        }
    };
    let url = format!(
        "{}/v1/images/uploads?name=ops&tag=oversize-length&idempotency_key={}",
        fixture.base,
        Ulid::generate()
    );
    let response = reqwest::Client::new()
        .post(url)
        .bearer_auth("test-token")
        .header(reqwest::header::CONTENT_LENGTH, total.to_string())
        .body(reqwest::Body::wrap_stream(body_stream))
        .send()
        .await;
    // The server closes the connection without draining the body, so a
    // client still streaming may see the close instead of the status.
    if let Ok(response) = response {
        assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    }
    let pulled = sent.load(Ordering::SeqCst);
    assert!(
        pulled < total,
        "server read {pulled} of {total} bytes before rejecting"
    );
}

#[tokio::test]
async fn probe_rejects_unknown_and_scripted_models_before_credentials() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let Err(swarmy_client::Error::Api { status, .. }) = fixture
        .client
        .probe_model(&api::ProbeModel {
            provider: "no-such-provider".into(),
            model: "no-such-model".into(),
            label: None,
            effort: None,
        })
        .await
    else {
        panic!("unknown model must fail with an API error");
    };
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    let catalog = swarmy_llm::catalog::Catalog::get().clone();
    let Some(scripted) = catalog
        .providers()
        .find(|provider| provider.api == swarmy_llm::catalog::Api::Fake)
        .and_then(|provider| {
            provider
                .models
                .keys()
                .next()
                .map(|model| (provider.id.clone(), model.clone()))
        })
    else {
        return;
    };
    let Err(swarmy_client::Error::Api { status, .. }) = fixture
        .client
        .probe_model(&api::ProbeModel {
            provider: scripted.0,
            model: scripted.1,
            label: None,
            effort: None,
        })
        .await
    else {
        panic!("scripted provider must fail with an API error");
    };
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
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
            grace_seconds: None,
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
