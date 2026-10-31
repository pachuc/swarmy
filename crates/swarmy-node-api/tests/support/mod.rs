#![deny(clippy::disallowed_methods)]
use axum::{Router, middleware, response::IntoResponse};
use jiff::Timestamp;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use swarmy_core::{NodeCapacity, NodeId, NodeRecord, NodeRole};
use swarmy_node_protocol::NodeAdvert;
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;
pub(crate) struct Fixture {
    pub(crate) store: Store,
    pub(crate) clock: Arc<Mutex<Timestamp>>,
    pub(crate) _guard: swarmy_testkit::StackGuard,
}
pub(crate) async fn fixture(tag: &str) -> Option<Fixture> {
    let stack = swarmy_testkit::Stack::load(tag)?;
    let (store, guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let clock = Arc::new(Mutex::new(Timestamp::now()));
    let copy = clock.clone();
    Some(Fixture {
        store: store.with_clock(move || *copy.lock().unwrap()),
        clock,
        _guard: guard,
    })
}
pub(crate) async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}
pub(crate) fn drop_first_response(
    router: Router,
    path: &'static str,
) -> (Router, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let copy = count.clone();
    let wrapped = router.layer(middleware::from_fn(
        move |request: axum::extract::Request, next: middleware::Next| {
            let count = copy.clone();
            async move {
                let selected = request.uri().path() == path;
                let answer = next.run(request).await;
                if selected
                    && count.fetch_add(1, Ordering::SeqCst) == 0
                    && answer.status().is_success()
                {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                } else {
                    answer
                }
            }
        },
    ));
    (wrapped, count)
}
pub(crate) async fn node(store: &Store, sandboxes: u32) -> NodeRecord {
    let record = NodeRecord {
        node_id: NodeId::from_ulid(Ulid::generate()),
        roles: vec![NodeRole::Sandbox, NodeRole::Volume],
        capacity: NodeCapacity {
            cpu_millis: 64_000,
            memory_bytes: 1 << 40,
            disk_bytes: 1 << 40,
            sandboxes,
        },
        last_heartbeat: store.now(),
        cached_images: Vec::new(),
        gpu: None,
    };
    store.put_node(&record).await.unwrap();
    record
}
pub(crate) fn advert(record: &NodeRecord) -> NodeAdvert {
    NodeAdvert {
        roles: record.roles.clone(),
        capacity: record.capacity.clone(),
        gpu: record.gpu.clone(),
    }
}
