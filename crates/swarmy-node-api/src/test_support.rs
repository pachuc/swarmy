//! In-process node API and response faults for tests.
use axum::{Router, middleware, response::IntoResponse};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

/// A router behind a relay whose existing connections can be dropped.
pub struct TestApi {
    url: String,
    relay: SocketAddr,
    backend: SocketAddr,
    server: Option<JoinHandle<()>>,
    relaying: Option<JoinHandle<()>>,
}

impl TestApi {
    /// Bind the router and relay to separate loopback ports.
    /// # Panics
    /// If a listener cannot bind.
    pub async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend bind");
        let backend = listener.local_addr().expect("backend address");
        // The test fixture owns this server until drop or detach.
        // ast-grep-ignore: no-spawn-in-libraries
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve node API");
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("relay bind");
        let relay = listener.local_addr().expect("relay address");
        let relaying = Self::relay(listener, backend);
        Self {
            url: format!("http://{relay}"),
            relay,
            backend,
            server: Some(server),
            relaying: Some(relaying),
        }
    }

    fn relay(listener: TcpListener, backend: SocketAddr) -> JoinHandle<()> {
        // Aborting this task drops its JoinSet and every accepted connection.
        // ast-grep-ignore: no-spawn-in-libraries
        tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((mut incoming, _)) = listener.accept().await {
                // Connections belong to the relay task, not the server.
                // ast-grep-ignore: no-spawn-in-libraries
                connections.spawn(async move {
                    if let Ok(mut outgoing) = TcpStream::connect(backend).await {
                        let _copy =
                            tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                    }
                });
            }
        })
    }

    /// The relay URL, stable across pause and resume.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Refuse new connections and drop all existing relay connections.
    pub fn pause(&mut self) {
        if let Some(handle) = self.relaying.take() {
            handle.abort();
        }
    }

    /// Rebind the original relay address.
    /// # Panics
    /// If the relay address cannot be rebound.
    pub async fn resume(&mut self) {
        if self.relaying.is_none() {
            let listener = TcpListener::bind(self.relay).await.expect("relay rebind");
            self.relaying = Some(Self::relay(listener, self.backend));
        }
    }

    /// Leave the server and relay running for the test process lifetime.
    #[must_use]
    pub fn detach(mut self) -> String {
        self.server.take();
        self.relaying.take();
        std::mem::take(&mut self.url)
    }
}

impl Drop for TestApi {
    fn drop(&mut self) {
        if let Some(handle) = self.relaying.take() {
            handle.abort();
        }
        if let Some(handle) = self.server.take() {
            handle.abort();
        }
    }
}

/// Replace the first successful response on `path` with a 503.
pub fn drop_first_response(router: Router, path: &'static str) -> (Router, Arc<AtomicUsize>) {
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
