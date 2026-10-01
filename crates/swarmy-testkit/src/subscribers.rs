//! Count NATS subscriptions through the server's monitoring endpoint.
//!
//! The dev stack runs the monitoring endpoint beside the client port, so a
//! test can ask the server how many subscriptions would receive a message on
//! a subject without publishing anything. Probing by sending a request would
//! put a message on a live subject; the monitor only reads server state.

/// How many subscriptions would receive a message published on `subject`.
///
/// The server matches `subject` against its subscription table, so wildcard
/// subscriptions count. Tests poll this inside [`eventually`](crate::eventually)
/// to wait for a reader to subscribe (count above zero) or unsubscribe
/// (count back at zero) before publishing.
///
/// # Panics
/// Panics when `SWARMY_NATS_MONITOR_URL` is unset (source `.dev/env`), when
/// the monitor request fails, or when its response has no `num_matches`.
pub async fn nats_subscribers(subject: &str) -> u64 {
    let base = std::env::var("SWARMY_NATS_MONITOR_URL").unwrap_or_else(|_| {
        panic!(
            "SWARMY_NATS_MONITOR_URL is not set; source .dev/env so tests can read the NATS monitor"
        )
    });
    let url = format!("{base}/subsz?subs=0&test={subject}");
    let body: serde_json::Value = reqwest::get(&url)
        .await
        .expect("NATS monitor request failed")
        .json()
        .await
        .expect("NATS monitor response is not JSON");
    body.get("num_matches")
        .and_then(serde_json::Value::as_u64)
        .expect("NATS monitor response has no num_matches")
}
