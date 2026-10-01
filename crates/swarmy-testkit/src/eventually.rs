//! Poll-until-ready helper for tests.

use std::time::Duration;

/// Poll `probe` until it returns `Some`, or panic with `label` after `budget`.
///
/// Probes run every 20 milliseconds, so fast machines finish quickly. The
/// budget fails the test loudly instead of hanging the suite, and the label
/// names the awaited condition, so a timeout points at the missing state.
///
/// Each probe call runs under the remaining budget, so a hung probe fails
/// the test instead of stalling the suite past the budget.
///
/// # Panics
/// Panics when `budget` elapses before `probe` returns `Some`.
pub async fn eventually<T>(
    label: &'static str,
    budget: Duration,
    mut probe: impl AsyncFnMut() -> Option<T>,
) -> T {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if let Ok(Some(value)) = tokio::time::timeout(remaining, probe()).await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {label}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
