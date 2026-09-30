//! Poll-until-ready helper replacing hand-rolled sleep loops.

use std::time::Duration;

/// Poll `probe` until it returns `Some`, or panic with `label` after `budget`.
///
/// Every poll loop in the suites had its own interval and its own silent
/// timeout. One helper keeps the shape identical everywhere: short ticks so
/// fast machines finish quickly, a budget that fails loudly instead of
/// hanging the suite, and a label naming the condition so the failure points
/// at the missing state rather than a bare timeout line.
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
        if let Some(value) = probe().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {label}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
