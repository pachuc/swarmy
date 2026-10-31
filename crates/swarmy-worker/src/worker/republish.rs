use jiff::Timestamp;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use swarmy_core::RequestId;
use tokio::sync::Mutex;

/// First republish of a job after a confirmed publish; doubles per publish.
pub(crate) const REPUBLISH_FIRST_DELAY: Duration = Duration::from_secs(5);
/// Longest wait between republishes of one pending job.
pub(crate) const REPUBLISH_MAX_DELAY: Duration = Duration::from_secs(60);

struct Published {
    at: Timestamp,
    delay: Duration,
}

/// Per-worker memory of tool publishes. Lost on restart on purpose: a fresh
/// worker republishes pending jobs once to repair a publish lost before NATS.
#[derive(Default)]
pub(crate) struct Republish(Mutex<HashMap<RequestId, Published>>);

impl Republish {
    pub(crate) async fn due(&self, id: RequestId, now: Timestamp) -> bool {
        self.0.lock().await.get(&id).is_none_or(|entry| {
            entry
                .at
                .checked_add(entry.delay)
                .is_ok_and(|due| now >= due)
        })
    }

    pub(crate) async fn published(&self, id: RequestId, now: Timestamp) {
        self.0
            .lock()
            .await
            .entry(id)
            .and_modify(|entry| {
                entry.at = now;
                entry.delay = (entry.delay * 2).min(REPUBLISH_MAX_DELAY);
            })
            .or_insert(Published {
                at: now,
                delay: REPUBLISH_FIRST_DELAY,
            });
    }

    pub(crate) async fn forget(&self, id: RequestId) {
        self.0.lock().await.remove(&id);
    }

    pub(crate) async fn retain(&self, seen: &HashSet<RequestId>, scan_started: Timestamp) {
        self.0
            .lock()
            .await
            .retain(|id, entry| seen.contains(id) || entry.at >= scan_started);
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[tokio::test]
    async fn republishes_with_exponential_backoff_capped_at_sixty_seconds() {
        let schedule = Republish::default();
        let id = RequestId::for_step(swarmy_core::SessionId::from_ulid(ulid::Ulid::generate()), 1);
        let mut now = Timestamp::now();
        assert!(schedule.due(id, now).await);
        for delay in [5, 10, 20, 40, 60, 60] {
            schedule.published(id, now).await;
            assert!(
                !schedule
                    .due(
                        id,
                        now.checked_add(Duration::from_millis(delay * 1000 - 100))
                            .unwrap()
                    )
                    .await
            );
            now = now.checked_add(Duration::from_secs(delay)).unwrap();
            assert!(schedule.due(id, now).await);
        }
    }
}
