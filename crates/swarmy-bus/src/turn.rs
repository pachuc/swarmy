use crate::{Bus, LiveFeed};
use std::sync::LazyLock;
use swarmy_core::{MessageId, RequestId, SessionId, TurnEvent, TurnStage};

static CLOCK_ID: LazyLock<String> = LazyLock::new(|| {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map_or_else(
        |_| format!("process-{}-{}", std::process::id(), jiff::Timestamp::now()),
        |id| id.trim().to_owned(),
    )
});

impl Bus {
    /// Capture the boundary before any instrumentation publication awaits.
    #[must_use]
    pub fn turn_event(
        session_id: SessionId,
        turn_id: MessageId,
        stage: TurnStage,
        request_id: Option<RequestId>,
    ) -> TurnEvent {
        let time = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
        let monotonic_ns = u64::try_from(time.tv_sec).unwrap_or_default() * 1_000_000_000
            + u64::try_from(time.tv_nsec).unwrap_or_default();
        TurnEvent {
            session_id,
            turn_id,
            stage,
            request_id,
            clock_id: CLOCK_ID.clone(),
            monotonic_ns,
            unix_ns: jiff::Timestamp::now().as_nanosecond(),
        }
    }

    /// Observability must not change the success of a committed operation.
    pub async fn record_turn(&self, event: &TurnEvent) {
        tracing::info!(session_id = %event.session_id, turn_id = %event.turn_id,
            stage = ?event.stage, request_id = ?event.request_id,
            monotonic_ns = event.monotonic_ns, clock_id = %event.clock_id,
            unix_ns = %event.unix_ns, "turn stage");
        if let Err(error) = self
            .publish_live(LiveFeed::TurnTimeline(event.session_id), event)
            .await
        {
            tracing::warn!(%error, "turn observation publication failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clock_domain_and_monotonic_timestamp_survive_encoding() {
        let id = SessionId::from_ulid(ulid::Ulid::generate());
        let turn = MessageId::from_ulid(ulid::Ulid::generate());
        let first = Bus::turn_event(id, turn, TurnStage::Appended, None);
        let second = Bus::turn_event(id, turn, TurnStage::Idle, None);
        assert_eq!(first.clock_id, second.clock_id);
        assert!(second.monotonic_ns >= first.monotonic_ns);
        assert_eq!(
            swarmy_core::decode::<TurnEvent>(&swarmy_core::encode(&first).unwrap()).unwrap(),
            first
        );
    }
}
