use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use swarmy_core::{MessageId, Nudge, SessionId, TurnStage, runnable_partition};
use tokio::sync::Mutex;

use crate::{Bus, Error, WorkQueue};

pub(crate) type RecentNudges = Arc<Mutex<NudgeCache>>;

#[derive(Default)]
pub(crate) struct NudgeCache {
    entries: HashMap<SessionId, (u64, Instant)>,
    pruned: Option<Instant>,
}

impl Bus {
    /// Publish readiness using the same partition and resend rule in every service.
    /// A new durable head bypasses suppression for the preceding step. Reaping a
    /// lease forces publication even if that worker never appended an event.
    /// The cache covers this connection and its clones; competing publishers are
    /// harmless because workers claim the session transactionally.
    /// # Errors
    /// Returns publication errors. Failed sends never advance the resend deadline.
    pub async fn nudge(
        &self,
        session_id: SessionId,
        head: u64,
        turn: Option<MessageId>,
        resend: Duration,
        force: bool,
    ) -> Result<(), Error> {
        let mut recent = self.recent_nudges.lock().await;
        if recent
            .pruned
            .is_none_or(|pruned| pruned.elapsed() >= resend)
        {
            recent
                .entries
                .retain(|_, (_, sent)| sent.elapsed() < resend);
            recent.pruned = Some(Instant::now());
        }
        if !force
            && recent
                .entries
                .get(&session_id)
                .is_some_and(|(old, sent)| *old == head && sent.elapsed() < resend)
        {
            return Ok(());
        }
        let observation =
            turn.map(|turn| Self::turn_event(session_id, turn, TurnStage::Nudged, None));
        let partition = runnable_partition(session_id);
        self.publish_work(&WorkQueue::Runnable(partition), &Nudge { session_id })
            .await?;
        recent.entries.insert(session_id, (head, Instant::now()));
        drop(recent);
        if let Some(event) = observation {
            self.record_turn(&event).await;
        }
        tracing::info!(%session_id, partition, head, "nudged runnable session");
        Ok(())
    }
}
