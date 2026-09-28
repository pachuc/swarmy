use crate::{DomainError, Result, Store, StoreError, read, write};
use swarmy_core::{Event, Lease, Message, MessageRole, SessionId, SessionState};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct QueuedMessage {
    message: Message,
    queued_at: jiff::Timestamp,
}

impl Store {
    pub(crate) fn queued_key(&self, id: SessionId) -> Vec<u8> {
        let mut key = self.session_key(id);
        key.push(0xff);
        key
    }

    /// Queue without changing the log head or the worker's lease. Idle sessions
    /// use the ordinary append path in the same transaction.
    /// # Errors
    /// Rejects terminal sessions, invalid messages, or storage failures.
    pub async fn queue_user_message_idempotent(
        &self,
        id: SessionId,
        message: &Message,
        key: &str,
    ) -> Result<(u64, bool, bool)> {
        if message.role != MessageRole::User {
            return Err(StoreError::Domain(DomainError::InvalidMessageRole));
        }
        let replay_key = crate::keys::Keys::new(&self.root).api_append(key);
        let queued_key = self.queued_key(id);
        let message = message.clone();
        self.transaction(|trx| {
            let (replay_key, queued_key, message) = (&replay_key, &queued_key, &message);
            async move {
                if let Some(previous) = read::<(u64, bool)>(&trx, replay_key).await? {
                    return Ok((previous.0, false, previous.1));
                }
                let mut session = self.session(&trx, id).await?;
                if session.state == SessionState::Completed {
                    return Err(StoreError::Domain(DomainError::SessionNotIdle));
                }
                let idle = session.state == SessionState::Idle;
                let head = if idle {
                    session.head_seq + 1
                } else {
                    session.head_seq
                };
                if idle {
                    let event = Event::MessageAppended {
                        seq: head,
                        message: message.clone(),
                    };
                    trx.set(&self.event_key(id, head), &self.prepare(&event).await?);
                    write(&trx, &self.turn_key(id), &message.id)?;
                    session.head_seq = head;
                    self.transition(&trx, session, SessionState::Runnable, self.now())
                        .await?;
                } else {
                    let mut queue = read::<Vec<QueuedMessage>>(&trx, queued_key)
                        .await?
                        .unwrap_or_default();
                    if queue.len() >= 128 {
                        return Err(StoreError::Storage(crate::StorageError::TooLarge));
                    }
                    queue.push(QueuedMessage {
                        message: message.clone(),
                        queued_at: self.now(),
                    });
                    if crate::encode(&queue)?.len() > crate::MAX_BATCH_BYTES {
                        return Err(StoreError::Storage(crate::StorageError::TooLarge));
                    }
                    write(&trx, queued_key, &queue)?;
                }
                write(&trx, replay_key, &(head, idle))?;
                Ok((head, true, idle))
            }
        })
        .await
    }

    /// Commit all waiting messages at this step boundary under the held lease.
    /// Removing the queue and appending the log are one transaction, including
    /// after an uncertain commit or a worker restart.
    /// # Errors
    /// Rejects stale leases or heads and storage failures.
    /// # Panics
    /// Panics only if a bounded in-memory queue cannot fit in u64.
    pub async fn deliver_queued(
        &self,
        id: SessionId,
        head: u64,
        lease: &Lease,
    ) -> Result<Vec<Event>> {
        let key = self.queued_key(id);
        self.transaction(|trx| {
            let key = &key;
            async move {
                self.check_worker_lease(&trx, id, lease, self.now()).await?;
                let mut session = self.session(&trx, id).await?;
                if session.head_seq != head {
                    return Err(StoreError::Fence(crate::FenceError::StaleSequence {
                        expected: head,
                        actual: session.head_seq,
                    }));
                }
                let queue = read::<Vec<QueuedMessage>>(&trx, key)
                    .await?
                    .unwrap_or_default();
                let mut events = Vec::with_capacity(queue.len() * 2);
                for item in queue {
                    let seq = head + u64::try_from(events.len()).expect("queue bounded") + 1;
                    events.push(Event::MessageQueued {
                        seq,
                        message: item.message.clone(),
                        queued_at: item.queued_at,
                    });
                    events.push(Event::MessageAppended {
                        seq: seq + 1,
                        message: item.message,
                    });
                }
                for event in &events {
                    trx.set(
                        &self.event_key(id, event.seq()),
                        &self.prepare(event).await?,
                    );
                }
                if !events.is_empty() {
                    trx.clear(key);
                    session.head_seq += u64::try_from(events.len()).expect("queue bounded");
                    self.write_session(&trx, &session)?;
                }
                Ok(events)
            }
        })
        .await
    }

    /// Whether a successor has pending input that should wake it.
    /// # Errors
    /// Returns storage failures.
    pub async fn has_queued(&self, id: SessionId) -> Result<bool> {
        let key = self.queued_key(id);
        self.transaction(|trx| {
            let key = &key;
            async move {
                Ok(read::<Vec<QueuedMessage>>(&trx, key)
                    .await?
                    .is_some_and(|queue| !queue.is_empty()))
            }
        })
        .await
    }

    pub(crate) async fn transfer_queued(
        &self,
        trx: &foundationdb::Transaction,
        old: SessionId,
        new: SessionId,
    ) -> Result<()> {
        let old_key = self.queued_key(old);
        let new_key = self.queued_key(new);
        if let Some(queue) = read::<Vec<QueuedMessage>>(trx, &old_key).await? {
            write(trx, &new_key, &queue)?;
            trx.clear(&old_key);
        }
        Ok(())
    }
}
