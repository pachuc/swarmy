use crate::{DomainError, Result, Store, StoreError, read, scan, write};
use swarmy_core::{Event, Lease, Message, MessageRole, SessionId, SessionState};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct QueuedMessage {
    message: Message,
    queued_at: jiff::Timestamp,
}

impl Store {
    async fn queued_in(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
    ) -> Result<Vec<(Vec<u8>, QueuedMessage)>> {
        let space = crate::keys::Keys::new(&self.root).queued_space(id);
        let mut result = Vec::new();
        for (key, value) in scan(trx, space.range(), crate::MAX_SCAN_LIMIT).await? {
            result.push((key, self.hydrate(&value).await?));
        }
        Ok(result)
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
        let replay_key = crate::keys::Keys::new(&self.root).queued_replay(key);
        let keys = crate::keys::Keys::new(&self.root);
        let counter_key = keys.queued_counter(id);
        let prepared = self
            .prepare(&QueuedMessage {
                message: message.clone(),
                queued_at: self.now(),
            })
            .await?;
        let message = message.clone();
        self.transaction(|trx| {
            let (replay_key, counter_key, message, prepared) =
                (&replay_key, &counter_key, &message, &prepared);
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
                    let event = self
                        .prepare(&Event::MessageAppended {
                            seq: head,
                            message: message.clone(),
                        })
                        .await?;
                    trx.set(&self.event_key(id, head), &event);
                    write(&trx, &self.turn_key(id), &message.id)?;
                    session.head_seq = head;
                    self.transition(&trx, session, SessionState::Runnable, self.now())
                        .await?;
                } else {
                    let index = read::<u64>(&trx, counter_key)
                        .await?
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
                    if scan(
                        &trx,
                        crate::keys::Keys::new(&self.root).queued_space(id).range(),
                        crate::MAX_SCAN_LIMIT,
                    )
                    .await?
                    .len()
                        >= crate::MAX_SCAN_LIMIT
                    {
                        return Err(StoreError::Storage(crate::StorageError::TooLarge));
                    }
                    trx.set(
                        &crate::keys::Keys::new(&self.root).queued_message(id, index),
                        prepared,
                    );
                    write(&trx, counter_key, &index)?;
                }
                write(&trx, replay_key, &(head, idle))?;
                Ok((head, true, idle))
            }
        })
        .await
    }

    /// Commit a completed tool message and waiting input together at the step
    /// boundary under the held lease.
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
        before: &[Event],
    ) -> Result<Vec<Event>> {
        self.transaction(|trx| {
            async move {
                self.check_worker_lease(&trx, id, lease, self.now()).await?;
                let mut session = self.session(&trx, id).await?;
                if session.head_seq != head {
                    return Err(StoreError::Fence(crate::FenceError::StaleSequence {
                        expected: head,
                        actual: session.head_seq,
                    }));
                }
                let queue = self.queued_in(&trx, id).await?;
                let mut events = Vec::with_capacity(before.len() + queue.len() * 2);
                let mut delivered_keys = Vec::new();
                // Each message is written twice (queue marker and user message).
                // Leave ample room for the tool result and transaction overhead.
                let mut queued_bytes = 0usize;
                for event in before {
                    if !matches!(event, Event::MessageAppended { message, .. } if message.role == MessageRole::Tool) {
                        return Err(StoreError::Domain(DomainError::InvalidMessageRole));
                    }
                    let mut event = event.clone();
                    event.set_seq(head + u64::try_from(events.len()).expect("batch bounded") + 1);
                    events.push(event);
                }
                for (key, item) in &queue {
                    let size = crate::encode(&item.message)?.len();
                    if !delivered_keys.is_empty() && queued_bytes + size > 512 * 1024 {
                        break;
                    }
                    queued_bytes += size;
                    delivered_keys.push(key);
                    let seq = head + u64::try_from(events.len()).expect("queue bounded") + 1;
                    events.push(Event::MessageQueued {
                        seq,
                        message: item.message.clone(),
                        queued_at: item.queued_at,
                    });
                    events.push(Event::MessageAppended {
                        seq: seq + 1,
                        message: item.message.clone(),
                    });
                }
                for event in &events {
                    trx.set(
                        &self.event_key(id, event.seq()),
                        &self.prepare(event).await?,
                    );
                }
                if !events.is_empty() {
                    for key in delivered_keys { trx.clear(key); }
                    session.head_seq += u64::try_from(events.len()).expect("queue bounded");
                    self.write_session(&trx, &session)?;
                }
                Ok(events)
            }
        })
        .await
    }

    pub(crate) async fn has_queued_in(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
    ) -> Result<bool> {
        Ok(!self.queued_in(trx, id).await?.is_empty())
    }

    pub(crate) async fn transfer_queued(
        &self,
        trx: &foundationdb::Transaction,
        old: SessionId,
        new: SessionId,
    ) -> Result<()> {
        let keys = crate::keys::Keys::new(&self.root);
        for (old_key, item) in self.queued_in(trx, old).await? {
            let space = keys.queued_space(old);
            let (index,): (u64,) = space
                .unpack(&old_key)
                .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
            trx.set(
                &keys.queued_message(new, index),
                &self.prepare(&item).await?,
            );
            trx.clear(&old_key);
        }
        if let Some(counter) = read::<u64>(trx, &keys.queued_counter(old)).await? {
            write(trx, &keys.queued_counter(new), &counter)?;
            trx.clear(&keys.queued_counter(old));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_queued_message_bytes() {
        let value = QueuedMessage {
            message: Message {
                id: swarmy_core::MessageId::from_ulid(ulid::Ulid::from(0_u128)),
                role: MessageRole::User,
                parts: Vec::new(),
            },
            queued_at: jiff::Timestamp::UNIX_EPOCH,
        };
        let bytes = crate::encode(&value).unwrap();
        let mut expected = vec![1, 26];
        expected.extend_from_slice(b"00000000000000000000000000");
        expected.extend_from_slice(&[1, 0, 20]);
        expected.extend_from_slice(b"1970-01-01T00:00:00Z");
        assert_eq!(bytes, expected);
        let decoded: QueuedMessage = crate::decode(&bytes).unwrap();
        assert_eq!(decoded.message, value.message);
        assert_eq!(decoded.queued_at, value.queued_at);
    }
}
