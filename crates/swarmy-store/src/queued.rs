use crate::{DomainError, Result, Store, StoreError, read, scan, write};
use swarmy_core::{Event, Lease, Message, MessageRole, SessionId, SessionState};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct QueuedMessage {
    message: Message,
    queued_at: jiff::Timestamp,
}

/// The result of submitting a user message through the idempotent API path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UserMessageAppend {
    /// The session head after the submission (unchanged when queued behind a busy session).
    pub sequence: u64,
    /// False when the key replayed an earlier submission.
    pub fresh: bool,
    /// True when the message was appended and the session made runnable;
    /// false when it waits in the queue.
    pub started: bool,
}

/// Events written by [`Store::deliver_queued`] and the session head after them.
#[derive(Debug)]
pub struct QueueDelivery {
    pub events: Vec<Event>,
    /// Equal to the `head` argument when nothing was delivered.
    pub head_seq: u64,
}

impl Store {
    async fn queued_in(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
    ) -> Result<Vec<(Vec<u8>, QueuedMessage)>> {
        let space = self.keys().queued_space(id);
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
    ) -> Result<UserMessageAppend> {
        if message.role != MessageRole::User {
            return Err(StoreError::Domain(DomainError::InvalidMessageRole));
        }
        let replay_key = self.keys().queued_replay(key);
        let keys = self.keys();
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
                    return Ok(UserMessageAppend {
                        sequence: previous.0,
                        fresh: false,
                        started: previous.1,
                    });
                }
                let mut session = self.session(&trx, id).await?;
                if session.state == SessionState::Completed {
                    return Err(StoreError::Domain(DomainError::SessionNotIdle));
                }
                let idle = session.state == SessionState::Idle;
                let head = if idle {
                    crate::seq_after(session.head_seq, 1)?
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
                    trx.set(&self.keys().event(id, head), &event);
                    write(&trx, &self.keys().turn(id), &message.id)?;
                    session.head_seq = head;
                    self.transition(&trx, session, SessionState::Runnable, self.now())
                        .await?;
                } else {
                    let index =
                        crate::seq_after(read::<u64>(&trx, counter_key).await?.unwrap_or(0), 1)?;
                    if scan(
                        &trx,
                        self.keys().queued_space(id).range(),
                        crate::MAX_SCAN_LIMIT,
                    )
                    .await?
                    .len()
                        >= crate::MAX_SCAN_LIMIT
                    {
                        return Err(StoreError::Storage(crate::StorageError::TooLarge));
                    }
                    trx.set(&self.keys().queued_message(id, index), prepared);
                    write(&trx, counter_key, &index)?;
                }
                write(&trx, replay_key, &(head, idle))?;
                Ok(UserMessageAppend {
                    sequence: head,
                    fresh: true,
                    started: idle,
                })
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
    pub async fn deliver_queued(
        &self,
        id: SessionId,
        head: u64,
        lease: &Lease,
        before: &[Event],
    ) -> Result<QueueDelivery> {
        self.transaction(|trx| {
            async move {
                self.check_worker_lease(&trx, id, lease, self.now()).await?;
                let mut session = self.session(&trx, id).await?;
                crate::check_head(session.head_seq, head)?;
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
                    event.set_seq(crate::seq_after(head, events.len() + 1)?);
                    events.push(event);
                }
                for (key, item) in &queue {
                    let size = crate::encode(&item.message)?.len();
                    if !delivered_keys.is_empty() && queued_bytes + size > 512 * 1024 {
                        break;
                    }
                    queued_bytes += size;
                    delivered_keys.push(key);
                    let seq = crate::seq_after(head, events.len() + 1)?;
                    events.push(Event::MessageQueued {
                        seq,
                        message: item.message.clone(),
                        queued_at: item.queued_at,
                    });
                    events.push(Event::MessageAppended {
                        seq: crate::seq_after(seq, 1)?,
                        message: item.message.clone(),
                    });
                }
                for event in &events {
                    trx.set(
                        &self.keys().event(id, event.seq()),
                        &self.prepare(event).await?,
                    );
                }
                if !events.is_empty() {
                    for key in delivered_keys { trx.clear(key); }
                    session.head_seq = crate::seq_after(head, events.len())?;
                    self.write_session(&trx, &session)?;
                }
                Ok(QueueDelivery { head_seq: session.head_seq, events })
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
        let keys = self.keys();
        for (old_key, item) in self.queued_in(trx, old).await? {
            let space = keys.queued_space(old);
            let (index,): (u64,) = space.unpack(&old_key)?;
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
    #![deny(clippy::disallowed_methods)]
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
