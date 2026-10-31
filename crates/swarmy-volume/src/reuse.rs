//! Coalesces concurrent reuse guards for already-stored chunks into one store
//! transaction (group commit). The caller holding the turn protects every
//! queued hash and reports each result without spawning a task.
use std::{
    future::Future,
    sync::{Mutex, PoisonError},
    time::Duration,
};

use swarmy_core::ContentHash;
use tokio::sync::oneshot;

use crate::Result;

const DELETING_RETRY: Duration = Duration::from_millis(10);

#[derive(Debug)]
enum Outcome {
    Protected,
    Deleting,
    Retry,
}

type Waiter = (ContentHash, oneshot::Sender<Outcome>);

#[derive(Default)]
pub(crate) struct ReuseBatcher {
    pending: Mutex<Vec<Waiter>>,
    turn: tokio::sync::Mutex<()>,
}

impl ReuseBatcher {
    pub(crate) async fn protect<F, Fut>(&self, hash: ContentHash, protect: &F) -> Result<()>
    where
        F: Fn(Vec<ContentHash>) -> Fut,
        Fut: Future<Output = Result<Vec<ContentHash>>>,
    {
        loop {
            let (sender, mut receiver) = oneshot::channel();
            self.pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((hash, sender));
            let outcome = {
                let _turn = self.turn.lock().await;
                match receiver.try_recv() {
                    Ok(outcome) => outcome,
                    Err(oneshot::error::TryRecvError::Closed) => Outcome::Retry,
                    Err(oneshot::error::TryRecvError::Empty) => {
                        self.run_batch(protect).await?;
                        receiver.try_recv().unwrap_or(Outcome::Retry)
                    }
                }
            };
            match outcome {
                Outcome::Protected => return Ok(()),
                Outcome::Deleting => tokio::time::sleep(DELETING_RETRY).await,
                Outcome::Retry => {}
            }
        }
    }

    async fn run_batch<F, Fut>(&self, protect: &F) -> Result<()>
    where
        F: Fn(Vec<ContentHash>) -> Fut,
        Fut: Future<Output = Result<Vec<ContentHash>>>,
    {
        let batch =
            std::mem::take(&mut *self.pending.lock().unwrap_or_else(PoisonError::into_inner));
        let hashes = batch.iter().map(|(hash, _)| *hash).collect();
        match protect(hashes).await {
            Ok(deleting) => {
                for (hash, sender) in batch {
                    let outcome = if deleting.contains(&hash) {
                        Outcome::Deleting
                    } else {
                        Outcome::Protected
                    };
                    swarmy_core::ignore_best_effort(
                        sender.send(outcome),
                        "report reuse protection",
                    );
                }
                Ok(())
            }
            Err(error) => {
                for (_, sender) in batch {
                    swarmy_core::ignore_best_effort(
                        sender.send(Outcome::Retry),
                        "report failed reuse batch",
                    );
                }
                Err(error)
            }
        }
    }

    #[cfg(test)]
    fn queued(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    #[tokio::test]
    async fn guards_queued_behind_a_running_batch_share_the_next_one() {
        let batcher = ReuseBatcher::default();
        let batches = Mutex::new(Vec::<Vec<ContentHash>>::new());
        let (open, gate) = oneshot::channel::<()>();
        let gate = tokio::sync::Mutex::new(Some(gate));
        let protect = |hashes: Vec<ContentHash>| {
            let (batches, gate) = (&batches, &gate);
            async move {
                if let Some(gate) = gate.lock().await.take() {
                    gate.await.expect("the test opens the gate");
                }
                batches.lock().unwrap().push(hashes);
                Ok(Vec::new())
            }
        };
        let hashes: Vec<_> = (1..=8u8).map(|n| ContentHash([n; 32])).collect();
        let guards = futures::future::try_join_all(
            hashes.iter().map(|&hash| batcher.protect(hash, &protect)),
        );
        let opener = async {
            while batcher.queued() < hashes.len() - 1 {
                tokio::task::yield_now().await;
            }
            open.send(()).expect("the first guard waits on the gate");
        };
        let (guarded, ()) = tokio::join!(guards, opener);
        guarded.unwrap();
        assert_eq!(
            *batches.lock().unwrap(),
            vec![vec![hashes[0]], hashes[1..].to_vec()]
        );
    }

    #[tokio::test]
    async fn a_hash_being_deleted_is_asked_again_until_protected() {
        let batcher = ReuseBatcher::default();
        let calls = Mutex::new(0);
        let hash = ContentHash([1; 32]);
        let protect = |hashes: Vec<ContentHash>| {
            let calls = &calls;
            async move {
                let mut calls = calls.lock().unwrap();
                *calls += 1;
                Ok(if *calls == 1 { hashes } else { Vec::new() })
            }
        };
        batcher.protect(hash, &protect).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn a_failed_batch_fails_its_leader() {
        let batcher = ReuseBatcher::default();
        let protect =
            |_: Vec<ContentHash>| async { Err::<Vec<ContentHash>, _>(crate::VolumeError::Corrupt) };
        assert!(matches!(
            batcher.protect(ContentHash([1; 32]), &protect).await,
            Err(crate::VolumeError::Corrupt)
        ));
    }
}
