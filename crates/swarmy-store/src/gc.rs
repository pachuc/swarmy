//! Collector leases use the same complete token and retained sequence fencing
//! as volume writer leases. Clock checks happen on every transaction retry.
use jiff::Timestamp;
use swarmy_core::{GcRun, Lease, LeaseOwnerId};

use crate::{Result, Store, StoreError, read, write};

impl Store {
    /// Claim the collector and durably record its start in the same transaction.
    /// # Errors
    /// Rejects a live collector, invalid expiry, and transaction failures.
    pub async fn acquire_gc_lease(&self, run: &GcRun, expires_at: Timestamp) -> Result<Lease> {
        self.transaction(|trx| async move {
            let now = Timestamp::now();
            let key = self.root.pack(&("gc_lease",));
            if expires_at <= now
                || read::<Lease>(&trx, &key)
                    .await?
                    .is_some_and(|lease| lease.expires_at > now)
            {
                return Err(StoreError::LeaseMismatch);
            }
            let sequence_key = self.root.pack(&("gc_sequence",));
            let seq = read::<u64>(&trx, &sequence_key)
                .await?
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(StoreError::SequenceOverflow)?;
            let lease = Lease {
                owner: run.owner,
                expires_at,
                seq,
            };
            write(&trx, &sequence_key, &seq)?;
            write(&trx, &key, &lease)?;
            write(&trx, &self.gc_run_key(run.owner), run)?;
            Ok(lease)
        })
        .await
    }

    /// Extend a live collector's full fencing token.
    /// # Errors
    /// Rejects expired or replaced leases and transaction failures.
    pub async fn renew_gc_lease(&self, expected: &Lease, expires_at: Timestamp) -> Result<Lease> {
        self.transaction(|trx| async move {
            let key = self.root.pack(&("gc_lease",));
            if read::<Lease>(&trx, &key).await?.as_ref() != Some(expected)
                || expected.expires_at <= Timestamp::now()
                || expires_at <= expected.expires_at
            {
                return Err(StoreError::LeaseMismatch);
            }
            let lease = Lease {
                expires_at,
                ..expected.clone()
            };
            write(&trx, &key, &lease)?;
            Ok(lease)
        })
        .await
    }

    /// Persist accounting and release only the current, unexpired collector.
    /// # Errors
    /// Rejects expired or replaced tokens, mismatched run ids, and transaction failures.
    pub async fn finish_gc_run(&self, expected: &Lease, run: &GcRun) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.root.pack(&("gc_lease",));
            if read::<Lease>(&trx, &key).await?.as_ref() != Some(expected)
                || expected.expires_at <= Timestamp::now()
                || expected.owner != run.owner
            {
                return Err(StoreError::LeaseMismatch);
            }
            write(&trx, &self.gc_run_key(run.owner), run)?;
            trx.clear(&key);
            Ok(())
        })
        .await
    }

    /// # Errors
    /// Returns decoding or transaction errors.
    pub async fn get_gc_run(&self, owner: LeaseOwnerId) -> Result<Option<GcRun>> {
        self.transaction(|trx| async move { read(&trx, &self.gc_run_key(owner)).await })
            .await
    }

    fn gc_run_key(&self, owner: LeaseOwnerId) -> Vec<u8> {
        self.root
            .pack(&("gc_run", owner.as_ulid().to_bytes().as_slice()))
    }
}

impl Store {
    /// Protect reuse of an existing content address before checking it again.
    /// An object may have been deleted between the first HEAD and this guard.
    /// # Errors
    /// Returns `LeaseMismatch` while a live collector is deleting this hash,
    /// or a storage error. The uploader can retry after the deletion finishes.
    pub async fn protect_reused_chunk(&self, hash: swarmy_core::ContentHash) -> Result<()> {
        self.transaction(|trx| async move {
            let deleting = self.root.pack(&("gc_deleting", hash.0.as_slice()));
            if let Some(owner) = read::<LeaseOwnerId>(&trx, &deleting).await?
                && read::<Lease>(&trx, &self.root.pack(&("gc_lease",)))
                    .await?
                    .is_some_and(|lease| {
                        lease.owner == owner && lease.expires_at > Timestamp::now()
                    })
            {
                return Err(StoreError::LeaseMismatch);
            }
            trx.clear(&deleting);
            write(
                &trx,
                &self.root.pack(&("chunk_reused", hash.0.as_slice())),
                &Timestamp::now(),
            )
        })
        .await
    }

    /// Serialize deletion against reuse of old objects by active uploaders.
    /// Pins newer than the fixed cutoff protect publications between mark pages.
    /// Dry runs only read eligibility and never reserve a deletion.
    /// # Errors
    /// Rejects expired/replaced collectors and returns transaction errors.
    pub async fn claim_gc_chunk(
        &self,
        owner: LeaseOwnerId,
        hash: swarmy_core::ContentHash,
        cutoff: Timestamp,
        dry_run: bool,
    ) -> Result<bool> {
        self.transaction(|trx| async move {
            if !read::<Lease>(&trx, &self.root.pack(&("gc_lease",)))
                .await?
                .is_some_and(|lease| lease.owner == owner && lease.expires_at > Timestamp::now())
            {
                return Err(StoreError::LeaseMismatch);
            }
            if read::<Timestamp>(&trx, &self.root.pack(&("chunk_reused", hash.0.as_slice())))
                .await?
                .is_some_and(|touched| touched >= cutoff)
            {
                return Ok(false);
            }
            if !dry_run {
                write(
                    &trx,
                    &self.root.pack(&("gc_deleting", hash.0.as_slice())),
                    &owner,
                )?;
            }
            Ok(true)
        })
        .await
    }

    /// Release a completed deletion so waiting uploaders can recreate the chunk.
    /// # Errors
    /// Rejects a replaced deletion reservation and returns transaction errors.
    pub async fn finish_gc_chunk(
        &self,
        owner: LeaseOwnerId,
        hash: swarmy_core::ContentHash,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.root.pack(&("gc_deleting", hash.0.as_slice()));
            if read::<LeaseOwnerId>(&trx, &key).await? != Some(owner) {
                return Err(StoreError::LeaseMismatch);
            }
            trx.clear(&key);
            trx.clear(&self.root.pack(&("chunk_reused", hash.0.as_slice())));
            Ok(())
        })
        .await
    }
}
