//! Collector leases use the same complete token and retained sequence fencing
//! as volume writer leases. Clock checks happen on every transaction retry.
use futures::future::try_join_all;
use jiff::Timestamp;
use swarmy_core::{ContentHash, GcRun, Lease, LeaseOwnerId};

use crate::{Result, Store, StoreError, read, write};

impl Store {
    /// Claim the collector and durably record its start in the same transaction.
    /// # Errors
    /// Rejects a live collector, invalid expiry, and transaction failures.
    pub async fn acquire_gc_lease(&self, run: &GcRun, expires_at: Timestamp) -> Result<Lease> {
        self.transaction(|trx| async move {
            let keys = crate::singleton_lease::SingletonKeys {
                lease: self.keys().gc_lease(),
                sequence: self.keys().gc_sequence(),
            };
            let lease =
                crate::singleton_lease::claim(&trx, &keys, run.owner, self.now(), expires_at)
                    .await?
                    .ok_or(crate::FenceError::GcLeaseMismatch)?;
            write(&trx, &self.keys().gc_run(run.owner), run)?;
            Ok(lease)
        })
        .await
    }

    /// Extend a live collector's full fencing token.
    /// # Errors
    /// Rejects expired or replaced leases and transaction failures.
    pub async fn renew_gc_lease(&self, expected: &Lease, expires_at: Timestamp) -> Result<Lease> {
        self.transaction(|trx| async move {
            let keys = crate::singleton_lease::SingletonKeys {
                lease: self.keys().gc_lease(),
                sequence: self.keys().gc_sequence(),
            };
            let lease =
                crate::singleton_lease::extend(&trx, &keys, expected, self.now(), expires_at)
                    .await?
                    .ok_or(crate::FenceError::GcLeaseMismatch)?;
            Ok(lease)
        })
        .await
    }

    /// Persist accounting and release only the current, unexpired collector.
    /// # Errors
    /// Rejects expired or replaced tokens, mismatched run ids, and transaction failures.
    pub async fn finish_gc_run(&self, expected: &Lease, run: &GcRun) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.keys().gc_lease();
            if !crate::singleton_lease::is_current(&trx, &key, expected, self.now()).await?
                || expected.owner != run.owner
            {
                return Err(StoreError::Fence(crate::FenceError::GcLeaseMismatch));
            }
            write(&trx, &self.keys().gc_run(run.owner), run)?;
            trx.clear(&key);
            Ok(())
        })
        .await
    }

    /// Record a finished run without holding the collector lease. A sweep that
    /// loses its lease can no longer release it, but followers poll `finished`
    /// on the run record; without this write they wait until their deadline.
    /// # Errors
    /// Rejects a mismatched run id and transaction failures.
    pub async fn fail_gc_run(&self, owner: LeaseOwnerId, run: &GcRun) -> Result<()> {
        if run.owner != owner {
            return Err(StoreError::Fence(crate::FenceError::GcLeaseMismatch));
        }
        let mut failed = run.clone();
        failed.finished = true;
        if failed.error.is_none() {
            failed.error = Some("gc lease lost".into());
        }
        self.transaction(|trx| {
            let failed = &failed;
            async move {
                write(&trx, &self.keys().gc_run(owner), failed)?;
                Ok(())
            }
        })
        .await
    }

    /// # Errors
    /// Returns decoding or transaction errors.
    pub async fn get_gc_run(&self, owner: LeaseOwnerId) -> Result<Option<GcRun>> {
        self.transaction(|trx| async move { read(&trx, &self.keys().gc_run(owner)).await })
            .await
    }
}

impl Store {
    /// Protect existing content addresses in one transaction. Returns hashes
    /// whose deletion is reserved by a live collector.
    /// # Errors
    /// Returns storage errors.
    pub async fn protect_reused_chunks(&self, hashes: &[ContentHash]) -> Result<Vec<ContentHash>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        self.transaction(|trx| async move {
            let reservations = try_join_all(hashes.iter().map(|hash| async {
                read::<LeaseOwnerId>(&trx, &self.keys().gc_deleting(*hash)).await
            }))
            .await?;
            let lease = if reservations.iter().any(Option::is_some) {
                read::<Lease>(&trx, &self.keys().gc_lease()).await?
            } else {
                None
            };
            let now = self.now();
            let mut deleting = Vec::new();
            for (&hash, reservation) in hashes.iter().zip(reservations) {
                if reservation.is_some_and(|owner| {
                    lease
                        .as_ref()
                        .is_some_and(|lease| lease.owner == owner && lease.expires_at > now)
                }) {
                    deleting.push(hash);
                    continue;
                }
                trx.clear(&self.keys().gc_deleting(hash));
                write(&trx, &self.keys().chunk_reused(hash), &now)?;
            }
            Ok(deleting)
        })
        .await
    }

    /// Protect one reused chunk for tests and test-support clients.
    /// # Errors
    /// Returns `GcLeaseMismatch` while a live collector deletes the hash,
    /// or a storage error.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn protect_reused_chunk(&self, hash: ContentHash) -> Result<()> {
        if self.protect_reused_chunks(&[hash]).await?.is_empty() {
            Ok(())
        } else {
            Err(StoreError::Fence(crate::FenceError::GcLeaseMismatch))
        }
    }

    /// Serialize deletion against reuse of old objects by active uploaders.
    /// Pins newer than the fixed cutoff protect publications between mark pages.
    /// Dry runs only read eligibility and never reserve a deletion.
    /// # Errors
    /// Rejects expired/replaced collectors and returns transaction errors.
    pub async fn claim_gc_chunks(
        &self,
        owner: LeaseOwnerId,
        hashes: &[ContentHash],
        cutoff: Timestamp,
        dry_run: bool,
    ) -> Result<Vec<ContentHash>> {
        self.transaction(|trx| async move {
            if !read::<Lease>(&trx, &self.keys().gc_lease())
                .await?
                .is_some_and(|lease| lease.owner == owner && lease.expires_at > self.now())
            {
                return Err(StoreError::Fence(crate::FenceError::GcLeaseMismatch));
            }
            let reuse = try_join_all(hashes.iter().map(|hash| async {
                let key = self.keys().chunk_reused(*hash);
                read::<Timestamp>(&trx, &key).await
            }))
            .await?;
            let mut claimed = Vec::with_capacity(hashes.len());
            for (&hash, touched) in hashes.iter().zip(reuse) {
                if touched.is_some_and(|touched| touched >= cutoff) {
                    continue;
                }
                if !dry_run {
                    write(&trx, &self.keys().gc_deleting(hash), &owner)?;
                }
                claimed.push(hash);
            }
            Ok(claimed)
        })
        .await
    }

    /// Release a completed deletion so waiting uploaders can recreate the chunk.
    /// # Errors
    /// Rejects a replaced deletion reservation and returns transaction errors.
    pub async fn finish_gc_chunks(
        &self,
        owner: LeaseOwnerId,
        hashes: &[ContentHash],
    ) -> Result<()> {
        self.transaction(|trx| async move {
            let reservations = try_join_all(hashes.iter().map(|hash| async {
                let key = self.keys().gc_deleting(*hash);
                read::<LeaseOwnerId>(&trx, &key).await
            }))
            .await?;
            for (&hash, reservation) in hashes.iter().zip(reservations) {
                if reservation != Some(owner) {
                    return Err(StoreError::Fence(crate::FenceError::GcLeaseMismatch));
                }
                let key = self.keys().gc_deleting(hash);
                trx.clear(&key);
                trx.clear(&self.keys().chunk_reused(hash));
            }
            Ok(())
        })
        .await
    }
}
