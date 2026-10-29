//! Lease for the single primary-host provisioner.
use crate::{
    FenceError, Result, Store,
    singleton_lease::{self, SingletonKeys},
};
use std::time::Duration;
use swarmy_core::{Lease, LeaseOwnerId};

impl Store {
    /// # Errors
    /// Returns `ProvisionerLeaseMismatch` when another live holder owns it.
    pub async fn acquire_provisioner_lease(
        &self,
        owner: LeaseOwnerId,
        ttl: Duration,
    ) -> Result<Lease> {
        self.transaction(|trx| async move {
            let keys = SingletonKeys {
                lease: self.keys().provisioner_lease(),
                sequence: self.keys().provisioner_sequence(),
            };
            singleton_lease::claim(&trx, &keys, owner, self.now(), self.now() + ttl)
                .await?
                .ok_or(FenceError::ProvisionerLeaseMismatch.into())
        })
        .await
    }
    /// # Errors
    /// Returns `ProvisionerLeaseMismatch` when the token has expired or changed.
    pub async fn renew_provisioner_lease(&self, expected: &Lease, ttl: Duration) -> Result<Lease> {
        self.transaction(|trx| async move {
            let keys = SingletonKeys {
                lease: self.keys().provisioner_lease(),
                sequence: self.keys().provisioner_sequence(),
            };
            singleton_lease::extend(&trx, &keys, expected, self.now(), self.now() + ttl)
                .await?
                .ok_or(FenceError::ProvisionerLeaseMismatch.into())
        })
        .await
    }
    /// Release only the current token.
    /// # Errors
    /// Returns storage failures.
    pub async fn release_provisioner_lease(&self, expected: &Lease) -> Result<()> {
        self.transaction(|trx| async move {
            let key = self.keys().provisioner_lease();
            if singleton_lease::is_current(&trx, &key, expected, self.now()).await? {
                trx.clear(&key);
            }
            Ok(())
        })
        .await
    }
}
