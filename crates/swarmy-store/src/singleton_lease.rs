//! One store-wide lease per job, with the full token and a retained sequence for fencing.
use crate::{Result, read, write};
use foundationdb::Transaction;
use jiff::Timestamp;
use swarmy_core::{Lease, LeaseOwnerId};

pub(crate) struct SingletonKeys {
    pub(crate) lease: Vec<u8>,
    pub(crate) sequence: Vec<u8>,
}
pub(crate) async fn claim(
    trx: &Transaction,
    keys: &SingletonKeys,
    owner: LeaseOwnerId,
    now: Timestamp,
    expires_at: Timestamp,
) -> Result<Option<Lease>> {
    if expires_at <= now
        || read::<Lease>(trx, &keys.lease)
            .await?
            .is_some_and(|lease| lease.expires_at > now)
    {
        return Ok(None);
    }
    let seq = crate::seq_after(read::<u64>(trx, &keys.sequence).await?.unwrap_or(0), 1)?;
    let lease = Lease {
        owner,
        expires_at,
        seq,
    };
    write(trx, &keys.sequence, &seq)?;
    write(trx, &keys.lease, &lease)?;
    Ok(Some(lease))
}
pub(crate) async fn extend(
    trx: &Transaction,
    keys: &SingletonKeys,
    expected: &Lease,
    now: Timestamp,
    expires_at: Timestamp,
) -> Result<Option<Lease>> {
    if !is_current(trx, &keys.lease, expected, now).await? || expires_at <= expected.expires_at {
        return Ok(None);
    }
    let lease = Lease {
        expires_at,
        ..expected.clone()
    };
    write(trx, &keys.lease, &lease)?;
    Ok(Some(lease))
}
pub(crate) async fn is_current(
    trx: &Transaction,
    lease_key: &[u8],
    expected: &Lease,
    now: Timestamp,
) -> Result<bool> {
    Ok(
        expected.expires_at > now
            && read::<Lease>(trx, lease_key).await?.as_ref() == Some(expected),
    )
}
