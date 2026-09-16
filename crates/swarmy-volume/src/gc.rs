//! Chunk collection uses separate transactions for volume pages, each volume's
//! retained roots, image pages, and manifest headers. A fleet cannot fit inside
//! `FoundationDB`'s five-second/ten-megabyte transaction budget. Publications
//! between pages are protected by the grace window, rather than a global read
//! version. The cutoff is captured before marking and never advances during a
//! run. The grace must exceed staging-to-publication time (including retries,
//! snapshot periods, and idle eviction). Inherited data is protected by the
//! predecessor's live manifest. Unretained manifests must not be resurrected.
//! Old deduplicated uploads use a store reuse timestamp and serialize with a
//! per-chunk deletion reservation. Object timestamps alone cannot protect reuse
//! of an old orphan. If deletion wins, the uploader rechecks and recreates it.
//!
//! A fixed-size Bloom filter can only retain extra chunks, even when saturated.
//! Never use its approximate membership to skip traversing a root or leaf:
//! that could omit real chunk references. Only canonical chunk keys are swept;
//! manifest roots/leaves and the zero sentinel are never deleted.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{StreamExt, TryStreamExt, stream};
use jiff::Timestamp;
use object_store::{ObjectStore, path::Path};
use swarmy_config::GarbageCollection;
use swarmy_core::{ContentHash, GcRun, LeaseOwnerId, ManifestId};
use swarmy_store::{MAX_SCAN_LIMIT, Store, StoreError};

use crate::{Manifest, Result, VolumeError, chunk_path};

const LEASE_SECONDS: i64 = 120;
const RENEW_SECONDS: u64 = 30;
const PREFIX_CONCURRENCY: usize = 16;

/// Collect unreferenced, old chunks. A busy lease returns `LeaseMismatch`.
/// The caller must dedicate this object namespace to this metadata store.
/// # Errors
/// Fails closed on invalid policy, incomplete marking, storage errors, or lease
/// loss. Failed attempts retain partial accounting when the lease is still live.
pub async fn collect(
    store: &Store,
    objects: Arc<dyn ObjectStore>,
    policy: GarbageCollection,
    dry_run: bool,
) -> Result<GcRun> {
    let started = Instant::now();
    let mut run = GcRun {
        owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
        started_at: Timestamp::now(),
        dry_run,
        finished: false,
        error: None,
        manifests: 0,
        scanned: 0,
        candidates: 0,
        candidate_bytes: 0,
        deleted: 0,
        bytes_freed: 0,
        duration_ms: 0,
    };
    let grace =
        i64::try_from(policy.grace_seconds.get()).map_err(|_| VolumeError::InvalidGcPolicy)?;
    let cutoff = run
        .started_at
        .as_second()
        .checked_sub(grace)
        .ok_or(VolumeError::InvalidGcPolicy)?;
    let cutoff = Timestamp::from_second(cutoff).map_err(|_| VolumeError::InvalidGcPolicy)?;
    let mut references = References::new(policy.filter_bytes.get())?;
    let mut deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut lease = store.acquire_gc_lease(&run, expiry()?).await?;
    let result = {
        let work = sweep(store, &*objects, &mut references, cutoff, &mut run);
        tokio::pin!(work);
        let mut renewal = tokio::time::interval(Duration::from_secs(RENEW_SECONDS));
        renewal.tick().await;
        loop {
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => break Err(StoreError::LeaseMismatch.into()),
                _ = renewal.tick() => {
                    let next_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
                    let renewed = tokio::time::timeout_at(deadline, async {
                        Ok::<_, VolumeError>(store.renew_gc_lease(&lease, expiry()?).await?)
                    }).await;
                    match renewed {
                        Ok(Ok(next)) => { lease = next; deadline = next_deadline; }
                        Ok(Err(error)) => break Err(error),
                        Err(_) => break Err(StoreError::LeaseMismatch.into()),
                    }
                }
                result = &mut work => break result,
            }
        }
    };
    run.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    run.finished = true;
    run.error = result.as_ref().err().map(ToString::to_string);
    let recorded = store.finish_gc_run(&lease, &run).await;
    result?;
    recorded?;
    Ok(run)
}

fn expiry() -> Result<Timestamp> {
    Timestamp::from_second(Timestamp::now().as_second() + LEASE_SECONDS)
        .map_err(|_| VolumeError::InvalidGcPolicy)
}

async fn mark(
    store: &Store,
    objects: &dyn ObjectStore,
    references: &mut References,
    id: ManifestId,
) -> Result<()> {
    let header = store
        .get_manifest(id)
        .await?
        .ok_or(StoreError::ManifestMissing)?;
    let manifest = Manifest::load(objects, header).await?;
    for (index, hash) in manifest.leaf_hashes().iter().enumerate() {
        if *hash != ContentHash::ZERO {
            for chunk in manifest.leaf(objects, index).await? {
                references.insert(chunk);
            }
        }
    }
    Ok(())
}

async fn sweep(
    store: &Store,
    objects: &dyn ObjectStore,
    references: &mut References,
    cutoff: Timestamp,
    run: &mut GcRun,
) -> Result<()> {
    let mut after = None;
    loop {
        let page = store.list_volumes(after, MAX_SCAN_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        for (id, _) in page {
            for root in store.volume_live_manifests(id).await? {
                mark(store, objects, references, root).await?;
                run.manifests += 1;
            }
            after = Some(id);
        }
    }
    let mut after = None;
    loop {
        let page = store
            .list_images(
                after
                    .as_ref()
                    .map(|(name, tag): &(String, swarmy_core::ImageTag)| (name.as_str(), tag)),
                MAX_SCAN_LIMIT,
            )
            .await?;
        if page.is_empty() {
            break;
        }
        for image in page {
            mark(store, objects, references, image.manifest_id).await?;
            run.manifests += 1;
            after = Some((image.name, image.tag));
        }
    }
    // Each list is streamed, and at most sixteen prefixes have a page in memory.
    let mut listed = stream::iter(0_u16..256)
        .map(|prefix| {
            let path = Path::from(format!("chunks/{prefix:02x}"));
            objects.list(Some(&path))
        })
        .flatten_unordered(PREFIX_CONCURRENCY);
    while let Some(meta) = listed.try_next().await? {
        run.scanned += 1;
        let Some(hash) = parse_chunk(&meta.location) else {
            continue;
        };
        if hash == ContentHash::ZERO
            || references.contains(hash)
            || meta.last_modified.timestamp() >= cutoff.as_second()
        {
            continue;
        }
        if !store
            .claim_gc_chunk(run.owner, hash, cutoff, run.dry_run)
            .await?
        {
            continue;
        }
        run.candidates += 1;
        run.candidate_bytes += meta.size;
        if !run.dry_run {
            objects.delete(&meta.location).await?;
            run.deleted += 1;
            run.bytes_freed += meta.size;
            store.finish_gc_chunk(run.owner, hash).await?;
        }
    }
    Ok(())
}

fn parse_chunk(path: &Path) -> Option<ContentHash> {
    let name = path.filename()?;
    if name.len() != 64 || !name.is_ascii() {
        return None;
    }
    let mut hash = [0; 32];
    for (index, byte) in hash.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&name[index * 2..index * 2 + 2], 16).ok()?;
    }
    let hash = ContentHash(hash);
    (chunk_path(hash) == *path).then_some(hash)
}

struct References {
    bytes: Vec<u8>,
}
impl References {
    fn new(size: usize) -> Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| VolumeError::InvalidGcPolicy)?;
        bytes.resize(size, 0);
        Ok(Self { bytes })
    }

    fn positions(&self, hash: ContentHash) -> impl Iterator<Item = (usize, u8)> + use<> {
        let first = u64::from_le_bytes(hash.0[..8].try_into().unwrap());
        let step = u64::from_le_bytes(hash.0[8..16].try_into().unwrap()) | 1;
        let size = u64::try_from(self.bytes.len()).unwrap();
        (0..7).map(move |index| {
            let value = first.wrapping_add(step.wrapping_mul(index));
            (
                usize::try_from((value >> 3) % size).unwrap(),
                1 << (value & 7),
            )
        })
    }

    fn insert(&mut self, hash: ContentHash) {
        for (byte, mask) in self.positions(hash) {
            self.bytes[byte] |= mask;
        }
    }

    fn contains(&self, hash: ContentHash) -> bool {
        self.positions(hash)
            .all(|(byte, mask)| self.bytes[byte] & mask != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturated_filter_never_forgets_a_reference() {
        let mut filter = References::new(8).unwrap();
        for value in 0_u64..10_000 {
            filter.insert(ContentHash(*blake3::hash(&value.to_le_bytes()).as_bytes()));
        }
        for value in 0_u64..10_000 {
            assert!(filter.contains(ContentHash(*blake3::hash(&value.to_le_bytes()).as_bytes())));
        }
    }

    #[test]
    fn only_canonical_chunk_keys_are_candidates() {
        let hash = ContentHash([123; 32]);
        assert_eq!(parse_chunk(&chunk_path(hash)), Some(hash));
        for key in [
            format!("manifests/{hash}"),
            format!("chunks/00/{hash}"),
            "chunks/00/invalid".into(),
            format!("chunks/7b/{hash}/nested"),
        ] {
            assert_eq!(parse_chunk(&Path::from(key)), None);
        }
    }
}
