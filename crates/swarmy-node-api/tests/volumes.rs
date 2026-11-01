#![deny(clippy::disallowed_methods)]
mod support;
use std::time::Duration;
use swarmy_core::{ContentHash, ManifestId};
use swarmy_node_protocol::{HostingOpenRequest, VolumePublishRequest, WriterToken};
use ulid::Ulid;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}
#[tokio::test]
async fn publish_moves_the_head_and_repeats() {
    let Some(f) = support::fixture("na5-publish").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let client = support::client(&support::served(&f).await, &a.token);
    let opened = client
        .open_hosting(
            &HostingOpenRequest {
                agent: a.agent,
                job: a.jobs[0].clone(),
                previous_epoch: None,
            },
            deadline(),
        )
        .await
        .unwrap();
    let next = ManifestId::from_ulid(Ulid::generate());
    let request = VolumePublishRequest {
        writer: WriterToken {
            volume: opened.volume,
            lease: opened.writer,
        },
        previous: opened.head,
        next,
        header: opened.header,
    };
    client.publish_volume(&request, deadline()).await.unwrap();
    assert_eq!(
        f.store
            .get_volume(opened.volume)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        next
    );
    client.publish_volume(&request, deadline()).await.unwrap();
    let mut expired_echo = request.clone();
    expired_echo.writer.lease.expires_at = jiff::Timestamp::UNIX_EPOCH;
    client
        .publish_volume(&expired_echo, deadline())
        .await
        .unwrap();
}
#[tokio::test]
async fn protect_accepts_any_hashes_in_chunks() {
    let Some(f) = support::fixture("na5-protect").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let client = support::client(&support::served(&f).await, &a.token);
    let hashes: Vec<ContentHash> = (0..2100_u16)
        .map(|n| {
            let mut bytes = [0; 32];
            bytes[..2].copy_from_slice(&n.to_le_bytes());
            ContentHash(bytes)
        })
        .collect();
    assert!(
        client
            .protect_chunks(&hashes, deadline())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn release_frees_the_writer_for_the_next_owner() {
    let Some(f) = support::fixture("na10-release").await else {
        return;
    };
    let hosted = support::hosted(&f.store).await;
    let client = support::client(&support::served(&f).await, &hosted.token);
    let request = swarmy_node_protocol::VolumeReleaseRequest {
        writer: WriterToken {
            volume: hosted.volume,
            lease: hosted.lease,
        },
    };
    client.release_volume(&request, deadline()).await.unwrap();
    assert!(
        f.store
            .get_volume(hosted.volume)
            .await
            .unwrap()
            .unwrap()
            .writer_lease
            .is_none()
    );
    let other = swarmy_core::LeaseOwnerId::from_ulid(Ulid::generate());
    f.store
        .open_writer_lease(hosted.volume, other, Duration::from_secs(60))
        .await
        .unwrap();
}

#[tokio::test]
async fn release_repeats_after_a_dropped_answer() {
    let Some(f) = support::fixture("na10-release-retry").await else {
        return;
    };
    let hosted = support::hosted(&f.store).await;
    let (router, count) = support::drop_first_response(
        swarmy_node_api::router(support::state(&f)),
        swarmy_node_protocol::VOLUME_RELEASE_PATH,
    );
    let client = support::client(&support::serve(router).await, &hosted.token);
    client
        .release_volume(
            &swarmy_node_protocol::VolumeReleaseRequest {
                writer: WriterToken {
                    volume: hosted.volume,
                    lease: hosted.lease,
                },
            },
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
}
