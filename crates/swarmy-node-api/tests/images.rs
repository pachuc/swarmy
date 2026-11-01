#![deny(clippy::disallowed_methods)]
mod support;
use std::time::Duration;
use swarmy_core::{CHUNK_SIZE, ContentHash, ImageProbe, ImageTag, ManifestHeader, ManifestId};
use swarmy_node_api::router;
use swarmy_node_protocol::{ClientError, ImageProbeFinishRequest, NodeRefusal};
use swarmy_store::{PutImageOptions, image_probe_volume};
use ulid::Ulid;

fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}

async fn setup(
    tag: &str,
) -> Option<(
    support::Fixture,
    swarmy_node_protocol::NodeClient,
    ImageProbe,
)> {
    let f = support::fixture(tag).await?;
    let node = support::dispatched(&f.store).await;
    let base = support::served(&f).await;
    let client = support::client(&base, &node.token);
    let manifest = ManifestId::from_ulid(Ulid::generate());
    f.store
        .put_manifest(
            manifest,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash::ZERO,
            },
        )
        .await
        .unwrap();
    let probe = ImageProbe {
        name: format!("probe-{tag}"),
        tag: ImageTag("test".into()),
        manifest,
    };
    f.store
        .put_image(
            &probe.name,
            &probe.tag,
            manifest,
            Some(PutImageOptions {
                scratch: vec!["/probe".into()],
                memory_mib: Some(768),
                display: false,
            }),
        )
        .await
        .unwrap();
    Some((f, client, probe))
}

#[tokio::test]
async fn probe_open_returns_a_writer_and_the_image_spec() {
    let Some((_f, client, probe)) = setup("probe-spec").await else {
        return;
    };
    let opened = client.open_image_probe(&probe, deadline()).await.unwrap();
    assert_eq!(opened.volume, image_probe_volume(probe.manifest));
    assert_eq!(opened.head, probe.manifest);
    assert_eq!(opened.spec.agent_id.as_ulid(), probe.manifest.as_ulid());
    assert_eq!(opened.spec.scratch, vec!["/probe"]);
    assert_eq!(opened.spec.requirements.memory_mib, 768);
}

#[tokio::test]
async fn probe_open_refuses_an_image_with_a_boot_set_or_a_moved_tag() {
    let Some((f, client, probe)) = setup("probe-refuse").await else {
        return;
    };
    f.store
        .record_boot_set(probe.manifest, &swarmy_core::boot_set_key(b"ready"))
        .await
        .unwrap();
    assert!(matches!(
        client.open_image_probe(&probe, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::AlreadyDone))
    ));
    let moved = ImageProbe {
        manifest: ManifestId::from_ulid(Ulid::generate()),
        ..probe
    };
    assert!(matches!(
        client.open_image_probe(&moved, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::AlreadyDone))
    ));
}

#[tokio::test]
async fn probe_finish_records_the_key_and_rejects_a_bad_one() {
    let Some((f, client, probe)) = setup("probe-finish").await else {
        return;
    };
    let opened = client.open_image_probe(&probe, deadline()).await.unwrap();
    let bad = ImageProbeFinishRequest {
        manifest: probe.manifest,
        key: Some("not-a-key".into()),
    };
    assert!(client.finish_image_probe(&bad, deadline()).await.is_err());
    let key = swarmy_core::boot_set_key(b"ready");
    let finished = ImageProbeFinishRequest {
        manifest: probe.manifest,
        key: Some(key.clone()),
    };
    client
        .finish_image_probe(&finished, deadline())
        .await
        .unwrap();
    client
        .finish_image_probe(&finished, deadline())
        .await
        .unwrap();
    assert_eq!(f.store.boot_set(probe.manifest).await.unwrap(), Some(key));
    assert!(f.store.get_volume(opened.volume).await.unwrap().is_none());
}
