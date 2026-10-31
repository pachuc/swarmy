#![deny(clippy::disallowed_methods)]
use super::*;
use swarmy_core::{
    AgentId, BashArguments, CHUNK_SIZE, ContentHash, GpuRequirement, Lease, LeaseOwnerId,
    ManifestHeader, ManifestId, MessageId, NodeId, PlacementChangeReason, PlacementRecord,
    RequestId, SandboxArguments, SandboxSpec, SessionId, ToolCallId, ToolJob, VolumeId,
};
use ulid::Ulid;
fn id() -> Ulid {
    Ulid::generate()
}
fn roundtrip<T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(
    value: T,
) {
    assert_eq!(
        serde_json::from_slice::<T>(&serde_json::to_vec(&value).unwrap()).unwrap(),
        value
    );
}
fn fixture() -> (
    ToolJob,
    PlacementRecord,
    Lease,
    VolumeId,
    ManifestId,
    ManifestHeader,
    SandboxSpec,
) {
    let session = SessionId::from_ulid(id());
    let agent = AgentId::from_ulid(id());
    let node = NodeId::from_ulid(id());
    let now = jiff::Timestamp::UNIX_EPOCH;
    let job = ToolJob {
        session_id: session,
        request_id: RequestId::for_step(session, 1),
        call_id: ToolCallId("bash".into()),
        step: 1,
        arguments: SandboxArguments::Bash(BashArguments {
            command: "true".into(),
            timeout_ms: 1000,
            yield_seconds: 1,
            output_budget_bytes: 1024,
        }),
    };
    let placement = PlacementRecord {
        agent_id: agent,
        node_id: node,
        epoch: 1,
        expires_at: now,
        last_change_reason: PlacementChangeReason::Initial,
        last_changed_at: now,
        gpu: GpuRequirement::default(),
    };
    let lease = Lease {
        owner: LeaseOwnerId::from(node),
        seq: 1,
        expires_at: now,
    };
    let header = ManifestHeader {
        size: u64::from(CHUNK_SIZE),
        chunk_size: CHUNK_SIZE,
        root_hash: ContentHash::ZERO,
    };
    let spec = SandboxSpec {
        agent_id: agent,
        scratch: vec![],
        requirements: Default::default(),
    };
    (
        job,
        placement,
        lease,
        VolumeId::from_ulid(id()),
        ManifestId::from_ulid(id()),
        header,
        spec,
    )
}
#[test]
fn tool_and_hosting_wire_types_round_trip() {
    let (job, placement, lease, volume, head, header, spec) = fixture();
    let now = jiff::Timestamp::UNIX_EPOCH;
    roundtrip(ToolRouteRequest { job: job.clone() });
    roundtrip(ToolRouteResponse {
        server_now: now,
        agent: placement.agent_id,
    });
    roundtrip(HostingOpenRequest {
        agent: placement.agent_id,
        job,
        previous_epoch: Some(1),
    });
    roundtrip(HostingOpenResponse {
        server_now: now,
        placement: placement.clone(),
        volume,
        writer: lease.clone(),
        head,
        header,
        spec,
        boot_set: None,
    });
    roundtrip(HostingReadyRequest {
        placement: placement.clone(),
        address: std::net::Ipv4Addr::LOCALHOST,
        metrics: vec![MetricBatch {
            session: SessionId::from_ulid(id()),
            turn: MessageId::from_ulid(id()),
            patches: vec![NodeMetric::Computer(swarmy_core::ComputerMetric::default())],
        }],
    });
    roundtrip(HostingCloseRequest {
        placement,
        writer: Some(WriterToken { volume, lease }),
    });
}
#[test]
fn volume_and_credential_wire_types_round_trip() {
    let (_, placement, lease, volume, head, header, _) = fixture();
    let now = jiff::Timestamp::UNIX_EPOCH;
    roundtrip(VolumePublishRequest {
        writer: WriterToken { volume, lease },
        previous: head,
        next: ManifestId::from_ulid(id()),
        header,
    });
    roundtrip(VolumeProtectRequest {
        hashes: vec![ContentHash::ZERO],
    });
    roundtrip(VolumeProtectResponse {
        server_now: now,
        deleting: vec![ContentHash::ZERO],
    });
    roundtrip(GithubTokenRequest {
        agent: placement.agent_id,
    });
    roundtrip(GithubTokenResponse {
        server_now: now,
        token: Some("secret".into()),
    });
    roundtrip(Ack { server_now: now });
}
