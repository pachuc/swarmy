//! Checked-in key layout fixture for the tuple registry.
use foundationdb::tuple::Subspace;
use jiff::Timestamp;
use swarmy_core::{
    AgentId, CredentialScope, ImageTag, LeaseOwnerId, ManifestId, MessageId, NodeId, RequestId,
    SessionId, TimerId, VolumeId,
};

use crate::keys::Keys;

use std::fmt::Write as _;

#[test]
fn no_raw_family_packing_outside_registry() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in std::fs::read_dir(src).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "rs")
            && path
                .file_name()
                .is_some_and(|name| name != "keys.rs" && name != "keys_tests.rs")
        {
            let text = std::fs::read_to_string(&path).unwrap();
            assert!(
                !text.contains(".pack(") && !text.contains(".subspace("),
                "raw key family in {}",
                path.display()
            );
        }
    }
}
#[expect(clippy::too_many_lines, reason = "the fixture lists complete master-era tuple keys for every family together, including nested time tuples, binary identifiers, and each final component, so an omitted constructor is visible")]
#[test]
fn family_key_layout_matches_checked_in_hex() {
    let root = Subspace::from_bytes(Vec::new());
    let keys = Keys::new(&root);
    let agentid = AgentId::from_ulid(u128::from_be_bytes([17; 16]).into());
    let sessionid = SessionId::from_ulid(u128::from_be_bytes([34; 16]).into());
    let nodeid = NodeId::from_ulid(u128::from_be_bytes([51; 16]).into());
    let volumeid = VolumeId::from_ulid(u128::from_be_bytes([68; 16]).into());
    let manifestid = ManifestId::from_ulid(u128::from_be_bytes([85; 16]).into());
    let leaseownerid = LeaseOwnerId::from_ulid(u128::from_be_bytes([102; 16]).into());
    let timerid = TimerId::from_ulid(u128::from_be_bytes([119; 16]).into());
    let messageid = MessageId::from_ulid(u128::from_be_bytes([136; 16]).into());
    let requestid = RequestId::from_bytes([0x99; 32]);
    let contenthash = swarmy_core::ContentHash([0xaa; 32]);
    let at = Timestamp::new(100, 200).unwrap();
    let tag = ImageTag("tag".to_owned());
    let actual = [
        ("agent", keys.agent(agentid)),
        ("agent_by_name", keys.agent_by_name("value")),
        ("agent_call_status", keys.agent_call_status(agentid)),
        ("agent_github_token", keys.agent_github_token(agentid)),
        ("api_append", keys.api_append("value")),
        ("api_idempotency", keys.api_idempotency("value")),
        ("api_session_id", keys.api_session_id("value")),
        ("chunk_reused", keys.chunk_reused(contenthash)),
        ("computer_deleted", keys.computer_deleted(agentid)),
        ("computer_memory", keys.computer_memory(agentid)),
        ("computer_notice", keys.computer_notice(agentid, 4)),
        (
            "computer_notice_delivered",
            keys.computer_notice_delivered(sessionid, 4),
        ),
        (
            "credential_entry",
            keys.credential_entry(CredentialScope::Cluster, "provider", "label"),
        ),
        (
            "credential_entry_lease",
            keys.credential_entry_lease(CredentialScope::Cluster, "provider", "label"),
        ),
        (
            "entry_quota_config",
            keys.entry_quota_config("provider", "label"),
        ),
        (
            "entry_quota_observed",
            keys.entry_quota_observed("provider", "label"),
        ),
        ("event", keys.event(sessionid, 5)),
        ("gateway_provider", keys.gateway_provider("value")),
        (
            "gateway_provider_entry",
            keys.gateway_provider_entry("provider", "label"),
        ),
        ("gc_deleting", keys.gc_deleting(contenthash)),
        ("gc_lease", keys.gc_lease()),
        ("gc_run", keys.gc_run(leaseownerid)),
        ("gc_sequence", keys.gc_sequence()),
        ("idem", keys.idem(requestid)),
        ("image", keys.image("name", &tag)),
        (
            "image_display",
            keys.image_display("name", &tag, manifestid),
        ),
        ("image_memory", keys.image_memory("name", &tag, manifestid)),
        (
            "image_scratch",
            keys.image_scratch("name", &tag, manifestid),
        ),
        (
            "inference_breaker",
            keys.inference_breaker("provider", "label"),
        ),
        ("inference_claim", keys.inference_claim(requestid)),
        ("inference_input", keys.inference_input(requestid)),
        ("inference_request", keys.inference_request(requestid)),
        ("inference_retry", keys.inference_retry(requestid)),
        ("inference_result", keys.inference_result(requestid)),
        ("inference_wait", keys.inference_wait(sessionid)),
        ("inference_wait_due", keys.inference_wait_due(at, sessionid)),
        ("inflight", keys.inflight(requestid)),
        ("lease", keys.lease(sessionid)),
        ("lease_by_expiry", keys.lease_by_expiry(at, sessionid)),
        ("manifest", keys.manifest(manifestid)),
        ("manifest_parent", keys.manifest_parent(manifestid)),
        (
            "metering_hour",
            keys.metering_hour_single("dimension", 3600, "key", "field"),
        ),
        ("node", keys.node(nodeid)),
        ("placed_tool_claim", keys.placed_tool_claim(requestid)),
        ("placement", keys.placement(agentid)),
        ("placement_address", keys.placement_address(agentid)),
        ("placement_by_node", keys.placement_by_node(nodeid, agentid)),
        ("placement_count", keys.placement_count(nodeid)),
        ("placement_epoch", keys.placement_epoch(agentid)),
        ("placement_hosting", keys.placement_hosting(agentid)),
        ("request_turn", keys.request_turn(requestid)),
        ("route", keys.route("value")),
        ("runnable", keys.runnable(2, 7, at, sessionid)),
        ("runnable_by_session", keys.runnable_by_session(sessionid)),
        ("scratch", keys.scratch(agentid)),
        (
            "service_heartbeat",
            keys.service_heartbeat("role", "instance"),
        ),
        ("session", keys.session(sessionid)),
        (
            "session_by_agent",
            keys.session_by_agent(agentid, sessionid),
        ),
        ("session_chain", keys.session_chain("direction", sessionid)),
        ("session_chunk", keys.session_chunk(sessionid, 9)),
        ("queued_message", keys.queued_message(sessionid, 5)),
        ("queued_counter", keys.queued_counter(sessionid)),
        ("queued_replay", keys.queued_replay("value")),
        ("session_tools", keys.session_tools(sessionid, requestid)),
        ("snapshot", keys.snapshot(sessionid, 5)),
        ("timer", keys.timer(agentid, timerid)),
        ("timer_active", keys.timer_active(agentid, timerid)),
        ("timer_due", keys.timer_due(at, agentid, timerid)),
        ("timer_origin", keys.timer_origin(agentid, timerid)),
        ("tool_done", keys.tool_done(requestid)),
        ("tool_job", keys.tool_job(requestid)),
        ("tool_placement", keys.tool_placement(requestid)),
        ("turn", keys.turn(sessionid)),
        (
            "turn_inference",
            keys.turn_inference(sessionid, messageid, "request"),
        ),
        ("turn_metrics", keys.turn_metrics(sessionid, messageid)),
        ("turn_tool", keys.turn_tool(sessionid, messageid, "call")),
        ("usage", keys.usage(sessionid)),
        ("usage_by_agent", keys.usage_by_agent(agentid)),
        ("usage_record", keys.usage_record(requestid)),
        (
            "usage_record_by_time",
            keys.usage_record_by_time(3600, requestid),
        ),
        ("volume", keys.volume(volumeid)),
        ("volume_lease_seq", keys.volume_lease_seq(volumeid)),
        ("volume_placement", keys.volume_placement(volumeid)),
        ("volume_snapshots", keys.volume_snapshots(volumeid)),
    ];
    let expected = include_str!("../tests/key-layout.hex");
    let mut rendered = String::new();
    for (name, bytes) in actual {
        write!(&mut rendered, "{name} ").unwrap();
        for byte in bytes {
            write!(&mut rendered, "{byte:02x}").unwrap();
        }
        rendered.push('\n');
    }
    assert_eq!(rendered, expected);
}
