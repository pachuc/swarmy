use foundationdb::{Transaction, tuple::Subspace};
use jiff::Timestamp;
use swarmy_core::{
    AgentId, CredentialScope, ImageTag, LeaseOwnerId, ManifestId, MessageId, NodeId, RequestId,
    RunnableEntry, SessionId, SessionState, TimerId, VolumeId,
};

use crate::{Result, Store, StoreError, read, scan, write};

pub use swarmy_core::{RUNNABLE_PARTITIONS, runnable_partition};

impl Store {
    pub(crate) fn request_turn_key(&self, id: swarmy_core::RequestId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).request_turn(id)
    }

    /// Resolve the original user turn even when old work is redelivered later.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn request_turn_id(
        &self,
        id: swarmy_core::RequestId,
    ) -> Result<Option<swarmy_core::MessageId>> {
        self.transaction(|trx| async move { read(&trx, &self.request_turn_key(id)).await })
            .await
    }

    pub(crate) fn turn_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).turn(id)
    }

    /// The latest user message identifies the turn, including after snapshots.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn turn_id(&self, id: SessionId) -> Result<Option<swarmy_core::MessageId>> {
        self.transaction(|trx| async move { read(&trx, &self.turn_key(id)).await })
            .await
    }

    pub(crate) fn volume_key(&self, id: VolumeId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).volume(id)
    }

    pub(crate) fn manifest_key(&self, id: ManifestId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).manifest(id)
    }

    pub(crate) fn image_key(&self, name: &str, tag: &ImageTag) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).image(name, tag)
    }

    pub(crate) fn volume_lease_seq_key(&self, id: VolumeId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).volume_lease_seq(id)
    }

    pub(crate) fn session_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session(id)
    }

    /// The last durable state transition, if it happened after this field was introduced.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn session_state_since(&self, id: SessionId) -> Result<Option<Timestamp>> {
        self.transaction(|trx| async move { Ok(self.session(&trx, id).await?.state_since) })
            .await
    }

    pub(crate) fn event_key(&self, id: SessionId, seq: u64) -> Vec<u8> {
        Keys::new(&self.root).event(id, seq)
    }

    pub(crate) fn session_tool_key(
        &self,
        id: SessionId,
        request: swarmy_core::RequestId,
    ) -> Vec<u8> {
        Keys::new(&self.root).session_tools(id, request)
    }

    pub(crate) fn event_space(&self, id: SessionId) -> Subspace {
        crate::keys::Keys::new(&self.root).event_space(id)
    }

    fn runnable_key(&self, entry: &RunnableEntry) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).runnable(
            runnable_partition(entry.session_id),
            entry.priority,
            entry.wake_at,
            entry.session_id,
        )
    }

    fn runnable_lookup(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).runnable_by_session(id)
    }

    pub(crate) async fn remove_runnable(&self, trx: &Transaction, id: SessionId) -> Result<()> {
        let lookup = self.runnable_lookup(id);
        if let Some(entry) = read::<RunnableEntry>(trx, &lookup).await? {
            trx.clear(&self.runnable_key(&entry));
            trx.clear(&lookup);
        }
        Ok(())
    }

    pub(crate) async fn index_runnable(
        &self,
        trx: &Transaction,
        entry: &RunnableEntry,
    ) -> Result<()> {
        self.remove_runnable(trx, entry.session_id).await?;
        self.write_runnable(trx, entry)
    }

    pub(crate) fn write_runnable(&self, trx: &Transaction, entry: &RunnableEntry) -> Result<()> {
        write(trx, &self.runnable_key(entry), &())?;
        write(trx, &self.runnable_lookup(entry.session_id), entry)
    }

    /// Insert or reschedule a Runnable session, replacing its previous index entry.
    /// # Errors
    /// Rejects missing or non-Runnable sessions and transaction failures.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn insert_runnable(&self, entry: &RunnableEntry) -> Result<()> {
        self.transaction(|trx| async move {
            if self.session(&trx, entry.session_id).await?.state != SessionState::Runnable {
                return Err(StoreError::Domain(
                    crate::DomainError::UnexpectedSessionState,
                ));
            }
            self.index_runnable(&trx, entry).await
        })
        .await
    }

    /// Scan one partition by priority, wake time, and id. `after` is exclusive.
    /// Wake times are returned for the scheduler to evaluate against its clock.
    /// # Errors
    /// Rejects invalid partitions, cursors, limits, and malformed stored keys.
    pub async fn scan_runnable(
        &self,
        partition: u16,
        after: Option<&RunnableEntry>,
        limit: usize,
    ) -> Result<Vec<RunnableEntry>> {
        if partition >= RUNNABLE_PARTITIONS
            || after.is_some_and(|entry| runnable_partition(entry.session_id) != partition)
        {
            return Err(StoreError::Domain(crate::DomainError::InvalidPartition));
        }
        self.transaction(|trx| async move {
            let space = crate::keys::Keys::new(&self.root).runnable_space(partition);
            let (mut begin, end) = space.range();
            if let Some(entry) = after {
                begin = self.runnable_key(entry);
                begin.push(0);
            }
            let mut entries = Vec::new();
            for (key, _) in scan(&trx, (begin, end), limit).await? {
                let (priority, (seconds, nanos), id): (i64, (i64, i32), Vec<u8>) = space
                    .unpack(&key)
                    .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
                entries.push(RunnableEntry {
                    session_id: session_id(id)?,
                    priority,
                    wake_at: Timestamp::new(seconds, nanos)
                        .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?,
                });
            }
            Ok(entries)
        })
        .await
    }
}

pub(crate) fn session_id(bytes: Vec<u8>) -> Result<SessionId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StoreError::Storage(crate::StorageError::Corrupt))?;
    Ok(SessionId::from_ulid(u128::from_be_bytes(bytes).into()))
}

impl Store {
    pub(crate) fn agent_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).agent(id)
    }
    pub(crate) fn agent_github_token_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).agent_github_token(id)
    }
    pub(crate) fn agent_name_key(&self, name: &str) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).agent_by_name(name)
    }
    pub(crate) fn computer_deleted_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).computer_deleted(id)
    }

    pub(crate) fn session_agent_key(&self, agent: swarmy_core::AgentId, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session_by_agent(agent, id)
    }
}

// The store's tuple key family registry. All families have one private name
// so point reads, indexes, and prefix scans share exactly the same bytes.

const AGENT: &str = "agent";
const AGENT_BY_NAME: &str = "agent_by_name";
const AGENT_CALL_STATUS: &str = "agent_call_status";
const AGENT_GITHUB_TOKEN: &str = "agent_github_token";
const API_APPEND: &str = "api_append";
const API_IDEMPOTENCY: &str = "api_idempotency";
const API_SESSION_ID: &str = "api_session_id";
const CHUNK_REUSED: &str = "chunk_reused";
const COMPUTER_DELETED: &str = "computer_deleted";
const COMPUTER_MEMORY: &str = "computer_memory";
const COMPUTER_NOTICE: &str = "computer_notice";
const COMPUTER_NOTICE_DELIVERED: &str = "computer_notice_delivered";
const CREDENTIAL_ENTRY: &str = "credential_entry";
const CREDENTIAL_ENTRY_LEASE: &str = "credential_entry_lease";
const ENTRY_QUOTA_CONFIG: &str = "entry_quota_config";
const ENTRY_QUOTA_OBSERVED: &str = "entry_quota_observed";
const EVENT: &str = "event";
const GATEWAY_PROVIDER: &str = "gateway_provider";
const GATEWAY_PROVIDER_ENTRY: &str = "gateway_provider_entry";
const GC_DELETING: &str = "gc_deleting";
const GC_LEASE: &str = "gc_lease";
const GC_RUN: &str = "gc_run";
const GC_SEQUENCE: &str = "gc_sequence";
const IDEM: &str = "idem";
const IMAGE: &str = "image";
const IMAGE_DISPLAY: &str = "image_display";
const IMAGE_MEMORY: &str = "image_memory";
const IMAGE_SCRATCH: &str = "image_scratch";
const INFERENCE_BREAKER: &str = "inference_breaker";
const INFERENCE_CLAIM: &str = "inference_claim";
const INFERENCE_INPUT: &str = "inference_input";
const INFERENCE_REQUEST: &str = "inference_request";
const INFERENCE_RETRY: &str = "inference_retry";
const INFERENCE_RESULT: &str = "inference_result";
const INFERENCE_WAIT: &str = "inference_wait";
const INFERENCE_WAIT_DUE: &str = "inference_wait_due";
const INFLIGHT: &str = "inflight";
const LEASE: &str = "lease";
const LEASE_BY_EXPIRY: &str = "lease_by_expiry";
const MANIFEST: &str = "manifest";
const MANIFEST_PARENT: &str = "manifest_parent";
const METERING_HOUR: &str = "metering_hour";
const NODE: &str = "node";
const PLACED_TOOL_CLAIM: &str = "placed_tool_claim";
const PLACEMENT: &str = "placement";
const PLACEMENT_ADDRESS: &str = "placement_address";
const PLACEMENT_BY_NODE: &str = "placement_by_node";
const PLACEMENT_COUNT: &str = "placement_count";
const PLACEMENT_EPOCH: &str = "placement_epoch";
const PLACEMENT_HOSTING: &str = "placement_hosting";
const REQUEST_TURN: &str = "request_turn";
const ROUTE: &str = "route";
const RUNNABLE: &str = "runnable";
const RUNNABLE_BY_SESSION: &str = "runnable_by_session";
const SCRATCH: &str = "scratch";
const SERVICE_HEARTBEAT: &str = "service_heartbeat";
const SESSION: &str = "session";
const SESSION_BY_AGENT: &str = "session_by_agent";
const SESSION_CHAIN: &str = "session_chain";
const SESSION_CHUNK: &str = "session_chunk";
/// Queued input lives outside the session key family so session scans stay valid.
const QUEUED_MESSAGE: &str = "queued_message";
const QUEUED_COUNTER: &str = "queued_counter";
const QUEUED_REPLAY: &str = "queued_replay";
const SESSION_TOOLS: &str = "session_tools";
const SNAPSHOT: &str = "snapshot";
const TIMER: &str = "timer";
const TIMER_ACTIVE: &str = "timer_active";
const TIMER_DUE: &str = "timer_due";
const TIMER_ORIGIN: &str = "timer_origin";
const TOOL_DONE: &str = "tool_done";
const TOOL_JOB: &str = "tool_job";
const TOOL_PLACEMENT: &str = "tool_placement";
const TURN: &str = "turn";
const TURN_INFERENCE: &str = "turn_inference";
const TURN_METRICS: &str = "turn_metrics";
const TURN_TOOL: &str = "turn_tool";
const USAGE: &str = "usage";
const USAGE_BY_AGENT: &str = "usage_by_agent";
const USAGE_RECORD: &str = "usage_record";
const USAGE_RECORD_BY_TIME: &str = "usage_record_by_time";
const VOLUME: &str = "volume";
const VOLUME_LEASE_SEQ: &str = "volume_lease_seq";
const VOLUME_PLACEMENT: &str = "volume_placement";
const VOLUME_SNAPSHOTS: &str = "volume_snapshots";

pub(crate) struct Keys<'a> {
    root: &'a Subspace,
}
impl<'a> Keys<'a> {
    pub(crate) fn new(root: &'a Subspace) -> Self {
        Self { root }
    }
    pub(crate) fn agent(&self, id: AgentId) -> Vec<u8> {
        self.agent_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn agent_space(&self) -> Subspace {
        self.root.subspace(&(AGENT,))
    }
    pub(crate) fn agent_by_name(&self, value: &str) -> Vec<u8> {
        self.agent_by_name_space().pack(&(value,))
    }
    pub(crate) fn agent_by_name_space(&self) -> Subspace {
        self.root.subspace(&(AGENT_BY_NAME,))
    }
    pub(crate) fn agent_call_status(&self, id: AgentId) -> Vec<u8> {
        self.agent_call_status_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn agent_call_status_space(&self) -> Subspace {
        self.root.subspace(&(AGENT_CALL_STATUS,))
    }
    pub(crate) fn agent_github_token(&self, id: AgentId) -> Vec<u8> {
        self.agent_github_token_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn agent_github_token_space(&self) -> Subspace {
        self.root.subspace(&(AGENT_GITHUB_TOKEN,))
    }
    pub(crate) fn api_append(&self, value: &str) -> Vec<u8> {
        self.api_append_space().pack(&(value,))
    }
    pub(crate) fn api_append_space(&self) -> Subspace {
        self.root.subspace(&(API_APPEND,))
    }
    pub(crate) fn api_idempotency(&self, value: &str) -> Vec<u8> {
        self.api_idempotency_space().pack(&(value,))
    }
    pub(crate) fn api_idempotency_space(&self) -> Subspace {
        self.root.subspace(&(API_IDEMPOTENCY,))
    }
    pub(crate) fn api_session_id(&self, value: &str) -> Vec<u8> {
        self.api_session_id_space().pack(&(value,))
    }
    pub(crate) fn api_session_id_space(&self) -> Subspace {
        self.root.subspace(&(API_SESSION_ID,))
    }
    pub(crate) fn chunk_reused(&self, hash: swarmy_core::ContentHash) -> Vec<u8> {
        self.chunk_reused_space().pack(&(hash.0.as_slice(),))
    }
    pub(crate) fn chunk_reused_space(&self) -> Subspace {
        self.root.subspace(&(CHUNK_REUSED,))
    }
    pub(crate) fn computer_deleted(&self, id: AgentId) -> Vec<u8> {
        self.computer_deleted_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn computer_deleted_space(&self) -> Subspace {
        self.root.subspace(&(COMPUTER_DELETED,))
    }
    pub(crate) fn computer_notice(&self, agent: AgentId, epoch: u64) -> Vec<u8> {
        self.computer_notice_space()
            .pack(&(agent.as_ulid().to_bytes().as_slice(), epoch))
    }
    pub(crate) fn computer_notice_space(&self) -> Subspace {
        self.root.subspace(&(COMPUTER_NOTICE,))
    }
    pub(crate) fn computer_notice_delivered(&self, session: SessionId, epoch: u64) -> Vec<u8> {
        self.computer_notice_delivered_space()
            .pack(&(session.as_ulid().to_bytes().as_slice(), epoch))
    }
    pub(crate) fn computer_notice_delivered_space(&self) -> Subspace {
        self.root.subspace(&(COMPUTER_NOTICE_DELIVERED,))
    }

    pub(crate) fn credential_entry(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
    ) -> Vec<u8> {
        self.credential_entry_space_root()
            .pack(&(scope.to_string(), provider, label))
    }
    pub(crate) fn credential_entry_space_root(&self) -> Subspace {
        self.root.subspace(&(CREDENTIAL_ENTRY,))
    }
    pub(crate) fn credential_entry_lease(
        &self,
        scope: CredentialScope,
        provider: &str,
        label: &str,
    ) -> Vec<u8> {
        self.credential_entry_lease_space()
            .pack(&(scope.to_string(), provider, label))
    }
    pub(crate) fn credential_entry_lease_space(&self) -> Subspace {
        self.root.subspace(&(CREDENTIAL_ENTRY_LEASE,))
    }

    pub(crate) fn entry_quota_config(&self, provider: &str, label: &str) -> Vec<u8> {
        self.entry_quota_config_space().pack(&(provider, label))
    }
    pub(crate) fn entry_quota_config_space(&self) -> Subspace {
        self.root.subspace(&(ENTRY_QUOTA_CONFIG,))
    }
    pub(crate) fn entry_quota_observed(&self, provider: &str, label: &str) -> Vec<u8> {
        self.entry_quota_observed_space().pack(&(provider, label))
    }
    pub(crate) fn entry_quota_observed_space(&self) -> Subspace {
        self.root.subspace(&(ENTRY_QUOTA_OBSERVED,))
    }
    pub(crate) fn event(&self, session: SessionId, seq: u64) -> Vec<u8> {
        self.event_space_root()
            .pack(&(session.as_ulid().to_bytes().as_slice(), seq))
    }
    pub(crate) fn event_space_root(&self) -> Subspace {
        self.root.subspace(&(EVENT,))
    }
    pub(crate) fn gateway_provider(&self, value: &str) -> Vec<u8> {
        self.gateway_provider_space().pack(&(value,))
    }
    pub(crate) fn gateway_provider_space(&self) -> Subspace {
        self.root.subspace(&(GATEWAY_PROVIDER,))
    }
    pub(crate) fn gateway_provider_entry(&self, provider: &str, label: &str) -> Vec<u8> {
        self.gateway_provider_entry_space().pack(&(provider, label))
    }
    pub(crate) fn gateway_provider_entry_space(&self) -> Subspace {
        self.root.subspace(&(GATEWAY_PROVIDER_ENTRY,))
    }
    pub(crate) fn gc_deleting(&self, hash: swarmy_core::ContentHash) -> Vec<u8> {
        self.gc_deleting_space().pack(&(hash.0.as_slice(),))
    }
    pub(crate) fn gc_deleting_space(&self) -> Subspace {
        self.root.subspace(&(GC_DELETING,))
    }
    pub(crate) fn gc_lease(&self) -> Vec<u8> {
        self.gc_lease_space().pack(&())
    }
    pub(crate) fn gc_lease_space(&self) -> Subspace {
        self.root.subspace(&(GC_LEASE,))
    }
    pub(crate) fn gc_run(&self, id: LeaseOwnerId) -> Vec<u8> {
        self.gc_run_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn gc_run_space(&self) -> Subspace {
        self.root.subspace(&(GC_RUN,))
    }
    pub(crate) fn gc_sequence(&self) -> Vec<u8> {
        self.gc_sequence_space().pack(&())
    }
    pub(crate) fn gc_sequence_space(&self) -> Subspace {
        self.root.subspace(&(GC_SEQUENCE,))
    }
    pub(crate) fn idem(&self, id: RequestId) -> Vec<u8> {
        self.idem_space().pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn idem_space(&self) -> Subspace {
        self.root.subspace(&(IDEM,))
    }
    pub(crate) fn image(&self, name: &str, tag: &ImageTag) -> Vec<u8> {
        self.image_space().pack(&(name, tag.0.as_str()))
    }
    pub(crate) fn image_space(&self) -> Subspace {
        self.root.subspace(&(IMAGE,))
    }
    pub(crate) fn image_display(
        &self,
        name: &str,
        tag: &ImageTag,
        manifest: ManifestId,
    ) -> Vec<u8> {
        self.image_display_space().pack(&(
            name,
            tag.0.as_str(),
            manifest.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn image_display_space(&self) -> Subspace {
        self.root.subspace(&(IMAGE_DISPLAY,))
    }
    pub(crate) fn image_memory(&self, name: &str, tag: &ImageTag, manifest: ManifestId) -> Vec<u8> {
        self.image_memory_space().pack(&(
            name,
            tag.0.as_str(),
            manifest.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn image_memory_space(&self) -> Subspace {
        self.root.subspace(&(IMAGE_MEMORY,))
    }
    pub(crate) fn image_scratch(
        &self,
        name: &str,
        tag: &ImageTag,
        manifest: ManifestId,
    ) -> Vec<u8> {
        self.image_scratch_space().pack(&(
            name,
            tag.0.as_str(),
            manifest.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn image_scratch_space(&self) -> Subspace {
        self.root.subspace(&(IMAGE_SCRATCH,))
    }
    pub(crate) fn inference_breaker(&self, provider: &str, label: &str) -> Vec<u8> {
        self.inference_breaker_space().pack(&(provider, label))
    }
    pub(crate) fn inference_breaker_space(&self) -> Subspace {
        self.root.subspace(&(INFERENCE_BREAKER,))
    }
    pub(crate) fn inference_input(&self, id: RequestId) -> Vec<u8> {
        self.inference_input_space()
            .pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn inference_input_space(&self) -> Subspace {
        self.root.subspace(&(INFERENCE_INPUT,))
    }
    pub(crate) fn inference_wait(&self, id: SessionId) -> Vec<u8> {
        self.inference_wait_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn inference_wait_space(&self) -> Subspace {
        self.root.subspace(&(INFERENCE_WAIT,))
    }
    pub(crate) fn inference_wait_due(&self, at: Timestamp, session: SessionId) -> Vec<u8> {
        self.inference_wait_due_space_root().pack(&(
            (at.as_second(), at.subsec_nanosecond()),
            session.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn inference_wait_due_space_root(&self) -> Subspace {
        self.root.subspace(&(INFERENCE_WAIT_DUE,))
    }
    pub(crate) fn inflight(&self, id: RequestId) -> Vec<u8> {
        self.inflight_space().pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn inflight_space(&self) -> Subspace {
        self.root.subspace(&(INFLIGHT,))
    }

    pub(crate) fn lease(&self, id: SessionId) -> Vec<u8> {
        self.lease_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn lease_space(&self) -> Subspace {
        self.root.subspace(&(LEASE,))
    }
    pub(crate) fn lease_by_expiry(&self, at: Timestamp, session: SessionId) -> Vec<u8> {
        self.lease_by_expiry_space_root().pack(&(
            (at.as_second(), at.subsec_nanosecond()),
            session.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn lease_by_expiry_space_root(&self) -> Subspace {
        self.root.subspace(&(LEASE_BY_EXPIRY,))
    }
    pub(crate) fn manifest(&self, id: ManifestId) -> Vec<u8> {
        self.manifest_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn manifest_space(&self) -> Subspace {
        self.root.subspace(&(MANIFEST,))
    }
    pub(crate) fn manifest_parent(&self, id: ManifestId) -> Vec<u8> {
        self.manifest_parent_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn manifest_parent_space(&self) -> Subspace {
        self.root.subspace(&(MANIFEST_PARENT,))
    }
    pub(crate) fn metering_hour_single(
        &self,
        dimension: &str,
        hour: i64,
        key: &str,
        field: &str,
    ) -> Vec<u8> {
        self.metering_hour_space_root()
            .pack(&(dimension, hour, key, field))
    }
    pub(crate) fn metering_hour_combined(
        &self,
        dimension: &str,
        owner: &str,
        hour: i64,
        entry: &str,
        field: &str,
    ) -> Vec<u8> {
        self.metering_hour_space_root()
            .pack(&(dimension, owner, hour, entry, field))
    }
    pub(crate) fn metering_hour_from(&self, dimension: &str, hour: i64) -> Vec<u8> {
        self.metering_hour_space_root().pack(&(dimension, hour))
    }
    pub(crate) fn metering_hour_owner_from(
        &self,
        dimension: &str,
        owner: &str,
        hour: i64,
    ) -> Vec<u8> {
        self.metering_hour_space_root()
            .pack(&(dimension, owner, hour))
    }
    pub(crate) fn metering_hour_space_root(&self) -> Subspace {
        self.root.subspace(&(METERING_HOUR,))
    }

    pub(crate) fn node(&self, id: NodeId) -> Vec<u8> {
        self.node_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn node_space(&self) -> Subspace {
        self.root.subspace(&(NODE,))
    }
    pub(crate) fn placement(&self, id: AgentId) -> Vec<u8> {
        self.placement_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn placement_space(&self) -> Subspace {
        self.root.subspace(&(PLACEMENT,))
    }
    pub(crate) fn placement_by_node(&self, node: NodeId, agent: AgentId) -> Vec<u8> {
        self.placement_by_node_space_root().pack(&(
            node.as_ulid().to_bytes().as_slice(),
            agent.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn placement_by_node_space_root(&self) -> Subspace {
        self.root.subspace(&(PLACEMENT_BY_NODE,))
    }
    pub(crate) fn placement_count(&self, id: NodeId) -> Vec<u8> {
        self.placement_count_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn placement_count_space(&self) -> Subspace {
        self.root.subspace(&(PLACEMENT_COUNT,))
    }
    pub(crate) fn request_turn(&self, id: RequestId) -> Vec<u8> {
        self.request_turn_space().pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn request_turn_space(&self) -> Subspace {
        self.root.subspace(&(REQUEST_TURN,))
    }
    pub(crate) fn route(&self, value: &str) -> Vec<u8> {
        self.route_space().pack(&(value,))
    }
    pub(crate) fn route_space(&self) -> Subspace {
        self.root.subspace(&(ROUTE,))
    }
    pub(crate) fn runnable(
        &self,
        partition: u16,
        priority: i64,
        at: Timestamp,
        session: SessionId,
    ) -> Vec<u8> {
        self.runnable_space_root().pack(&(
            partition,
            priority,
            (at.as_second(), at.subsec_nanosecond()),
            session.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn runnable_space_root(&self) -> Subspace {
        self.root.subspace(&(RUNNABLE,))
    }
    pub(crate) fn runnable_by_session(&self, id: SessionId) -> Vec<u8> {
        self.runnable_by_session_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn runnable_by_session_space(&self) -> Subspace {
        self.root.subspace(&(RUNNABLE_BY_SESSION,))
    }
    pub(crate) fn service_heartbeat(&self, role: &str, instance: &str) -> Vec<u8> {
        self.service_heartbeat_space().pack(&(role, instance))
    }
    pub(crate) fn service_heartbeat_space(&self) -> Subspace {
        self.root.subspace(&(SERVICE_HEARTBEAT,))
    }
    pub(crate) fn queued_space(&self, id: SessionId) -> Subspace {
        self.root
            .subspace(&(QUEUED_MESSAGE, id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn queued_message(&self, id: SessionId, index: u64) -> Vec<u8> {
        self.queued_space(id).pack(&(index,))
    }
    pub(crate) fn queued_counter(&self, id: SessionId) -> Vec<u8> {
        self.root
            .subspace(&(QUEUED_COUNTER,))
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn queued_replay(&self, key: &str) -> Vec<u8> {
        self.root.subspace(&(QUEUED_REPLAY,)).pack(&(key,))
    }
    pub(crate) fn session(&self, id: SessionId) -> Vec<u8> {
        self.session_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn session_space(&self) -> Subspace {
        self.root.subspace(&(SESSION,))
    }
    pub(crate) fn session_by_agent(&self, agent: AgentId, session: SessionId) -> Vec<u8> {
        self.session_by_agent_space_root().pack(&(
            agent.as_ulid().to_bytes().as_slice(),
            session.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn session_by_agent_space_root(&self) -> Subspace {
        self.root.subspace(&(SESSION_BY_AGENT,))
    }
    pub(crate) fn session_chain(&self, direction: &str, session: SessionId) -> Vec<u8> {
        self.session_chain_space()
            .pack(&(direction, session.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_chain_space(&self) -> Subspace {
        self.root.subspace(&(SESSION_CHAIN,))
    }
    pub(crate) fn session_chunk(&self, session: SessionId, index: u16) -> Vec<u8> {
        self.session_chunk_space_root()
            .pack(&(session.as_ulid().to_bytes().as_slice(), index))
    }
    pub(crate) fn session_chunk_space_root(&self) -> Subspace {
        self.root.subspace(&(SESSION_CHUNK,))
    }

    pub(crate) fn session_tools(&self, session: SessionId, request: RequestId) -> Vec<u8> {
        self.session_tools_space_root().pack(&(
            session.as_ulid().to_bytes().as_slice(),
            request.as_bytes().as_slice(),
        ))
    }
    pub(crate) fn session_tools_space_root(&self) -> Subspace {
        self.root.subspace(&(SESSION_TOOLS,))
    }
    pub(crate) fn snapshot(&self, session: SessionId, seq: u64) -> Vec<u8> {
        self.snapshot_space()
            .pack(&(session.as_ulid().to_bytes().as_slice(), seq))
    }
    pub(crate) fn snapshot_space(&self) -> Subspace {
        self.root.subspace(&(SNAPSHOT,))
    }
    pub(crate) fn timer(&self, agent: AgentId, timer: TimerId) -> Vec<u8> {
        self.timer_space().pack(&(
            agent.as_ulid().to_bytes().as_slice(),
            timer.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn timer_space(&self) -> Subspace {
        self.root.subspace(&(TIMER,))
    }
    pub(crate) fn timer_active(&self, agent: AgentId, timer: TimerId) -> Vec<u8> {
        self.timer_active_space_root().pack(&(
            agent.as_ulid().to_bytes().as_slice(),
            timer.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn timer_active_space_root(&self) -> Subspace {
        self.root.subspace(&(TIMER_ACTIVE,))
    }
    pub(crate) fn timer_due(&self, at: Timestamp, agent: AgentId, timer: TimerId) -> Vec<u8> {
        self.timer_due_space_root().pack(&(
            at.as_millisecond(),
            agent.as_ulid().to_bytes().as_slice(),
            timer.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn timer_due_space_root(&self) -> Subspace {
        self.root.subspace(&(TIMER_DUE,))
    }
    pub(crate) fn timer_origin(&self, agent: AgentId, timer: TimerId) -> Vec<u8> {
        self.timer_origin_space().pack(&(
            agent.as_ulid().to_bytes().as_slice(),
            timer.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn timer_origin_space(&self) -> Subspace {
        self.root.subspace(&(TIMER_ORIGIN,))
    }
    pub(crate) fn tool_job(&self, id: RequestId) -> Vec<u8> {
        self.tool_job_space().pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn tool_job_space(&self) -> Subspace {
        self.root.subspace(&(TOOL_JOB,))
    }
    pub(crate) fn turn(&self, id: SessionId) -> Vec<u8> {
        self.turn_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn turn_space(&self) -> Subspace {
        self.root.subspace(&(TURN,))
    }
    pub(crate) fn turn_inference(
        &self,
        session: SessionId,
        turn: MessageId,
        request: &str,
    ) -> Vec<u8> {
        self.turn_inference_space_root().pack(&(
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
            request.as_bytes(),
        ))
    }
    pub(crate) fn turn_inference_space_root(&self) -> Subspace {
        self.root.subspace(&(TURN_INFERENCE,))
    }
    pub(crate) fn turn_metrics(&self, session: SessionId, turn: MessageId) -> Vec<u8> {
        self.turn_metrics_space_root().pack(&(
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
        ))
    }
    pub(crate) fn turn_metrics_space_root(&self) -> Subspace {
        self.root.subspace(&(TURN_METRICS,))
    }
    pub(crate) fn turn_tool(&self, session: SessionId, turn: MessageId, call: &str) -> Vec<u8> {
        self.turn_tool_space_root().pack(&(
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
            call.as_bytes(),
        ))
    }
    pub(crate) fn turn_tool_space_root(&self) -> Subspace {
        self.root.subspace(&(TURN_TOOL,))
    }
    pub(crate) fn usage(&self, id: SessionId) -> Vec<u8> {
        self.usage_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn usage_space(&self) -> Subspace {
        self.root.subspace(&(USAGE,))
    }
    pub(crate) fn usage_by_agent(&self, id: AgentId) -> Vec<u8> {
        self.usage_by_agent_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn usage_by_agent_space(&self) -> Subspace {
        self.root.subspace(&(USAGE_BY_AGENT,))
    }
    pub(crate) fn usage_record(&self, id: RequestId) -> Vec<u8> {
        self.usage_record_space().pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn usage_record_space(&self) -> Subspace {
        self.root.subspace(&(USAGE_RECORD,))
    }
    pub(crate) fn usage_record_by_time(&self, hour: i64, request: RequestId) -> Vec<u8> {
        self.usage_record_by_time_space()
            .pack(&(hour, request.as_bytes().as_slice()))
    }
    pub(crate) fn usage_record_by_time_from(&self, hour: i64) -> Vec<u8> {
        self.usage_record_by_time_space().pack(&(hour,))
    }
    pub(crate) fn usage_record_by_time_space(&self) -> Subspace {
        self.root.subspace(&(USAGE_RECORD_BY_TIME,))
    }
    pub(crate) fn volume(&self, id: VolumeId) -> Vec<u8> {
        self.volume_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn volume_space(&self) -> Subspace {
        self.root.subspace(&(VOLUME,))
    }
    pub(crate) fn volume_lease_seq(&self, id: VolumeId) -> Vec<u8> {
        self.volume_lease_seq_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn volume_lease_seq_space(&self) -> Subspace {
        self.root.subspace(&(VOLUME_LEASE_SEQ,))
    }
    pub(crate) fn volume_placement(&self, id: VolumeId) -> Vec<u8> {
        self.volume_placement_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn volume_placement_space(&self) -> Subspace {
        self.root.subspace(&(VOLUME_PLACEMENT,))
    }
    pub(crate) fn volume_snapshots(&self, id: VolumeId) -> Vec<u8> {
        self.volume_snapshots_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn volume_snapshots_space(&self) -> Subspace {
        self.root.subspace(&(VOLUME_SNAPSHOTS,))
    }
    pub(crate) fn inference_request(&self, id: RequestId) -> Vec<u8> {
        self.inference_request_space()
            .pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn inference_request_space(&self) -> Subspace {
        self.root.subspace(&(INFERENCE_REQUEST,))
    }
    pub(crate) fn inference_retry(&self, id: RequestId) -> Vec<u8> {
        self.root
            .subspace(&(INFERENCE_RETRY,))
            .pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn inference_claim(&self, id: RequestId) -> Vec<u8> {
        self.inference_claim_space()
            .pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn inference_claim_space(&self) -> Subspace {
        self.root.subspace(&(INFERENCE_CLAIM,))
    }
    pub(crate) fn inference_result(&self, id: RequestId) -> Vec<u8> {
        self.inference_result_space()
            .pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn inference_result_space(&self) -> Subspace {
        self.root.subspace(&(INFERENCE_RESULT,))
    }
    pub(crate) fn tool_placement(&self, id: RequestId) -> Vec<u8> {
        self.tool_placement_space()
            .pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn tool_placement_space(&self) -> Subspace {
        self.root.subspace(&(TOOL_PLACEMENT,))
    }
    pub(crate) fn placed_tool_claim(&self, id: RequestId) -> Vec<u8> {
        self.placed_tool_claim_space()
            .pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn placed_tool_claim_space(&self) -> Subspace {
        self.root.subspace(&(PLACED_TOOL_CLAIM,))
    }
    pub(crate) fn tool_done(&self, id: RequestId) -> Vec<u8> {
        self.tool_done_space().pack(&(id.as_bytes().as_slice(),))
    }
    pub(crate) fn tool_done_space(&self) -> Subspace {
        self.root.subspace(&(TOOL_DONE,))
    }
    pub(crate) fn scratch(&self, id: AgentId) -> Vec<u8> {
        self.scratch_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn scratch_space(&self) -> Subspace {
        self.root.subspace(&(SCRATCH,))
    }
    pub(crate) fn placement_hosting(&self, id: AgentId) -> Vec<u8> {
        self.placement_hosting_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn placement_hosting_space(&self) -> Subspace {
        self.root.subspace(&(PLACEMENT_HOSTING,))
    }
    pub(crate) fn placement_address(&self, id: AgentId) -> Vec<u8> {
        self.placement_address_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn placement_address_space(&self) -> Subspace {
        self.root.subspace(&(PLACEMENT_ADDRESS,))
    }
    pub(crate) fn placement_epoch(&self, id: AgentId) -> Vec<u8> {
        self.placement_epoch_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn placement_epoch_space(&self) -> Subspace {
        self.root.subspace(&(PLACEMENT_EPOCH,))
    }
    pub(crate) fn computer_memory(&self, id: AgentId) -> Vec<u8> {
        self.computer_memory_space()
            .pack(&(id.as_ulid().to_bytes().as_slice(),))
    }
    pub(crate) fn computer_memory_space(&self) -> Subspace {
        self.root.subspace(&(COMPUTER_MEMORY,))
    }
    pub(crate) fn event_space(&self, session: SessionId) -> Subspace {
        self.event_space_root()
            .subspace(&(session.as_ulid().to_bytes().as_slice(),))
    }

    pub(crate) fn runnable_space(&self, partition: u16) -> Subspace {
        self.runnable_space_root().subspace(&(partition,))
    }

    pub(crate) fn credential_entry_space(&self, scope: CredentialScope) -> Subspace {
        self.credential_entry_space_root()
            .subspace(&(scope.to_string(),))
    }

    pub(crate) fn credential_entry_space_provider(
        &self,
        scope: CredentialScope,
        provider: &str,
    ) -> Subspace {
        self.credential_entry_space_root()
            .subspace(&(scope.to_string(), provider))
    }

    pub(crate) fn inference_wait_due_space(&self, at: Timestamp) -> Subspace {
        self.inference_wait_due_space_root()
            .subspace(&((at.as_second(), at.subsec_nanosecond()),))
    }

    pub(crate) fn lease_by_expiry_space(&self, at: Timestamp) -> Subspace {
        self.lease_by_expiry_space_root()
            .subspace(&((at.as_second(), at.subsec_nanosecond()),))
    }

    pub(crate) fn metering_hour_space(&self, dimension: &str) -> Subspace {
        self.metering_hour_space_root().subspace(&(dimension,))
    }

    pub(crate) fn metering_hour_space_owner(&self, dimension: &str, owner: &str) -> Subspace {
        self.metering_hour_space_root()
            .subspace(&(dimension, owner))
    }

    pub(crate) fn placement_by_node_space(&self, node: NodeId) -> Subspace {
        self.placement_by_node_space_root()
            .subspace(&(node.as_ulid().to_bytes().as_slice(),))
    }

    pub(crate) fn session_by_agent_space(&self, agent: AgentId) -> Subspace {
        self.session_by_agent_space_root()
            .subspace(&(agent.as_ulid().to_bytes().as_slice(),))
    }

    pub(crate) fn session_chunk_space(&self, session: SessionId) -> Subspace {
        self.session_chunk_space_root()
            .subspace(&(session.as_ulid().to_bytes().as_slice(),))
    }

    pub(crate) fn session_tools_space(&self, session: SessionId) -> Subspace {
        self.session_tools_space_root()
            .subspace(&(session.as_ulid().to_bytes().as_slice(),))
    }

    pub(crate) fn timer_active_space(&self, agent: AgentId) -> Subspace {
        self.timer_active_space_root()
            .subspace(&(agent.as_ulid().to_bytes().as_slice(),))
    }

    pub(crate) fn timer_due_space(&self, at: Timestamp) -> Subspace {
        self.timer_due_space_root()
            .subspace(&(at.as_millisecond(),))
    }

    pub(crate) fn turn_inference_space(&self, session: SessionId, turn: MessageId) -> Subspace {
        self.turn_inference_space_root().subspace(&(
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
        ))
    }

    pub(crate) fn turn_metrics_space(&self, session: SessionId) -> Subspace {
        self.turn_metrics_space_root()
            .subspace(&(session.as_ulid().to_bytes().as_slice(),))
    }

    pub(crate) fn turn_tool_space(&self, session: SessionId, turn: MessageId) -> Subspace {
        self.turn_tool_space_root().subspace(&(
            session.as_ulid().to_bytes().as_slice(),
            turn.as_ulid().to_bytes().as_slice(),
        ))
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;
    use std::fmt::Write as _;

    #[test]
    fn no_raw_family_packing_outside_registry() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        for entry in std::fs::read_dir(src).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "rs")
                && path.file_name().is_some_and(|name| name != "keys.rs")
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
    // The fixture contains complete master-era tuple keys, including nested time
    // tuples, binary identifiers, and each family's final component.
    // Listing every family together makes an omitted constructor visible.
    #[allow(clippy::too_many_lines)]
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
}
