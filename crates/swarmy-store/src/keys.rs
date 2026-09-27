use foundationdb::{Transaction, tuple::Subspace};
use jiff::Timestamp;
use swarmy_core::{ImageTag, ManifestId, RunnableEntry, SessionId, SessionState, VolumeId};

use crate::{Result, Store, StoreError, read, scan, write};

pub use swarmy_core::{RUNNABLE_PARTITIONS, runnable_partition};

impl Store {
    pub(crate) fn request_turn_key(&self, id: swarmy_core::RequestId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).request_turn(&(id.as_bytes().as_slice()))
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
        crate::keys::Keys::new(&self.root).turn(&(id.as_ulid().to_bytes().as_slice()))
    }

    /// The latest user message identifies the turn, including after snapshots.
    /// # Errors
    /// Returns database and decoding errors.
    pub async fn turn_id(&self, id: SessionId) -> Result<Option<swarmy_core::MessageId>> {
        self.transaction(|trx| async move { read(&trx, &self.turn_key(id)).await })
            .await
    }

    pub(crate) fn volume_key(&self, id: VolumeId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).volume(&(id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn manifest_key(&self, id: ManifestId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).manifest(&(id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn image_key(&self, name: &str, tag: &ImageTag) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).image(&(name, tag.0.as_str()))
    }

    pub(crate) fn volume_lease_seq_key(&self, id: VolumeId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).volume_lease_seq(&(id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn session_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session(&(id.as_ulid().to_bytes().as_slice()))
    }

    pub(crate) fn session_state_since_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root)
            .session_state_since(&(id.as_ulid().to_bytes().as_slice()))
    }

    /// The last durable state transition, if it happened after this field was introduced.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn session_state_since(&self, id: SessionId) -> Result<Option<Timestamp>> {
        self.transaction(|trx| async move { Ok(self.session(&trx, id).await?.state_since) })
            .await
    }

    pub(crate) fn event_key(&self, id: SessionId, seq: u64) -> Vec<u8> {
        let id_bytes = id.as_ulid().to_bytes();
        Keys::new(&self.root).event(&(id_bytes.as_slice(), seq))
    }

    pub(crate) fn session_tool_key(
        &self,
        id: SessionId,
        request: swarmy_core::RequestId,
    ) -> Vec<u8> {
        let id_bytes = id.as_ulid().to_bytes();
        Keys::new(&self.root).session_tools(&(id_bytes.as_slice(), request.as_bytes().as_slice()))
    }

    pub(crate) fn event_space(&self, id: SessionId) -> Subspace {
        crate::keys::Keys::new(&self.root).event_space(&(id.as_ulid().to_bytes().as_slice()))
    }

    fn runnable_key(&self, entry: &RunnableEntry) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).runnable(&(
            runnable_partition(entry.session_id),
            entry.priority,
            (entry.wake_at.as_second(), entry.wake_at.subsec_nanosecond()),
            entry.session_id.as_ulid().to_bytes().as_slice(),
        ))
    }

    fn runnable_lookup(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root)
            .runnable_by_session(&(id.as_ulid().to_bytes().as_slice()))
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
    pub async fn insert_runnable(&self, entry: &RunnableEntry) -> Result<()> {
        self.transaction(|trx| async move {
            if self.session(&trx, entry.session_id).await?.state != SessionState::Runnable {
                return Err(StoreError::InvalidState);
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
            return Err(StoreError::InvalidState);
        }
        self.transaction(|trx| async move {
            let space = crate::keys::Keys::new(&self.root).runnable_space(&(partition));
            let (mut begin, end) = space.range();
            if let Some(entry) = after {
                begin = self.runnable_key(entry);
                begin.push(0);
            }
            let mut entries = Vec::new();
            for (key, _) in scan(&trx, (begin, end), limit).await? {
                let (priority, (seconds, nanos), id): (i64, (i64, i32), Vec<u8>) =
                    space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                entries.push(RunnableEntry {
                    session_id: session_id(id)?,
                    priority,
                    wake_at: Timestamp::new(seconds, nanos).map_err(|_| StoreError::Corrupt)?,
                });
            }
            Ok(entries)
        })
        .await
    }
}

pub(crate) fn session_id(bytes: Vec<u8>) -> Result<SessionId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| StoreError::Corrupt)?;
    Ok(SessionId::from_ulid(u128::from_be_bytes(bytes).into()))
}

impl Store {
    pub(crate) fn agent_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).agent(&(id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn agent_github_token_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).agent_github_token(&(id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn agent_name_key(&self, name: &str) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).agent_by_name(&(name))
    }
    pub(crate) fn computer_deleted_key(&self, id: swarmy_core::AgentId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).computer_deleted(&(id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_kind_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session_kind(&(id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_route_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session_route(&(id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_route_step_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session_route_step(&(id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_idle_key(&self, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session_idle(&(id.as_ulid().to_bytes().as_slice()))
    }
    pub(crate) fn session_agent_key(&self, agent: swarmy_core::AgentId, id: SessionId) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).session_by_agent(&(
            agent.as_ulid().to_bytes().as_slice(),
            id.as_ulid().to_bytes().as_slice(),
        ))
    }
}

// The store's tuple key family registry. All families have one private name
// so point reads, indexes, and prefix scans share exactly the same bytes.
use foundationdb::tuple::TuplePack;

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
const CREDENTIAL: &str = "credential";
const CREDENTIAL_ENTRY: &str = "credential_entry";
const CREDENTIAL_ENTRY_LEASE: &str = "credential_entry_lease";
const CREDENTIAL_LEASE: &str = "credential_lease";
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
const INFERENCE_RESULT: &str = "inference_result";
const INFERENCE_WAIT: &str = "inference_wait";
const INFERENCE_WAIT_DUE: &str = "inference_wait_due";
const INFLIGHT: &str = "inflight";
/// Legacy session side row, read and cleared during V1 hydration.
const INTERRUPT_REQUESTED: &str = "interrupt_requested";
const LEASE: &str = "lease";
const LEASE_BY_EXPIRY: &str = "lease_by_expiry";
const MANIFEST: &str = "manifest";
const MANIFEST_PARENT: &str = "manifest_parent";
const METERING_HOUR: &str = "metering_hour";
const METERING_LEGACY_PRUNED: &str = "metering_legacy_pruned";
const METERING_PRUNE_CURSOR: &str = "metering_prune_cursor";
const METERING_UPGRADE_AT: &str = "metering_upgrade_at";
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
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_IDLE: &str = "session_idle";
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_IMAGE: &str = "session_image";
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_INFERENCE: &str = "session_inference";
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_KIND: &str = "session_kind";
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_PLAN: &str = "session_plan";
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_ROUTE: &str = "session_route";
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_ROUTE_STEP: &str = "session_route_step";
/// Legacy session side row, read and cleared during V1 hydration.
const SESSION_STATE_SINCE: &str = "session_state_since";
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
    pub(crate) fn agent<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.agent_space(&()).pack(suffix)
    }
    pub(crate) fn agent_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(AGENT,)).subspace(suffix)
    }
    pub(crate) fn agent_by_name<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.agent_by_name_space(&()).pack(suffix)
    }
    pub(crate) fn agent_by_name_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(AGENT_BY_NAME,)).subspace(suffix)
    }
    pub(crate) fn agent_call_status<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.agent_call_status_space(&()).pack(suffix)
    }
    pub(crate) fn agent_call_status_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(AGENT_CALL_STATUS,)).subspace(suffix)
    }
    pub(crate) fn agent_github_token<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.agent_github_token_space(&()).pack(suffix)
    }
    pub(crate) fn agent_github_token_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(AGENT_GITHUB_TOKEN,)).subspace(suffix)
    }
    pub(crate) fn api_append<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.api_append_space(&()).pack(suffix)
    }
    pub(crate) fn api_append_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(API_APPEND,)).subspace(suffix)
    }
    pub(crate) fn api_idempotency<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.api_idempotency_space(&()).pack(suffix)
    }
    pub(crate) fn api_idempotency_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(API_IDEMPOTENCY,)).subspace(suffix)
    }
    pub(crate) fn api_session_id<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.api_session_id_space(&()).pack(suffix)
    }
    pub(crate) fn api_session_id_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(API_SESSION_ID,)).subspace(suffix)
    }
    pub(crate) fn chunk_reused<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.chunk_reused_space(&()).pack(suffix)
    }
    pub(crate) fn chunk_reused_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(CHUNK_REUSED,)).subspace(suffix)
    }
    pub(crate) fn computer_deleted<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.computer_deleted_space(&()).pack(suffix)
    }
    pub(crate) fn computer_deleted_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(COMPUTER_DELETED,)).subspace(suffix)
    }
    pub(crate) fn computer_notice<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.computer_notice_space(&()).pack(suffix)
    }
    pub(crate) fn computer_notice_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(COMPUTER_NOTICE,)).subspace(suffix)
    }
    pub(crate) fn computer_notice_delivered<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.computer_notice_delivered_space(&()).pack(suffix)
    }
    pub(crate) fn computer_notice_delivered_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root
            .subspace(&(COMPUTER_NOTICE_DELIVERED,))
            .subspace(suffix)
    }
    pub(crate) fn credential<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.credential_space(&()).pack(suffix)
    }
    pub(crate) fn credential_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(CREDENTIAL,)).subspace(suffix)
    }
    pub(crate) fn credential_entry<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.credential_entry_space(&()).pack(suffix)
    }
    pub(crate) fn credential_entry_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(CREDENTIAL_ENTRY,)).subspace(suffix)
    }
    pub(crate) fn credential_entry_lease<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.credential_entry_lease_space(&()).pack(suffix)
    }
    pub(crate) fn credential_entry_lease_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root
            .subspace(&(CREDENTIAL_ENTRY_LEASE,))
            .subspace(suffix)
    }
    pub(crate) fn credential_lease<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.credential_lease_space(&()).pack(suffix)
    }
    pub(crate) fn credential_lease_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(CREDENTIAL_LEASE,)).subspace(suffix)
    }
    pub(crate) fn entry_quota_config<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.entry_quota_config_space(&()).pack(suffix)
    }
    pub(crate) fn entry_quota_config_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(ENTRY_QUOTA_CONFIG,)).subspace(suffix)
    }
    pub(crate) fn entry_quota_observed<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.entry_quota_observed_space(&()).pack(suffix)
    }
    pub(crate) fn entry_quota_observed_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root
            .subspace(&(ENTRY_QUOTA_OBSERVED,))
            .subspace(suffix)
    }
    pub(crate) fn event<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.event_space(&()).pack(suffix)
    }
    pub(crate) fn event_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(EVENT,)).subspace(suffix)
    }
    pub(crate) fn gateway_provider<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.gateway_provider_space(&()).pack(suffix)
    }
    pub(crate) fn gateway_provider_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(GATEWAY_PROVIDER,)).subspace(suffix)
    }
    pub(crate) fn gateway_provider_entry<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.gateway_provider_entry_space(&()).pack(suffix)
    }
    pub(crate) fn gateway_provider_entry_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root
            .subspace(&(GATEWAY_PROVIDER_ENTRY,))
            .subspace(suffix)
    }
    pub(crate) fn gc_deleting<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.gc_deleting_space(&()).pack(suffix)
    }
    pub(crate) fn gc_deleting_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(GC_DELETING,)).subspace(suffix)
    }
    pub(crate) fn gc_lease<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.gc_lease_space(&()).pack(suffix)
    }
    pub(crate) fn gc_lease_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(GC_LEASE,)).subspace(suffix)
    }
    pub(crate) fn gc_run<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.gc_run_space(&()).pack(suffix)
    }
    pub(crate) fn gc_run_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(GC_RUN,)).subspace(suffix)
    }
    pub(crate) fn gc_sequence<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.gc_sequence_space(&()).pack(suffix)
    }
    pub(crate) fn gc_sequence_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(GC_SEQUENCE,)).subspace(suffix)
    }
    pub(crate) fn idem<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.idem_space(&()).pack(suffix)
    }
    pub(crate) fn idem_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(IDEM,)).subspace(suffix)
    }
    pub(crate) fn image<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.image_space(&()).pack(suffix)
    }
    pub(crate) fn image_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(IMAGE,)).subspace(suffix)
    }
    pub(crate) fn image_display<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.image_display_space(&()).pack(suffix)
    }
    pub(crate) fn image_display_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(IMAGE_DISPLAY,)).subspace(suffix)
    }
    pub(crate) fn image_memory<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.image_memory_space(&()).pack(suffix)
    }
    pub(crate) fn image_memory_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(IMAGE_MEMORY,)).subspace(suffix)
    }
    pub(crate) fn image_scratch<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.image_scratch_space(&()).pack(suffix)
    }
    pub(crate) fn image_scratch_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(IMAGE_SCRATCH,)).subspace(suffix)
    }
    pub(crate) fn inference_breaker<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inference_breaker_space(&()).pack(suffix)
    }
    pub(crate) fn inference_breaker_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFERENCE_BREAKER,)).subspace(suffix)
    }
    pub(crate) fn inference_input<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inference_input_space(&()).pack(suffix)
    }
    pub(crate) fn inference_input_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFERENCE_INPUT,)).subspace(suffix)
    }
    pub(crate) fn inference_wait<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inference_wait_space(&()).pack(suffix)
    }
    pub(crate) fn inference_wait_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFERENCE_WAIT,)).subspace(suffix)
    }
    pub(crate) fn inference_wait_due<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inference_wait_due_space(&()).pack(suffix)
    }
    pub(crate) fn inference_wait_due_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFERENCE_WAIT_DUE,)).subspace(suffix)
    }
    pub(crate) fn inflight<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inflight_space(&()).pack(suffix)
    }
    pub(crate) fn inflight_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFLIGHT,)).subspace(suffix)
    }
    pub(crate) fn interrupt_requested<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.interrupt_requested_space(&()).pack(suffix)
    }
    pub(crate) fn interrupt_requested_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INTERRUPT_REQUESTED,)).subspace(suffix)
    }
    pub(crate) fn lease<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.lease_space(&()).pack(suffix)
    }
    pub(crate) fn lease_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(LEASE,)).subspace(suffix)
    }
    pub(crate) fn lease_by_expiry<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.lease_by_expiry_space(&()).pack(suffix)
    }
    pub(crate) fn lease_by_expiry_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(LEASE_BY_EXPIRY,)).subspace(suffix)
    }
    pub(crate) fn manifest<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.manifest_space(&()).pack(suffix)
    }
    pub(crate) fn manifest_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(MANIFEST,)).subspace(suffix)
    }
    pub(crate) fn manifest_parent<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.manifest_parent_space(&()).pack(suffix)
    }
    pub(crate) fn manifest_parent_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(MANIFEST_PARENT,)).subspace(suffix)
    }
    pub(crate) fn metering_hour<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.metering_hour_space(&()).pack(suffix)
    }
    pub(crate) fn metering_hour_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(METERING_HOUR,)).subspace(suffix)
    }
    pub(crate) fn metering_legacy_pruned<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.metering_legacy_pruned_space(&()).pack(suffix)
    }
    pub(crate) fn metering_legacy_pruned_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root
            .subspace(&(METERING_LEGACY_PRUNED,))
            .subspace(suffix)
    }
    pub(crate) fn metering_prune_cursor<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.metering_prune_cursor_space(&()).pack(suffix)
    }
    pub(crate) fn metering_prune_cursor_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root
            .subspace(&(METERING_PRUNE_CURSOR,))
            .subspace(suffix)
    }
    pub(crate) fn metering_upgrade_at<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.metering_upgrade_at_space(&()).pack(suffix)
    }
    pub(crate) fn metering_upgrade_at_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(METERING_UPGRADE_AT,)).subspace(suffix)
    }
    pub(crate) fn node<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.node_space(&()).pack(suffix)
    }
    pub(crate) fn node_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(NODE,)).subspace(suffix)
    }
    pub(crate) fn placement<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.placement_space(&()).pack(suffix)
    }
    pub(crate) fn placement_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(PLACEMENT,)).subspace(suffix)
    }
    pub(crate) fn placement_by_node<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.placement_by_node_space(&()).pack(suffix)
    }
    pub(crate) fn placement_by_node_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(PLACEMENT_BY_NODE,)).subspace(suffix)
    }
    pub(crate) fn placement_count<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.placement_count_space(&()).pack(suffix)
    }
    pub(crate) fn placement_count_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(PLACEMENT_COUNT,)).subspace(suffix)
    }
    pub(crate) fn request_turn<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.request_turn_space(&()).pack(suffix)
    }
    pub(crate) fn request_turn_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(REQUEST_TURN,)).subspace(suffix)
    }
    pub(crate) fn route<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.route_space(&()).pack(suffix)
    }
    pub(crate) fn route_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(ROUTE,)).subspace(suffix)
    }
    pub(crate) fn runnable<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.runnable_space(&()).pack(suffix)
    }
    pub(crate) fn runnable_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(RUNNABLE,)).subspace(suffix)
    }
    pub(crate) fn runnable_by_session<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.runnable_by_session_space(&()).pack(suffix)
    }
    pub(crate) fn runnable_by_session_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(RUNNABLE_BY_SESSION,)).subspace(suffix)
    }
    pub(crate) fn service_heartbeat<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.service_heartbeat_space(&()).pack(suffix)
    }
    pub(crate) fn service_heartbeat_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SERVICE_HEARTBEAT,)).subspace(suffix)
    }
    pub(crate) fn session<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_space(&()).pack(suffix)
    }
    pub(crate) fn session_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION,)).subspace(suffix)
    }
    pub(crate) fn session_by_agent<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_by_agent_space(&()).pack(suffix)
    }
    pub(crate) fn session_by_agent_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_BY_AGENT,)).subspace(suffix)
    }
    pub(crate) fn session_chain<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_chain_space(&()).pack(suffix)
    }
    pub(crate) fn session_chain_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_CHAIN,)).subspace(suffix)
    }
    pub(crate) fn session_chunk<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_chunk_space(&()).pack(suffix)
    }
    pub(crate) fn session_chunk_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_CHUNK,)).subspace(suffix)
    }
    pub(crate) fn session_idle<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_idle_space(&()).pack(suffix)
    }
    pub(crate) fn session_idle_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_IDLE,)).subspace(suffix)
    }
    pub(crate) fn session_image<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_image_space(&()).pack(suffix)
    }
    pub(crate) fn session_image_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_IMAGE,)).subspace(suffix)
    }
    pub(crate) fn session_inference<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_inference_space(&()).pack(suffix)
    }
    pub(crate) fn session_inference_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_INFERENCE,)).subspace(suffix)
    }
    pub(crate) fn session_kind<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_kind_space(&()).pack(suffix)
    }
    pub(crate) fn session_kind_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_KIND,)).subspace(suffix)
    }
    pub(crate) fn session_plan<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_plan_space(&()).pack(suffix)
    }
    pub(crate) fn session_plan_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_PLAN,)).subspace(suffix)
    }
    pub(crate) fn session_route<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_route_space(&()).pack(suffix)
    }
    pub(crate) fn session_route_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_ROUTE,)).subspace(suffix)
    }
    pub(crate) fn session_route_step<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_route_step_space(&()).pack(suffix)
    }
    pub(crate) fn session_route_step_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_ROUTE_STEP,)).subspace(suffix)
    }
    pub(crate) fn session_state_since<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_state_since_space(&()).pack(suffix)
    }
    pub(crate) fn session_state_since_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_STATE_SINCE,)).subspace(suffix)
    }
    pub(crate) fn session_tools<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.session_tools_space(&()).pack(suffix)
    }
    pub(crate) fn session_tools_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SESSION_TOOLS,)).subspace(suffix)
    }
    pub(crate) fn snapshot<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.snapshot_space(&()).pack(suffix)
    }
    pub(crate) fn snapshot_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SNAPSHOT,)).subspace(suffix)
    }
    pub(crate) fn timer<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.timer_space(&()).pack(suffix)
    }
    pub(crate) fn timer_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TIMER,)).subspace(suffix)
    }
    pub(crate) fn timer_active_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TIMER_ACTIVE,)).subspace(suffix)
    }
    pub(crate) fn timer_due<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.timer_due_space(&()).pack(suffix)
    }
    pub(crate) fn timer_due_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TIMER_DUE,)).subspace(suffix)
    }
    pub(crate) fn timer_origin<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.timer_origin_space(&()).pack(suffix)
    }
    pub(crate) fn timer_origin_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TIMER_ORIGIN,)).subspace(suffix)
    }
    pub(crate) fn tool_job<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.tool_job_space(&()).pack(suffix)
    }
    pub(crate) fn tool_job_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TOOL_JOB,)).subspace(suffix)
    }
    pub(crate) fn turn<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.turn_space(&()).pack(suffix)
    }
    pub(crate) fn turn_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TURN,)).subspace(suffix)
    }
    pub(crate) fn turn_inference<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.turn_inference_space(&()).pack(suffix)
    }
    pub(crate) fn turn_inference_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TURN_INFERENCE,)).subspace(suffix)
    }
    pub(crate) fn turn_metrics<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.turn_metrics_space(&()).pack(suffix)
    }
    pub(crate) fn turn_metrics_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TURN_METRICS,)).subspace(suffix)
    }
    pub(crate) fn turn_tool<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.turn_tool_space(&()).pack(suffix)
    }
    pub(crate) fn turn_tool_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TURN_TOOL,)).subspace(suffix)
    }
    pub(crate) fn usage<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.usage_space(&()).pack(suffix)
    }
    pub(crate) fn usage_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(USAGE,)).subspace(suffix)
    }
    pub(crate) fn usage_by_agent<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.usage_by_agent_space(&()).pack(suffix)
    }
    pub(crate) fn usage_by_agent_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(USAGE_BY_AGENT,)).subspace(suffix)
    }
    pub(crate) fn usage_record<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.usage_record_space(&()).pack(suffix)
    }
    pub(crate) fn usage_record_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(USAGE_RECORD,)).subspace(suffix)
    }
    pub(crate) fn usage_record_by_time<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.usage_record_by_time_space(&()).pack(suffix)
    }
    pub(crate) fn usage_record_by_time_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root
            .subspace(&(USAGE_RECORD_BY_TIME,))
            .subspace(suffix)
    }
    pub(crate) fn volume<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.volume_space(&()).pack(suffix)
    }
    pub(crate) fn volume_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(VOLUME,)).subspace(suffix)
    }
    pub(crate) fn volume_lease_seq<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.volume_lease_seq_space(&()).pack(suffix)
    }
    pub(crate) fn volume_lease_seq_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(VOLUME_LEASE_SEQ,)).subspace(suffix)
    }
    pub(crate) fn volume_placement<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.volume_placement_space(&()).pack(suffix)
    }
    pub(crate) fn volume_placement_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(VOLUME_PLACEMENT,)).subspace(suffix)
    }
    pub(crate) fn volume_snapshots<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.volume_snapshots_space(&()).pack(suffix)
    }
    pub(crate) fn volume_snapshots_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(VOLUME_SNAPSHOTS,)).subspace(suffix)
    }
    pub(crate) fn inference_request<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inference_request_space(&()).pack(suffix)
    }
    pub(crate) fn inference_request_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFERENCE_REQUEST,)).subspace(suffix)
    }
    pub(crate) fn inference_claim<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inference_claim_space(&()).pack(suffix)
    }
    pub(crate) fn inference_claim_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFERENCE_CLAIM,)).subspace(suffix)
    }
    pub(crate) fn inference_result<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.inference_result_space(&()).pack(suffix)
    }
    pub(crate) fn inference_result_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(INFERENCE_RESULT,)).subspace(suffix)
    }
    pub(crate) fn tool_placement<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.tool_placement_space(&()).pack(suffix)
    }
    pub(crate) fn tool_placement_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TOOL_PLACEMENT,)).subspace(suffix)
    }
    pub(crate) fn placed_tool_claim<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.placed_tool_claim_space(&()).pack(suffix)
    }
    pub(crate) fn placed_tool_claim_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(PLACED_TOOL_CLAIM,)).subspace(suffix)
    }
    pub(crate) fn tool_done<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.tool_done_space(&()).pack(suffix)
    }
    pub(crate) fn tool_done_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(TOOL_DONE,)).subspace(suffix)
    }
    pub(crate) fn scratch<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.scratch_space(&()).pack(suffix)
    }
    pub(crate) fn scratch_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(SCRATCH,)).subspace(suffix)
    }
    pub(crate) fn placement_hosting<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.placement_hosting_space(&()).pack(suffix)
    }
    pub(crate) fn placement_hosting_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(PLACEMENT_HOSTING,)).subspace(suffix)
    }
    pub(crate) fn placement_address<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.placement_address_space(&()).pack(suffix)
    }
    pub(crate) fn placement_address_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(PLACEMENT_ADDRESS,)).subspace(suffix)
    }
    pub(crate) fn placement_epoch<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.placement_epoch_space(&()).pack(suffix)
    }
    pub(crate) fn placement_epoch_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(PLACEMENT_EPOCH,)).subspace(suffix)
    }
    pub(crate) fn computer_memory<T: TuplePack>(&self, suffix: &T) -> Vec<u8> {
        self.computer_memory_space(&()).pack(suffix)
    }
    pub(crate) fn computer_memory_space<T: TuplePack>(&self, suffix: &T) -> Subspace {
        self.root.subspace(&(COMPUTER_MEMORY,)).subspace(suffix)
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;

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
                    !text.contains("pack(&("),
                    "raw key family in {}",
                    path.display()
                );
            }
        }
    }

    #[test]
    fn family_prefix_layout_matches_checked_in_hex() {
        let root = Subspace::from_bytes(Vec::new());
        let keys = Keys::new(&root);
        let actual = [
            ("agent", keys.agent_space(&()).pack(&())),
            ("agent_by_name", keys.agent_by_name_space(&()).pack(&())),
            (
                "agent_call_status",
                keys.agent_call_status_space(&()).pack(&()),
            ),
            (
                "agent_github_token",
                keys.agent_github_token_space(&()).pack(&()),
            ),
            ("api_append", keys.api_append_space(&()).pack(&())),
            ("api_idempotency", keys.api_idempotency_space(&()).pack(&())),
            ("api_session_id", keys.api_session_id_space(&()).pack(&())),
            ("chunk_reused", keys.chunk_reused_space(&()).pack(&())),
            (
                "computer_deleted",
                keys.computer_deleted_space(&()).pack(&()),
            ),
            ("computer_memory", keys.computer_memory_space(&()).pack(&())),
            ("computer_notice", keys.computer_notice_space(&()).pack(&())),
            (
                "computer_notice_delivered",
                keys.computer_notice_delivered_space(&()).pack(&()),
            ),
            ("credential", keys.credential_space(&()).pack(&())),
            (
                "credential_entry",
                keys.credential_entry_space(&()).pack(&()),
            ),
            (
                "credential_entry_lease",
                keys.credential_entry_lease_space(&()).pack(&()),
            ),
            (
                "credential_lease",
                keys.credential_lease_space(&()).pack(&()),
            ),
            (
                "entry_quota_config",
                keys.entry_quota_config_space(&()).pack(&()),
            ),
            (
                "entry_quota_observed",
                keys.entry_quota_observed_space(&()).pack(&()),
            ),
            ("event", keys.event_space(&()).pack(&())),
            (
                "gateway_provider",
                keys.gateway_provider_space(&()).pack(&()),
            ),
            (
                "gateway_provider_entry",
                keys.gateway_provider_entry_space(&()).pack(&()),
            ),
            ("gc_deleting", keys.gc_deleting_space(&()).pack(&())),
            ("gc_lease", keys.gc_lease_space(&()).pack(&())),
            ("gc_run", keys.gc_run_space(&()).pack(&())),
            ("gc_sequence", keys.gc_sequence_space(&()).pack(&())),
            ("idem", keys.idem_space(&()).pack(&())),
            ("image", keys.image_space(&()).pack(&())),
            ("image_display", keys.image_display_space(&()).pack(&())),
            ("image_memory", keys.image_memory_space(&()).pack(&())),
            ("image_scratch", keys.image_scratch_space(&()).pack(&())),
            (
                "inference_breaker",
                keys.inference_breaker_space(&()).pack(&()),
            ),
            ("inference_claim", keys.inference_claim_space(&()).pack(&())),
            ("inference_input", keys.inference_input_space(&()).pack(&())),
            (
                "inference_request",
                keys.inference_request_space(&()).pack(&()),
            ),
            (
                "inference_result",
                keys.inference_result_space(&()).pack(&()),
            ),
            ("inference_wait", keys.inference_wait_space(&()).pack(&())),
            (
                "inference_wait_due",
                keys.inference_wait_due_space(&()).pack(&()),
            ),
            ("inflight", keys.inflight_space(&()).pack(&())),
            (
                "interrupt_requested",
                keys.interrupt_requested_space(&()).pack(&()),
            ),
            ("lease", keys.lease_space(&()).pack(&())),
            ("lease_by_expiry", keys.lease_by_expiry_space(&()).pack(&())),
            ("manifest", keys.manifest_space(&()).pack(&())),
            ("manifest_parent", keys.manifest_parent_space(&()).pack(&())),
            ("metering_hour", keys.metering_hour_space(&()).pack(&())),
            (
                "metering_legacy_pruned",
                keys.metering_legacy_pruned_space(&()).pack(&()),
            ),
            (
                "metering_prune_cursor",
                keys.metering_prune_cursor_space(&()).pack(&()),
            ),
            (
                "metering_upgrade_at",
                keys.metering_upgrade_at_space(&()).pack(&()),
            ),
            ("node", keys.node_space(&()).pack(&())),
            (
                "placed_tool_claim",
                keys.placed_tool_claim_space(&()).pack(&()),
            ),
            ("placement", keys.placement_space(&()).pack(&())),
            (
                "placement_address",
                keys.placement_address_space(&()).pack(&()),
            ),
            (
                "placement_by_node",
                keys.placement_by_node_space(&()).pack(&()),
            ),
            ("placement_count", keys.placement_count_space(&()).pack(&())),
            ("placement_epoch", keys.placement_epoch_space(&()).pack(&())),
            (
                "placement_hosting",
                keys.placement_hosting_space(&()).pack(&()),
            ),
            ("request_turn", keys.request_turn_space(&()).pack(&())),
            ("route", keys.route_space(&()).pack(&())),
            ("runnable", keys.runnable_space(&()).pack(&())),
            (
                "runnable_by_session",
                keys.runnable_by_session_space(&()).pack(&()),
            ),
            ("scratch", keys.scratch_space(&()).pack(&())),
            (
                "service_heartbeat",
                keys.service_heartbeat_space(&()).pack(&()),
            ),
            ("session", keys.session_space(&()).pack(&())),
            (
                "session_by_agent",
                keys.session_by_agent_space(&()).pack(&()),
            ),
            ("session_chain", keys.session_chain_space(&()).pack(&())),
            ("session_chunk", keys.session_chunk_space(&()).pack(&())),
            ("session_idle", keys.session_idle_space(&()).pack(&())),
            ("session_image", keys.session_image_space(&()).pack(&())),
            (
                "session_inference",
                keys.session_inference_space(&()).pack(&()),
            ),
            ("session_kind", keys.session_kind_space(&()).pack(&())),
            ("session_plan", keys.session_plan_space(&()).pack(&())),
            ("session_route", keys.session_route_space(&()).pack(&())),
            (
                "session_route_step",
                keys.session_route_step_space(&()).pack(&()),
            ),
            (
                "session_state_since",
                keys.session_state_since_space(&()).pack(&()),
            ),
            ("session_tools", keys.session_tools_space(&()).pack(&())),
            ("snapshot", keys.snapshot_space(&()).pack(&())),
            ("timer", keys.timer_space(&()).pack(&())),
            ("timer_active", keys.timer_active_space(&()).pack(&())),
            ("timer_due", keys.timer_due_space(&()).pack(&())),
            ("timer_origin", keys.timer_origin_space(&()).pack(&())),
            ("tool_done", keys.tool_done_space(&()).pack(&())),
            ("tool_job", keys.tool_job_space(&()).pack(&())),
            ("tool_placement", keys.tool_placement_space(&()).pack(&())),
            ("turn", keys.turn_space(&()).pack(&())),
            ("turn_inference", keys.turn_inference_space(&()).pack(&())),
            ("turn_metrics", keys.turn_metrics_space(&()).pack(&())),
            ("turn_tool", keys.turn_tool_space(&()).pack(&())),
            ("usage", keys.usage_space(&()).pack(&())),
            ("usage_by_agent", keys.usage_by_agent_space(&()).pack(&())),
            ("usage_record", keys.usage_record_space(&()).pack(&())),
            (
                "usage_record_by_time",
                keys.usage_record_by_time_space(&()).pack(&()),
            ),
            ("volume", keys.volume_space(&()).pack(&())),
            (
                "volume_lease_seq",
                keys.volume_lease_seq_space(&()).pack(&()),
            ),
            (
                "volume_placement",
                keys.volume_placement_space(&()).pack(&()),
            ),
            (
                "volume_snapshots",
                keys.volume_snapshots_space(&()).pack(&()),
            ),
        ];
        let expected = include_str!("../tests/key-layout.hex");
        let rendered: String = actual
            .iter()
            .map(|(name, bytes)| {
                let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
                format!("{name} {hex}\n")
            })
            .collect();
        assert_eq!(rendered, expected);
    }
}
