//! Records shared by the store, scheduler, and inference services.
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::SessionId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdempotencyState {
    /// Work was requested or started and can be retried until Completed.
    Requested,
    Completed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub state: IdempotencyState,
    pub result_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InflightRecord {
    pub session_id: SessionId,
    pub seq: u64,
    pub provider: String,
    pub key_id: String,
}

/// Lower priorities sort first within a partition, then earlier wake times.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnableEntry {
    pub session_id: SessionId,
    pub priority: i64,
    pub wake_at: Timestamp,
}
