//! Elastic machine groups: node groups whose machine count the swarm's provisioner owns.
use crate::HourlyPrice;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

pub const MAX_ELASTIC_MACHINES: u32 = 50;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElasticGroupDefinition {
    pub swarm: String,
    pub group: String,
    pub module: String,
    pub size: String,
    pub location: String,
    pub sandboxes: u32,
    pub local_storage: String,
    pub max: u32,
    pub hourly_price: HourlyPrice,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElasticGroupRecord {
    pub definition: ElasticGroupDefinition,
    pub desired: u32,
    pub desired_reason: String,
    pub generation: u64,
    pub applied: u32,
    pub reconciled_generation: u64,
    pub updated_at: Timestamp,
}
impl ElasticGroupRecord {
    #[must_use]
    pub fn settled(&self) -> bool {
        self.reconciled_generation == self.generation
    }
}
