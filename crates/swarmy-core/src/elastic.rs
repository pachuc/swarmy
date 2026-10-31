//! Elastic machine groups: node groups whose machine count the swarm's provisioner owns.
use crate::HourlyPrice;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// Maximum number of machines in an elastic group.
pub const MAX_ELASTIC_MACHINES: u32 = 50;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElasticGroupDefinition {
    pub swarm: String,
    pub group: String,
    pub module: String,
    pub size: String,
    pub location: String,
    /// Used by task 6b when it provisions joined machines.
    pub sandboxes: u32,
    /// Used by task 6b when it provisions joined machines.
    pub local_storage: String,
    pub max: u32,
    pub hourly_price: HourlyPrice,
    /// Public IPv4 address of the stack host. Elastic machines reach it through
    /// their SSH tunnel, like every node.
    pub primary_address: std::net::Ipv4Addr,
    /// Service login that owns swarmy on every host of the swarm.
    pub service_user: String,
    /// The provisioner sets desired from sessions waiting for a GPU and from idle
    /// machines. Set it only on a group whose machines have an NVIDIA card. At most
    /// one group of a swarm autoscales.
    pub autoscale: bool,
    /// Whether joined machines use the swarm's external bucket with the
    /// primary's S3 settings. False means the stack host runs its own object
    /// store and machines reach it through their SSH tunnel.
    pub external_bucket: bool,
    /// Swarm network the group joins; empty means it creates its own.
    pub network_id: String,
    /// Machine image; `None` uses the module default.
    pub image: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElasticGroupRecord {
    pub definition: ElasticGroupDefinition,
    pub desired: u32,
    /// Why desired last changed. The provisioner copies it into the machine records it creates.
    pub desired_cause: crate::MachineCause,
    pub generation: u64,
    /// Machines `OpenTofu` held after the provisioner's last apply.
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

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn fixed_elastic_group_bytes() {
        let record = ElasticGroupRecord {
            definition: ElasticGroupDefinition {
                swarm: "s".into(),
                group: "g".into(),
                module: "aws".into(),
                size: "x".into(),
                location: "eu".into(),
                sandboxes: 1,
                local_storage: "dir:/x".into(),
                max: 2,
                hourly_price: HourlyPrice::parse("0.75", "EUR").unwrap(),
                primary_address: std::net::Ipv4Addr::new(192, 0, 2, 1),
                service_user: "swarmy".into(),
                autoscale: false,
                external_bucket: true,
                network_id: "4711".into(),
                image: None,
            },
            desired: 1,
            desired_cause: crate::MachineCause::from_reason("manual"),
            generation: 1,
            applied: 0,
            reconciled_generation: 0,
            updated_at: Timestamp::from_second(0).unwrap(),
        };
        let bytes = crate::encode(&record).unwrap();
        assert_eq!(
            bytes,
            [
                1, 1, 115, 1, 103, 3, 97, 119, 115, 1, 120, 2, 101, 117, 1, 6, 100, 105, 114, 58,
                47, 120, 2, 176, 227, 45, 3, 69, 85, 82, 192, 0, 2, 1, 6, 115, 119, 97, 114, 109,
                121, 0, 1, 4, 52, 55, 49, 49, 0, 1, 0, 0, 6, 109, 97, 110, 117, 97, 108, 1, 0, 0,
                20, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90
            ]
        );
        assert_eq!(crate::decode::<ElasticGroupRecord>(&bytes).unwrap(), record);
    }
}
