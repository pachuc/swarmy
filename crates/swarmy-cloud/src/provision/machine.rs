//! Host description used by provisioning, SSH, and upgrades.
use std::path::PathBuf;
use swarmy_config::BucketSpec;
#[derive(Clone, Debug)]
pub struct Machine {
    pub name: String,
    pub public_ip: String,
    pub private_ip: String,
    pub key_path: PathBuf,
    pub ssh_user: String,
    pub service_user: String,
    pub local_storage: String,
    pub sandboxes: u32,
    /// The swarm bucket and keys; none selects the stack host's object store.
    pub bucket: Option<BucketSpec>,
    pub services: bool,
}
impl Machine {
    #[must_use]
    pub fn service_user(&self) -> &str {
        &self.service_user
    }
    #[must_use]
    pub fn local_storage(&self) -> &str {
        &self.local_storage
    }
    #[must_use]
    pub fn bucket_spec(&self) -> Option<BucketSpec> {
        self.bucket.clone()
    }
    #[must_use]
    pub fn region(&self) -> &str {
        self.bucket
            .as_ref()
            .map_or("us-east-1", |b| b.region.as_str())
    }
}
