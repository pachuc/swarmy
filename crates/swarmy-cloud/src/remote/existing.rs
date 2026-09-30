//! Existing-host remotes: operator-owned machines with no cloud machine APIs.
//!
//! Bucket lifecycle still goes through object-storage APIs (the same calls
//! `remote up` uses, including S3-compatible endpoints with static keys), so
//! bucket methods delegate to an inner [`Aws`]. Every machine operation
//! fails: there is no instance to create, describe, or terminate and no
//! cloud key pair to import or delete. The provisioning paths never call
//! them; the error only fires on a programming mistake.
use super::{Cloud, Machine, MachineSpec, ObjectBucket, Ownership, aws::Aws};
use crate::{BucketRemoval, Result};

pub(super) struct ExistingHost {
    aws: Aws,
}

impl ExistingHost {
    pub(super) async fn new(region: &str) -> Self {
        Self {
            aws: Aws::new(region).await,
        }
    }
}

fn no_machines() -> crate::Error {
    crate::Error::other(
        "existing-host remotes have no cloud machines; decommission the server itself",
    )
}

impl Cloud for ExistingHost {
    async fn ensure_bucket(&self, bucket: &ObjectBucket) -> Result<()> {
        self.aws.ensure_bucket(bucket).await
    }
    fn base_image(&self) -> impl Future<Output = Result<String>> {
        std::future::ready(Err(no_machines()))
    }
    fn import_ssh_key(&self, _: &str, _: Vec<u8>, _: &str) -> impl Future<Output = Result<()>> {
        std::future::ready(Err(no_machines()))
    }
    fn create(&self, _: &MachineSpec) -> impl Future<Output = Result<String>> {
        std::future::ready(Err(no_machines()))
    }
    fn get(&self, _: &str) -> impl Future<Output = Result<Option<Machine>>> {
        std::future::ready(Err(no_machines()))
    }
    fn find_by_tag(&self, _: &str) -> impl Future<Output = Result<Option<String>>> {
        std::future::ready(Err(no_machines()))
    }
    fn destroy(&self, _: &str) -> impl Future<Output = Result<()>> {
        std::future::ready(Err(no_machines()))
    }
    async fn bucket_ownership(&self, bucket: &ObjectBucket) -> Result<Ownership> {
        self.aws.bucket_ownership(bucket).await
    }
    async fn role_ownership(&self, name: &str, owner: &str) -> Result<(Ownership, Ownership)> {
        self.aws.role_ownership(name, owner).await
    }
    async fn tag_bucket(&self, bucket: &ObjectBucket) -> Result<()> {
        self.aws.tag_bucket(bucket).await
    }
    async fn tag_node_role(&self, name: &str, owner: &str) -> Result<()> {
        self.aws.tag_node_role(name, owner).await
    }
    async fn delete_bucket(&self, bucket: &ObjectBucket) -> Result<BucketRemoval> {
        self.aws.delete_bucket(bucket).await
    }
    async fn delete_node_role(&self, name: &str, owner: &str) -> Result<(bool, bool)> {
        self.aws.delete_node_role(name, owner).await
    }
    fn delete_ssh_key(&self, _: &str) -> impl Future<Output = Result<()>> {
        std::future::ready(Err(no_machines()))
    }
}
