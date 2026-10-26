# Cloud substrate

`swarmy swarm create NAME --host ADDRESS` and `swarmy swarm up NAME` provision existing hosts over SSH. The opt-in `remote` feature supplies the S3 client for static-key swarm buckets. `swarmy-cloud` contains the object-storage and SSH boundaries; machine creation belongs to `swarmy-infra`.

## The interface

```rust
pub trait Cloud {
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    fn verify_bucket_access(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    fn bucket_ownership(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<Ownership>>;
    fn delete_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<BucketRemoval>>;
}
```

`S3Buckets` uses static credentials and a configured endpoint. It creates a
bucket or adopts an empty one, then records ownership with tags or a marker
object when bucket tags are unsupported. `HeadBucket` verifies access before
adoption saves state. Teardown removes the swarm's prefix scope and deletes
the bucket only when empty and owned. It makes no IAM calls. A bucket using
an AWS instance role is left in place with a warning.

`provision::SwarmHost` handles SSH provisioning, service installation, image builds, copying
the operator's bootstrap key and decommissioning swarmy services. The SSH
implementation does not generate a key or inspect block devices through
this boundary. Adopted hosts remain operator-owned, and `swarm down` never
terminates them.

## Machines

Swarmy no longer creates machines in Rust. EC2 provisioning support was
removed on 2026-10-26. Cloud machines come from the OpenTofu modules under
`infra/modules` (see [infra/README.md](../infra/README.md)), driven by
`swarmy-infra`. Legacy EC2-launched remote records require manual cleanup.
