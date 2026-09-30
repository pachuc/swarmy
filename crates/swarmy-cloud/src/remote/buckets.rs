//! Bucket lifecycle through the S3 API for static-key buckets.
//!
//! Any S3-compatible bucket reached by endpoint URL with static keys is
//! created, adopted, and torn down here with plain S3 calls: no IAM,
//! public-access-block, or encryption calls. Ownership is recorded with
//! bucket tags where the provider supports them, else in a marker object
//! under the prefix. The prefix-scoped delete loop and the S3 error helpers
//! are shared with the AWS path in `aws.rs`.
use aws_sdk_s3::error::ProvideErrorMetadata;

use super::aws::AwsContext;
use crate::{BucketRemoval, ObjectBucket, Ownership, Result};
use swarmy_config::BucketCredentials;

pub(super) const REMOTE_TAG: &str = "swarmy-remote";
pub(super) const MANAGED_TAG: &str = "managed-by";
pub(super) const MANAGER: &str = "swarmy";

/// S3 client authenticated with the bucket's static keys against its
/// endpoint. Path-style requests work on providers that do not serve
/// virtual-hosted buckets, matching the object-store client.
pub(super) fn client(
    sdk_config: &aws_config::SdkConfig,
    bucket: &ObjectBucket,
) -> Result<aws_sdk_s3::Client> {
    let BucketCredentials::StaticKeys {
        access_key,
        secret_key,
    } = &bucket.spec.credentials
    else {
        return Err(crate::Error::other("static bucket has no keys"));
    };
    crate::Error::ensure(
        !bucket.spec.endpoint.is_empty(),
        "static bucket has no endpoint",
    )?;
    let credentials = aws_sdk_s3::config::Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "swarmy-static",
    );
    let config = aws_sdk_s3::config::Builder::from(sdk_config)
        .region(aws_sdk_s3::config::Region::new(
            bucket.spec.region.clone(),
        ))
        .endpoint_url(&bucket.spec.endpoint)
        .credentials_provider(credentials)
        .force_path_style(true)
        .build();
    Ok(aws_sdk_s3::Client::from_conf(config))
}

/// Create the bucket through the S3 API when absent, or adopt an existing
/// empty bucket. Ownership is recorded with bucket tags where the provider
/// supports them, else in the marker object. No IAM, public-access-block,
/// or encryption calls: those are AWS-only.
pub(super) async fn ensure(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<()> {
    match client.head_bucket().bucket(&bucket.spec.bucket).send().await {
        Ok(_) => match ownership(client, bucket).await? {
            Ownership::Owned => Ok(()),
            Ownership::Absent => record_ownership(client, bucket).await,
            Ownership::Unmanaged => Err(crate::Error::other(format!(
                "bucket {} exists and is not empty; use an empty bucket or one owned by remote {}",
                bucket.spec.bucket, bucket.owner
            ))),
        },
        Err(error) if is_no_such_bucket(&error) => {
            client
                .create_bucket()
                .bucket(&bucket.spec.bucket)
                .send()
                .await
                .aws_context("s3:CreateBucket")?;
            record_ownership(client, bucket).await
        }
        Err(error) => Err(error).aws_context("s3:HeadBucket"),
    }
}

/// Ownership from bucket tags, or from the marker object where the provider
/// does not support tagging. No tags and no marker means the bucket is
/// adoptable only when its prefix scope is empty.
pub(super) async fn ownership(
    client: &aws_sdk_s3::Client,
    bucket: &ObjectBucket,
) -> Result<Ownership> {
    match client
        .get_bucket_tagging()
        .bucket(&bucket.spec.bucket)
        .send()
        .await
    {
        Ok(output) => Ok(owned(
            output.tag_set().iter().map(|tag| (tag.key(), tag.value())),
            &bucket.owner,
        )),
        Err(error) if is_no_such_tag_set(&error) => marker_ownership(client, bucket).await,
        Err(error) if tagging_unsupported(error_code(&error)) => {
            marker_ownership(client, bucket).await
        }
        Err(error) if is_no_such_bucket(&error) => Ok(Ownership::Absent),
        Err(error) => Err(error).aws_context("s3:GetBucketTagging"),
    }
}

/// Ownership from the marker object under the prefix.
async fn marker_ownership(
    client: &aws_sdk_s3::Client,
    bucket: &ObjectBucket,
) -> Result<Ownership> {
    match client
        .get_object()
        .bucket(&bucket.spec.bucket)
        .key(swarmy_config::ownership_marker_key(
            bucket.spec.prefix.as_str(),
        ))
        .send()
        .await
    {
        Ok(output) => {
            let body = output.body.collect().await.map_err(|source| {
                crate::Error::context(source, "s3:GetObject ownership marker")
            })?;
            let owner = String::from_utf8(body.into_bytes().to_vec())?;
            Ok(if owner.trim() == bucket.owner {
                Ownership::Owned
            } else {
                Ownership::Unmanaged
            })
        }
        Err(error) if is_no_such_key(&error) => {
            if prefix_empty(client, bucket).await? {
                Ok(Ownership::Absent)
            } else {
                Ok(Ownership::Unmanaged)
            }
        }
        Err(error) if is_no_such_bucket(&error) => Ok(Ownership::Absent),
        Err(error) => Err(error).aws_context("s3:GetObject"),
    }
}

/// Whether the bucket holds any object under the remote's prefix scope.
async fn prefix_empty(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<bool> {
    let mut request = client
        .list_objects_v2()
        .bucket(&bucket.spec.bucket)
        .max_keys(1);
    if !bucket.spec.prefix.as_str().is_empty() {
        request = request.prefix(format!("{}/", bucket.spec.prefix.as_str()));
    }
    let page = request.send().await.aws_context("s3:ListObjects")?;
    Ok(page.key_count().unwrap_or(0) == 0)
}

/// Record ownership with bucket tags, or with the marker object where the
/// provider does not support tagging. Denied tagging warns and still records
/// the marker, mirroring bucket creation on AWS.
async fn record_ownership(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<()> {
    let tags = merged_bucket_tags(Vec::new(), &bucket.owner);
    match client
        .put_bucket_tagging()
        .bucket(&bucket.spec.bucket)
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .set_tag_set(Some(tags))
                .build()?,
        )
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if tagging_unsupported(error_code(&error)) => {
            write_marker(client, bucket).await
        }
        Err(error) => {
            let result: Result<()> = Err(error).aws_context("s3:PutBucketTagging");
            if let Err(mapped) = result {
                warn_tag_denied(mapped, "s3:PutBucketTagging", &bucket.spec.bucket, &bucket.owner)?;
            }
            // Tagging is best effort here: denied tags warn as on AWS, so the
            // marker always records ownership.
            write_marker(client, bucket).await
        }
    }
}

async fn write_marker(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<()> {
    client
        .put_object()
        .bucket(&bucket.spec.bucket)
        .key(swarmy_config::ownership_marker_key(
            bucket.spec.prefix.as_str(),
        ))
        .body(format!("{}\n", bucket.owner).into_bytes().into())
        .send()
        .await
        .aws_context("s3:PutObject")?;
    Ok(())
}

/// Delete an owned static bucket's prefix scope, then remove the bucket
/// itself when nothing else remains. Shared buckets keep their other
/// content; only what the swarm owns is deleted.
pub(super) async fn delete(
    client: &aws_sdk_s3::Client,
    bucket: &ObjectBucket,
) -> Result<BucketRemoval> {
    match ownership(client, bucket).await? {
        Ownership::Absent => return Ok(BucketRemoval::Absent),
        Ownership::Unmanaged => {
            return Err(crate::Error::other("bucket ownership tags do not match"));
        }
        Ownership::Owned => {}
    }
    delete_prefix_scope(client, &bucket.spec.bucket, &scope_prefix(bucket)).await?;
    match client
        .delete_bucket()
        .bucket(&bucket.spec.bucket)
        .send()
        .await
    {
        Ok(_) => Ok(BucketRemoval::Removed),
        Err(error) if is_no_such_bucket(&error) => Ok(BucketRemoval::Absent),
        Err(error) if is_bucket_not_empty(&error) => Ok(BucketRemoval::Retained),
        Err(error) => Err(error).aws_context("s3:DeleteBucket"),
    }
}

/// The listing prefix bounding one remote's objects: its prefix scope, or
/// the whole bucket when no prefix is set.
fn scope_prefix(bucket: &ObjectBucket) -> String {
    let prefix = bucket.spec.prefix.as_str();
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

/// Delete every object version under `prefix`, re-reading the first page
/// after every deletion. A stored page would skip keys when the deletion
/// changes the listing beneath the cursor, so there are no continuation
/// tokens here. Versions are listed because the bucket may be versioned.
pub(super) async fn delete_prefix_scope(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Result<usize> {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    let mut count = 0usize;
    loop {
        let page = match client
            .list_object_versions()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(1000)
            .send()
            .await
        {
            Ok(page) => page,
            Err(error) if is_no_such_bucket(&error) => return Ok(count),
            Err(error) => return Err(error).aws_context("s3:ListBucketVersions"),
        };
        let objects: Vec<_> = page
            .versions()
            .iter()
            .map(|object| {
                ObjectIdentifier::builder()
                    .key(object.key().unwrap_or_default())
                    .set_version_id(object.version_id().map(str::to_owned))
                    .build()
            })
            .chain(page.delete_markers().iter().map(|object| {
                ObjectIdentifier::builder()
                    .key(object.key().unwrap_or_default())
                    .set_version_id(object.version_id().map(str::to_owned))
                    .build()
            }))
            .collect::<std::result::Result<_, _>>()?;
        if objects.is_empty() {
            break;
        }
        let size = objects.len();
        let result = client
            .delete_objects()
            .bucket(bucket)
            .delete(Delete::builder().set_objects(Some(objects)).build()?)
            .send()
            .await
            .aws_context("s3:DeleteObjects")?;
        crate::Error::ensure(
            result.errors().is_empty(),
            format!(
                "s3:DeleteObjects failed for {} objects",
                result.errors().len()
            ),
        )?;
        count += size;
        if count / 10_000 != (count - size) / 10_000 {
            cloud_out!("Deleted {count} objects from bucket {bucket}");
        }
    }
    Ok(count)
}

/// Adopt a static bucket after the operator confirmed its name. Tags where
/// supported, else the marker object after checking no other owner holds it.
pub(super) async fn adopt(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<()> {
    match client
        .get_bucket_tagging()
        .bucket(&bucket.spec.bucket)
        .send()
        .await
    {
        Ok(output) => {
            let tags = output.tag_set().to_vec();
            ensure_not_another_remote(
                tags.iter().map(|tag| (tag.key(), tag.value())),
                &bucket.owner,
            )?;
            write_tags(client, bucket, tags).await
        }
        Err(error) if is_no_such_tag_set(&error) => {
            write_tags(client, bucket, Vec::new()).await
        }
        Err(error) if tagging_unsupported(error_code(&error)) => {
            match marker_ownership(client, bucket).await? {
                Ownership::Unmanaged => {
                    Err(crate::Error::other("resource is tagged for another remote"))
                }
                Ownership::Absent | Ownership::Owned => write_marker(client, bucket).await,
            }
        }
        Err(error) => Err(error).aws_context("s3:GetBucketTagging"),
    }
}

async fn write_tags(
    client: &aws_sdk_s3::Client,
    bucket: &ObjectBucket,
    existing: Vec<aws_sdk_s3::types::Tag>,
) -> Result<()> {
    let tags = merged_bucket_tags(existing, &bucket.owner);
    client
        .put_bucket_tagging()
        .bucket(&bucket.spec.bucket)
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .set_tag_set(Some(tags))
                .build()?,
        )
        .send()
        .await
        .aws_context("s3:PutBucketTagging")?;
    Ok(())
}

pub(super) fn error_code<E>(error: &aws_sdk_s3::error::SdkError<E>) -> Option<&str>
where
    E: ProvideErrorMetadata + std::fmt::Debug + Send + Sync + 'static,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
}

pub(super) fn is_no_such_bucket<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata + std::fmt::Debug + Send + Sync + 'static,
{
    matches!(error_code(error), Some("NoSuchBucket" | "NotFound"))
}

fn is_no_such_tag_set<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata + std::fmt::Debug + Send + Sync + 'static,
{
    matches!(error_code(error), Some("NoSuchTagSet"))
}

fn is_no_such_key<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata + std::fmt::Debug + Send + Sync + 'static,
{
    matches!(
        error_code(error),
        Some("NoSuchKey" | "NotFound" | "NoSuchTagSet")
    )
}

fn is_bucket_not_empty<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata + std::fmt::Debug + Send + Sync + 'static,
{
    matches!(error_code(error), Some("BucketNotEmpty"))
}

/// Providers differ in which optional S3 features they support. Bucket tags
/// are optional in the S3 API; only these codes mean the tagging call itself
/// is unavailable, so ownership falls back to the marker object. Anything
/// else propagates as an error.
fn tagging_unsupported(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("NotImplemented" | "NotSupported" | "MethodNotAllowed")
    )
}

pub(super) fn warn_untagged(permission: &str, resource: &str, owner: &str) {
    cloud_err!(
        "Warning: missing {permission} for {resource}; remote down will leave it in place. Grant {permission} and run swarmy remote tag {owner} later."
    );
}

pub(super) fn warn_tag_denied(
    error: crate::Error,
    permission: &str,
    resource: &str,
    owner: &str,
) -> Result<()> {
    if error.permission().is_none() {
        return Err(error);
    }
    warn_untagged(permission, resource, owner);
    Ok(())
}

pub(super) fn s3_tag(key: &str, value: &str) -> aws_sdk_s3::types::Tag {
    aws_sdk_s3::types::Tag::builder()
        .key(key)
        .value(value)
        .build()
        .expect("tag fields")
}

pub(super) fn merged_bucket_tags(
    existing: Vec<aws_sdk_s3::types::Tag>,
    owner: &str,
) -> Vec<aws_sdk_s3::types::Tag> {
    let mut tags: Vec<_> = existing
        .into_iter()
        .filter(|tag| tag.key() != MANAGED_TAG && tag.key() != REMOTE_TAG)
        .collect();
    tags.push(s3_tag(MANAGED_TAG, MANAGER));
    tags.push(s3_tag(REMOTE_TAG, owner));
    tags
}

pub(super) fn ensure_not_another_remote<'a>(
    tags: impl Iterator<Item = (&'a str, &'a str)>,
    owner: &str,
) -> Result<()> {
    crate::Error::ensure(
        !tags
            .into_iter()
            .any(|(key, value)| key == REMOTE_TAG && value != owner),
        "resource is tagged for another remote",
    )
}

pub(super) fn owned<'a>(tags: impl Iterator<Item = (&'a str, &'a str)>, owner: &str) -> Ownership {
    let tags: Vec<_> = tags.collect();
    if tags
        .iter()
        .any(|(key, value)| *key == MANAGED_TAG && *value == MANAGER)
        && tags
            .iter()
            .any(|(key, value)| *key == REMOTE_TAG && *value == owner)
    {
        Ownership::Owned
    } else {
        Ownership::Unmanaged
    }
}
