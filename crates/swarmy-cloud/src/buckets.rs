//! Bucket lifecycle through the S3 API for static-key buckets.
//!
//! Any S3-compatible bucket reached by endpoint URL with static keys is
//! created, adopted, and torn down here with plain S3 calls: no IAM,
//! public-access-block, or encryption calls. Ownership is recorded with
//! bucket tags where the provider supports them, else in a marker object
//! under the prefix.
use aws_sdk_s3::error::ProvideErrorMetadata;

use crate::Cloud;
use crate::{BucketRemoval, ObjectBucket, Ownership, Result};
use swarmy_config::BucketCredentials;

// AWS error codes, rather than rendered SDK messages, determine whether an
// operation can be retried after an operator grants a missing permission.
trait AwsContext<T> {
    fn aws_context(self, operation: &'static str) -> Result<T>;
}

impl<T, E> AwsContext<T> for std::result::Result<T, E>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    fn aws_context(self, operation: &'static str) -> Result<T> {
        self.map_err(|error| {
            // Without a service code the failure is local (credentials,
            // dispatch, network): keep the SDK error as the cause so the
            // report shows it instead of a synthesized "unknown".
            let Some(code) = error.code() else {
                return crate::Error::context(error, operation);
            };
            if access_denied(code) {
                crate::Error::MissingPermission {
                    operation: operation.to_owned(),
                    source: Box::new(error),
                }
            } else {
                crate::Error::Aws {
                    operation: operation.to_owned(),
                    code: code.to_owned(),
                    message: error.message().unwrap_or("unknown").to_owned(),
                }
            }
        })
    }
}

fn access_denied(code: &str) -> bool {
    matches!(
        code,
        "AccessDenied" | "AccessDeniedException" | "UnauthorizedOperation"
    )
}

const REMOTE_TAG: &str = "swarmy-remote";
const MANAGED_TAG: &str = "managed-by";
const MANAGER: &str = "swarmy";

/// S3 client authenticated with the bucket's static keys against its
/// endpoint. Path-style requests work on providers that do not serve
/// virtual-hosted buckets, matching the object-store client.
fn client(bucket: &ObjectBucket) -> Result<aws_sdk_s3::Client> {
    let BucketCredentials::StaticKeys {
        access_key,
        secret_key,
    } = &bucket.spec.credentials
    else {
        return Err(crate::Error::other(format!(
            "bucket {} uses an AWS instance role; swarmy manages only buckets reached with static keys",
            bucket.spec.bucket
        )));
    };
    crate::Error::ensure(
        !bucket.spec.endpoint.is_empty(),
        "static bucket has no endpoint",
    )?;
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .http_client(swarmy_aws::https_client())
        .region(aws_sdk_s3::config::Region::new(bucket.spec.region.clone()))
        .endpoint_url(&bucket.spec.endpoint)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "swarmy-static",
        ))
        .force_path_style(true)
        .build();
    Ok(aws_sdk_s3::Client::from_conf(config))
}

pub(crate) struct S3Buckets;

impl Cloud for S3Buckets {
    async fn verify_bucket_access(&self, bucket: &ObjectBucket) -> Result<()> {
        let client = client(bucket)?;
        match client
            .head_bucket()
            .bucket(&bucket.spec.bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_no_such_bucket(&error) => Ok(()),
            Err(error) => Err(error).aws_context("s3:HeadBucket"),
        }
    }
    async fn ensure_bucket(&self, bucket: &ObjectBucket) -> Result<()> {
        ensure(&client(bucket)?, bucket).await
    }
    async fn bucket_ownership(&self, bucket: &ObjectBucket) -> Result<Ownership> {
        ownership(&client(bucket)?, bucket).await
    }
    async fn delete_bucket(&self, bucket: &ObjectBucket) -> Result<BucketRemoval> {
        delete(&client(bucket)?, bucket).await
    }
}

/// Create the bucket through the S3 API when absent, or adopt an existing
/// empty bucket. Ownership is recorded with bucket tags where the provider
/// supports them, else in the marker object. No IAM, public-access-block,
/// or encryption calls: those are AWS-only.
async fn ensure(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<()> {
    match client
        .head_bucket()
        .bucket(&bucket.spec.bucket)
        .send()
        .await
    {
        Ok(_) => match ownership(client, bucket).await? {
            Ownership::Owned => Ok(()),
            Ownership::Absent => record_ownership(client, bucket).await,
            Ownership::Unmanaged => Err(crate::Error::other(format!(
                "bucket {} exists and is not empty; use an empty bucket or one owned by swarm {}",
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
async fn ownership(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<Ownership> {
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
async fn marker_ownership(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<Ownership> {
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
            let body =
                output.body.collect().await.map_err(|source| {
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
    let page = client
        .list_objects_v2()
        .bucket(&bucket.spec.bucket)
        .prefix(scope_prefix(bucket))
        .max_keys(1)
        .send()
        .await
        .aws_context("s3:ListObjects")?;
    Ok(page.key_count().unwrap_or(0) == 0)
}

/// Record ownership with bucket tags, or with the marker object where the
/// provider does not support tagging. Denied tagging warns and still records
/// the marker, mirroring bucket creation on AWS.
async fn record_ownership(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<()> {
    let tags = ownership_tags(&bucket.owner);
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
        Err(error) if tagging_unsupported(error_code(&error)) => write_marker(client, bucket).await,
        Err(error) => {
            let result: Result<()> = Err(error).aws_context("s3:PutBucketTagging");
            if let Err(mapped) = result {
                warn_tag_denied(
                    mapped,
                    "s3:PutBucketTagging",
                    &bucket.spec.bucket,
                    &bucket.owner,
                )?;
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
async fn delete(client: &aws_sdk_s3::Client, bucket: &ObjectBucket) -> Result<BucketRemoval> {
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
async fn delete_prefix_scope(
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

fn error_code<E>(error: &aws_sdk_s3::error::SdkError<E>) -> Option<&str>
where
    E: ProvideErrorMetadata + std::fmt::Debug + Send + Sync + 'static,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
}

fn is_no_such_bucket<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
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

fn warn_untagged(permission: &str, resource: &str, owner: &str) {
    cloud_err!(
        "Warning: missing {permission} for {resource}; swarmy swarm down will leave it in place. Grant {permission}, then tag the bucket managed-by=swarmy and swarmy-remote={owner} by hand."
    );
}

fn warn_tag_denied(
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

fn s3_tag(key: &str, value: &str) -> aws_sdk_s3::types::Tag {
    aws_sdk_s3::types::Tag::builder()
        .key(key)
        .value(value)
        .build()
        .expect("tag fields")
}

fn ownership_tags(owner: &str) -> Vec<aws_sdk_s3::types::Tag> {
    vec![s3_tag(MANAGED_TAG, MANAGER), s3_tag(REMOTE_TAG, owner)]
}

fn owned<'a>(tags: impl Iterator<Item = (&'a str, &'a str)>, owner: &str) -> Ownership {
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

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::AwsContext;

    fn mapped(operation: &'static str, code: &str, message: &str) -> crate::Error {
        let metadata = aws_sdk_s3::error::ErrorMetadata::builder()
            .code(code)
            .message(message)
            .build();
        Err::<(), _>(metadata).aws_context(operation).unwrap_err()
    }

    #[test]
    fn denial_codes_become_missing_permission() {
        for code in [
            "AccessDenied",
            "AccessDeniedException",
            "UnauthorizedOperation",
        ] {
            match mapped("s3:HeadBucket", code, "not authorized") {
                crate::Error::MissingPermission { operation, .. } => {
                    assert_eq!(operation, "s3:HeadBucket");
                }
                error => panic!("expected MissingPermission for {code}, got {error:?}"),
            }
        }
    }

    #[test]
    fn other_errors_carry_code_and_message_from_metadata() {
        match mapped("s3:HeadBucket", "InvalidRequest", "bad bucket request") {
            crate::Error::Aws {
                operation,
                code,
                message,
            } => {
                assert_eq!(operation, "s3:HeadBucket");
                assert_eq!(code, "InvalidRequest");
                assert_eq!(message, "bad bucket request");
            }
            error => panic!("expected Aws, got {error:?}"),
        }
    }
}
