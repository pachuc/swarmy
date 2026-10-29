//! Bucket coordinates and credential handling shared by swarm provisioning and nodes.
use crate::{Error, ObjectPrefix, Settings};
use serde::{Deserialize, Serialize};

/// Credential source for a remote object bucket. The secret value only ever
/// lives in 0600 state files and 0600 node environment files; [`std::fmt::Debug`]
/// redacts both keys so logs and status output can print the description safely.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum BucketCredentials {
    /// AWS today: nodes reach the bucket through the instance role.
    #[default]
    InstanceRole,
    /// Any S3-compatible bucket reached by endpoint URL with static keys.
    StaticKeys {
        access_key: String,
        secret_key: String,
    },
}

impl std::fmt::Debug for BucketCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InstanceRole => formatter.write_str("InstanceRole"),
            Self::StaticKeys { .. } => formatter.write_str("StaticKeys(..redacted..)"),
        }
    }
}

/// Provider-neutral description of the object bucket backing a remote.
///
/// One type covers both AWS S3 through an IAM instance role (empty endpoint,
/// [`BucketCredentials::InstanceRole`]) and any S3-compatible bucket reached
/// by endpoint URL with static keys. `up`, `add-node`, `connect`, `upgrade`,
/// and the node environment all use this; there are no parallel AWS and
/// non-AWS copies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BucketSpec {
    /// Storage endpoint override for S3-compatible providers. Empty selects
    /// the AWS regional endpoints.
    pub endpoint: String,
    /// Bucket region. Empty falls back to the remote's configured region.
    pub region: String,
    /// Bucket name.
    pub bucket: String,
    /// Object namespace inside the bucket shared by every service.
    pub prefix: ObjectPrefix,
    /// How nodes authenticate to the bucket.
    pub credentials: BucketCredentials,
    /// Write chunks and manifests with a create-only PUT
    /// (`If-None-Match: *`). Providers that reject the header need `false`,
    /// which makes the call a plain PUT. The objects are content-addressed,
    /// so overwriting identical bytes is safe. Carried into the node
    /// environment and the connect profile, which is what the nodes read.
    pub conditional_create: bool,
}

impl Default for BucketSpec {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: String::new(),
            bucket: String::new(),
            prefix: ObjectPrefix::default(),
            credentials: BucketCredentials::default(),
            conditional_create: true,
        }
    }
}

impl BucketSpec {
    /// The bucket the settings select, or None when `s3.bucket` is empty. Non-empty
    /// access keys select static keys; empty ones select the instance role.
    #[must_use]
    pub fn from_settings(s3: &crate::S3Settings) -> Option<Self> {
        if s3.bucket.is_empty() {
            return None;
        }
        let credentials = if s3.access_key.is_empty() {
            BucketCredentials::InstanceRole
        } else {
            BucketCredentials::StaticKeys {
                access_key: s3.access_key.clone(),
                secret_key: s3.secret_key.clone(),
            }
        };
        Some(Self {
            endpoint: s3.endpoint.clone(),
            region: s3.region.clone(),
            bucket: s3.bucket.clone(),
            prefix: s3.prefix.clone(),
            conditional_create: s3.conditional_create,
            credentials,
        })
    }

    /// An AWS instance-role bucket named `bucket`.
    #[must_use]
    pub fn aws(bucket: &str) -> Self {
        Self {
            bucket: bucket.into(),
            ..Self::default()
        }
    }

    /// Whether this bucket uses the AWS regional endpoints with an instance
    /// role. Anything else is an S3-compatible bucket with static keys.
    #[must_use]
    pub fn is_aws(&self) -> bool {
        self.endpoint.is_empty() && self.credentials == BucketCredentials::InstanceRole
    }

    /// Whether nodes need static keys uploaded to reach this bucket.
    #[must_use]
    pub fn needs_static_keys(&self) -> bool {
        matches!(self.credentials, BucketCredentials::StaticKeys { .. })
    }

    /// Fill an unset region from the remote's configured region so saved
    /// records always carry a concrete region.
    pub fn resolve_region(&mut self, fallback: &str) {
        if self.region.is_empty() {
            fallback.clone_into(&mut self.region);
        }
    }

    /// Short description without secrets, for logs and status output. The
    /// region is filled from the remote's configured region when the remote
    /// is saved, so it is always concrete here.
    #[must_use]
    pub fn describe(&self) -> String {
        if self.endpoint.is_empty() {
            format!("{} ({})", self.bucket, self.region)
        } else {
            format!("{} at {} ({})", self.bucket, self.endpoint, self.region)
        }
    }

    /// Copy the bucket coordinates into service settings. Static keys become
    /// the S3 credentials; the instance-role source clears them so nodes fall
    /// back to the instance-metadata provider. The create-only switch travels
    /// with the description, which is what the nodes read.
    pub fn apply_to_settings(&self, settings: &mut Settings) {
        settings.s3.endpoint.clone_from(&self.endpoint);
        if !self.region.is_empty() {
            settings.s3.region.clone_from(&self.region);
        }
        settings.s3.bucket.clone_from(&self.bucket);
        settings.s3.prefix.clone_from(&self.prefix);
        settings.s3.conditional_create = self.conditional_create;
        match &self.credentials {
            BucketCredentials::InstanceRole => {
                settings.s3.access_key.clear();
                settings.s3.secret_key.clear();
            }
            BucketCredentials::StaticKeys {
                access_key,
                secret_key,
            } => {
                settings.s3.access_key.clone_from(access_key);
                settings.s3.secret_key.clone_from(secret_key);
            }
        }
    }

    /// Check the bucket name before creating cloud resources. HTTPS
    /// virtual-hosted requests fail for other names, so reject them early.
    /// # Errors
    /// Rejects names that are not 3-63 character lowercase DNS labels.
    pub fn validate_name(&self) -> Result<(), Error> {
        if !valid_bucket_name(&self.bucket) {
            return Err(Error::Invalid(
                "bucket must be a 3-63 character lowercase DNS name without dots (HTTPS virtual-hosted S3 requires this)",
            ));
        }
        Ok(())
    }
}

/// Marker object recording swarm ownership under a prefix, for providers
/// without bucket-tag support. One implementation serves the bucket
/// description and the provisioning client.
#[must_use]
pub fn ownership_marker_key(prefix: &str) -> String {
    if prefix.is_empty() {
        ".swarmy-owner".into()
    } else {
        format!("{prefix}/.swarmy-owner")
    }
}

/// S3 bucket names are lowercase DNS labels; HTTPS virtual-hosted requests
/// fail otherwise, so reject them before creating cloud resources.
fn valid_bucket_name(bucket: &str) -> bool {
    fn dns(byte: u8) -> bool {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
    }
    (3..=63).contains(&bucket.len())
        && bucket.bytes().all(dns)
        && !bucket.starts_with('-')
        && !bucket.ends_with('-')
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn settings_round_trip_through_bucket_spec() {
        for credentials in [
            BucketCredentials::InstanceRole,
            BucketCredentials::StaticKeys {
                access_key: "key".into(),
                secret_key: "secret".into(),
            },
        ] {
            let spec = BucketSpec {
                bucket: "bucket".into(),
                region: "region".into(),
                credentials,
                ..BucketSpec::default()
            };
            let mut settings = Settings::default();
            spec.apply_to_settings(&mut settings);
            assert_eq!(BucketSpec::from_settings(&settings.s3), Some(spec));
        }
        let mut settings = Settings::default();
        settings.s3.bucket.clear();
        assert_eq!(BucketSpec::from_settings(&settings.s3), None);
    }
}
