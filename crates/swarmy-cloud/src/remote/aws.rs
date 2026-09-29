use crate::Result;
use aws_sdk_ec2::{
    error::ProvideErrorMetadata,
    primitives::Blob,
    types::{
        BlockDeviceMapping, EbsBlockDevice, Filter, InstanceNetworkInterfaceSpecification,
        InstanceType, ResourceType, Tag, TagSpecification, VolumeType,
    },
};
use std::time::Duration;

use super::{Cloud, Machine, MachineSpec, ObjectBucket, Ownership, retry_profile_propagation};

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
            let code = error.code().map(str::to_owned);
            let message = error.message().map(str::to_owned);
            if access_denied(code.as_deref()) {
                crate::Error::MissingPermission {
                    operation: operation.to_owned(),
                    source: Box::new(error),
                }
            } else {
                crate::Error::Aws {
                    operation: operation.to_owned(),
                    code,
                    message,
                    source: Box::new(error),
                }
            }
        })
    }
}

const UBUNTU_IMAGE: &str =
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id";

const REMOTE_TAG: &str = "swarmy-remote";
const MANAGED_TAG: &str = "managed-by";
const MANAGER: &str = "swarmy";

pub struct Aws {
    ec2: aws_sdk_ec2::Client,
    ssm: aws_sdk_ssm::Client,
    s3: aws_sdk_s3::Client,
    iam: aws_sdk_iam::Client,
}

impl Aws {
    pub async fn new(region: &str) -> Self {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_owned()))
            .load()
            .await;
        Self {
            ec2: aws_sdk_ec2::Client::new(&config),
            ssm: aws_sdk_ssm::Client::new(&config),
            s3: aws_sdk_s3::Client::new(&config),
            iam: aws_sdk_iam::Client::new(&config),
        }
    }

    async fn write_bucket_tags(
        &self,
        name: &str,
        owner: &str,
        existing: Vec<aws_sdk_s3::types::Tag>,
    ) -> Result<()> {
        let tags = merged_bucket_tags(existing, owner);
        self.s3
            .put_bucket_tagging()
            .bucket(name)
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

    async fn delete_role_policies(&self, name: &str) -> Result<()> {
        loop {
            let policies = self
                .iam
                .list_role_policies()
                .role_name(name)
                .send()
                .await
                .aws_context("iam:ListRolePolicies")?;
            if policies.policy_names().is_empty() {
                break;
            }
            for policy in policies.policy_names() {
                self.iam
                    .delete_role_policy()
                    .role_name(name)
                    .policy_name(policy)
                    .send()
                    .await
                    .aws_context("iam:DeleteRolePolicy")?;
            }
        }
        loop {
            let policies = self
                .iam
                .list_attached_role_policies()
                .role_name(name)
                .send()
                .await
                .aws_context("iam:ListAttachedRolePolicies")?;
            if policies.attached_policies().is_empty() {
                break;
            }
            for policy in policies.attached_policies() {
                let Some(arn) = policy.policy_arn() else {
                    return Err(crate::Error::other("attached policy has no ARN"));
                };
                {
                    self.iam
                        .detach_role_policy()
                        .role_name(name)
                        .policy_arn(arn)
                        .send()
                        .await
                        .aws_context("iam:DetachRolePolicy")?;
                }
            }
        }
        Ok(())
    }

    async fn create_bucket(&self, bucket: &str, region: &str) -> Result<()> {
        let mut request = self.s3.create_bucket().bucket(bucket);
        if region != "us-east-1" {
            request = request.create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(region))
                    .build(),
            );
        }
        request
            .send()
            .await
            .aws_context("s3:CreateBucket (bucket may belong to another account)")?;
        Ok(())
    }

    async fn ensure_bucket_exists(&self, bucket: &str, region: &str, owner: &str) -> Result<()> {
        let location = self.s3.get_bucket_location().bucket(bucket).send().await;
        let mut created = false;
        match location {
            Ok(output) => {
                let found = bucket_region(output.location_constraint());
                if found != region {
                    return Err(crate::Error::other(format!(
                        "bucket {bucket} is in {found}, not {region}"
                    )));
                }
            }
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchBucket") =>
            {
                self.create_bucket(bucket, region).await?;
                created = true;
                if let Err(error) = self.write_bucket_tags(bucket, owner, Vec::new()).await {
                    warn_tag_denied(error, "s3:PutBucketTagging", bucket, owner)?;
                }
            }
            Err(error) => {
                return Err(error)
                    .aws_context("s3:GetBucketLocation (bucket may belong to another account)");
            }
        }
        let needs_encryption = if created {
            true
        } else {
            match self.s3.get_bucket_encryption().bucket(bucket).send().await {
                Ok(output) => output
                    .server_side_encryption_configuration()
                    .is_none_or(|configuration| configuration.rules().is_empty()),
                Err(error)
                    if error
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::code)
                        == Some("ServerSideEncryptionConfigurationNotFoundError") =>
                {
                    true
                }
                Err(error) => {
                    return Err(error).aws_context(
                        "s3:GetEncryptionConfiguration (bucket may belong to another account)",
                    );
                }
            }
        };
        if needs_encryption {
            self.s3
                .put_bucket_encryption()
                .bucket(bucket)
                .server_side_encryption_configuration(
                    aws_sdk_s3::types::ServerSideEncryptionConfiguration::builder()
                        .rules(
                            aws_sdk_s3::types::ServerSideEncryptionRule::builder()
                                .apply_server_side_encryption_by_default(
                                    aws_sdk_s3::types::ServerSideEncryptionByDefault::builder()
                                        .sse_algorithm(
                                            aws_sdk_s3::types::ServerSideEncryption::Aes256,
                                        )
                                        .build()?,
                                )
                                .build(),
                        )
                        .build()?,
                )
                .send()
                .await
                .aws_context(
                    "s3:PutEncryptionConfiguration (bucket may belong to another account)",
                )?;
        }
        self.s3
            .put_public_access_block()
            .bucket(bucket)
            .public_access_block_configuration(
                aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
                    .block_public_acls(true)
                    .ignore_public_acls(true)
                    .block_public_policy(true)
                    .restrict_public_buckets(true)
                    .build(),
            )
            .send()
            .await
            .aws_context("s3:PutBucketPublicAccessBlock (bucket may belong to another account)")?;
        Ok(())
    }

    async fn ensure_profile(&self, bucket: &str, role: &str, owner: &str) -> Result<()> {
        let mut created = false;
        let existing = self.iam.get_role().role_name(role).send().await;
        if let Err(error) = existing {
            if error
                .as_service_error()
                .and_then(ProvideErrorMetadata::code)
                != Some("NoSuchEntity")
            {
                return Err(error).aws_context("iam:GetRole");
            }
            let request = self.iam.create_role().role_name(role)
                .assume_role_policy_document(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#);
            if let Err(error) = request
                .clone()
                .tags(iam_tag(MANAGED_TAG, MANAGER))
                .tags(iam_tag(REMOTE_TAG, owner))
                .send()
                .await
            {
                if !access_denied(error.code()) {
                    return Err(error).aws_context("iam:CreateRole");
                }
                // IAM can reject tags on CreateRole separately from role creation.
                request
                    .send()
                    .await
                    .aws_context("iam:CreateRole (without tags)")?;
                warn_untagged("iam:TagRole", role, owner);
            }
            created = true;
        }
        let policy = bucket_policy(bucket);
        self.iam
            .put_role_policy()
            .role_name(role)
            .policy_name("swarmy-bucket")
            .policy_document(policy.to_string())
            .send()
            .await
            .aws_context("iam:PutRolePolicy")?;
        let profile = self
            .iam
            .get_instance_profile()
            .instance_profile_name(role)
            .send()
            .await;
        let has_role = match profile {
            Ok(output) => output
                .instance_profile()
                .is_some_and(|p| p.roles().iter().any(|r| r.role_name() == role)),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchEntity") =>
            {
                let request = self
                    .iam
                    .create_instance_profile()
                    .instance_profile_name(role);
                if let Err(error) = request
                    .clone()
                    .tags(iam_tag(MANAGED_TAG, MANAGER))
                    .tags(iam_tag(REMOTE_TAG, owner))
                    .send()
                    .await
                {
                    if !access_denied(error.code()) {
                        return Err(error).aws_context("iam:CreateInstanceProfile");
                    }
                    request
                        .send()
                        .await
                        .aws_context("iam:CreateInstanceProfile (without tags)")?;
                    warn_untagged("iam:TagInstanceProfile", role, owner);
                }
                created = true;
                false
            }
            Err(error) => return Err(error).aws_context("iam:GetInstanceProfile"),
        };
        if !has_role {
            self.iam
                .add_role_to_instance_profile()
                .instance_profile_name(role)
                .role_name(role)
                .send()
                .await
                .aws_context("iam:AddRoleToInstanceProfile")?;
        }
        if created {
            // A role or profile is visible to EC2 and STS only after IAM has
            // propagated it. Launching sooner can bind the instance to stale
            // identity data whose credentials are then rejected.
            cloud_out!("Waiting for the new IAM role and instance profile to propagate");
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        Ok(())
    }
}

fn access_denied(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("AccessDenied" | "AccessDeniedException" | "UnauthorizedOperation")
    )
}

fn warn_untagged(permission: &str, resource: &str, owner: &str) {
    cloud_err!(
        "Warning: missing {permission} for {resource}; remote down will leave it in place. Grant {permission} and run swarmy remote tag {owner} later."
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

fn iam_tag(key: &str, value: &str) -> aws_sdk_iam::types::Tag {
    aws_sdk_iam::types::Tag::builder()
        .key(key)
        .value(value)
        .build()
        .expect("tag fields")
}

fn s3_tag(key: &str, value: &str) -> aws_sdk_s3::types::Tag {
    aws_sdk_s3::types::Tag::builder()
        .key(key)
        .value(value)
        .build()
        .expect("tag fields")
}

fn merged_bucket_tags(
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

fn ensure_not_another_remote<'a>(
    tags: impl Iterator<Item = (&'a str, &'a str)>,
    owner: &str,
) -> Result<()> {
    if tags
        .into_iter()
        .any(|(key, value)| key == REMOTE_TAG && value != owner)
    {
        return Err(crate::Error::other("resource is tagged for another remote"));
    }
    Ok(())
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

fn tags(resource: ResourceType, name: &str, owner: &str) -> TagSpecification {
    TagSpecification::builder()
        .resource_type(resource)
        .tags(Tag::builder().key("Name").value(name).build())
        .tags(Tag::builder().key(MANAGED_TAG).value(owner).build())
        .build()
}

fn launch_input(
    spec: &MachineSpec,
    root_device: &str,
) -> Result<aws_sdk_ec2::operation::run_instances::RunInstancesInput> {
    let disk_gb = i32::try_from(spec.disk_gb)?;
    Ok(
        aws_sdk_ec2::operation::run_instances::RunInstancesInput::builder()
            .image_id(&spec.image)
            .instance_type(InstanceType::from(spec.instance_type.as_str()))
            .min_count(1)
            .max_count(1)
            .key_name(&spec.key_name)
            .set_iam_instance_profile(spec.profile.as_ref().map(|name| {
                aws_sdk_ec2::types::IamInstanceProfileSpecification::builder()
                    .name(name)
                    .build()
            }))
            .client_token(&spec.key_name)
            .network_interfaces(
                InstanceNetworkInterfaceSpecification::builder()
                    .device_index(0)
                    .set_subnet_id(spec.subnet.clone())
                    .set_groups(spec.security_group.clone().map(|group| vec![group]))
                    .associate_public_ip_address(true)
                    .delete_on_termination(true)
                    .build(),
            )
            .block_device_mappings(
                BlockDeviceMapping::builder()
                    .device_name(root_device)
                    .ebs(
                        EbsBlockDevice::builder()
                            .volume_size(disk_gb)
                            .volume_type(VolumeType::Gp3)
                            .delete_on_termination(true)
                            .encrypted(true)
                            .build(),
                    )
                    .build(),
            )
            .tag_specifications(tags(ResourceType::Instance, &spec.name, &spec.managed_by))
            .tag_specifications(tags(ResourceType::Volume, &spec.name, &spec.managed_by))
            .build()
            .expect("both instance counts are present"),
    )
}

fn bucket_region(location: Option<&aws_sdk_s3::types::BucketLocationConstraint>) -> &str {
    match location.map(aws_sdk_s3::types::BucketLocationConstraint::as_str) {
        None | Some("") => "us-east-1",
        Some("EU") => "eu-west-1",
        Some(region) => region,
    }
}

fn bucket_policy(bucket: &str) -> serde_json::Value {
    serde_json::json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":format!("arn:aws:s3:::{bucket}")},
        {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":format!("arn:aws:s3:::{bucket}/*")}
    ]})
}

impl Cloud for Aws {
    async fn ensure_bucket(&self, bucket: &ObjectBucket) -> Result<()> {
        self.ensure_bucket_exists(&bucket.name, &bucket.region, &bucket.owner)
            .await?;
        let role = bucket
            .node_credentials
            .clone()
            .unwrap_or_else(|| format!("swarmy-{}", bucket.owner));
        self.ensure_profile(&bucket.name, &role, &bucket.owner)
            .await
    }

    async fn base_image(&self) -> Result<String> {
        let output = self
            .ssm
            .get_parameter()
            .name(UBUNTU_IMAGE)
            .send()
            .await
            .aws_context("ssm:GetParameter")?;
        let Some(value) = output.parameter().and_then(|p| p.value()) else {
            return Err(crate::Error::other("Ubuntu SSM parameter has no value"));
        };
        Ok(value.into())
    }

    async fn import_ssh_key(&self, name: &str, public_key: Vec<u8>, owner: &str) -> Result<()> {
        self.ec2
            .import_key_pair()
            .key_name(name)
            .public_key_material(Blob::new(public_key))
            .tag_specifications(tags(ResourceType::KeyPair, name, owner))
            .send()
            .await
            .aws_context("ec2:ImportKeyPair")?;
        Ok(())
    }

    async fn create(&self, spec: &MachineSpec) -> Result<String> {
        let images = self
            .ec2
            .describe_images()
            .image_ids(&spec.image)
            .send()
            .await
            .aws_context("ec2:DescribeImages")?;
        let Some(root) = images
            .images()
            .first()
            .and_then(|image| image.root_device_name())
        else {
            return Err(crate::Error::other("AMI has no root device"));
        };
        let output = retry_profile_propagation(
            || async {
                let input = launch_input(spec, root)?;
                self.ec2
                    .run_instances()
                    .set_image_id(input.image_id)
                    .set_instance_type(input.instance_type)
                    .set_min_count(input.min_count)
                    .set_max_count(input.max_count)
                    .set_key_name(input.key_name)
                    .set_iam_instance_profile(input.iam_instance_profile)
                    .set_client_token(input.client_token)
                    .set_network_interfaces(input.network_interfaces)
                    .set_block_device_mappings(input.block_device_mappings)
                    .set_tag_specifications(input.tag_specifications)
                    .send()
                    .await
                    .aws_context("ec2:RunInstances")
            },
            spec.profile.is_some(),
            Duration::from_secs(2),
        )
        .await?;
        let Some(id) = output.instances().first().and_then(|i| i.instance_id()) else {
            return Err(crate::Error::other("EC2 returned no instance id"));
        };
        Ok(id.into())
    }

    async fn get(&self, id: &str) -> Result<Option<Machine>> {
        let output = match self.ec2.describe_instances().instance_ids(id).send().await {
            Ok(output) => output,
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("InvalidInstanceID.NotFound") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error).aws_context("ec2:DescribeInstances"),
        };
        Ok(output
            .reservations()
            .iter()
            .flat_map(aws_sdk_ec2::types::Reservation::instances)
            .next()
            .map(|i| Machine {
                id: i.instance_id().unwrap_or_default().into(),
                state: i
                    .state()
                    .and_then(|s| s.name())
                    .map_or("unknown", aws_sdk_ec2::types::InstanceStateName::as_str)
                    .into(),
                public_ip: i.public_ip_address().unwrap_or_default().into(),
                private_ip: i.private_ip_address().unwrap_or_default().into(),
            }))
    }

    async fn find_by_tag(&self, token: &str) -> Result<Option<String>> {
        let output = self
            .ec2
            .describe_instances()
            .filters(Filter::builder().name("client-token").values(token).build())
            .send()
            .await
            .aws_context("ec2:DescribeInstances")?;
        Ok(output
            .reservations()
            .iter()
            .flat_map(aws_sdk_ec2::types::Reservation::instances)
            .find_map(|i| i.instance_id().map(str::to_owned)))
    }

    async fn destroy(&self, id: &str) -> Result<()> {
        // Tag-restricted policies cannot authorize mutations of missing resources.
        if self
            .get(id)
            .await?
            .is_none_or(|machine| machine.state == "terminated")
        {
            return Ok(());
        }
        match self.ec2.terminate_instances().instance_ids(id).send().await {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("InvalidInstanceID.NotFound") =>
            {
                Ok(())
            }
            Err(error) => Err(error).aws_context("ec2:TerminateInstances"),
        }
    }

    async fn bucket_ownership(&self, name: &str, owner: &str) -> Result<Ownership> {
        let output = match self.s3.get_bucket_tagging().bucket(name).send().await {
            Ok(output) => output,
            Err(error) => {
                return match error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                {
                    Some("NoSuchBucket") => Ok(Ownership::Absent),
                    Some("NoSuchTagSet") => Ok(Ownership::Unmanaged),
                    _ => Err(error).aws_context("s3:GetBucketTagging"),
                };
            }
        };
        Ok(owned(
            output.tag_set().iter().map(|tag| (tag.key(), tag.value())),
            owner,
        ))
    }

    async fn role_ownership(&self, name: &str, owner: &str) -> Result<(Ownership, Ownership)> {
        let profile = match self
            .iam
            .get_instance_profile()
            .instance_profile_name(name)
            .send()
            .await
        {
            Ok(output) => output
                .instance_profile
                .map_or(Ownership::Absent, |profile| {
                    owned(
                        profile.tags().iter().map(|tag| (tag.key(), tag.value())),
                        owner,
                    )
                }),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchEntity") =>
            {
                Ownership::Absent
            }
            Err(error) => return Err(error).aws_context("iam:GetInstanceProfile"),
        };
        let role = match self.iam.get_role().role_name(name).send().await {
            Ok(output) => output.role.map_or(Ownership::Absent, |role| {
                owned(
                    role.tags().iter().map(|tag| (tag.key(), tag.value())),
                    owner,
                )
            }),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchEntity") =>
            {
                Ownership::Absent
            }
            Err(error) => return Err(error).aws_context("iam:GetRole"),
        };
        Ok((profile, role))
    }

    async fn tag_bucket(&self, name: &str, owner: &str) -> Result<()> {
        let tags = match self.s3.get_bucket_tagging().bucket(name).send().await {
            Ok(output) => output.tag_set().to_vec(),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchTagSet") =>
            {
                Vec::new()
            }
            Err(error) => return Err(error).aws_context("s3:GetBucketTagging"),
        };
        ensure_not_another_remote(tags.iter().map(|tag| (tag.key(), tag.value())), owner)?;
        self.write_bucket_tags(name, owner, tags).await
    }

    async fn tag_node_role(&self, name: &str, owner: &str) -> Result<()> {
        let role = self
            .iam
            .get_role()
            .role_name(name)
            .send()
            .await
            .aws_context("iam:GetRole")?
            .role
            .ok_or_else(|| crate::Error::other("role is absent"))?;
        let profile = self
            .iam
            .get_instance_profile()
            .instance_profile_name(name)
            .send()
            .await
            .aws_context("iam:GetInstanceProfile")?
            .instance_profile
            .ok_or_else(|| crate::Error::other("instance profile is absent"))?;
        // Check both before changing either one, so a conflicting profile cannot leave a tagged role.
        ensure_not_another_remote(
            role.tags().iter().map(|tag| (tag.key(), tag.value())),
            owner,
        )?;
        ensure_not_another_remote(
            profile.tags().iter().map(|tag| (tag.key(), tag.value())),
            owner,
        )?;
        self.iam
            .tag_role()
            .role_name(name)
            .tags(iam_tag(MANAGED_TAG, MANAGER))
            .tags(iam_tag(REMOTE_TAG, owner))
            .send()
            .await
            .aws_context("iam:TagRole")?;
        self.iam
            .tag_instance_profile()
            .instance_profile_name(name)
            .tags(iam_tag(MANAGED_TAG, MANAGER))
            .tags(iam_tag(REMOTE_TAG, owner))
            .send()
            .await
            .aws_context("iam:TagInstanceProfile")?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &str, owner: &str) -> Result<bool> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};
        match self.bucket_ownership(name, owner).await? {
            Ownership::Absent => return Ok(false),
            Ownership::Unmanaged => {
                return Err(crate::Error::other("bucket ownership tags do not match"));
            }
            Ownership::Owned => {}
        }
        let mut count = 0usize;
        // Re-read the first page after every deletion. Markers would skip keys
        // when the page just deleted changes the listing beneath the cursor.
        loop {
            let page = match self
                .s3
                .list_object_versions()
                .bucket(name)
                .max_keys(1000)
                .send()
                .await
            {
                Ok(page) => page,
                Err(error)
                    if error
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::code)
                        == Some("NoSuchBucket") =>
                {
                    return Ok(false);
                }
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
            let result = self
                .s3
                .delete_objects()
                .bucket(name)
                .delete(Delete::builder().set_objects(Some(objects)).build()?)
                .send()
                .await
                .aws_context("s3:DeleteObjects")?;
            if !result.errors().is_empty() {
                return Err(crate::Error::other(format!(
                    "s3:DeleteObjects failed for {} objects",
                    result.errors().len()
                )));
            }
            count += size;
            if count / 10_000 != (count - size) / 10_000 {
                cloud_out!("Deleted {count} objects from bucket {name}");
            }
        }
        match self.s3.delete_bucket().bucket(name).send().await {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchBucket") =>
            {
                Ok(false)
            }
            Err(error) => Err(error).aws_context("s3:DeleteBucket"),
        }
    }

    async fn delete_node_role(&self, name: &str, owner: &str) -> Result<(bool, bool)> {
        let (profile_status, role_status) = self.role_ownership(name, owner).await?;
        if profile_status == Ownership::Unmanaged || role_status == Ownership::Unmanaged {
            return Err(crate::Error::other("IAM ownership tags do not match"));
        }
        let profile = match self
            .iam
            .get_instance_profile()
            .instance_profile_name(name)
            .send()
            .await
        {
            Ok(output) => output.instance_profile,
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchEntity") =>
            {
                None
            }
            Err(error) => return Err(error).aws_context("iam:GetInstanceProfile"),
        };
        let profile_removed = profile.is_some();
        if let Some(profile) = profile {
            for role in profile.roles() {
                match self
                    .iam
                    .remove_role_from_instance_profile()
                    .instance_profile_name(name)
                    .role_name(role.role_name())
                    .send()
                    .await
                {
                    Ok(_) => {}
                    Err(error)
                        if error
                            .as_service_error()
                            .and_then(ProvideErrorMetadata::code)
                            == Some("NoSuchEntity") => {}
                    Err(error) => {
                        return Err(error).aws_context("iam:RemoveRoleFromInstanceProfile");
                    }
                }
            }
            match self
                .iam
                .delete_instance_profile()
                .instance_profile_name(name)
                .send()
                .await
            {
                Ok(_) => {}
                Err(error)
                    if error
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::code)
                        == Some("NoSuchEntity") => {}
                Err(error) => return Err(error).aws_context("iam:DeleteInstanceProfile"),
            }
        }
        let role = match self.iam.get_role().role_name(name).send().await {
            Ok(output) => output.role,
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchEntity") =>
            {
                None
            }
            Err(error) => return Err(error).aws_context("iam:GetRole"),
        };
        if role.is_some() {
            self.delete_role_policies(name).await?;
            match self.iam.delete_role().role_name(name).send().await {
                Ok(_) => {}
                Err(error)
                    if error
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::code)
                        == Some("NoSuchEntity") => {}
                Err(error) => return Err(error).aws_context("iam:DeleteRole"),
            }
        }
        Ok((profile_removed, role.is_some()))
    }

    async fn delete_ssh_key(&self, name: &str) -> Result<()> {
        let keys = self
            .ec2
            .describe_key_pairs()
            .filters(Filter::builder().name("key-name").values(name).build())
            .send()
            .await
            .aws_context("ec2:DescribeKeyPairs")?;
        if keys.key_pairs().is_empty() {
            return Ok(());
        }
        match self.ec2.delete_key_pair().key_name(name).send().await {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("InvalidKeyPair.NotFound") =>
            {
                Ok(())
            }
            Err(error) => Err(error).aws_context("ec2:DeleteKeyPair"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_location_normalizes_legacy_and_empty_values() {
        use aws_sdk_s3::types::BucketLocationConstraint as Location;
        assert_eq!(bucket_region(None), "us-east-1");
        assert_eq!(bucket_region(Some(&Location::from(""))), "us-east-1");
        assert_eq!(bucket_region(Some(&Location::from("EU"))), "eu-west-1");
        assert_eq!(
            bucket_region(Some(&Location::from("ap-south-1"))),
            "ap-south-1"
        );
    }

    #[test]
    fn bucket_policy_restricts_actions_to_one_bucket() {
        let policy = bucket_policy("only-this-bucket");
        let statements = policy["Statement"].as_array().unwrap();
        assert_eq!(statements[0]["Resource"], "arn:aws:s3:::only-this-bucket");
        assert_eq!(statements[1]["Resource"], "arn:aws:s3:::only-this-bucket/*");
        assert_eq!(
            statements[0]["Action"],
            serde_json::json!(["s3:ListBucket", "s3:GetBucketLocation"])
        );
        assert_eq!(
            statements[1]["Action"],
            serde_json::json!(["s3:GetObject", "s3:PutObject", "s3:DeleteObject"])
        );
    }

    #[test]
    fn ec2_request_tags_disk_network_and_key() {
        let request = MachineSpec {
            name: "test".into(),
            image: "ami-test".into(),
            key_name: "unique-key".into(),
            ssh_public_key: b"ssh-ed25519 test".to_vec(),
            cpus: 4,
            memory_mib: 16 * 1024,
            disk_gb: 100,
            instance_type: "m6id.xlarge".into(),
            subnet: Some("subnet-test".into()),
            security_group: Some("sg-test".into()),
            managed_by: "codex-launcher".into(),
            profile: None,
            bootstrap: None,
        };
        let input = launch_input(&request, "/dev/sda1").unwrap();
        assert_eq!(input.image_id(), Some("ami-test"));
        assert_eq!(input.key_name(), Some("unique-key"));
        assert_eq!(input.client_token(), Some("unique-key"));
        assert!(input.iam_instance_profile().is_none());
        let with_profile = launch_input(
            &MachineSpec {
                profile: Some("swarmy-test".into()),
                ..request
            },
            "/dev/sda1",
        )
        .unwrap();
        assert_eq!(
            with_profile
                .iam_instance_profile()
                .and_then(aws_sdk_ec2::types::IamInstanceProfileSpecification::name),
            Some("swarmy-test")
        );
        assert_eq!(input.instance_type(), Some(&InstanceType::M6idXlarge));
        assert_eq!((input.min_count(), input.max_count()), (Some(1), Some(1)));
        let network = &input.network_interfaces()[0];
        assert_eq!(network.subnet_id(), Some("subnet-test"));
        assert_eq!(network.groups(), &["sg-test"]);
        assert_eq!(network.associate_public_ip_address(), Some(true));
        let disk = input.block_device_mappings()[0].ebs().unwrap();
        assert_eq!(disk.volume_size(), Some(100));
        assert_eq!(disk.volume_type(), Some(&VolumeType::Gp3));
        assert_eq!(disk.delete_on_termination(), Some(true));
        assert_eq!(disk.encrypted(), Some(true));
        assert_eq!(
            input.block_device_mappings()[0].device_name(),
            Some("/dev/sda1")
        );
        let mut specifications = input.tag_specifications().to_vec();
        specifications.push(tags(ResourceType::KeyPair, "test", "codex-launcher"));
        for (spec, resource) in specifications.iter().zip([
            ResourceType::Instance,
            ResourceType::Volume,
            ResourceType::KeyPair,
        ]) {
            assert_eq!(spec.resource_type(), Some(&resource));
            assert!(spec.tags().iter().any(
                |tag| tag.key() == Some("managed-by") && tag.value() == Some("codex-launcher")
            ));
            assert!(
                spec.tags()
                    .iter()
                    .any(|tag| tag.key() == Some("Name") && tag.value() == Some("test"))
            );
        }
    }
}

#[cfg(test)]
mod ownership_tag_tests {
    use super::{ensure_not_another_remote, merged_bucket_tags, s3_tag};

    #[test]
    fn adoption_preserves_unrelated_bucket_tags() {
        let tags = merged_bucket_tags(
            vec![s3_tag("purpose", "backup"), s3_tag("managed-by", "legacy")],
            "mine",
        );
        assert_eq!(tags.len(), 3);
        assert!(
            tags.iter()
                .any(|tag| tag.key() == "purpose" && tag.value() == "backup")
        );
        assert!(
            tags.iter()
                .any(|tag| tag.key() == "swarmy-remote" && tag.value() == "mine")
        );
    }

    #[test]
    fn adoption_rejects_foreign_owner_even_without_managed_by() {
        let tags = [s3_tag("swarmy-remote", "other")];
        assert!(
            ensure_not_another_remote(tags.iter().map(|t| (t.key(), t.value())), "mine").is_err()
        );
        assert!(
            ensure_not_another_remote(tags.iter().map(|t| (t.key(), t.value())), "other").is_ok()
        );
    }
}

#[cfg(test)]
mod tag_denial_tests {
    use super::warn_tag_denied;

    #[test]
    fn denied_tagging_does_not_abort_creation_but_other_errors_do() {
        assert!(
            warn_tag_denied(
                crate::Error::MissingPermission {
                    operation: "s3:PutBucketTagging".into(),
                    source: Box::new(std::io::Error::other("AccessDenied"))
                },
                "s3:PutBucketTagging",
                "swarmy-NAME",
                "NAME"
            )
            .is_ok()
        );
        assert!(
            warn_tag_denied(
                crate::Error::other("s3:PutBucketTagging: network failure"),
                "s3:PutBucketTagging",
                "swarmy-NAME",
                "NAME"
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod aws_context_tests {
    use super::AwsContext;

    fn metadata(code: &str, message: &str) -> aws_sdk_ec2::error::ErrorMetadata {
        aws_sdk_ec2::error::ErrorMetadata::builder()
            .code(code)
            .message(message)
            .build()
    }

    fn mapped(operation: &'static str, code: &str, message: &str) -> crate::Error {
        Err::<(), _>(metadata(code, message))
            .aws_context(operation)
            .unwrap_err()
    }

    #[test]
    fn denial_codes_become_missing_permission() {
        for code in [
            "AccessDenied",
            "AccessDeniedException",
            "UnauthorizedOperation",
        ] {
            match mapped("ec2:RunInstances", code, "not authorized") {
                crate::Error::MissingPermission { operation, .. } => {
                    assert_eq!(operation, "ec2:RunInstances");
                }
                error => panic!("expected MissingPermission for {code}, got {error:?}"),
            }
        }
    }

    #[test]
    fn other_errors_carry_code_and_message_from_metadata() {
        match mapped(
            "ec2:RunInstances",
            "InvalidParameterValue",
            "Invalid IAM Instance Profile name: fixture-profile",
        ) {
            crate::Error::Aws {
                operation,
                code,
                message,
                ..
            } => {
                assert_eq!(operation, "ec2:RunInstances");
                assert_eq!(code.as_deref(), Some("InvalidParameterValue"));
                assert!(
                    message
                        .as_deref()
                        .unwrap_or_default()
                        .contains("Invalid IAM Instance Profile")
                );
            }
            error => panic!("expected Aws, got {error:?}"),
        }
    }

    #[test]
    fn profile_retry_needs_the_instance_profile_message() {
        assert!(super::super::profile_not_propagated(&mapped(
            "ec2:RunInstances",
            "InvalidParameterValue",
            "Invalid IAM Instance Profile name: fixture-profile",
        )));
        // Same code without the profile text is a real misconfiguration.
        assert!(!super::super::profile_not_propagated(&mapped(
            "ec2:RunInstances",
            "InvalidParameterValue",
            "value is not valid",
        )));
        // Denials map to MissingPermission, never to the retryable variant.
        assert!(!super::super::profile_not_propagated(&mapped(
            "ec2:RunInstances",
            "AccessDenied",
            "Invalid IAM Instance Profile name: fixture-profile",
        )));
        // Other operations never retry even with the profile text.
        assert!(!super::super::profile_not_propagated(&mapped(
            "ec2:DescribeInstances",
            "InvalidParameterValue",
            "Invalid IAM Instance Profile name: fixture-profile",
        )));
    }
}
