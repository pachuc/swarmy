use anyhow::{Context, Result};
use aws_sdk_ec2::{
    error::ProvideErrorMetadata,
    primitives::Blob,
    types::{
        BlockDeviceMapping, EbsBlockDevice, Filter, InstanceNetworkInterfaceSpecification,
        InstanceType, ResourceType, Tag, TagSpecification, VolumeType,
    },
};

use super::{Cloud, Instance, Launch};

const UBUNTU_IMAGE: &str =
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id";

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

    async fn ensure_bucket(&self, bucket: &str, region: &str) -> Result<()> {
        let location = self.s3.get_bucket_location().bucket(bucket).send().await;
        match location {
            Ok(output) => {
                let found = output
                    .location_constraint()
                    .map_or("us-east-1", |v| v.as_str());
                anyhow::ensure!(
                    found == region,
                    "bucket {bucket} is in {found}, not {region}"
                );
            }
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchBucket") =>
            {
                let mut request = self.s3.create_bucket().bucket(bucket);
                if region != "us-east-1" {
                    request = request.create_bucket_configuration(
                        aws_sdk_s3::types::CreateBucketConfiguration::builder()
                            .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                                region,
                            ))
                            .build(),
                    );
                }
                request
                    .send()
                    .await
                    .context("s3:CreateBucket (bucket may belong to another account)")?;
            }
            Err(error) => {
                return Err(error)
                    .context("s3:GetBucketLocation (bucket may belong to another account)");
            }
        }
        self.s3
            .put_bucket_encryption()
            .bucket(bucket)
            .server_side_encryption_configuration(
                aws_sdk_s3::types::ServerSideEncryptionConfiguration::builder()
                    .rules(
                        aws_sdk_s3::types::ServerSideEncryptionRule::builder()
                            .apply_server_side_encryption_by_default(
                                aws_sdk_s3::types::ServerSideEncryptionByDefault::builder()
                                    .sse_algorithm(aws_sdk_s3::types::ServerSideEncryption::Aes256)
                                    .build()?,
                            )
                            .build(),
                    )
                    .build()?,
            )
            .send()
            .await
            .context("s3:PutBucketEncryption (bucket may belong to another account)")?;
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
            .context("s3:PutBucketPublicAccessBlock (bucket may belong to another account)")?;
        Ok(())
    }

    async fn ensure_profile(&self, bucket: &str, name: &str) -> Result<()> {
        let role = format!("swarmy-{name}");
        let existing = self.iam.get_role().role_name(&role).send().await;
        if let Err(error) = existing {
            if error
                .as_service_error()
                .and_then(ProvideErrorMetadata::code)
                != Some("NoSuchEntity")
            {
                return Err(error).context("iam:GetRole");
            }
            self.iam.create_role().role_name(&role)
                .assume_role_policy_document(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#)
                .send().await.context("iam:CreateRole")?;
        }
        let policy = bucket_policy(bucket);
        self.iam
            .put_role_policy()
            .role_name(&role)
            .policy_name("swarmy-bucket")
            .policy_document(policy.to_string())
            .send()
            .await
            .context("iam:PutRolePolicy")?;
        let profile = self
            .iam
            .get_instance_profile()
            .instance_profile_name(&role)
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
                self.iam
                    .create_instance_profile()
                    .instance_profile_name(&role)
                    .send()
                    .await
                    .context("iam:CreateInstanceProfile")?;
                false
            }
            Err(error) => return Err(error).context("iam:GetInstanceProfile"),
        };
        if !has_role {
            self.iam
                .add_role_to_instance_profile()
                .instance_profile_name(&role)
                .role_name(&role)
                .send()
                .await
                .context("iam:AddRoleToInstanceProfile")?;
        }
        Ok(())
    }
}

fn tags(resource: ResourceType, name: &str, owner: &str) -> TagSpecification {
    TagSpecification::builder()
        .resource_type(resource)
        .tags(Tag::builder().key("Name").value(name).build())
        .tags(Tag::builder().key("managed-by").value(owner).build())
        .build()
}

fn launch_input(
    request: &Launch,
    root_device: &str,
) -> Result<aws_sdk_ec2::operation::run_instances::RunInstancesInput> {
    let disk_gb = i32::try_from(request.settings.disk_gb).context("remote.disk_gb is too large")?;
    Ok(
        aws_sdk_ec2::operation::run_instances::RunInstancesInput::builder()
            .image_id(&request.image)
            .instance_type(InstanceType::from(request.settings.instance_type.as_str()))
            .min_count(1)
            .max_count(1)
            .key_name(&request.key_name)
            .set_iam_instance_profile(request.profile.as_ref().map(|name| {
                aws_sdk_ec2::types::IamInstanceProfileSpecification::builder()
                    .name(name)
                    .build()
            }))
            .client_token(&request.key_name)
            .network_interfaces(
                InstanceNetworkInterfaceSpecification::builder()
                    .device_index(0)
                    .set_subnet_id(request.settings.subnet.clone())
                    .set_groups(
                        request
                            .settings
                            .security_group
                            .clone()
                            .map(|group| vec![group]),
                    )
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
            .tag_specifications(tags(
                ResourceType::Instance,
                &request.name,
                &request.settings.managed_by_tag,
            ))
            .tag_specifications(tags(
                ResourceType::Volume,
                &request.name,
                &request.settings.managed_by_tag,
            ))
            .build()
            .expect("both instance counts are present"),
    )
}

fn bucket_policy(bucket: &str) -> serde_json::Value {
    serde_json::json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":format!("arn:aws:s3:::{bucket}")},
        {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":format!("arn:aws:s3:::{bucket}/*")}
    ]})
}

impl Cloud for Aws {
    async fn prepare_bucket(&self, bucket: &str, region: &str, name: &str) -> Result<()> {
        self.ensure_bucket(bucket, region).await?;
        self.ensure_profile(bucket, name).await
    }

    async fn delete_profile(&self, name: &str) -> Result<()> {
        let profile = self
            .iam
            .get_instance_profile()
            .instance_profile_name(name)
            .send()
            .await;
        match profile {
            Ok(output) => {
                if output
                    .instance_profile()
                    .is_some_and(|p| p.roles().iter().any(|r| r.role_name() == name))
                {
                    self.iam
                        .remove_role_from_instance_profile()
                        .instance_profile_name(name)
                        .role_name(name)
                        .send()
                        .await
                        .context("iam:RemoveRoleFromInstanceProfile")?;
                }
                self.iam
                    .delete_instance_profile()
                    .instance_profile_name(name)
                    .send()
                    .await
                    .context("iam:DeleteInstanceProfile")?;
            }
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchEntity") => {}
            Err(error) => return Err(error).context("iam:GetInstanceProfile"),
        }
        let role = self.iam.get_role().role_name(name).send().await;
        match role {
            Ok(_) => {
                match self
                    .iam
                    .delete_role_policy()
                    .role_name(name)
                    .policy_name("swarmy-bucket")
                    .send()
                    .await
                {
                    Ok(_) => {}
                    Err(error)
                        if error
                            .as_service_error()
                            .and_then(ProvideErrorMetadata::code)
                            == Some("NoSuchEntity") => {}
                    Err(error) => return Err(error).context("iam:DeleteRolePolicy"),
                }
                self.iam
                    .delete_role()
                    .role_name(name)
                    .send()
                    .await
                    .context("iam:DeleteRole")?;
            }
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchEntity") => {}
            Err(error) => return Err(error).context("iam:GetRole"),
        }
        Ok(())
    }

    async fn stock_image(&self) -> Result<String> {
        let output = self.ssm.get_parameter().name(UBUNTU_IMAGE).send().await?;
        Ok(output
            .parameter()
            .and_then(|p| p.value())
            .context("Ubuntu SSM parameter has no value")?
            .into())
    }

    async fn import_key(&self, name: &str, public_key: Vec<u8>, owner: &str) -> Result<()> {
        self.ec2
            .import_key_pair()
            .key_name(name)
            .public_key_material(Blob::new(public_key))
            .tag_specifications(tags(ResourceType::KeyPair, name, owner))
            .send()
            .await?;
        Ok(())
    }

    async fn launch(&self, request: &Launch) -> Result<String> {
        let images = self
            .ec2
            .describe_images()
            .image_ids(&request.image)
            .send()
            .await?;
        let root = images
            .images()
            .first()
            .and_then(|image| image.root_device_name())
            .context("AMI has no root device")?;
        let input = launch_input(request, root)?;
        let output = self
            .ec2
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
            .context("ec2:RunInstances and iam:PassRole (when using a bucket profile)")?;
        Ok(output
            .instances()
            .first()
            .and_then(|i| i.instance_id())
            .context("EC2 returned no instance id")?
            .into())
    }

    async fn instance(&self, id: &str) -> Result<Option<Instance>> {
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
            Err(error) => return Err(error.into()),
        };
        Ok(output
            .reservations()
            .iter()
            .flat_map(aws_sdk_ec2::types::Reservation::instances)
            .next()
            .map(|i| Instance {
                id: i.instance_id().unwrap_or_default().into(),
                status: i
                    .state()
                    .and_then(|s| s.name())
                    .map_or("unknown", aws_sdk_ec2::types::InstanceStateName::as_str)
                    .into(),
                public_ip: i.public_ip_address().unwrap_or_default().into(),
                private_ip: i.private_ip_address().unwrap_or_default().into(),
            }))
    }

    async fn find_launch(&self, token: &str) -> Result<Option<String>> {
        let output = self
            .ec2
            .describe_instances()
            .filters(Filter::builder().name("client-token").values(token).build())
            .send()
            .await?;
        Ok(output
            .reservations()
            .iter()
            .flat_map(aws_sdk_ec2::types::Reservation::instances)
            .find_map(|i| i.instance_id().map(str::to_owned)))
    }

    async fn terminate(&self, id: &str) -> Result<()> {
        // Tag-restricted policies cannot authorize mutations of missing resources.
        if self
            .instance(id)
            .await?
            .is_none_or(|instance| instance.status == "terminated")
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
            Err(error) => Err(error.into()),
        }
    }

    async fn delete_key(&self, name: &str) -> Result<()> {
        let keys = self
            .ec2
            .describe_key_pairs()
            .filters(Filter::builder().name("key-name").values(name).build())
            .send()
            .await?;
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
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let request = Launch {
            settings: swarmy_config::RemoteSettings {
                subnet: Some("subnet-test".into()),
                security_group: Some("sg-test".into()),
                managed_by_tag: "codex-launcher".into(),
                ..Default::default()
            },
            image: "ami-test".into(),
            name: "test".into(),
            key_name: "unique-key".into(),
            profile: None,
        };
        let input = launch_input(&request, "/dev/sda1").unwrap();
        assert_eq!(input.image_id(), Some("ami-test"));
        assert_eq!(input.key_name(), Some("unique-key"));
        assert_eq!(input.client_token(), Some("unique-key"));
        assert!(input.iam_instance_profile().is_none());
        let with_profile = launch_input(
            &Launch {
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
