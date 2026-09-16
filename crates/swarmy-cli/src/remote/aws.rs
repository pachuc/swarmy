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
        }
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
) -> aws_sdk_ec2::operation::run_instances::RunInstancesInput {
    aws_sdk_ec2::operation::run_instances::RunInstancesInput::builder()
        .image_id(&request.image)
        .instance_type(InstanceType::from(request.settings.instance_type.as_str()))
        .min_count(1)
        .max_count(1)
        .key_name(&request.key_name)
        .client_token(&request.key_name)
        .network_interfaces(
            InstanceNetworkInterfaceSpecification::builder()
                .device_index(0)
                .subnet_id(&request.settings.subnet)
                .groups(&request.settings.security_group)
                .associate_public_ip_address(true)
                .delete_on_termination(true)
                .build(),
        )
        .block_device_mappings(
            BlockDeviceMapping::builder()
                .device_name(root_device)
                .ebs(
                    EbsBlockDevice::builder()
                        .volume_size(request.settings.disk_gb)
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
        .expect("both instance counts are present")
}

impl Cloud for Aws {
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
        let input = launch_input(request, root);
        let output = self
            .ec2
            .run_instances()
            .set_image_id(input.image_id)
            .set_instance_type(input.instance_type)
            .set_min_count(input.min_count)
            .set_max_count(input.max_count)
            .set_key_name(input.key_name)
            .set_client_token(input.client_token)
            .set_network_interfaces(input.network_interfaces)
            .set_block_device_mappings(input.block_device_mappings)
            .set_tag_specifications(input.tag_specifications)
            .send()
            .await?;
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
    fn ec2_request_tags_disk_network_and_key() {
        let request = Launch {
            settings: swarmy_config::RemoteSettings {
                subnet: "subnet-test".into(),
                security_group: "sg-test".into(),
                managed_by_tag: "codex-launcher".into(),
                ..Default::default()
            },
            image: "ami-test".into(),
            name: "test".into(),
            key_name: "unique-key".into(),
        };
        let input = launch_input(&request, "/dev/sda1");
        assert_eq!(input.image_id(), Some("ami-test"));
        assert_eq!(input.key_name(), Some("unique-key"));
        assert_eq!(input.client_token(), Some("unique-key"));
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
