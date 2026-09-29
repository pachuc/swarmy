use std::time::{Duration, Instant};

use crate::Result;
use swarmy_config::{RemoteNode, RemotePorts, RemoteSettings};

use super::{Cloud, Host, MachineSpec, ObjectBucket, key_name, state::State, wait_running};

#[derive(Clone, Copy)]
pub(super) struct NewNode<'a> {
    pub name: &'a str,
    pub sandboxes: u32,
}

pub(super) async fn run(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    settings: &RemoteSettings,
    request: NewNode<'_>,
    options: super::services::Options<'_>,
    delay: Duration,
) -> Result<()> {
    let NewNode { name, sandboxes } = request;
    if let Some(existing) = state.read(name)? {
        if settings.bucket.is_some()
            && existing.launch_settings.as_ref().is_some_and(|saved| {
                saved.bucket == settings.bucket && saved.region == settings.region
            })
            && !existing.instance_id.is_empty()
            && !existing.public_ip.is_empty()
        {
            cloud_out!(
                "Remote node {name} already exists; ignored --services, --copy-credential, and image recipe/build options. Use remote down before reprovisioning."
            );
            return Ok(());
        }
        return Err(crate::Error::AlreadyExists(name.to_owned()));
    }
    validate(settings, name)?;
    let started = Instant::now();
    cloud_out!("Resolving Ubuntu image in {}", settings.region);
    let image = match &settings.aws.image {
        Some(image) => image.clone(),
        None => cloud.base_image().await?,
    };
    let mut node = RemoteNode {
        name: name.into(),
        region: settings.region.clone(),
        instance_id: String::new(),
        launch_attempted: false,
        public_ip: String::new(),
        private_ip: String::new(),
        key_path: state
            .directory
            .join(format!("swarmy-{}", ulid::Ulid::generate())),
        ssh_user: "ubuntu".into(),
        ports: RemotePorts::default(),
        nodes: Vec::new(),
        sandboxes,
        default_image: None,
        launch_settings: Some(RemoteSettings {
            aws: swarmy_config::AwsSettings {
                image: Some(image.clone()),
                ..settings.aws.clone()
            },
            ..settings.clone()
        }),
        created_at: jiff::Timestamp::now().to_string(),
    };
    // Write the key name before any AWS mutation so down can recover an interrupted launch.
    state.save(&node)?;
    let result = async {
        if let Some(bucket) = &settings.bucket {
            cloud
                .ensure_bucket(&ObjectBucket {
                    name: bucket.clone(),
                    region: settings.region.clone(),
                    owner: name.into(),
                    endpoint: None,
                    node_credentials: settings.instance_profile(name),
                })
                .await?;
        }
        let address = provision(cloud, host, state, settings, image, &mut node, delay).await?;
        if settings.services == swarmy_config::RemoteServices::Node {
            host.services(&node, &address, &options).await?;
        }
        if let Some(recipe) = options.recipe {
            cloud_out!("Building base-ubuntu:{name} (this takes several minutes)");
            let build_started = Instant::now();
            host.build_image(&node, &address, recipe).await?;
            node.default_image = Some(format!("base-ubuntu:{name}"));
            state.save(&node)?;
            cloud_out!(
                "Image base-ubuntu:{name} built and registered in {:.1}s",
                build_started.elapsed().as_secs_f64()
            );
        } else {
            cloud_out!("Skipping image build (--no-image)");
        }
        Ok::<_, crate::Error>(address)
    }
    .await;
    let address = match result {
        Ok(address) => address,
        Err(error) => {
            cloud_err!("remote up failed; cleanup with swarmy remote down {name}");
            return Err(error);
        }
    };
    cloud_out!(
        "Remote node {name} ready in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    cloud_out!("{}", super::ssh::command_line(&node, &address)?);
    Ok(())
}

async fn provision(
    cloud: &impl Cloud,
    host: &impl Host,
    state: &State,
    settings: &RemoteSettings,
    image: String,
    node: &mut RemoteNode,
    delay: Duration,
) -> Result<String> {
    cloud_out!("Generating and importing ed25519 SSH key");
    let public_key = host.generate_key(node).await?;
    let key_name = key_name(node)?.to_owned();
    cloud
        .import_ssh_key(&key_name, public_key.clone(), &settings.managed_by_tag)
        .await?;
    cloud_out!("Launching {} from {image}", settings.aws.instance_type);
    node.launch_attempted = true;
    state.save(node)?;
    node.instance_id = cloud
        .create(&MachineSpec::from_settings(
            &node.name,
            &image,
            &key_name,
            public_key,
            settings,
            settings.instance_profile(&node.name),
        ))
        .await?;
    state.save(node)?;
    cloud_out!("Waiting for {} to run", node.instance_id);
    let machine = wait_running(cloud, &node.instance_id, delay).await?;
    node.public_ip = machine.public_ip;
    node.private_ip = machine.private_ip;
    state.save(node)?;
    host.provision(node, None).await
}

fn validate(settings: &RemoteSettings, name: &str) -> Result<()> {
    crate::Error::ensure(
        !settings.region.is_empty(),
        "configure remote.region in config.toml before running swarmy remote up",
    )?;
    for (field, value) in [
        ("subnet", &settings.aws.subnet),
        ("security_group", &settings.aws.security_group),
    ] {
        if value.as_deref().is_none_or(str::is_empty) {
            return Err(crate::Error::other(format!(
                "remote.aws.{field} is not configured; set [remote.aws] {field} in config.toml before running swarmy remote up"
            )));
        }
    }
    crate::Error::ensure(
        settings.disk_gb != 0
            && !settings.managed_by_tag.is_empty()
            && !settings.aws.instance_type.is_empty(),
        "remote disk_gb must be positive and aws.instance_type and managed_by_tag must not be empty",
    )?;
    if let Some(bucket) = &settings.bucket {
        crate::Error::ensure(
            name.len() <= 57,
            "bucket-backed remote name must be at most 57 characters to fit the IAM role name",
        )?;
        crate::Error::ensure(
            valid_bucket_name(bucket),
            "remote.bucket must be a 3-63 character lowercase DNS name without dots (HTTPS virtual-hosted S3 requires this)",
        )?;
    }
    Ok(())
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
