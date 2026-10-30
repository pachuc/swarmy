//! Apply the host's JSON CDI spec to GPU sandbox runc configs.
use crate::{Error, Result};
use serde::Deserialize;
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::Path,
    sync::Arc,
};
use swarmy_core::GpuRequirement;

const CDI_KIND: &str = "nvidia.com/gpu";
const CDI_DEVICE: &str = "all";
const DRIVER_CAPABILITIES: &str = "graphics,display,compute,utility";
const GPU_EXTRA_CAPABILITIES: &[&str] = &[];
const HOOK_NAMES: [&str; 6] = [
    "prestart",
    "createRuntime",
    "createContainer",
    "startContainer",
    "poststart",
    "poststop",
];

/// Devices available to a sandbox on this node.
#[derive(Clone, Debug, Default)]
pub enum NodeGpu {
    /// No usable GPU specification.
    #[default]
    Absent,
    /// Advertised cards without real devices.
    Fake,
    /// Resolved host CDI edits.
    Cdi(Arc<GpuEdits>),
}

/// Resolved edits for the spec and its `all` device.
#[derive(Debug, PartialEq, Eq)]
pub struct GpuEdits {
    env: Vec<String>,
    devices: Vec<Device>,
    mounts: Vec<SpecMount>,
    hooks: Vec<SpecHook>,
    additional_gids: Vec<u32>,
}
#[derive(Debug, PartialEq, Eq)]
struct Device {
    path: String,
    kind: String,
    major: u64,
    minor: u64,
    file_mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    access: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HostDevice {
    kind: char,
    major: u64,
    minor: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    kind: String,
    #[serde(default)]
    devices: Vec<SpecDevice>,
    #[serde(default)]
    container_edits: ContainerEdits,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpecDevice {
    name: String,
    #[serde(default)]
    container_edits: ContainerEdits,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContainerEdits {
    #[serde(default)]
    env: Vec<String>,
    #[serde(default)]
    device_nodes: Vec<SpecDeviceNode>,
    #[serde(default)]
    mounts: Vec<SpecMount>,
    #[serde(default)]
    hooks: Vec<SpecHook>,
    #[serde(default)]
    additional_gids: Vec<u32>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpecDeviceNode {
    path: String,
    host_path: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    major: Option<u64>,
    minor: Option<u64>,
    file_mode: Option<u32>,
    permissions: Option<String>,
    uid: Option<u32>,
    gid: Option<u32>,
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpecMount {
    host_path: String,
    container_path: String,
    #[serde(default)]
    options: Vec<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpecHook {
    hook_name: String,
    path: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: Vec<String>,
    timeout: Option<i64>,
}

impl GpuEdits {
    /// Read the JSON CDI spec and resolve host device numbers.
    /// # Errors
    /// Returns an error for unreadable or invalid specs and missing device nodes.
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_with(path, &host_device)
    }
    fn load_with(
        path: &Path,
        stat: &impl Fn(&Path) -> std::io::Result<HostDevice>,
    ) -> Result<Self> {
        if path.extension().is_none_or(|extension| extension != "json") {
            return Err(Error::Operation(format!(
                "CDI spec {} must be a .json file; write it with nvidia-ctk cdi generate --format=json",
                path.display()
            )));
        }
        let text = std::fs::read(path).map_err(|error| {
            Error::Operation(format!("read CDI spec {}: {error}", path.display()))
        })?;
        Self::parse(&text, stat)
            .map_err(|error| Error::Operation(format!("CDI spec {}: {error}", path.display())))
    }
    fn parse(
        text: &[u8],
        stat: &impl Fn(&Path) -> std::io::Result<HostDevice>,
    ) -> std::result::Result<Self, String> {
        let spec: Spec = match serde_json::from_slice(text) {
            Ok(spec) => spec,
            Err(error) => return Err(error.to_string()),
        };
        if spec.kind != CDI_KIND {
            return Err(format!("kind is {:?}, expected {CDI_KIND:?}", spec.kind));
        }
        let names: Vec<String> = spec
            .devices
            .iter()
            .map(|device| device.name.clone())
            .collect();
        let device = spec
            .devices
            .into_iter()
            .find(|device| device.name == CDI_DEVICE)
            .ok_or_else(|| format!("no device named {CDI_DEVICE:?}; the spec has {names:?}"))?;
        let mut edits = Self {
            env: Vec::new(),
            devices: Vec::new(),
            mounts: Vec::new(),
            hooks: Vec::new(),
            additional_gids: Vec::new(),
        };
        edits.add(spec.container_edits, stat)?;
        edits.add(device.container_edits, stat)?;
        edits
            .mounts
            .sort_by_key(|mount| Path::new(&mount.container_path).components().count());
        Ok(edits)
    }
    fn add(
        &mut self,
        part: ContainerEdits,
        stat: &impl Fn(&Path) -> std::io::Result<HostDevice>,
    ) -> std::result::Result<(), String> {
        for variable in part.env {
            if variable
                .split_once('=')
                .is_none_or(|(key, _)| key.is_empty())
            {
                return Err(format!("environment entry {variable:?} is not KEY=VALUE"));
            }
            self.env.push(variable);
        }
        for node in part.device_nodes {
            let device = resolve(node, stat)?;
            self.devices.retain(|existing| existing.path != device.path);
            self.devices.push(device);
        }
        for mount in part.mounts {
            if !Path::new(&mount.container_path).is_absolute() {
                return Err(format!(
                    "mount destination {:?} is not absolute",
                    mount.container_path
                ));
            }
            self.mounts.push(mount);
        }
        for hook in part.hooks {
            if !HOOK_NAMES.contains(&hook.hook_name.as_str()) {
                return Err(format!("unknown hook name {:?}", hook.hook_name));
            }
            if !Path::new(&hook.path).is_absolute() {
                return Err(format!("hook path {:?} is not absolute", hook.path));
            }
            self.hooks.push(hook);
        }
        for gid in part.additional_gids {
            if !self.additional_gids.contains(&gid) {
                self.additional_gids.push(gid);
            }
        }
        Ok(())
    }
}
fn resolve(
    node: SpecDeviceNode,
    stat: &impl Fn(&Path) -> std::io::Result<HostDevice>,
) -> std::result::Result<Device, String> {
    let (kind, major, minor) = match (node.kind, node.major, node.minor) {
        (Some(kind), Some(major), Some(minor)) => (kind, major, minor),
        (kind, major, minor) => {
            let host = node.host_path.as_deref().unwrap_or(&node.path);
            let found = match stat(Path::new(host)) {
                Ok(found) => found,
                Err(error) => return Err(format!("device node {host}: {error}")),
            };
            (
                kind.unwrap_or_else(|| found.kind.to_string()),
                major.unwrap_or(found.major),
                minor.unwrap_or(found.minor),
            )
        }
    };
    if kind != "c" && kind != "b" {
        return Err(format!(
            "device node {} has type {kind:?}, expected c or b",
            node.path
        ));
    }
    let access = node.permissions.unwrap_or_else(|| "rwm".into());
    if access.is_empty() || !access.chars().all(|c| matches!(c, 'r' | 'w' | 'm')) {
        return Err(format!(
            "device node {} has permissions {access:?}, expected letters from rwm",
            node.path
        ));
    }
    Ok(Device {
        path: node.path,
        kind,
        major,
        minor,
        file_mode: node.file_mode,
        uid: node.uid,
        gid: node.gid,
        access,
    })
}
fn host_device(path: &Path) -> std::io::Result<HostDevice> {
    let metadata = std::fs::metadata(path)?;
    let kind = if metadata.file_type().is_char_device() {
        'c'
    } else if metadata.file_type().is_block_device() {
        'b'
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a character or block device",
        ));
    };
    let (major, minor) = split_dev(metadata.rdev());
    Ok(HostDevice { kind, major, minor })
}
fn split_dev(dev: u64) -> (u64, u64) {
    (
        ((dev >> 8) & 0xfff) | ((dev >> 32) & 0xffff_f000),
        (dev & 0xff) | ((dev >> 12) & 0xffff_ff00),
    )
}
/// Merge CDI edits for GPU sandboxes; leave CPU configs untouched.
pub(crate) fn apply(
    config: &mut serde_json::Value,
    requirement: GpuRequirement,
    gpu: &NodeGpu,
) -> Result<()> {
    if !requirement.needs_card() {
        return Ok(());
    }
    let edits = match gpu {
        NodeGpu::Cdi(edits) => edits,
        NodeGpu::Fake => {
            tracing::debug!("fake GPU node: the sandbox gets no device");
            return Ok(());
        }
        NodeGpu::Absent => {
            return Err(Error::Operation(
                "this node has no usable CDI spec, so it cannot start a GPU sandbox".into(),
            ));
        }
    };
    let mut env: Vec<String> = serde_json::from_value(config["process"]["env"].take())?;
    for variable in &edits.env {
        crate::runc::set_env(&mut env, variable.clone());
    }
    crate::runc::set_env(
        &mut env,
        format!("NVIDIA_DRIVER_CAPABILITIES={DRIVER_CAPABILITIES}"),
    );
    config["process"]["env"] = serde_json::json!(env);
    for device in &edits.devices {
        let mut node = serde_json::json!({"path": device.path, "type": device.kind, "major": device.major, "minor": device.minor});
        if let Some(mode) = device.file_mode {
            node["fileMode"] = mode.into();
        }
        if let Some(uid) = device.uid {
            node["uid"] = uid.into();
        }
        if let Some(gid) = device.gid {
            node["gid"] = gid.into();
        }
        array(&mut config["linux"]["devices"])?.push(node);
        array(&mut config["linux"]["resources"]["devices"])?.push(serde_json::json!({"allow": true, "type": device.kind, "major": device.major, "minor": device.minor, "access": device.access}));
    }
    for mount in &edits.mounts {
        array(&mut config["mounts"])?.push(serde_json::json!({"destination": mount.container_path, "type": mount.kind.as_deref().unwrap_or("bind"), "source": mount.host_path, "options": mount.options}));
    }
    for hook in &edits.hooks {
        let mut entry = serde_json::json!({"path": hook.path, "args": hook.args});
        if !hook.env.is_empty() {
            entry["env"] = serde_json::json!(hook.env);
        }
        if let Some(timeout) = hook.timeout {
            entry["timeout"] = timeout.into();
        }
        array(&mut config["hooks"][hook.hook_name.as_str()])?.push(entry);
    }
    if !edits.additional_gids.is_empty() {
        let gids = array(&mut config["process"]["user"]["additionalGids"])?;
        for gid in &edits.additional_gids {
            let gid = serde_json::Value::from(*gid);
            if !gids.contains(&gid) {
                gids.push(gid);
            }
        }
    }
    for set in ["bounding", "effective", "permitted"] {
        let caps = array(&mut config["process"]["capabilities"][set])?;
        for cap in GPU_EXTRA_CAPABILITIES {
            let cap = serde_json::Value::from(*cap);
            if !caps.contains(&cap) {
                caps.push(cap);
            }
        }
    }
    Ok(())
}
fn array(value: &mut serde_json::Value) -> Result<&mut Vec<serde_json::Value>> {
    if value.is_null() {
        *value = serde_json::json!([]);
    }
    value.as_array_mut().ok_or(Error::State)
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use serde_json::{Value, json};
    const SPEC: &str = include_str!("../tests/fixtures/nvidia-cdi.json");
    const RUNC: &str = include_str!("../tests/fixtures/runc-spec.json");
    fn card_stat(path: &Path) -> std::io::Result<HostDevice> {
        let (major, minor) = match path.to_str() {
            Some("/dev/nvidia0") => (195, 0),
            Some("/dev/nvidiactl") => (195, 255),
            Some("/dev/nvidia-modeset") => (195, 254),
            Some("/dev/nvidia-uvm") => (509, 0),
            Some("/dev/nvidia-uvm-tools") => (509, 1),
            Some("/dev/dri/card1") => (226, 1),
            Some("/dev/dri/renderD128") => (226, 128),
            _ => return Err(std::io::ErrorKind::NotFound.into()),
        };
        Ok(HostDevice {
            kind: 'c',
            major,
            minor,
        })
    }
    fn l4() -> NodeGpu {
        NodeGpu::Cdi(Arc::new(
            GpuEdits::parse(SPEC.as_bytes(), &card_stat).unwrap(),
        ))
    }
    fn base() -> Value {
        serde_json::from_str(RUNC).unwrap()
    }
    fn gpu_config(requirement: GpuRequirement) -> Value {
        let mut config = base();
        apply(&mut config, requirement, &l4()).unwrap();
        config
    }
    fn inline(edits: &str) -> String {
        format!(
            r#"{{"cdiVersion":"0.5.0","kind":"nvidia.com/gpu","devices":[{{"name":"all","containerEdits":{edits}}}]}}"#
        )
    }
    #[test]
    fn cpu_sandbox_config_is_byte_identical() {
        for gpu in [l4(), NodeGpu::Fake, NodeGpu::Absent] {
            let mut config = base();
            let before = serde_json::to_vec_pretty(&config).unwrap();
            apply(&mut config, GpuRequirement::None, &gpu).unwrap();
            assert_eq!(serde_json::to_vec_pretty(&config).unwrap(), before);
        }
    }
    #[test]
    fn gpu_sandbox_gets_every_card_device_node() {
        let config = gpu_config(GpuRequirement::Shared);
        let expected = [
            ("/dev/nvidia-modeset", 195, 254),
            ("/dev/nvidia-uvm", 509, 0),
            ("/dev/nvidia-uvm-tools", 509, 1),
            ("/dev/nvidiactl", 195, 255),
            ("/dev/nvidia0", 195, 0),
            ("/dev/dri/card1", 226, 1),
            ("/dev/dri/renderD128", 226, 128),
        ];
        let nodes = config["linux"]["devices"].as_array().unwrap();
        assert_eq!(nodes.len(), expected.len());
        for (node, (path, major, minor)) in nodes.iter().zip(expected) {
            assert_eq!(
                *node,
                json!({"path":path,"type":"c","major":major,"minor":minor})
            );
        }
    }
    #[test]
    fn gpu_sandbox_device_rules_allow_only_the_cards() {
        let config = gpu_config(GpuRequirement::Shared);
        let rules = config["linux"]["resources"]["devices"].as_array().unwrap();
        assert_eq!(rules[0], json!({"allow":false,"access":"rwm"}));
        let numbers = [
            (195, 254),
            (509, 0),
            (509, 1),
            (195, 255),
            (195, 0),
            (226, 1),
            (226, 128),
        ];
        assert_eq!(rules.len(), numbers.len() + 1);
        for (rule, (major, minor)) in rules[1..].iter().zip(numbers) {
            assert_eq!(
                *rule,
                json!({"allow":true,"type":"c","major":major,"minor":minor,"access":"rwm"})
            );
        }
    }
    #[test]
    fn gpu_sandbox_mounts_driver_libraries_parents_first() {
        let config = gpu_config(GpuRequirement::Shared);
        let mounts = config["mounts"].as_array().unwrap();
        assert_eq!(&mounts[..4], base()["mounts"].as_array().unwrap());
        let destinations: Vec<&str> = mounts[4..]
            .iter()
            .map(|m| m["destination"].as_str().unwrap())
            .collect();
        assert_eq!(
            destinations,
            [
                "/run/nvidia-persistenced/socket",
                "/usr/bin/nvidia-smi",
                "/usr/lib/x86_64-linux-gnu/libEGL_nvidia.so.570.124.06",
                "/usr/lib/x86_64-linux-gnu/libGLX_nvidia.so.570.124.06",
                "/usr/lib/x86_64-linux-gnu/libcuda.so.570.124.06",
                "/usr/lib/x86_64-linux-gnu/libnvidia-eglcore.so.570.124.06",
                "/usr/lib/x86_64-linux-gnu/libnvidia-glcore.so.570.124.06",
                "/usr/lib/x86_64-linux-gnu/libnvidia-glsi.so.570.124.06",
                "/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.570.124.06",
                "/usr/lib/x86_64-linux-gnu/libnvidia-tls.so.570.124.06",
                "/lib/firmware/nvidia/570.124.06/gsp_ga10x.bin",
                "/usr/share/glvnd/egl_vendor.d/10_nvidia.json",
                "/usr/share/vulkan/icd.d/nvidia_icd.json",
                "/usr/lib/xorg/modules/drivers/nvidia_drv.so",
                "/usr/lib/xorg/modules/extensions/libglxserver_nvidia.so.570.124.06",
            ]
        );
        assert_eq!(
            mounts[8],
            json!({"destination":"/usr/lib/x86_64-linux-gnu/libcuda.so.570.124.06","type":"bind","source":"/usr/lib/x86_64-linux-gnu/libcuda.so.570.124.06","options":["ro","nosuid","nodev","rbind","rprivate"]})
        );
    }
    #[test]
    fn gpu_sandbox_runs_the_cdi_hooks_in_order() {
        let config = gpu_config(GpuRequirement::Shared);
        let hooks = &config["hooks"];
        let entries = hooks["createContainer"].as_array().unwrap();
        assert_eq!(hooks.as_object().unwrap().len(), 1);
        assert_eq!(entries.len(), 4);
        for entry in entries {
            assert_eq!(entry["path"], "/usr/bin/nvidia-cdi-hook");
            assert_eq!(entry.as_object().unwrap().len(), 2);
        }
        assert_eq!(
            entries[0]["args"],
            json!([
                "nvidia-cdi-hook",
                "create-symlinks",
                "--link",
                "libglxserver_nvidia.so.570.124.06::/usr/lib/xorg/modules/extensions/libglxserver_nvidia.so"
            ])
        );
        assert_eq!(
            entries[1]["args"],
            json!([
                "nvidia-cdi-hook",
                "update-ldcache",
                "--folder",
                "/usr/lib/x86_64-linux-gnu"
            ])
        );
        assert_eq!(
            entries[2]["args"],
            json!([
                "nvidia-cdi-hook",
                "create-symlinks",
                "--link",
                "../card1::/dev/dri/by-path/pci-0000:31:00.0-card",
                "--link",
                "../renderD128::/dev/dri/by-path/pci-0000:31:00.0-render"
            ])
        );
        assert_eq!(
            entries[3]["args"],
            json!([
                "nvidia-cdi-hook",
                "chmod",
                "--mode",
                "755",
                "--path",
                "/dev/dri"
            ])
        );
    }
    #[test]
    fn gpu_sandbox_environment_adds_driver_capabilities() {
        let mut config = base();
        config["process"]["env"]
            .as_array_mut()
            .unwrap()
            .push(json!("NVIDIA_DRIVER_CAPABILITIES=compute"));
        apply(&mut config, GpuRequirement::Shared, &l4()).unwrap();
        assert_eq!(
            config["process"]["env"],
            json!([
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "TERM=xterm",
                "NVIDIA_VISIBLE_DEVICES=void",
                "NVIDIA_DRIVER_CAPABILITIES=graphics,display,compute,utility"
            ])
        );
    }
    #[test]
    fn gpu_sandbox_keeps_the_fixed_capabilities_and_user() {
        let config = gpu_config(GpuRequirement::Shared);
        let before = base();
        assert_eq!(
            config["process"]["capabilities"],
            before["process"]["capabilities"]
        );
        assert_eq!(config["process"]["user"], before["process"]["user"]);
    }
    #[test]
    fn shared_and_dedicated_get_the_same_config() {
        assert_eq!(
            gpu_config(GpuRequirement::Shared),
            gpu_config(GpuRequirement::Dedicated)
        );
    }
    #[test]
    fn fake_node_exposes_no_device() {
        let mut config = base();
        let before = serde_json::to_vec_pretty(&config).unwrap();
        apply(&mut config, GpuRequirement::Shared, &NodeGpu::Fake).unwrap();
        assert_eq!(serde_json::to_vec_pretty(&config).unwrap(), before);
    }
    #[test]
    fn node_without_spec_refuses_gpu_sandbox() {
        assert!(
            apply(&mut base(), GpuRequirement::Dedicated, &NodeGpu::Absent)
                .unwrap_err()
                .to_string()
                .contains("no usable CDI spec")
        );
    }
    #[test]
    fn explicit_device_fields_skip_stat_and_reach_the_config() {
        let text = inline(
            r#"{"deviceNodes":[{"path":"/dev/nvidia0","type":"c","major":195,"minor":0,"fileMode":438,"uid":0,"gid":44,"permissions":"rw"}],"additionalGids":[44,44],"hooks":[{"hookName":"prestart","path":"/usr/bin/x","args":["x"],"env":["A=1"],"timeout":5}],"mounts":[{"hostPath":"/srv/a/b","containerPath":"/a/b","options":["ro"]},{"hostPath":"/srv/a","containerPath":"/a","options":["rbind"],"type":"bind"}]}"#,
        );
        let edits = GpuEdits::parse(
            text.as_bytes(),
            &|_: &Path| -> std::io::Result<HostDevice> {
                panic!("stat must not run when the spec gives the numbers")
            },
        )
        .unwrap();
        let mut config = base();
        apply(
            &mut config,
            GpuRequirement::Shared,
            &NodeGpu::Cdi(Arc::new(edits)),
        )
        .unwrap();
        assert_eq!(
            config["linux"]["devices"]
                .as_array()
                .unwrap()
                .last()
                .unwrap(),
            &json!({"path":"/dev/nvidia0","type":"c","major":195,"minor":0,"fileMode":438,"uid":0,"gid":44})
        );
        assert_eq!(
            config["linux"]["resources"]["devices"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()["access"],
            "rw"
        );
        assert_eq!(config["process"]["user"]["additionalGids"], json!([44]));
        assert_eq!(
            config["hooks"]["prestart"],
            json!([{"path":"/usr/bin/x","args":["x"],"env":["A=1"],"timeout":5}])
        );
        let mounts = config["mounts"].as_array().unwrap();
        assert_eq!(mounts[mounts.len() - 2]["destination"], "/a");
        assert_eq!(mounts.last().unwrap()["destination"], "/a/b");
        assert_eq!(mounts[mounts.len() - 2]["type"], "bind");
        assert_eq!(mounts.last().unwrap()["type"], "bind");
    }
    #[test]
    fn host_path_is_what_gets_stat_ed() {
        let text = inline(r#"{"deviceNodes":[{"path":"/dev/gpu0","hostPath":"/dev/nvidia0"}]}"#);
        let edits = GpuEdits::parse(text.as_bytes(), &card_stat).unwrap();
        assert_eq!(edits.devices[0].path, "/dev/gpu0");
        assert_eq!((edits.devices[0].major, edits.devices[0].minor), (195, 0));
    }
    #[test]
    fn unusable_specs_are_rejected_with_the_reason() {
        let cases = [
            (r#"{"kind":"amd.com/gpu","devices":[]}"#.to_string(), "kind"),
            (
                r#"{"kind":"nvidia.com/gpu","devices":[{"name":"0","containerEdits":{}}]}"#
                    .to_string(),
                "no device named \"all\"",
            ),
            (inline(r#"{"intelRdt":{}}"#), "intelRdt"),
            (inline(r#"{"env":["NOEQUALS"]}"#), "NOEQUALS"),
            (
                inline(r#"{"hooks":[{"hookName":"preStart","path":"/usr/bin/x"}]}"#),
                "preStart",
            ),
            (
                inline(r#"{"hooks":[{"hookName":"prestart","path":"x"}]}"#),
                "not absolute",
            ),
            (
                inline(r#"{"mounts":[{"hostPath":"/a","containerPath":"a"}]}"#),
                "not absolute",
            ),
            (
                inline(r#"{"deviceNodes":[{"path":"/dev/nvidia9"}]}"#),
                "/dev/nvidia9",
            ),
            (
                inline(r#"{"deviceNodes":[{"path":"/dev/nvidia0","permissions":"rwx"}]}"#),
                "permissions",
            ),
            (
                inline(r#"{"deviceNodes":[{"path":"/dev/x","type":"p","major":1,"minor":1}]}"#),
                "type",
            ),
            ("not json".to_string(), ""),
        ];
        for (text, reason) in cases {
            let error = GpuEdits::parse(text.as_bytes(), &card_stat).unwrap_err();
            assert!(!error.is_empty() && error.contains(reason), "{error}");
        }
    }
    #[test]
    fn load_reads_only_json_files() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            GpuEdits::load(&dir.path().join("nvidia.yaml"))
                .unwrap_err()
                .to_string()
                .contains("--format=json")
        );
        assert!(
            GpuEdits::load(&dir.path().join("missing.json"))
                .unwrap_err()
                .to_string()
                .contains("missing.json")
        );
        let path = dir.path().join("nvidia.json");
        std::fs::write(&path, SPEC).unwrap();
        assert_eq!(
            GpuEdits::load_with(&path, &card_stat).unwrap(),
            GpuEdits::parse(SPEC.as_bytes(), &card_stat).unwrap()
        );
    }
    #[test]
    fn host_device_reads_character_devices() {
        assert_eq!(
            host_device(Path::new("/dev/null")).unwrap(),
            HostDevice {
                kind: 'c',
                major: 1,
                minor: 3
            }
        );
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(host_device(file.path()).is_err());
    }
    #[test]
    fn split_dev_matches_glibc() {
        assert_eq!(split_dev(49920), (195, 0));
        assert_eq!(split_dev(130_305), (509, 1));
        assert_eq!(split_dev(259), (1, 3));
        assert_eq!(split_dev(17_592_187_093_036), (4096, 300));
    }
}
