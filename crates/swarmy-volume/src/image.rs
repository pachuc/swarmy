//! Offline root filesystem construction and chunk ingestion.
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};

use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use swarmy_core::{CHUNK_SIZE, ContentHash, ManifestHeader};
use tempfile::TempDir;

use crate::{ChunkStore, Manifest, ManifestBuilder, VolumeError};

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Recipe(#[from] toml::de::Error),
    #[error(transparent)]
    Volume(#[from] VolumeError),
    #[error("invalid recipe: {0}")]
    Invalid(String),
    #[error("{program} failed with {status}")]
    Command {
        program: String,
        status: std::process::ExitStatus,
    },
}

type Result<T> = std::result::Result<T, ImageError>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recipe {
    pub disk_size: u64,
    /// Stable filesystem creation time, in seconds since the Unix epoch.
    pub source_date_epoch: u32,
    pub source: Source,
    #[serde(default)]
    pub sandbox: SandboxRecipe,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRecipe {
    #[serde(default)]
    pub scratch: Vec<String>,
    pub memory_mib: Option<u64>,
    #[serde(default)]
    pub display: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Source {
    Debootstrap {
        suite: String,
        packages: Vec<String>,
        mirror: String,
        components: Option<Vec<String>>,
        /// Optional pinned checkout passed to the setup script.
        source_commit: Option<String>,
        /// Optional setup script run inside the installed system.
        script: Option<PathBuf>,
        /// Setup scripts run in order in the same chroot.
        #[serde(default)]
        scripts: Vec<PathBuf>,
    },
    Directory {
        path: PathBuf,
    },
    /// The seed must already contain /bin/sh and any tools the script needs.
    Shell {
        rootfs: PathBuf,
        script: PathBuf,
    },
    Oci {
        reference: String,
    },
}

impl Recipe {
    /// Read a recipe directory or its `recipe.toml` file.
    /// # Errors
    /// Rejects malformed recipes, invalid sizes, and unreadable files.
    pub fn load(path: &Path) -> Result<(Self, PathBuf, String)> {
        let file = if path.is_dir() {
            path.join("recipe.toml")
        } else {
            path.to_owned()
        }
        .canonicalize()?;
        let directory = file
            .parent()
            .ok_or_else(|| ImageError::Invalid("recipe has no directory".into()))?
            .to_owned();
        let name = directory
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ImageError::Invalid("recipe name is not UTF-8".into()))?
            .to_owned();
        validate_label(&name)?;
        let recipe: Self = toml::from_str(&fs::read_to_string(file)?)?;
        if let Source::Debootstrap {
            source_commit: Some(commit),
            ..
        } = &recipe.source
            && (commit.len() != 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(ImageError::Invalid(
                "source_commit must be a 40-digit Git commit hash".into(),
            ));
        }
        if let Source::Debootstrap {
            script: Some(_),
            scripts,
            ..
        } = &recipe.source
            && !scripts.is_empty()
        {
            return Err(ImageError::Invalid(
                "use script or scripts, not both".into(),
            ));
        }
        Manifest::empty(recipe.disk_size)?;
        if recipe.disk_size < 16 * 1024 * 1024 {
            return Err(ImageError::Invalid("ext4 requires at least 16 MiB".into()));
        }
        for (index, path) in recipe.sandbox.scratch.iter().enumerate() {
            let path = Path::new(path);
            if !path.is_absolute()
                || path.components().any(|part| {
                    !matches!(
                        part,
                        std::path::Component::RootDir | std::path::Component::Normal(_)
                    )
                })
                || path == Path::new("/")
                || recipe.sandbox.scratch[..index].iter().any(|earlier| {
                    path.starts_with(earlier) || Path::new(earlier).starts_with(path)
                })
            {
                return Err(ImageError::Invalid(
                    "scratch paths must be distinct, absolute, and non-overlapping".into(),
                ));
            }
        }
        Ok((recipe, directory, name))
    }
}

/// Image names and tags must be unambiguous in `NAME:TAG` output.
/// # Errors
/// Rejects empty labels and characters outside ASCII letters, digits, dot, dash, and underscore.
pub fn validate_label(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(ImageError::Invalid(
            "names and tags must contain 1-128 ASCII letters, digits, dots, dashes, or underscores"
                .into(),
        ));
    }
    Ok(())
}

/// Owns the scratch directory, including files made by privileged subprocesses.
pub struct BuiltImage {
    scratch: TempDir,
    path: PathBuf,
}

impl BuiltImage {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BuiltImage {
    fn drop(&mut self) {
        if let Err(error) = run(privileged("rm")
            .args(["-rf", "--"])
            .arg(self.scratch.path()))
        {
            tracing::warn!(%error, path = %self.scratch.path().display(), "could not clean image build directory");
        }
    }
}

fn privileged(program: &str) -> Command {
    let mut command = Command::new("sudo");
    command.args(["-n", "--", program]);
    command
}

fn run(command: &mut Command) -> Result<()> {
    let program = command.get_program().to_string_lossy().into_owned();
    // Keep tool output out of the CLI's JSON stream.
    let status = command.stdout(Stdio::from(std::io::stderr())).status()?;
    if !status.success() {
        return Err(ImageError::Command { program, status });
    }
    Ok(())
}

/// Build a fixed-size ext4 image without a loop device or a mount.
/// This is blocking work; async callers should use `spawn_blocking`.
/// Recipes are trusted programs and run with root privileges through sudo.
/// # Errors
/// Returns invalid recipe, filesystem, or external command failures.
pub fn build_ext4(recipe: &Recipe, directory: &Path) -> Result<BuiltImage> {
    Manifest::empty(recipe.disk_size)?;
    let scratch = tempfile::Builder::new().prefix("swarmy-image-").tempdir()?;
    let path = scratch.path().join("disk.ext4");
    let image = BuiltImage { scratch, path };
    let root = image.scratch.path().join("rootfs");
    fs::create_dir(&root)?;
    populate(&recipe.source, directory, &root, image.scratch.path())?;
    let file = File::create(&image.path)?;
    file.set_len(recipe.disk_size)?;
    // Fixed UUID and directory hash seed prevent random metadata from changing
    // otherwise identical chunks. Clones intentionally share the base identity.
    let uuid = "6b34c6ca-2a02-4b84-8ff1-737761726d79";
    run(privileged("env")
        .arg(format!("E2FSPROGS_FAKE_TIME={}", recipe.source_date_epoch))
        .args([
            "LC_ALL=C", "mke2fs", "-q", "-t", "ext4", "-F", "-b", "4096", "-I", "256", "-m", "0",
            "-U", uuid, "-E",
        ])
        .arg(format!(
            "root_owner=0:0,lazy_itable_init=0,lazy_journal_init=0,hash_seed={uuid}"
        ))
        .arg("-d")
        .arg(&root)
        .arg(&image.path))?;
    // The populated tree can be much larger than the sparse ext4 file. Free
    // it before the caller uploads chunks so a warm development image fits
    // on an ordinary build node.
    run(privileged("rm").args(["-rf", "--"]).arg(&root))?;
    normalize_times(&image, recipe.source_date_epoch)?;
    Ok(image)
}

fn populate(source: &Source, directory: &Path, root: &Path, scratch: &Path) -> Result<()> {
    match source {
        Source::Debootstrap {
            suite,
            packages,
            mirror,
            components,
            source_commit,
            script,
            scripts,
        } => {
            bootstrap_packages(suite, packages, mirror, components.as_deref(), root)?;
            let scripts: Vec<&PathBuf> = script.iter().chain(scripts.iter()).collect();
            if !scripts.is_empty() {
                // rustup and other installers inspect /proc/self/exe. Keep
                // proc mounted only while the setup script runs.
                let proc = root.join("proc");
                run(privileged("mount").args(["-t", "proc", "proc"]).arg(&proc))?;
                let script_result: Result<()> = (|| {
                    for script in scripts {
                        let input = File::open(directory.join(script))?;
                        let mut command = privileged("env");
                        if let Some(commit) = source_commit {
                            command.arg(format!("SWARMY_SOURCE_COMMIT={commit}"));
                        }
                        run(command
                            .arg("chroot")
                            .arg(root)
                            .args(["/bin/sh", "-es"])
                            .stdin(input))?;
                    }
                    Ok(())
                })();
                let unmount_result = run(privileged("umount").arg(&proc));
                script_result?;
                unmount_result?;
            }
            // ldconfig's auxiliary cache contains host inode numbers, which
            // become invalid when files are copied into ext4.
            run(privileged("chroot").arg(root).args([
                "/bin/sh",
                "-ec",
                "apt-get clean
rm -rf /var/lib/apt/lists/* /var/cache/apt/*
rm -f /var/cache/ldconfig/aux-cache
find /usr -type d -name __pycache__ -prune -exec rm -rf {} +
find /var/log -type f -exec truncate -s 0 {} +
rm -f /etc/machine-id /var/lib/dbus/machine-id
: > /etc/machine-id",
            ]))?;
        }
        Source::Directory { path } => copy_root(&directory.join(path), root)?,
        Source::Shell { rootfs, script } => {
            copy_root(&directory.join(rootfs), root)?;
            let script = File::open(directory.join(script))?;
            run(privileged("chroot")
                .arg(root)
                .args(["/bin/sh", "-es"])
                .stdin(script))?;
        }
        Source::Oci { reference } => {
            let layout = scratch.join("oci");
            run(Command::new("skopeo")
                .args(["copy", "--", reference])
                .arg(format!("oci:{}:image", layout.display())))?;
            let bundle = scratch.join("bundle");
            run(privileged("umoci")
                .args(["unpack", "--image"])
                .arg(format!("{}:image", layout.display()))
                .arg(&bundle))?;
            copy_root(&bundle.join("rootfs"), root)?;
        }
    }
    Ok(())
}

fn bootstrap_packages(
    suite: &str,
    packages: &[String],
    mirror: &str,
    components: Option<&[String]>,
    root: &Path,
) -> Result<()> {
    validate_label(suite)?;
    if packages.is_empty()
        || packages.iter().any(|package| {
            package.is_empty()
                || package.starts_with('-')
                || package.contains(',')
                || package.contains(char::is_whitespace)
        })
    {
        return Err(ImageError::Invalid(
            "invalid debootstrap package set".into(),
        ));
    }
    let mut command = privileged("debootstrap");
    command.arg("--variant=minbase");
    if let Some(components) = components {
        if components.is_empty() {
            return Err(ImageError::Invalid("empty debootstrap components".into()));
        }
        for component in components {
            validate_label(component)?;
        }
        command.arg(format!("--components={}", components.join(",")));
    }
    run(command.arg(suite).arg(root).arg(mirror))?;
    // Apt resolves virtual and versioned dependencies (notably npm's)
    // that debootstrap's limited --include resolver cannot configure.
    run(privileged("chroot").arg(root).args([
        "/usr/bin/env",
        "DEBIAN_FRONTEND=noninteractive",
        "apt-get",
        "update",
    ]))?;
    run(privileged("chroot")
        .arg(root)
        .args([
            "/usr/bin/env",
            "DEBIAN_FRONTEND=noninteractive",
            "apt-get",
            "install",
            "--yes",
            "--no-install-recommends",
            "--",
        ])
        .args(packages))?;
    Ok(())
}

fn copy_root(source: &Path, destination: &Path) -> Result<()> {
    let source = source.canonicalize()?;
    if !source.is_dir() {
        return Err(ImageError::Invalid("rootfs must be a directory".into()));
    }
    run(privileged("cp")
        .args(["-a", "--"])
        .arg(source.join("."))
        .arg(destination))
}

fn normalize_times(image: &BuiltImage, epoch: u32) -> Result<()> {
    // mke2fs copies host ctime, which touch cannot set. Walk directories by
    // inode number so guest filenames never become debugfs commands.
    let inodes = image_inodes(image)?;
    let commands = image.scratch.path().join("times.debugfs");
    let mut file = File::create(&commands)?;
    for inode in inodes {
        for field in ["atime", "mtime", "ctime", "crtime"] {
            writeln!(file, "set_inode_field <{inode}> {field} @{epoch}")?;
        }
    }
    let output = Command::new("debugfs")
        .env("E2FSPROGS_FAKE_TIME", epoch.to_string())
        .args(["-w", "-f"])
        .arg(commands)
        .arg(&image.path)
        .output()?;
    // debugfs reports command errors on stderr even when its exit status is zero.
    if !output.status.success()
        || String::from_utf8_lossy(&output.stderr)
            .lines()
            .any(|line| !line.starts_with("debugfs "))
    {
        return Err(ImageError::Invalid(format!(
            "normalizing ext4 timestamps: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn image_inodes(image: &BuiltImage) -> Result<std::collections::BTreeSet<u64>> {
    let mut inodes = std::collections::BTreeSet::from([2_u64]);
    let mut directories = vec![2_u64];
    let commands = image.scratch.path().join("list.debugfs");
    while !directories.is_empty() {
        let mut file = File::create(&commands)?;
        for inode in directories.drain(..) {
            writeln!(file, "ls -p <{inode}>")?;
        }
        let output = Command::new("debugfs")
            .arg("-f")
            .arg(&commands)
            .arg(&image.path)
            .output()?;
        if !output.status.success()
            || String::from_utf8_lossy(&output.stderr)
                .lines()
                .any(|line| !line.starts_with("debugfs "))
        {
            return Err(ImageError::Invalid(format!(
                "listing ext4 inodes: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        for line in String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| line.starts_with('/'))
        {
            let mut fields = line.split('/').skip(1);
            let inode = fields
                .next()
                .and_then(|word| word.parse::<u64>().ok())
                .ok_or_else(|| ImageError::Invalid("invalid debugfs inode number".into()))?;
            let mode = fields
                .next()
                .and_then(|word| u32::from_str_radix(word, 8).ok())
                .ok_or_else(|| ImageError::Invalid("invalid debugfs inode mode".into()))?;
            if inode != 0 && inodes.insert(inode) && mode & 0o170_000 == 0o040_000 {
                directories.push(inode);
            }
        }
    }
    Ok(inodes)
}

#[derive(Debug, Serialize)]
pub struct ImageManifest {
    pub header: ManifestHeader,
    pub chunks_total: u64,
    pub chunks_stored: u64,
    pub chunks_uploaded: u64,
}

/// Upload an image's chunks and manifest objects before publishing its header.
/// Zero chunks do not consume object storage. Counts describe data chunks only.
/// For a bucket managed by GC, use `upload_image_protected` instead.
/// # Errors
/// Returns invalid dimensions, local read errors, or object storage failures.
pub async fn upload_image(path: &Path, objects: Arc<dyn ObjectStore>) -> Result<ImageManifest> {
    upload_image_inner(path, objects, None).await
}

/// Upload with reuse guards for a bucket managed by the chunk collector.
/// # Errors
/// Returns image, object storage, and metadata errors.
pub async fn upload_image_protected(
    path: &Path,
    objects: Arc<dyn ObjectStore>,
    metadata: swarmy_store::Store,
) -> Result<ImageManifest> {
    upload_image_inner(path, objects, Some(metadata)).await
}

async fn upload_image_inner(
    path: &Path,
    objects: Arc<dyn ObjectStore>,
    metadata: Option<swarmy_store::Store>,
) -> Result<ImageManifest> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut builder = ManifestBuilder::new(objects.clone(), Manifest::empty(size)?);
    let chunks = match metadata {
        Some(metadata) => ChunkStore::with_gc_protection(objects, metadata),
        None => ChunkStore::new(objects),
    };
    let chunks_total = size / u64::from(CHUNK_SIZE);
    let mut chunks_stored = 0;
    let mut chunks_uploaded = 0;
    let mut buffer = vec![0; CHUNK_SIZE as usize];
    for block in 0..chunks_total {
        file.read_exact(&mut buffer)?;
        let put = chunks.put_chunk(&buffer).await?;
        if put.hash != ContentHash::ZERO {
            chunks_stored += 1;
            builder.set_chunk(block, put.hash)?;
        }
        chunks_uploaded += u64::from(put.uploaded);
    }
    let manifest = builder.build().await?;
    Ok(ImageManifest {
        header: manifest.header().clone(),
        chunks_total,
        chunks_stored,
        chunks_uploaded,
    })
}
