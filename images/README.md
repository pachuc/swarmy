# Base images

Each `images/<name>/recipe.toml` declares a fixed disk size in bytes, a fixed
`source_date_epoch` in Unix seconds, and a `[source]` table. The directory name
is the registered image name. Disk sizes must be multiples of 256 KiB and at
least 16 MiB. Names and tags use ASCII letters, digits, dots, dashes, and
underscores, with a maximum length of 128 characters.

```sh
scripts/dev-stack.sh start
source .dev/env
cargo build -p swarmy-cli --bins --locked
target/debug/swarmy image build images/base-ubuntu --tag noble
target/debug/swarmy image ls
target/debug/swarmy --json image show base-ubuntu:noble
```

Build output includes the byte size, total block count, nonzero blocks stored,
and new chunk objects uploaded. Repeated blocks count toward `chunks_stored`
but upload only once. Manifest root and leaf objects are excluded from these
counts. JSON output is one object per line; subprocess logs go to stderr.
The manifest header is registered only after all objects are uploaded, and
the name/tag pointer is updated only after the header is durable. Rebuilding
a tag replaces its pointer; existing volumes keep their original manifest.

## Recipe sources

Common fields for every recipe:

```toml
disk_size = 8589934592
source_date_epoch = 1714003200
```

Choose one source table:

```toml
[source]
kind = "debootstrap"
suite = "noble"
mirror = "http://archive.ubuntu.com/ubuntu"
packages = ["bash", "coreutils", "git", "curl", "ca-certificates", "python3", "build-essential"]
```

Debootstrap installs the minimal base system, then apt installs the listed
packages without recommendations so virtual dependencies resolve correctly.
Optional `components = ["main", "universe"]` selects additional archive
components. Use either `script = "setup.sh"` or
`scripts = ["../common/agent-setup.sh", "setup.sh"]` in the source table.
Paths are relative to the recipe directory. Scripts run in order through
`/bin/sh -es` inside the installed system before cleanup. See
[the developer image](base-ubuntu/README.md) for its tools and credential setup,
[the desktop image](base-desktop/README.md) for its supervised display and
software renderers. The
optional `source_commit = "..."` must be a full 40-digit Git hash and is passed
to that script as `SWARMY_SOURCE_COMMIT`; the swarmy-dev recipe uses it to pin
the checkout that warms Cargo.
The builder temporarily mounts `/proc` during debootstrap setup so rustup can
inspect its own executable, then unmounts it before creating the filesystem.
The builder removes apt caches and package lists, clears logs, and resets the
machine identity. It also removes the ldconfig auxiliary cache, whose host
inode numbers become invalid when files are copied into ext4, and removes Python
bytecode caches whose headers contain installation times. The package names are fixed; versions follow the configured
archive. For reproducibility across archive updates, use a frozen mirror.
The included recipe builds Ubuntu 24.04 on the host's architecture.

```toml
[source]
kind = "directory"
path = "rootfs"
```

The directory is copied with ownership, permissions, links, and extended
attributes preserved. The source is never modified. Relative paths resolve
against the recipe directory.

```toml
[source]
kind = "shell"
rootfs = "seed"
script = "build.sh"
```

The seed is copied first, then `/bin/sh -es` runs the script on standard input
inside that root. The seed must contain the shell, its libraries, and any
required build tools. No host directories are mounted into the chroot.

```toml
[source]
kind = "oci"
reference = "docker://docker.io/library/ubuntu:24.04"
```

Install `skopeo` and `umoci` for OCI recipes. Skopeo copies the reference into
an OCI layout; umoci applies its layers, including whiteouts and ownership.
Use an image digest to keep the source fixed. Container entrypoints and
runtime settings are not part of the filesystem image.

## Host requirements and repeat builds

Builds require Linux, passwordless sudo, e2fsprogs (`mke2fs` and `debugfs`),
coreutils, and the selected source tools. Recipes are trusted input: chroot
scripts run as root and chroot is not a security boundary. Scratch files are
removed when the build completes or fails. A forced process termination can
leave a `swarmy-image-*` scratch directory in the temporary directory.

The builder creates a sparse file and populates ext4 using
`mke2fs -d ... -E root_owner=0:0` without mounting it. Filesystem UUID,
directory hash seed, and creation time are fixed. All inode access,
modification, change, and birth times are normalized to `source_date_epoch`
using debugfs, so host creation times do not invalidate inode-table chunks.
File modes, ownership, contents, links, and extended attributes are preserved.
Identical package contents should reuse chunks; generated files can still
change. Base images and their clones share a filesystem UUID: mount them by
device path, not by UUID.

To retain the raw sparse image for inspection, pass `--output /tmp/base.ext4`.
The destination must not exist. For a manual check:

```sh
sudo mkdir -p /mnt/swarmy-image
sudo mount -o loop /tmp/base.ext4 /mnt/swarmy-image
# Always unmount, including if a command fails.
trap 'sudo umount /mnt/swarmy-image' EXIT
sudo chroot /mnt/swarmy-image /bin/bash -c 'git --version'
sudo umount /mnt/swarmy-image
trap - EXIT
```

Volume creation uses `Store::create_volume(id, manifest_id)` to write a pointer.
Device attachment belongs to the NBD task. Guest-agent installation is deferred
to slice 3.

## Tests

The ordinary suite checks recipe validation and chunk ingestion without root.
The ext4 mount test prints a skip message unless its test binary runs as root:

```sh
cargo test -p swarmy-volume --test image --no-run
sudo -E "$(cargo test -p swarmy-volume --test image --no-run --message-format=json 2>/dev/null | jq -r 'select(.executable != null and .target.name == "image") | .executable')" --nocapture
```

The root test builds independent filesystems, checks chunk reuse, and mounts
one read-only to verify the stored files and symlinks. A Drop guard unmounts it
even when an assertion fails.

The CLI acceptance test builds Ubuntu twice, checks registration and JSON
output, creates a volume pointer, mounts the raw file, and runs bash and git
in a chroot. It also checks that retagging leaves the existing volume on its
original manifest. It requires the dev stack and leaves its image/volume
records in the configured development store:

```sh
source .dev/env
cargo test -p swarmy-cli --test image --no-run
sudo -E "$(cargo test -p swarmy-cli --test image --no-run --message-format=json 2>/dev/null | jq -r 'select(.executable != null and .target.name == "image") | .executable')" --nocapture
```

The volume root tests also exercise a chroot script and an OCI image with a
deleted file in a later layer. The OCI test skips if skopeo or umoci is absent;
it creates a local OCI layout and does not need a container registry.

## Desktop acceptance on a node

A fleet sandbox has no sudo or NBD devices, so it cannot build or validate the
desktop image. The operator must use a node with sudo, debootstrap, and NBD:

1. Start the dev stack (`scripts/dev-stack.sh start; source .dev/env`) and run
   `swarmy image build images/base-ubuntu` and
   `swarmy image build images/base-desktop`. Compare their build output's
   nonzero chunk coverage, not just their sparse virtual disk sizes.
2. Run the root-only `swarmy-cli --test image`, `swarmy-volume --test image`,
   `swarmy-volume --test nbd`, and `swarmyd --test node` suites, followed by
   the bash, continuity, and
   coding chaos suites and `scripts/chaos-ci.sh`.
3. In a session on `base-desktop:dev`, run `xdpyinfo`, `glxinfo -B`, and
   `vulkaninfo --summary`. Check that the renderer names are llvmpipe and
   lavapipe. Start `chromium about:blank` and request
   `http://127.0.0.1:9222/json/version` inside the sandbox. Render a small
   scene with `blender --background --python-expr` to a file. Kill Xvfb and
   confirm that `xdpyinfo` succeeds again within a few seconds.

See [the desktop image](base-desktop/README.md) for example commands.
