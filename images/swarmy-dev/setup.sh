# The builder runs this script in the new root as root, without host mounts.
set -eu

# Use a public clone so the image never carries a build credential.
source_commit=${SWARMY_SOURCE_COMMIT:?source_commit is required in recipe.toml}
scratch=/tmp/swarmy-image-source
git clone --no-checkout https://github.com/pachuc/swarmy.git "$scratch"
git -C "$scratch" checkout --detach "$source_commit"

# Reuse the base image's account, GitHub helper, and shell tools.
/bin/sh -e "$scratch/images/base-ubuntu/setup.sh"
chown -R agent:agent "$scratch"

# Run the repository installer as the agent so its binaries and FDB library
# live in the same home that development sessions use.
su -s /bin/bash agent -c 'HOME=/home/agent bash /tmp/swarmy-image-source/scripts/install-dev-tools.sh'

# rust-toolchain.toml supplies the version, minimal profile, rustfmt, and clippy.
# Install as agent so rustup and cargo can update their own directories later.
su -s /bin/bash agent -c 'export HOME=/home/agent PATH=/home/agent/.local/bin:/usr/local/bin:/usr/bin:/bin; curl --retry 3 -fsSL https://sh.rustup.rs | sh -s -- -y --no-modify-path --default-toolchain none; export PATH="$HOME/.cargo/bin:$PATH"; cd /tmp/swarmy-image-source; rustup show; rustup default "$(rustup show active-toolchain | cut -d" " -f1)"'

mkdir -p /home/agent/.cargo-target
chown agent:agent /home/agent/.cargo-target
cat >> /home/agent/.profile <<'PROFILE'
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/.cargo-target"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
PROFILE
cat >> /home/agent/.bashrc <<'BASHRC'
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/.cargo-target"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
BASHRC

su -s /bin/bash agent -c 'export HOME=/home/agent PATH=/home/agent/.cargo/bin:/home/agent/.local/bin:/usr/local/bin:/usr/bin:/bin CARGO_TARGET_DIR=/home/agent/.cargo-target SWARMY_FDB_LIB_DIR=/home/agent/.local/lib; cd /tmp/swarmy-image-source; cargo fetch --locked && cargo build --workspace --all-targets --locked && cargo build --workspace --locked'
rm -rf "$scratch"
