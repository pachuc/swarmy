# Development image for Codex agents working on swarmy inside Daytona sandboxes.
# Environment only: the launcher clones the repository at runtime, and no
# credentials or project source belong in this image.

FROM node:22-bookworm

ARG FDB_VERSION=7.3.79
ARG NATS_VERSION=2.14.6
ARG SEAWEEDFS_VERSION=4.47
ARG RUST_VERSION=1.98.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git python3 tar ca-certificates curl jq ripgrep \
    build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# FoundationDB client library, fdbcli, and fdbserver. The packages are
# extracted rather than installed so that no postinst script tries to start a
# system service inside the image build.
RUN set -eux; cd /tmp; \
    for pkg in clients server; do \
      deb="foundationdb-${pkg}_${FDB_VERSION}-1_amd64.deb"; \
      curl -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${deb}"; \
      curl -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${deb}.sha256"; \
      sha256sum -c "${deb}.sha256"; \
      dpkg-deb -x "${deb}" /; \
    done; \
    ldconfig; \
    rm -f /tmp/foundationdb-*; \
    fdbcli --version; fdbserver --version

# NATS server with JetStream.
RUN set -eux; cd /tmp; \
    curl -fsSL "https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/nats-server-v${NATS_VERSION}-linux-amd64.tar.gz" \
      | tar -xz; \
    install -m 0755 "nats-server-v${NATS_VERSION}-linux-amd64/nats-server" /usr/local/bin/nats-server; \
    rm -rf "/tmp/nats-server-v${NATS_VERSION}-linux-amd64"; \
    nats-server --version

# SeaweedFS provides the S3-compatible object store for local development.
RUN set -eux; cd /tmp; \
    curl -fsSL "https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/linux_amd64.tar.gz" \
      | tar -xz; \
    install -m 0755 weed /usr/local/bin/weed; \
    rm -f /tmp/weed; \
    weed version

USER node
WORKDIR /home/node
ENV PATH=/home/node/.cargo/bin:$PATH

# The pinned toolchain matches rust-toolchain.toml so runs do not download it.
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --no-modify-path \
      --default-toolchain "${RUST_VERSION}" --profile minimal \
      --component clippy --component rustfmt \
    && cargo --version && cargo clippy --version && cargo fmt --version
