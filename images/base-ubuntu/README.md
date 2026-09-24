# Ubuntu developer image

The 8 GiB sparse ext4 image contains git, GitHub CLI, ripgrep, fd-find (also
available as `fd`), jq, curl, CA certificates, Python 3 and pip, Node.js and npm,
build-essential, pkg-config, and OpenSSL development headers. The `agent` user
has `/home/agent/work`. Packages come from Ubuntu Noble main and universe;
versions follow the configured archive. Use a frozen mirror to reproduce builds
across archive updates.

`../common/agent-setup.sh` runs after debootstrap and installs the credential helper and gh
wrapper. The node applies the same script when opening an existing image.
The sandbox inherits `HOME=/home/agent` and a PATH preferring `/usr/local/bin`.

Create or update a credential with `swarmy agent create NAME --github-token TOKEN`
or `swarmy agent set NAME --github-token TOKEN`. Remove it with
`swarmy agent set NAME --clear-github-token`. Creation stores the identity and
credential atomically. The `github_token` field is a private FoundationDB side
row, preserving the original agent record encoding. Show, list, create, and set
output never include it, including JSON output. Deleting the agent removes it.

The per-agent socket at `/run/swarmy/github.sock` is served by swarmyd's sandbox
runtime. Identity comes from the listening socket, with no agent selector in
the request. Each request reads FoundationDB again; clearing, rotation, and
deletion take effect on the next use. Database failures and missing credentials
refuse access. A sandbox sees only its own socket, never the node control socket.

Git's credential helper serves HTTPS requests to github.com, or the explicit
`GH_HOST` for GitHub Enterprise. Store and erase operations do not cache
credentials. The gh wrapper fetches the token and execs `/usr/bin/gh` with
`GH_TOKEN` and `GH_ENTERPRISE_TOKEN` in its environment. Its `GH_CONFIG_DIR`
is a separate tmpfs mount at `/run/swarmy-gh`. Version and help commands do not
need a token. Core dumps are disabled. The token is never placed in a helper,
process argument, git config, home file, or volume snapshot by this delivery
path. An agent that explicitly prints or saves its own credential can of course
persist it; agents are trusted under the swarmy design.

The root acceptance test authenticates actual git and gh requests to a local
TLS fake, rotates and clears the token, checkpoints, and scans every regular
file and every referenced snapshot chunk for both credential values. The fake's
expected values arrive over stdin and never enter the sandbox disk.

Measured on the Ubuntu 24.04 x86-64 EC2 launcher on 2026-09-18 (4 cores,
15 GiB RAM, local FoundationDB and SeaweedFS):

| Measurement | First build | Second build |
| --- | ---: | ---: |
| Wall time, including upload and raw image copy | 287.36 s | 182.23 s |
| Virtual disk size | 8,589,934,592 bytes | 8,589,934,592 bytes |
| Nonzero chunk coverage | 864,813,056 bytes (824.75 MiB) | 864,813,056 bytes (824.75 MiB) |
| Nonzero chunks | 3,299 | 3,299 |
| New chunk uploads | 3,298 | 0 |

Both fresh builds produced root hash
`36f876c15c35d1eba51cd975ea0b01855612632d9a1e1a973b872abe8b883285`.
Builds used the same live Noble archive. Archive updates can change subsequent
root hashes; use a frozen mirror for later repeatability. Other validation ran
concurrently during these measurements.

The root image test ran all listed tools in a chroot. The node test also ran
their version commands inside runc, authenticated git and gh against a local
TLS fake, verified rotation and refusal after clearing, and scanned the complete
disk and all referenced snapshot chunks for both tokens. That test passed in
232.00 seconds. Its gh configuration file existed only in the tmpfs mount.
