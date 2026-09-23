# Sandbox runtime

`SandboxRuntime` implements the compute seam from design section 11, with exec
added for this slice. Shared request/result types live in `swarmy-core`.
`RuncRuntime::open` takes a node state directory and the shared volume server
configuration. Keep one runtime alive for the node and call `shutdown` before
dropping it. The owner must run as root on Linux with runc, ext4 mount tools,
the `passt` package (for `pasta`), `iproute2`, `nsenter`, and free NBD devices.

See [the node agent documentation](../swarmyd/README.md) for configuration,
control protocol, lifecycle behavior, and integration tests.
