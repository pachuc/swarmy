# Cloud credentials

Cloud keys create machines before a swarm exists. They are stored on the client,
separate from `swarmy auth set`, whose credential store lives inside a running
swarm. The file is `~/.local/share/swarmy/cloud-credentials.toml` by default;
`XDG_DATA_HOME` moves it. It is written mode 0600 and refused if readable by
other users.

```sh
swarmy cloud login hetzner-cloud            # paste the token at the hidden prompt
swarmy cloud login hetzner-s3               # access key, then secret key (made in the Hetzner console)
swarmy cloud login aws --region eu-central-1
swarmy cloud ls
swarmy cloud logout aws
```

Pipe one value per line for unattended use, for example
`printf '%s\n' "$HCLOUD_TOKEN" | swarmy cloud login hetzner-cloud`.
Secrets are never accepted as arguments: arguments can appear in shell history
and process lists. Ctrl-C during a hidden prompt may leave terminal echo off;
run `stty echo` or `reset` to restore it.

| Entry | OpenTofu environment variables |
| --- | --- |
| hetzner-cloud | `HCLOUD_TOKEN` |
| hetzner-s3 | `MINIO_USER`, `MINIO_PASSWORD` |
| aws | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_REGION` |

Values reach OpenTofu only through the environment of each run. The Hetzner S3
endpoint comes from the bucket location in the swarm specification, not from
credentials.
