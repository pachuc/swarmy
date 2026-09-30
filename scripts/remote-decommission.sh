#!/usr/bin/env bash
# Stop swarmy services and remove swarmy files on an adopted host.
# Arguments: SERVICE_USER, using the same service paths as provisioning.
# Thin wrapper over decommission_remove in remote-provision-env.sh, where
# the shared unit and binary table lives; `down` without a checkout pipes
# that file's decommission_probe instead. Idempotent, so a failed `down`
# can retry. Leaves the service user, the fstab line and its mount, local
# sandbox disk data, and tunnel keys authorized on the primary in place;
# see docs/REMOTE.md for the by-hand removals.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
service_user=${1:?Usage: remote-decommission.sh SERVICE_USER}
decommission_remove "$service_user"
