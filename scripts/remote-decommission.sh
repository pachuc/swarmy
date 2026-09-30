#!/usr/bin/env bash
# Stop swarmy services and remove swarmy files on an adopted host.
# Arguments: SERVICE_USER, using the same service paths as provisioning.
# Units and binaries come from the shared lists in remote-provision-env.sh,
# so installs and teardown can never drift apart (a unit missing from the
# shared list would be left running while its binary is deleted).
# Idempotent: every stop, disable, and removal tolerates a missing unit or
# file so a failed `down` can retry. Leaves the service user, the fstab
# line and its mount, local sandbox disk data, and tunnel keys authorized
# on the primary in place; see docs/REMOTE.md for the by-hand removals.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
service_user=${1:?Usage: remote-decommission.sh SERVICE_USER}
validate_service_user "$service_user"
disable_previous_units
mapfile -t units < <(swarmy_unit_names)
for unit in "${units[@]}"; do
    sudo rm -f "/etc/systemd/system/$unit"
done
sudo systemctl daemon-reload
mapfile -t binaries < <(swarmy_binary_names)
for binary in "${binaries[@]}"; do
    sudo rm -f "/usr/local/bin/$binary"
done
sudo rm -f /etc/modules-load.d/swarmy.conf
sudo rm -rf /etc/swarmy
service_repo=$(service_repo_for "$service_user")
sudo rm -rf "$service_repo"
