#!/usr/bin/env bash
# Install the optional control plane after its configuration and credentials arrive.
# Arguments: SERVICE_USER, using the same service paths as provisioning.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
service_user=${1:?Usage: remote-services.sh SERVICE_USER}
validate_service_user "$service_user"
service_home=$(service_home_for "$service_user")
repo_dir=$(service_repo_for "$service_user")
[[ $(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) == "$repo_dir" ]] || { echo "Expected checkout at $repo_dir" >&2; exit 1; }
# Service units and their binaries come from the shared table so installs
# and teardown agree. The short name below is only prose for unit
# descriptions and readiness messages.
read_shared_list service_units swarmy_service_units
stack_unit=$(swarmy_mode_backing_unit stack)
node_unit=$(swarmy_agent_unit)
for unit in "${service_units[@]}"; do
    binary=$(swarmy_unit_binary "$unit")
    short=${unit#swarmy-}
    short=${short%.service}
    sudo tee "/etc/systemd/system/$unit" >/dev/null <<UNIT
[Unit]
Description=Swarmy $short
Requires=$stack_unit
After=network-online.target $stack_unit

[Service]
Type=exec
User=$service_user
WorkingDirectory=$repo_dir
EnvironmentFile=/etc/swarmy/node.env
Environment=HOME=$service_home
Environment=TOKIO_WORKER_THREADS=2
ExecStart=/usr/local/bin/$binary
Restart=always
RestartSec=2
TimeoutStopSec=40

[Install]
WantedBy=multi-user.target
UNIT
done
sudo systemctl daemon-reload
# Reload the same namespace configuration in the execution node.
sudo systemctl restart "$node_unit"
for unit in "${service_units[@]}"; do
    sudo systemctl enable "$unit"
    sudo systemctl restart "$unit"
done
for unit in "${service_units[@]}"; do
    invocation=$(sudo systemctl show -p InvocationID --value "$unit")
    ready=false
    short=${unit#swarmy-}
    short=${short%.service}
    message="$short ready"
    if [[ $short == scheduler ]]; then message="scheduler started"; fi
    for _ in {1..60}; do
        if sudo journalctl "_SYSTEMD_INVOCATION_ID=$invocation" --no-pager | grep -q "$message"; then
            ready=true
            break
        fi
        sleep 1
    done
    if [[ $ready != true ]]; then
        sudo journalctl -u "$unit" -n 30 --no-pager
        exit 1
    fi
done
echo 'scheduler, worker, gateway, and api ready on node and enabled at boot'
