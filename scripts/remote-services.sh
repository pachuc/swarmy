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
# Service names come from the shared list so installs and teardown agree.
mapfile -t services < <(swarmy_service_names)
for service in "${services[@]}"; do
    sudo tee "/etc/systemd/system/swarmy-$service.service" >/dev/null <<UNIT
[Unit]
Description=Swarmy $service
Requires=swarmy-stack.service
After=network-online.target swarmy-stack.service

[Service]
Type=exec
User=$service_user
WorkingDirectory=$repo_dir
EnvironmentFile=/etc/swarmy/node.env
Environment=HOME=$service_home
Environment=TOKIO_WORKER_THREADS=2
ExecStart=/usr/local/bin/swarmy-$service
Restart=always
RestartSec=2
TimeoutStopSec=40

[Install]
WantedBy=multi-user.target
UNIT
done
sudo systemctl daemon-reload
# Reload the same namespace configuration in the execution node.
sudo systemctl restart swarmyd.service
for service in "${services[@]}"; do
    sudo systemctl enable "swarmy-$service.service"
    sudo systemctl restart "swarmy-$service.service"
done
for service in "${services[@]}"; do
    invocation=$(sudo systemctl show -p InvocationID --value "swarmy-$service")
    ready=false
    message="$service ready"
    if [[ $service == scheduler ]]; then message="scheduler started"; fi
    for _ in {1..60}; do
        if sudo journalctl "_SYSTEMD_INVOCATION_ID=$invocation" --no-pager | grep -q "$message"; then
            ready=true
            break
        fi
        sleep 1
    done
    if [[ $ready != true ]]; then
        sudo journalctl -u "swarmy-$service" -n 30 --no-pager
        exit 1
    fi
done
echo 'scheduler, worker, gateway, and api ready on node and enabled at boot'
