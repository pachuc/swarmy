#!/usr/bin/env bash
# Install the optional control plane after its configuration and credentials arrive.
set -euo pipefail
for service in scheduler worker gateway; do
    sudo tee "/etc/systemd/system/swarmy-$service.service" >/dev/null <<UNIT
[Unit]
Description=Swarmy $service
Requires=swarmy-stack.service
After=network-online.target swarmy-stack.service

[Service]
Type=exec
User=ubuntu
WorkingDirectory=/home/ubuntu/swarmy
EnvironmentFile=/etc/swarmy/node.env
Environment=HOME=/home/ubuntu
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
for service in scheduler worker gateway; do
    sudo systemctl enable "swarmy-$service.service"
    sudo systemctl restart "swarmy-$service.service"
done
for service in scheduler worker gateway; do
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
echo 'scheduler, worker, and gateway ready on node and enabled at boot'
