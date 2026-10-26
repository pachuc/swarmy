#!/bin/sh
# Keep the shell entry point stable; the Python runner validates tasks and JSON.
set -eu
if [ "$#" -lt 2 ]; then
    echo "usage: $0 SWARM LABEL [--dry-run]   (SWARM is a registered swarm, or local on a swarm's control node)" >&2
    exit 2
fi
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$SCRIPT_DIR/run_swarm.py" "$@"
