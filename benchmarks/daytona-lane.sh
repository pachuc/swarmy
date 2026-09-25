#!/bin/sh
# Keep the shell entry point stable; the Python adapter drives codex-daytona.
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$SCRIPT_DIR/daytona_lane.py" "$@"
