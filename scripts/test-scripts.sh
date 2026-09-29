#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m unittest discover -s scripts/fleet -p 'test_*.py'
python3 -m unittest discover -s scripts/models -p 'test_*.py'
python3 -m unittest discover -s scripts/providers -p 'test_*.py'
python3 -m unittest discover -s scripts/benchmarks -p 'test_*.py'
python3 scripts/test_repo_metrics.py
python3 -m unittest discover -s benchmarks -p 'test_*.py'
python3 images/base-desktop/tests/browser-helper.py
python3 crates/swarmyd/tests/files_test.py
bash scripts/test-check-openapi-compat.sh
bash scripts/test-remote-upgrade.sh
bash scripts/test-remote-s3-env.sh
bash scripts/test-dev-stack.sh
bash scripts/remote-provision-test.sh
