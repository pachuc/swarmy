#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-upgrade-lib.sh"
upgrade_args stack all 600
upgrade_args node services-only 1
for args in 'node all -1' 'node all x' 'bad all 60' 'stack bad 60' 'stack all'; do
    read -r -a parts <<< "$args"
    if upgrade_args "${parts[@]}" >/dev/null 2>&1; then
        echo "accepted invalid upgrade arguments: $args" >&2
        exit 1
    fi
done
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
printf 'before' > "$work/old"
printf 'before' > "$work/new"
if binary_changed "$work/old" "$work/new"; then
    echo 'treated matching hashes as changed' >&2
    exit 1
fi
printf 'after' > "$work/new"
binary_changed "$work/old" "$work/new"
binary_changed "$work/absent" "$work/new"
if binary_changed "$work/old" "$work/missing" >/dev/null 2>&1; then
    echo 'accepted missing upgrade candidate' >&2
    exit 1
fi
systemctl() { [[ $1 == show ]] && printf '%s\n' "$PPID"; }
sudo() { [[ $1 == -n ]] && shift; "$@"; }
cp /proc/$PPID/exe "$work/running"
if unit_needs_restart fake.service "$work/running"; then
    echo 'restarted an unchanged process' >&2
    exit 1
fi
printf 'different binary' > "$work/running"
unit_needs_restart fake.service "$work/running"
systemctl() { printf '0\n'; }
unit_needs_restart fake.service "$work/running"
printf 'remote upgrade arguments and hashes: ok\n'
token_dir="$work/api-token"
mkdir -p "$token_dir"
# A control node provisioned before token provisioning gets a non-empty token.
[[ $(ensure_api_token "$token_dir/missing.toml") == generated ]]
first=$(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/missing.toml")
[[ -n $first ]]
[[ $(stat -c %a "$token_dir/missing.toml") == 600 ]]
# Rerunning the upgrade path keeps the existing token.
[[ $(ensure_api_token "$token_dir/missing.toml") == unchanged ]]
[[ $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/missing.toml") == "$first" ]]
# Empty and whitespace-only tokens are replaced; other sections are preserved.
printf '[api]\ntoken = ""\n[remote]\nregion = "us-east-1"\n' > "$token_dir/empty.toml"
[[ $(ensure_api_token "$token_dir/empty.toml") == generated ]]
[[ -n $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/empty.toml") ]]
grep -q 'region = "us-east-1"' "$token_dir/empty.toml"
printf '[api]\ntoken = "   "\n' > "$token_dir/blank.toml"
[[ $(ensure_api_token "$token_dir/blank.toml") == generated ]]
[[ -n $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/blank.toml") ]]
# A section without a token gains one; an existing token never rotates.
printf '[api]\nlisten = "127.0.0.1:8742"\n' > "$token_dir/notoken.toml"
[[ $(ensure_api_token "$token_dir/notoken.toml") == generated ]]
[[ -n $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/notoken.toml") ]]
grep -q 'listen = "127.0.0.1:8742"' "$token_dir/notoken.toml"
printf '[api]\ntoken = "already-set"\n' > "$token_dir/kept.toml"
[[ $(ensure_api_token "$token_dir/kept.toml") == unchanged ]]
[[ $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/kept.toml") == already-set ]]
printf 'api token provisioning: ok\n'
