#!/usr/bin/env bash
# Usage: nbd-orphans.sh — detach NBD devices whose owning process is gone.
# An interrupted nbd or node test can leave /dev/nbdN attached and mounted with
# no server behind it; later tests that expect the device free then fail.
set -u
for dir in /sys/block/nbd*; do
  pid=$(cat "$dir/pid" 2>/dev/null) || continue
  kill -0 "$pid" 2>/dev/null && continue
  dev=/dev/$(basename "$dir")
  for mount in $(awk -v d="$dev" '$1 == d {print $2}' /proc/mounts); do
    sudo umount -l "$mount"
  done
  # NBD_DISCONNECT (0xab08) then NBD_CLEAR_SOCK (0xab04); no nbd-client needed.
  sudo python3 -c "import fcntl, os; f = os.open('$dev', os.O_RDWR); [fcntl.ioctl(f, r) for r in (0xab08, 0xab04)]" 2>/dev/null
  echo "== detached orphaned $dev (owner $pid gone)"
done
