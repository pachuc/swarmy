set -eu

if [ "$(uname -m)" != x86_64 ]; then
    echo 'base-desktop requires x86-64 (Chromium snapshot and lavapipe ICD)' >&2
    exit 1
fi

# Ubuntu's chromium-browser package is a snap launcher, which cannot work in
# this container. Pin an official Chromium snapshot instead of installing snapd.
revision=1704040
curl --retry 3 -fsSL "https://storage.googleapis.com/chromium-browser-snapshots/Linux_x64/$revision/chrome-linux.zip" -o /tmp/chromium.zip
printf '%s  %s\n' '3077c26bcc11b1e8abad762122a26698e3d620d97dd7d9620a22c52fa9eeb2fa' /tmp/chromium.zip | sha256sum -c -
unzip -q /tmp/chromium.zip -d /opt
rm /tmp/chromium.zip
cat > /usr/local/bin/chromium <<'SH'
#!/bin/sh
set -eu
export DISPLAY=${DISPLAY:-:99}
export LIBGL_ALWAYS_SOFTWARE=1
export VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
# The sandbox is already isolated by runc; Chromium's namespace sandbox is
# unavailable there. Do not run the browser as root.
if [ "$(id -u)" = 0 ]; then
    exec su -p -s /bin/sh agent -c 'exec /usr/local/bin/chromium "$@"' chromium "$@"
fi
exec /opt/chrome-linux/chrome --no-sandbox --no-first-run --no-default-browser-check \
    --disable-background-networking --disable-component-update \
    --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 \
    --user-data-dir="${CHROMIUM_USER_DATA_DIR:-/home/agent/.config/chromium-debug}" "$@"
SH
chmod 755 /usr/local/bin/chromium
mkdir -p /etc/swarmy
cat > /etc/swarmy/environment <<'ENV'
DISPLAY=:99
LIBGL_ALWAYS_SOFTWARE=1
GALLIUM_DRIVER=llvmpipe
VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
ENV
cat > /usr/local/libexec/swarmy-init <<'SH'
#!/bin/sh
# PID 1 keeps the display alive even if an application kills Xvfb.
set -u
export DISPLAY=:99
export LIBGL_ALWAYS_SOFTWARE=1
export GALLIUM_DRIVER=llvmpipe
export VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
trap 'kill "$xpid" "$wpid" 2>/dev/null || :; wait 2>/dev/null || :; exit 0' TERM INT
while :; do
    mkdir -p /tmp/.X11-unix
    chmod 1777 /tmp/.X11-unix
    rm -f /tmp/.X99-lock /tmp/.X11-unix/X99
    setpriv --reuid agent --regid agent --clear-groups Xvfb :99 -screen 0 1280x800x24 -nolisten tcp &
    xpid=$!
    # Wait until clients can connect before starting the window manager.
    while kill -0 "$xpid" 2>/dev/null && ! xdpyinfo -display :99 >/dev/null 2>&1; do
        sleep 0.2
    done
    if ! kill -0 "$xpid" 2>/dev/null; then
        wait "$xpid" 2>/dev/null || :
        sleep 1
        continue
    fi
    setpriv --reuid agent --regid agent --clear-groups openbox &
    wpid=$!
    wait "$xpid" 2>/dev/null || :
    kill "$wpid" 2>/dev/null || :
    wait "$wpid" 2>/dev/null || :
    sleep 1
done
SH
chmod 755 /usr/local/libexec/swarmy-init
