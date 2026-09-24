# Desktop image

`swarmy image build images/base-desktop` builds and registers the Ubuntu Noble
`base-desktop:dev` image on x86-64 Linux. Use `--tag NAME` for another tag. This variant keeps
the base-ubuntu developer packages and GitHub credential helper, and adds an
Xvfb display (`:99`, 1280x800), Openbox, Mesa's llvmpipe OpenGL and lavapipe
Vulkan software renderers, DejaVu and Liberation fonts, Blender, and a pinned
upstream Chromium snapshot. Ubuntu Noble's `chromium-browser`
package requires snapd and cannot run in the sandbox, so this image downloads
an official standalone Chromium snapshot instead. It does not install an update
service. Its `chromium` wrapper disables first-run and background update
requests and binds DevTools to `127.0.0.1:9222`; it runs as `agent` and uses
`--no-sandbox` because runc provides the container boundary. DevTools is only
reachable from inside the sandbox. A fresh browser profile is stored under
`/home/agent/.config/chromium-debug` unless `CHROMIUM_USER_DATA_DIR` is set.

The sandbox starts `/usr/local/libexec/swarmy-init` as PID 1. It starts Xvfb
and Openbox and restarts the display within a few seconds if Xvfb dies.
`DISPLAY=:99`, `LIBGL_ALWAYS_SOFTWARE=1`, `GALLIUM_DRIVER=llvmpipe`, and the
lavapipe ICD path are set on container processes. The default memory limit is
3 GiB and `/tmp` is scratch space. The recipe reserves a 12 GiB sparse virtual
disk (12,884,901,888 bytes), compared with base-ubuntu's 8 GiB
(8,589,934,592 bytes). Actual nonzero chunk coverage must be measured on a
node with sudo, debootstrap, and the development object store; it cannot be
inferred from the virtual size.

To add another program, copy this recipe directory to `images/my-desktop`, add
the Ubuntu package to `packages` in `recipe.toml` (or install it in `setup.sh`),
and run `swarmy image build images/my-desktop`. Change `disk_size` if the
program exceeds the available disk; the value must be a multiple of 256 KiB.

On a node with sudo and NBD support, validate in a session on this image:

```sh
xdpyinfo | head
glxinfo -B | grep -i 'renderer string'  # llvmpipe
vulkaninfo --summary | grep -i lavapipe
chromium about:blank &
curl -fsS http://127.0.0.1:9222/json/version
blender --background --python-expr 'import bpy; bpy.context.scene.render.resolution_x = 32; bpy.context.scene.render.resolution_y = 32; bpy.context.scene.render.resolution_percentage = 100; bpy.context.scene.render.filepath = "/tmp/desktop-test.png"; bpy.ops.render.render(write_still=True)'
kill "$(pgrep -x Xvfb | head -1)"
sleep 3
xdpyinfo | head  # the supervisor restarted Xvfb
```

The root-only image and node acceptance suites must also be run on that node
for recipe or sandbox runtime changes. Build output reports the nonzero chunk
coverage and uploaded blocks for comparing size with `base-ubuntu`.
