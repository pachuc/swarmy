# Desktop graphics and image inputs

## What the desktop images give an agent

`base-desktop` and `swarmy-dev` both give an agent a browser, an X11 desktop,
and OpenGL and Vulkan through one shared setup script,
`images/common/desktop-setup.sh`. `base-desktop` adds Blender. `swarmy-dev`
adds the Rust toolchain and is the image every fleet worker runs. Without a
GPU, rendering runs on the CPU through Mesa's llvmpipe and lavapipe. Mesa is
the standard open-source OpenGL and Vulkan implementation on Linux. In a
sandbox with a card, the display runs on the card instead.

## GPU sandboxes

On a GPU node, a sandbox with a GPU requirement sees the node's NVIDIA cards ([GPU nodes](swarms.md#gpu-nodes)). The device nodes let any process in the sandbox run work on the card and see other processes using it; the driver keeps their memory apart. For a fleet of the owner's own agents this is acceptable; use `dedicated` when a sandbox must not share the card. The display stack described below still renders in software.

## The display image

Xvfb is an X11 display server that draws into memory, rather than on a physical
monitor. Each image runs Xvfb on `:99` at 1280x800 and 24-bit color on the
software path. Openbox places and decorates windows. PID 1 first runs
`/usr/local/libexec/swarmy-display-select`, writes the answer to
`/run/swarmy-display/renderer`, and keeps the chosen display alive.

The recipe's `[sandbox] display = true` becomes the stored `image_display` flag.
It also installs a checksum-verified, pinned Chromium snapshot, not Ubuntu's
snap-based Chromium package. Chromium binds its debugging port only to
`127.0.0.1:9222` inside the sandbox. Build and inspect the image following
[the image README](../images/base-desktop/README.md).

## The GPU display and the software fallback

The images install no NVIDIA package. CDI (Container Device Interface) describes
the device nodes and library mounts a container needs. On a GPU node, the
sandbox's CDI edits mount the host driver's GLX and EGL libraries, `nvidia_drv.so`,
`libglxserver_nvidia.so`, `nvidia_icd.json`, and `10_nvidia.json` into it.
The selector chooses `nvidia` only when `/dev/nvidia0`, the X driver, the X GLX
module, and the GLX client library are all present. Otherwise it chooses
`software`.

For NVIDIA, PID 1 runs Xorg as container root using
`/etc/X11/swarmy-nvidia.conf`, a 1280x800 virtual screen,
`UseDisplayDevice "None"`, and `AllowEmptyInitialConfiguration`. Its log is
`/run/swarmy-display/Xorg.99.log`. The software Xvfb path is unchanged.
The glvnd dispatch library routes OpenGL calls to the vendor selected by the
X server. The Vulkan loader reads ICD files, small JSON descriptions of driver
libraries, and offers the devices those drivers report.

`/etc/swarmy/environment` sets `DISPLAY`, `LIBGL_ALWAYS_SOFTWARE`, and
`GALLIUM_DRIVER`. Only Mesa reads the last two, so NVIDIA's libraries ignore
them. Vulkan is not pinned: NVIDIA's Vulkan driver is visible in a GPU sandbox,
and lavapipe is the only device in a CPU sandbox. The `chromium` wrapper pins
Chromium to software rendering only on the software display. An agent can run
`cat /run/swarmy-display/renderer` to check the choice.

The NVIDIA path has not run on a card yet. Elastic GPU end-to-end acceptance
validates it on an L4. If Xorg keeps failing there, PID 1 keeps restarting it
and the renderer file still says `nvidia`; there is no silent fallback.

## Browser and screen tools

Chromium DevTools protocol (CDP) exchanges JSON over a WebSocket to navigate,
inspect and capture a browser page. The accessibility tree is a structured list
of roles and names used by assistive technology; `browser_snapshot` prints it
with short element refs such as `[e3]`. Refs remain stable within a page but
expire on navigation. Prefer snapshots for controls and text, screenshots for
visual layout.

The node runs the Python helper `/usr/local/libexec/swarmy-browser` for eight
browser tools and two screen tools: `browser_navigate`, `browser_snapshot`, `browser_click`,
`browser_type`, `browser_select`, `browser_scroll`, `browser_screenshot`, and
`browser_evaluate`. `browser_select` takes an option value or visible label.
`browser_evaluate` executes page JavaScript; use it only for pages the agent
is authorized to control. `screen_screenshot` uses scrot to capture
all of Xvfb, while `screen_windows` uses wmctrl to list windows by title.
Snapshots are limited to 32 KiB, screenshots to 5 MiB, and each helper call
to 40 seconds. The worker advertises these ten tools and their prompt only when
the agent's image has the display flag; the node rejects display calls on
other images.

## Image inputs

The helper returns PNG bytes as base64. Tool-result screenshots request high image detail so small text is not downscaled by the provider. The node stores them in a tool blob and
replaces inline bytes in the completed event with `image_object_key` and
`image_media_type`. The worker loads those bytes into a user image part for
models whose catalog lists image input. Text-only models receive an omission
note instead. The summarizer budgets 4,800 tokens per image. Download a
completed call's image with `swarmy session image SESSION_ID CALL_ID --output FILE`;
the API only serves images referenced by that session's own completed tool
calls. Reading an image file with `read` also produces a downloadable image. The scripted fake model accepts image parts too, and its scripts can
resolve accessibility refs through `{{ref:LABEL}}` placeholders using earlier
tool-result text.

## Memory sizing

The recipe defaults to 3072 MiB; `swarmy agent create --memory` can override
it. The node reserves that amount against its capacity. Chromium dominates
memory use; Xvfb's 1280x800x24 framebuffer is about 4 MB and Blender adds its
own usage. The desktop proof records the sandbox's `memory.peak` in its
`summary.json` evidence. On the software display no GPU memory is involved. On the NVIDIA display the X framebuffer and GPU programs use card memory.

## How it is verified

`python3 -m unittest discover -s images/common/tests -p 'test_*.py'` runs the
selector against a fake `/dev` and fake library directories. The last step of
the desktop proof checks the renderer file, `glxinfo -B`, and
`vulkaninfo --summary` on the software path.


`python3 images/base-desktop/tests/browser-helper.py` checks the helper against
a fake CDP endpoint without a display. `python3
images/base-desktop/tests/live-smoke.py` exercises Chromium, Xvfb and Blender
inside an actual desktop sandbox. `swarmy-chaos --scenario desktop` runs a scripted
agent against an app in its sandbox: the root test `swarmy-chaos --test desktop`
is wired through `scripts/node-suites/root-suites-plus.sh`. Its harness checks
the app's own record, tool calls, PNG pixels, and exact image bytes in provider
requests. The fake cannot interpret pixels.

`scripts/desktop-agent-check.sh` checks that the chosen model advertises image
input. It then runs the same task using a real vision model,
checks the recorded browser/screen-only task window, downloads the screenshots,
and compares the model's reported canvas code to the app's record. Only this
real-model run proves that the model read pixels. Example:

```sh
scripts/desktop-agent-check.sh --swarm NAME --provider anthropic --model MODEL
```

## Not covered

Audio and multiple displays are separate work. The NVIDIA display path is validated only by the elastic GPU end-to-end acceptance on an L4.
