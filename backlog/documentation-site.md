# Documentation site

Recorded 2026-09-21.

## What it is

A public documentation site for swarmy: getting started, concepts (agents,
sessions, computers, swarms, topologies), operating a swarm, the provider
guide, the API reference served from the OpenAPI document, and the design
document. A twelve-page prototype exists at `~/code/swarmy-website/docs`
outside this repository.

## Why it matters

Once install is one command and the API is public, people who did not write
swarmy will use it, and the repository's `docs/` directory is written for
contributors, not users.

## Why not now

The commands and concepts it would document are changing through the swarm
model, single-binary, and cloud-topology goals. Writing user documentation
before those land means writing it twice. The contributor docs in `docs/` and
the getting-started section of the README carry the load until then.

## What it would take

1. Move the prototype into this repository under `site/` or keep it separate
   and pull `docs/` from here at build time; decide based on whether the
   design document and API reference should be versioned with the code
   (they should, which argues for this repository).
2. A static site generator that renders Markdown and an OpenAPI document.
   mdBook is the Rust ecosystem's usual choice for Markdown; the API
   reference can be a page embedding the same viewer the API serves at
   `/v1/docs`.
3. A publish step in the release workflow so the site tracks releases, with a
   "latest" and a per-version path.
4. Rewrite `docs/swarms.md`, `docs/providers.md`, and the agent lifecycle
   document for a reader who is not a contributor, and keep `docs/DESIGN.md`
   as the design chapter.

## When to pick it up

When the `swarm-model` goal's documentation rewrite is done and the install
script exists, so the site's first page can be followed by a stranger.
