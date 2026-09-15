# Goal 1 feedback: immortal echo agent

Notes from reviewing slice 1 after it was completed on 2026-09-14. Each item
below is something to keep, change, or carry into later slices. Add entries as
you find them; reference crates, files, or pull requests where it helps.

Slice 1 delivered: the dev stack, swarmy-core, swarmy-bus, swarmy-llm with the
ChatGPT subscription provider, swarmy-store, swarmy-harness, the scheduler,
the inference gateway, the step worker, the swarmy CLI, and the swarmy-chaos
failure-injection program. Pull requests 6 through 19.

## What worked well

- 

## What should change

- The ergonomics of testing this locally are awful. There are many paths to add,
  environment variables to set, and magic scripts and commands to know about.
  Streamline and package it better. Specifics seen while running the live test:
  - Binaries only run with LD_LIBRARY_PATH and PATH pointing at ~/.local, and
    they live in target/debug rather than being installed as commands.
  - The dev stack writes bash-only export lines, so fish cannot source them, and
    every command has to be wrapped in bash -c.
  - SWARMY_STORE_DIRECTORY and SWARMY_BUS_PREFIX have to match between the
    services and the CLI or the CLI silently looks in the wrong place.
  - Starting a working system means starting the stack, then three service
    binaries with their own environment, then the CLI; there is no single
    command that brings the whole thing up or tears it down.
  - Running the ChatGPT provider needs its own credential path variable on top
    of all of the above.

- Build a TUI client so a person can talk to an agent across multiple turns on
  this infrastructure. Today the only way in is `swarmy run PROMPT`, which
  creates a fresh session per invocation and exits when it goes idle, so there
  is no way to experience a conversation: send a message, watch the reply
  stream in, send a follow-up into the same session, see the tool calls as they
  happen. The pieces already exist underneath (append a user message, wake the
  session, subscribe to the live feeds, read the log), so this is a client, not
  new infrastructure.

## Questions and open decisions

- 

## Carry into slice 2

- 
