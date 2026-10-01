# One-command install and check for swarmy.
#
#   make install         install the client plus every service binary into ~/.cargo/bin
#   make install-client  install the chat client, provisioning, and auth helper (needs no libfdb_c)
#   make install-core    install the service binaries (need libfdb_c)
#   make install-node    install headless client, services, and swarmyd (requires root)
#   make dev-tools       install FoundationDB, NATS, and SeaweedFS under ~/.local
#   make models          regenerate the provider and model catalog
#   make check           every pull-request CI job below, in CI order (start the dev stack first)
#   make check-lint      identifier, docs, anyhow, and ast-grep checks; fmt, clippy, cargo doc
#   make check-deps      cargo deny (licenses, bans, sources) and cargo machete
#   make check-workspace-tests  workspace tests except swarmy-e2e
#   make check-e2e       gateway, scheduler, and worker end-to-end suites, then reduced chaos
#   make check-cli-session      the cli_session end-to-end suite
#   make check-remote    remote-feature tests for swarmy-cloud and swarmy-cli
#   make check-openapi   OpenAPI compatibility against origin/master
#   make check-scripts   script and fixture tests
#   make check-advisories  cargo deny advisories (CI runs it weekly, not per pull request)
#   make uninstall       remove the installed swarmy binaries
#
# The services link against libfdb_c. SWARMY_FDB_LIB_DIR points the build at the
# directory holding it; when unset, the first directory below that contains the
# library is used. Run `make dev-tools` first on a machine without it.
# The client (swarmy-cli) links no database library and needs no libfdb_c.

CORE_CRATES ?= scheduler worker gateway api
NODE_CRATES ?= swarmyd
CARGO ?= cargo
AST_GREP ?= npx --yes --package @ast-grep/cli@0.45.3 ast-grep
FDB_CANDIDATES := $(HOME)/.local/lib /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
FDB_LIB_DIR ?= $(SWARMY_FDB_LIB_DIR)
ifeq ($(strip $(FDB_LIB_DIR)),)
FDB_LIB_DIR := $(firstword $(foreach dir,$(FDB_CANDIDATES),$(if $(wildcard $(dir)/libfdb_c.so $(dir)/libfdb_c.dylib),$(dir),)))
endif

.PHONY: help install install-client install-core install-node dev-tools uninstall fdb-check models \
	check check-lint check-deps check-workspace-tests check-e2e check-cli-session \
	check-remote check-openapi check-scripts check-advisories

help:
	@sed -n '2,20p' Makefile | sed 's/^# \{0,1\}//'

fdb-check:
	@if [ -z "$(FDB_LIB_DIR)" ]; then \
		echo "libfdb_c was not found in: $(FDB_CANDIDATES)"; \
		echo "Run 'make dev-tools' first, or set SWARMY_FDB_LIB_DIR to the directory that holds it."; \
		exit 1; \
	fi
	@echo "Using FoundationDB client library from $(FDB_LIB_DIR)"

# The client links no database library and installs without libfdb_c. The
# `remote` feature compiles the EC2, SSM, S3, and IAM SDKs for provisioning;
# plain cargo builds leave it off for the slimmer node binary.
install-client:
	@echo "==> swarmy-cli"
	@$(CARGO) install --locked --features remote,chat --path "crates/swarmy-cli" || exit 1
	@$(CARGO) install --locked --path "crates/swarmy-devtools" || exit 1
	@echo "Installed: $$(ls $(HOME)/.cargo/bin | grep '^swarmy' | tr '\n' ' ')"
	@$(HOME)/.cargo/bin/swarmy --version

install-core: fdb-check
	@for crate in $(CORE_CRATES); do \
		echo "==> swarmy-$$crate"; \
		SWARMY_FDB_LIB_DIR="$(FDB_LIB_DIR)" $(CARGO) install --locked --path "crates/swarmy-$$crate" || exit 1; \
	done

install: install-client install-core

install-node: install-core
	@$(CARGO) install --locked --no-default-features --path "crates/swarmy-cli"
	@for crate in $(NODE_CRATES); do \
		echo "==> $$crate"; \
		SWARMY_FDB_LIB_DIR="$(FDB_LIB_DIR)" $(CARGO) install --locked --path "crates/$$crate" || exit 1; \
	done

dev-tools:
	scripts/install-dev-tools.sh

models:
	python3 scripts/models/generate.py

# Each check-* target is exactly one CI job: .github/workflows/ci.yml calls
# the target and nothing else, so add or change a CI command here, never in
# the workflow. `make check` runs every pull-request job in CI order.
check-lint:
	scripts/check-public-ids.sh
	scripts/check-docs-accuracy.py
	scripts/check-anyhow-in-libraries.sh
	scripts/check-test-sleep-ban.py
	scripts/check-ast-grep-rules.sh
	$(AST_GREP) scan --config ast-grep/sgconfig.yml --error=unused-suppression
	$(AST_GREP) test --config ast-grep/sgconfig.yml --skip-snapshot-tests
	$(CARGO) fmt --all --check
	$(CARGO) build --locked -p swarmy-cli --no-default-features
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings
	$(CARGO) clippy --locked -p swarmy-cloud --features remote --all-targets -- -D warnings
	$(CARGO) clippy --locked -p swarmy-cli --features remote --all-targets -- -D warnings
	$(CARGO) clippy --locked -p swarmy-llm --no-default-features --all-targets -- -D warnings
	$(CARGO) test --locked -p swarmy-llm --no-default-features
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps --locked

check-deps:
	$(CARGO) deny check licenses bans sources
	$(CARGO) machete --with-metadata

check-workspace-tests:
	$(CARGO) build --workspace --locked
	$(CARGO) test --workspace --locked --exclude swarmy-e2e

check-e2e:
	$(CARGO) build --workspace --locked
	$(CARGO) test --locked -p swarmy-e2e --test gateway --test scheduler --test worker -- --test-threads=1
	scripts/chaos-ci.sh

check-cli-session:
	$(CARGO) build --workspace --locked
	$(CARGO) test --locked -p swarmy-e2e --test cli_session -- --test-threads=1

check-remote:
	$(CARGO) test --locked -p swarmy-cloud --features remote
	$(CARGO) test --locked -p swarmy-cli --features remote -- --skip dev_up_run_recover_reconfigure_and_down

check-openapi:
	scripts/check-openapi-compat.sh origin/master

check-scripts:
	scripts/test-scripts.sh

check-advisories:
	$(CARGO) deny check advisories

# Sequential on purpose: the suites share the dev stack. CI's script-tests
# job starts no stack; locally the stack holds the fixed ports the script
# tests use, so stop it first.
check:
	$(MAKE) check-lint
	$(MAKE) check-deps
	$(MAKE) check-workspace-tests
	$(MAKE) check-e2e
	$(MAKE) check-cli-session
	$(MAKE) check-remote
	$(MAKE) check-openapi
	scripts/dev-stack.sh stop
	$(MAKE) check-scripts

uninstall:
	@for bin in swarmy swarmy-auth swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api swarmyd; do \
		if [ -e "$(HOME)/.cargo/bin/$$bin" ]; then rm -v "$(HOME)/.cargo/bin/$$bin"; fi; \
	done
