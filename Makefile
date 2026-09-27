# One-command install and check for swarmy.
#
#   make install       install the client plus every service binary into ~/.cargo/bin
#   make install-client install the chat client, provisioning, and auth helper (needs no libfdb_c)
#   make install-core   install the service binaries (need libfdb_c)
#   make install-node  also install swarmyd (only useful on a machine with root)
#   make dev-tools     install FoundationDB, NATS, and SeaweedFS under ~/.local
#   make models        regenerate the provider and model catalog
#   make check         the CI commands: fmt, test, clippy, plus the remote-feature pass
#   make uninstall     remove the installed swarmy binaries
#
# The services link against libfdb_c. SWARMY_FDB_LIB_DIR points the build at the
# directory holding it; when unset, the first directory below that contains the
# library is used. Run `make dev-tools` first on a machine without it.
# The client (swarmy-cli) links no database library and needs no libfdb_c.

CORE_CRATES ?= scheduler worker gateway api
NODE_CRATES ?= swarmyd
CARGO ?= cargo
FDB_CANDIDATES := $(HOME)/.local/lib /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
FDB_LIB_DIR ?= $(SWARMY_FDB_LIB_DIR)
ifeq ($(strip $(FDB_LIB_DIR)),)
FDB_LIB_DIR := $(firstword $(foreach dir,$(FDB_CANDIDATES),$(if $(wildcard $(dir)/libfdb_c.so $(dir)/libfdb_c.dylib),$(dir),)))
endif

.PHONY: help install install-client install-core install-node dev-tools check uninstall fdb-check models

help:
	@sed -n '2,12p' Makefile | sed 's/^# \{0,1\}//'

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

check:
	python3 -m unittest discover -s benchmarks -p 'test_*.py'
	python3 images/base-desktop/tests/browser-helper.py
	python3 crates/swarmyd/tests/files_test.py
	bash scripts/test-remote-s3-env.sh
	bash scripts/test-check-openapi-compat.sh
	bash scripts/test-remote-upgrade.sh
	$(CARGO) fmt --all --check
	$(CARGO) test --workspace --locked
	# The workspace test and clippy leave the opt-in `remote` feature off;
	# build the provisioning client once and test and lint it with it on.
	$(CARGO) test --locked -p swarmy-cloud --features remote
	$(CARGO) test --locked -p swarmy-cli --features remote
	# The cloud provider gates must not rot: this build refuses Bedrock,
	# Gemini, and Azure with a clear error instead of failing to compile.
	$(CARGO) test --locked -p swarmy-llm --no-default-features
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings
	$(CARGO) clippy --locked -p swarmy-cloud --features remote --all-targets -- -D warnings
	$(CARGO) clippy --locked -p swarmy-cli --features remote --all-targets -- -D warnings

uninstall:
	@for bin in swarmy swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api swarmyd; do \
		if [ -e "$(HOME)/.cargo/bin/$$bin" ]; then rm -v "$(HOME)/.cargo/bin/$$bin"; fi; \
	done
