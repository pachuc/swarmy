# One-command install and check for swarmy.
#
#   make install       build and install every swarmy binary into ~/.cargo/bin
#   make install-node  also install swarmyd (only useful on a machine with root)
#   make dev-tools     install FoundationDB, NATS, and SeaweedFS under ~/.local
#   make models        regenerate the provider and model catalog
#   make check         the three CI commands: fmt, test, clippy
#   make uninstall     remove the installed swarmy binaries
#
# The store links against libfdb_c. SWARMY_FDB_LIB_DIR points the build at the
# directory holding it; when unset, the first directory below that contains the
# library is used. Run `make dev-tools` first on a machine without it.

CRATES ?= cli scheduler worker gateway
NODE_CRATES ?= swarmyd
CARGO ?= cargo
FDB_CANDIDATES := $(HOME)/.local/lib /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
FDB_LIB_DIR ?= $(SWARMY_FDB_LIB_DIR)
ifeq ($(strip $(FDB_LIB_DIR)),)
FDB_LIB_DIR := $(firstword $(foreach dir,$(FDB_CANDIDATES),$(if $(wildcard $(dir)/libfdb_c.so $(dir)/libfdb_c.dylib),$(dir),)))
endif

.PHONY: help install install-node dev-tools check uninstall fdb-check models

help:
	@sed -n '2,12p' Makefile | sed 's/^# \{0,1\}//'

fdb-check:
	@if [ -z "$(FDB_LIB_DIR)" ]; then \
		echo "libfdb_c was not found in: $(FDB_CANDIDATES)"; \
		echo "Run 'make dev-tools' first, or set SWARMY_FDB_LIB_DIR to the directory that holds it."; \
		exit 1; \
	fi
	@echo "Using FoundationDB client library from $(FDB_LIB_DIR)"

install: fdb-check
	@for crate in $(CRATES); do \
		echo "==> swarmy-$$crate"; \
		SWARMY_FDB_LIB_DIR="$(FDB_LIB_DIR)" $(CARGO) install --locked --path "crates/swarmy-$$crate" || exit 1; \
	done
	@echo "Installed: $$(ls $(HOME)/.cargo/bin | grep '^swarmy' | tr '\n' ' ')"
	@$(HOME)/.cargo/bin/swarmy --version

install-node: install
	@for crate in $(NODE_CRATES); do \
		echo "==> $$crate"; \
		SWARMY_FDB_LIB_DIR="$(FDB_LIB_DIR)" $(CARGO) install --locked --path "crates/$$crate" || exit 1; \
	done

dev-tools:
	scripts/install-dev-tools.sh

models:
	python3 scripts/models/generate.py

check:
	bash scripts/test-remote-s3-env.sh
	$(CARGO) fmt --all --check
	$(CARGO) test --workspace --locked
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings

uninstall:
	@for bin in swarmy swarmy-session swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api swarmyd; do \
		if [ -e "$(HOME)/.cargo/bin/$$bin" ]; then rm -v "$(HOME)/.cargo/bin/$$bin"; fi; \
	done
