# One-command install and check for swarmy.
#
#   make install       install the client plus every service binary into ~/.cargo/bin
#   make install-client install the chat client, provisioning, and auth helper (needs no libfdb_c)
#   make install-core   install the service binaries (need libfdb_c)
#   make install-node  install headless client, services, and swarmyd (requires root)
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
	scripts/check-public-ids.sh
	$(CARGO) fmt --all --check
	$(CARGO) build --locked -p swarmy-cli --no-default-features
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings
	$(CARGO) clippy --locked -p swarmy-cloud --features remote --all-targets -- -D warnings
	$(CARGO) clippy --locked -p swarmy-cli --features remote --all-targets -- -D warnings
	$(CARGO) test --locked -p swarmy-llm --no-default-features
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps --locked
	$(CARGO) install cargo-deny --version 0.20.2 --locked
	$(CARGO) deny check
	$(CARGO) install cargo-machete --version 0.9.2 --locked
	$(CARGO) machete
	$(CARGO) build --workspace --locked
	$(CARGO) test --workspace --locked --exclude swarmy-e2e
	$(CARGO) test --locked -p swarmy-e2e --test gateway --test scheduler --test worker -- --test-threads=1
	scripts/chaos-ci.sh
	$(CARGO) test --locked -p swarmy-e2e --test cli_session -- --test-threads=1
	$(CARGO) test --locked -p swarmy-cloud --features remote
	$(CARGO) test --locked -p swarmy-cli --features remote -- --skip dev_up_run_recover_reconfigure_and_down
	scripts/check-openapi-compat.sh origin/master
	scripts/test-scripts.sh

uninstall:
	@for bin in swarmy swarmy-auth swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api swarmyd; do \
		if [ -e "$(HOME)/.cargo/bin/$$bin" ]; then rm -v "$(HOME)/.cargo/bin/$$bin"; fi; \
	done
