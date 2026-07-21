CARGO ?= cargo
INSTALL ?= install
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
DESTDIR ?=
RELEASE_BINARY ?= target/release/flywheel

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
LLVM_PREFIX ?= $(shell brew --prefix llvm 2>/dev/null)
ifneq ($(strip $(LLVM_PREFIX)),)
LIBCLANG_PATH ?= $(LLVM_PREFIX)/lib
DYLD_FALLBACK_LIBRARY_PATH ?= $(LLVM_PREFIX)/lib
export LIBCLANG_PATH
export DYLD_FALLBACK_LIBRARY_PATH
endif
endif

.DEFAULT_GOAL := build

.PHONY: build release install check fmt fmt-check lint test ci clean help

build: ## Build the development binary.
	$(CARGO) build

release: ## Build the optimized production binary from the locked dependency graph.
	$(CARGO) build --release --locked

install: release ## Install the production binary (PREFIX=/usr/local by default).
	$(INSTALL) -d "$(DESTDIR)$(BINDIR)"
	$(INSTALL) -m 0755 "$(RELEASE_BINARY)" "$(DESTDIR)$(BINDIR)/flywheel"

check: ## Type-check all targets without producing binaries.
	$(CARGO) check --all-targets

fmt: ## Format Rust sources.
	$(CARGO) fmt --all

fmt-check: ## Verify Rust formatting without changing files.
	$(CARGO) fmt --all -- --check

lint: ## Run Clippy and reject every warning.
	$(CARGO) clippy --all-targets -- -D warnings

test: ## Run the hermetic test suite.
	$(CARGO) test

ci: fmt-check lint test ## Run every CI quality gate.

clean: ## Remove Cargo build output.
	$(CARGO) clean

help: ## List available targets.
	@awk 'BEGIN {FS = ":.*## "; print "Flywheel targets:"} /^[a-zA-Z_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
