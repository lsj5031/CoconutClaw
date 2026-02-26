SHELL := /bin/bash

.PHONY: help build dev release install uninstall start stop restart status logs logs-reflection lint test hooks

help: ## Show this help
	@grep -E '^[a-z][-a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build coconutclaw binary (debug)
	cargo build -p coconutclaw

dev: build ## Alias for build (debug)

release: ## Build optimized release binary
	cargo build -p coconutclaw --release

install: ## Install background services (Linux: systemd user, macOS: launchd)
	cargo run -p coconutclaw -- service install

uninstall: ## Remove background services
	cargo run -p coconutclaw -- service uninstall

start: ## Start runtime service + timers
	cargo run -p coconutclaw -- service start

stop: ## Stop runtime service + timers
	cargo run -p coconutclaw -- service stop

restart: ## Restart runtime service + timers
	$(MAKE) --no-print-directory stop
	sleep 1
	$(MAKE) --no-print-directory start

status: ## Show runtime service status
	cargo run -p coconutclaw -- service status

logs: ## Follow main agent logs (Linux systemd only)
	journalctl --user -u coconutclaw.service -f

logs-reflection: ## Follow nightly reflection logs (Linux systemd only)
	journalctl --user -u coconutclaw-nightly-reflection.service -f

lint: ## Shellcheck maintained shell helpers
	shellcheck scripts/asr.sh scripts/tts.sh

test: ## Rust tests + one injected smoke run
	cargo test
	cargo run -q -p coconutclaw -- once --inject-text "ping"

hooks: ## Install git pre-commit hook
	git config core.hooksPath .githooks
	@echo "git hooks installed from .githooks/"
