SHELL := /bin/bash
UNITS := coconutclaw.service coconutclaw-heartbeat.service coconutclaw-heartbeat.timer coconutclaw-nightly-reflection.service coconutclaw-nightly-reflection.timer
SYSTEMD_DIR := $(HOME)/.config/systemd/user

.PHONY: help build install uninstall start stop restart status logs logs-reflection lint test

help: ## Show this help
	@grep -E '^[a-z][-a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build coconutclaw binary
	cargo build -p coconutclaw

install: ## Install systemd units and enable linger
	mkdir -p $(SYSTEMD_DIR)
	cp systemd/coconutclaw*.service systemd/coconutclaw*.timer $(SYSTEMD_DIR)/
	systemctl --user daemon-reload
	systemctl --user enable $(UNITS)
	loginctl enable-linger $(USER)
	@echo "units installed"

uninstall: stop ## Remove systemd units
	systemctl --user disable $(UNITS) 2>/dev/null || true
	cd $(SYSTEMD_DIR) && rm -f $(UNITS)
	systemctl --user daemon-reload

start: ## Start coconutclaw + timers
	systemctl --user start coconutclaw.service
	systemctl --user start coconutclaw-heartbeat.timer
	systemctl --user start coconutclaw-nightly-reflection.timer
	@$(MAKE) --no-print-directory status

stop: ## Stop coconutclaw + timers
	systemctl --user stop coconutclaw.service coconutclaw-heartbeat.timer coconutclaw-nightly-reflection.timer 2>/dev/null || true

restart: ## Restart coconutclaw + timers
	$(MAKE) --no-print-directory stop
	sleep 1
	$(MAKE) --no-print-directory start

status: ## Show unit status
	@systemctl --user status $(UNITS) --no-pager 2>/dev/null || true

logs: ## Follow main agent logs
	journalctl --user -u coconutclaw.service -f

logs-reflection: ## Follow nightly reflection logs
	journalctl --user -u coconutclaw-nightly-reflection.service -f

lint: ## Shellcheck maintained shell helpers
	shellcheck agent.sh scripts/asr.sh scripts/heartbeat.sh scripts/nightly_reflection.sh scripts/tts.sh

test: ## Rust tests + one injected smoke run
	cargo test
	cargo run -q -p coconutclaw -- once --inject-text "ping"
