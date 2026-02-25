SHELL := /bin/bash
UNITS := coconutclaw.service coconutclaw-webhook.service coconutclaw-tunnel.service coconutclaw-heartbeat.service coconutclaw-heartbeat.timer coconutclaw-nightly-reflection.service coconutclaw-nightly-reflection.timer
SYSTEMD_DIR := $(HOME)/.config/systemd/user

.PHONY: help install uninstall start stop restart status logs logs-webhook logs-tunnel webhook-register webhook-unregister webhook-status lint test

help: ## Show this help
	@grep -E '^[a-z][-a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Service lifecycle ──────────────────────────────────────────────

install: ## Install systemd units and enable linger
	mkdir -p $(SYSTEMD_DIR)
	cp systemd/coconutclaw*.service systemd/coconutclaw*.timer $(SYSTEMD_DIR)/
	systemctl --user daemon-reload
	systemctl --user enable $(UNITS)
	loginctl enable-linger $(USER)
	@echo "✓ units installed, linger enabled (services survive logout & start on boot)"

uninstall: stop ## Remove systemd units
	systemctl --user disable $(UNITS) 2>/dev/null || true
	cd $(SYSTEMD_DIR) && rm -f $(UNITS)
	systemctl --user daemon-reload

start: ## Start all services
	systemctl --user start coconutclaw-webhook.service
	systemctl --user start coconutclaw-tunnel.service
	systemctl --user start coconutclaw.service
	systemctl --user start coconutclaw-heartbeat.timer
	systemctl --user start coconutclaw-nightly-reflection.timer
	@$(MAKE) --no-print-directory status

stop: ## Stop all services
	systemctl --user stop coconutclaw.service coconutclaw-webhook.service coconutclaw-tunnel.service coconutclaw-heartbeat.timer coconutclaw-nightly-reflection.timer 2>/dev/null || true

restart: ## Restart all services
	$(MAKE) --no-print-directory stop
	sleep 1
	$(MAKE) --no-print-directory start

status: ## Show service status
	@systemctl --user status $(UNITS) --no-pager 2>/dev/null || true

# ── Logs ───────────────────────────────────────────────────────────

logs: ## Follow agent logs
	journalctl --user -u coconutclaw.service -f

logs-webhook: ## Follow webhook server logs
	journalctl --user -u coconutclaw-webhook.service -f

logs-tunnel: ## Follow tunnel logs
	journalctl --user -u coconutclaw-tunnel.service -f

logs-reflection: ## Follow nightly reflection logs
	journalctl --user -u coconutclaw-nightly-reflection.service -f

# ── Webhook management ─────────────────────────────────────────────

webhook-register: ## Register Telegram webhook
	./scripts/webhook_manage.sh register

webhook-unregister: ## Unregister Telegram webhook (reverts to poll)
	./scripts/webhook_manage.sh unregister

webhook-status: ## Show Telegram webhook info
	./scripts/webhook_manage.sh status

# ── Dev ────────────────────────────────────────────────────────────

lint: ## Shellcheck all scripts
	shellcheck agent.sh scripts/asr.sh scripts/telegram_api.sh scripts/heartbeat.sh scripts/nightly_reflection.sh scripts/tts.sh scripts/webhook_manage.sh scripts/setup.sh lib/common.sh

test: ## Quick smoke test (inject text)
	./agent.sh --inject-text "ping"
