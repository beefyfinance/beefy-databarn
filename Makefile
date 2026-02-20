.PHONY: help infra dbt dlt grafana clickhouse superset api deps-check setup dev
.DEFAULT_GOAL := help

# Prevent execution in production (user "databarn")
CURRENT_USER := $(shell whoami 2>/dev/null)
ifeq ($(CURRENT_USER),databarn)
    $(error This Makefile should not be run in production. Use infra/prod/Makefile instead.)
endif

ROOT_DIR := $(shell pwd)
DC := docker compose -f $(ROOT_DIR)/infra/dev/docker-compose.yml --env-file $(ROOT_DIR)/.env
UV := uv run --env-file $(ROOT_DIR)/.env

# Main help
help: ## Show this help message
	@echo "Beefy Databarn - Available commands:"
	@echo ""
	@echo "Usage: make <command> [subcommand]"
	@echo ""
	@$(MAKE) -s --no-print-directory dlt
	@$(MAKE) -s --no-print-directory infra
	@$(MAKE) -s --no-print-directory dbt
	@$(MAKE) -s --no-print-directory grafana
	@$(MAKE) -s --no-print-directory clickhouse
	@$(MAKE) -s --no-print-directory superset
	@$(MAKE) -s --no-print-directory api
	@echo "Dependencies:"
	@echo "  make deps-check          Check for outdated dependencies"
	@echo ""
	@echo "Workflows:"
	@echo "  make setup               Initial setup (copy .env, install deps)"
	@echo "  make start               Start infrastructure and initialize"
	@echo "  make dev                 Full development workflow"

# Infrastructure commands - using subcommands
infra:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		start) \
			echo "Starting infrastructure services (rebuilding images if needed)..."; \
			$(DC) up -d --build; \
			echo "✓ Infrastructure services started"; \
			$(MAKE) -s _print-urls \
			;; \
		build) \
			echo "Rebuilding infrastructure images..."; \
			$(DC) build; \
			echo "✓ Infrastructure images rebuilt" \
			;; \
		stop) \
			echo "Stopping infrastructure services..."; \
			$(DC) down \
			;; \
		restart) \
			echo "Restarting infrastructure services..."; \
			$(DC) restart; \
			echo "✓ Infrastructure services restarted" \
			;; \
		logs) \
			$(DC) logs -f \
			;; \
		ps) \
			$(DC) ps \
			;; \
		help|"") \
			echo "Infrastructure:"; \
			echo "  make infra start         Start infrastructure services (rebuilds if needed)"; \
			echo "  make infra build         Rebuild infrastructure images"; \
			echo "  make infra stop          Stop infrastructure services"; \
			echo "  make infra restart       Restart infrastructure services"; \
			echo "  make infra logs          View infrastructure logs"; \
			echo "  make infra ps            Show service status"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make infra [start|build|stop|restart|logs|ps|help]"; \
			exit 1 \
			;; \
	esac

# dbt commands - using subcommands
dbt:
	@cd dbt && unset VIRTUAL_ENV && \
	SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	MODEL="$(word 3,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		run) \
			if [ -n "$$MODEL" ]; then \
				echo "Running dbt model: $$MODEL..."; \
				$(UV) dbt run --select $$MODEL --show-all-deprecations; \
			else \
				echo "Running dbt models..."; \
				$(UV) dbt run --show-all-deprecations; \
			fi \
			;; \
		test) \
			if [ -n "$$MODEL" ]; then \
				echo "Running dbt tests for model: $$MODEL..."; \
				$(UV) dbt test --select $$MODEL --write-json; \
			else \
				echo "Running dbt tests..."; \
				$(UV) dbt test --write-json; \
			fi \
			;; \
		compile) \
			echo "Compiling dbt models..."; \
			$(UV) dbt compile \
			;; \
		refresh) \
			MODEL="$(word 3,$(MAKECMDGOALS))" && \
			if [ -n "$$MODEL" ]; then \
				echo "Refreshing dbt model: $$MODEL..."; \
				$(UV) dbt run --select $$MODEL --full-refresh --show-all-deprecations; \
			else \
				echo "Refreshing all dbt models (full-refresh)..."; \
				$(UV) dbt run --full-refresh --show-all-deprecations; \
			fi; \
			echo "dbt refresh completed successfully"; \
			;; \
		sql) \
			echo "Compiling and showing SQL (no queries executed)..."; \
			if [ -n "$$MODEL" ]; then \
				$(UV) dbt compile --select $$MODEL > /dev/null 2>&1 && \
				COMPILED_FILE=$$(find target/compiled/beefy_databarn/models -name "$$MODEL.sql" -type f | head -1) && \
				if [ -n "$$COMPILED_FILE" ]; then \
					echo "=== Compiled SQL for $$MODEL ===" && \
					cat "$$COMPILED_FILE"; \
				else \
					echo "Error: Could not find compiled SQL for $$MODEL"; \
					exit 1; \
				fi; \
			else \
				$(UV) dbt compile > /dev/null 2>&1 && \
				echo "Compiled SQL files are in target/compiled/beefy_databarn/models/"; \
				find target/compiled/beefy_databarn/models -name "*.sql" -type f | head -10; \
			fi \
			;; \
		sql-explain) \
			if [ -z "$$MODEL" ]; then \
				echo "Usage: make dbt sql-explain <model>"; \
				exit 1; \
			fi; \
			echo "Explaining SQL for model: $$MODEL..."; \
			COMPILED_FILE=$$(find target/compiled/beefy_databarn/models -name "$$MODEL.sql" -type f | head -1) && \
			if [ -z "$$COMPILED_FILE" ]; then \
				echo "Error: Could not find compiled SQL for $$MODEL"; \
				exit 1; \
			fi; \
			echo "EXPLAIN query below in ClickHouse:"; \
			QUERY=$$(sed 's/"/\\"/g' "$$COMPILED_FILE") && \
			echo "========== RAW QUERY =========="; \
			cat "$$COMPILED_FILE"; \
			echo "========== EXPLAIN PIPELINE ==========\n"; \
			$(DC) exec clickhouse clickhouse-client --query="EXPLAIN PIPELINE  $${QUERY}"; \
			echo "========== EXPLAIN PLAN ==========\n"; \
			$(DC) exec clickhouse clickhouse-client --query="EXPLAIN PLAN indexes = 1, description = 1 $${QUERY}"; \
			echo "\n========== END =========="; \
			;; \
		docs) \
			echo "Generating dbt documentation..."; \
			$(UV) dbt docs generate && $(UV) dbt docs serve \
			;; \
		help|"") \
			echo "dbt:"; \
			echo "  make dbt run             Run dbt models"; \
			echo "  make dbt run <model>     Run a specific dbt model"; \
			echo "  make dbt refresh         Full refresh all dbt models"; \
			echo "  make dbt refresh <model> Full refresh a specific dbt model"; \
			echo "  make dbt test            Run dbt tests"; \
			echo "  make dbt compile         Compile dbt models"; \
			echo "  make dbt sql [<model>]   Show compiled SQL (optionally for specific model)"; \
			echo "  make dbt docs            Generate and serve documentation"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make dbt [run [model]|refresh [model]|test [model]|compile|sql [model_name]|docs|help]"; \
			exit 1 \
			;; \
	esac

# dlt commands - using subcommands (infra/dlt/set_dlt_env.sh maps .env to DLT env vars)
dlt:
	@cd dlt && unset VIRTUAL_ENV && . ../infra/dlt/set_dlt_env.sh && \
	SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	SOURCE="$(word 3,$(MAKECMDGOALS))" && \
	RESOURCE="$(word 4,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		run) \
			if [ -n "$$RESOURCE" ] && [ -n "$$SOURCE" ]; then \
				echo "Running dlt source: $$SOURCE, resource: $$RESOURCE..."; \
				$(UV) ./$${SOURCE}_pipeline.py $$RESOURCE; \
			elif [ -n "$$SOURCE" ]; then \
				echo "Running dlt source: $$SOURCE..."; \
				$(UV) ./$${SOURCE}_pipeline.py; \
			elif [ -z "$$SOURCE" ]; then \
				echo "Running all dlt pipelines..."; \
				$(UV) ./all_pipeline.py; \
			else \
				echo "Usage: make dlt run <source> [resource]"; \
				exit 1; \
			fi \
			;; \
		loop) \
			if [ -n "$$RESOURCE" ] && [ -n "$$SOURCE" ]; then \
				echo "Looping dlt pipeline: $$SOURCE, resource: $$RESOURCE..."; \
				$(UV) ./$${SOURCE}_pipeline.py $$RESOURCE --loop; \
			else \
				echo "Usage: make dlt loop <source> <resource>"; \
				exit 1; \
			fi \
			;; \
		info|show|failed-jobs|drop-pending-packages|sync|trace|schema|load-package|mcp) \
			if [ -n "$$SOURCE" ]; then \
				echo "Running: $(UV) dlt pipeline $$SOURCE $$SUBCMD"; \
				$(UV) dlt pipeline -v $$SOURCE $$SUBCMD; \
			else \
				echo "Usage: make dlt <action> <pipeline>"; \
				echo "  action: info, show, failed-jobs, drop-pending-packages, sync, trace, schema, load-package, mcp"; \
				echo "  pipeline: e.g. beefy_db, beefy_api"; \
				exit 1; \
			fi \
			;; \
		drop) \
			if [ -n "$$SOURCE" ]; then \
				echo "Running: $(UV) dlt pipeline $$SOURCE drop $$RESOURCE"; \
				$(UV) dlt pipeline -v $$SOURCE drop $$RESOURCE; \
			else \
				echo "Usage: make dlt drop <pipeline> <resource>"; \
				echo "  pipeline: e.g. beefy_db, beefy_api, github_files"; \
				echo "  resource: e.g. zap_events, harvest_events, vaults, tokens"; \
				exit 1; \
			fi \
			;; \
		help|"") \
			echo "dlt:"; \
			echo "  make dlt run                    Run all dlt pipelines"; \
			echo "  make dlt run <source> [resource]         Run a specific pipeline or resource"; \
			echo "                                  Examples: beefy_db vaults, beefy_api tokens"; \
			echo "  make dlt <action> <pipeline>    Run dlt pipeline command (uvx dlt pipeline ...)"; \
			echo "                                  action: info, show, failed-jobs, drop-pending-packages, sync, trace, schema, drop, load-package, mcp"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make dlt [run <source> [resource]|loop <source> <resource>|<action> <pipeline>|help]"; \
			echo "  source/pipeline: e.g. beefy_db, beefy_api, github_files"; \
			echo "  resource: e.g. feebatch_harvests, vaults, tokens"; \
			echo "  action: info, show, failed-jobs, drop-pending-packages, sync, trace, schema, drop, load-package, mcp"; \
			exit 1 \
			;; \
	esac


# Grafana commands - using subcommands
gf: grafana # alias for grafana
grafana:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		restart) \
			echo "Re-restarting Grafana (restarting service to reload configs)..."; \
			$(DC) restart grafana; \
			echo "✓ Grafana re-restarted" \
			;; \
		help|"") \
			echo "Grafana:"; \
			echo "  make [grafana|gf] restart     Re-restart Grafana (reload configs)"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make [grafana|gf] [restart|help]"; \
			exit 1 \
			;; \
	esac

# ClickHouse commands - using subcommands
ch: clickhouse # alias for clickhouse
clickhouse:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	USER="$(word 3,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		restart) \
			echo "Restarting ClickHouse..."; \
			$(DC) restart clickhouse; \
			echo "✓ ClickHouse restarted" \
			;; \
		client|cli) \
			if [ -n "$$USER" ]; then \
				echo "Opening ClickHouse client shell as user: $$USER..."; \
				$(DC) exec clickhouse clickhouse-client --user $$USER; \
			else \
				echo "Opening ClickHouse client shell (default user)..."; \
				$(DC) exec clickhouse clickhouse-client; \
			fi \
			;; \
		help|"") \
			echo "ClickHouse:"; \
			echo "  make [clickhouse|ch] restart          Re-restart ClickHouse (reload configs)"; \
			echo "  make [clickhouse|ch] client [<user>]  Open ClickHouse client shell (default user)"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make [clickhouse|ch] [restart|client [user]|help]"; \
			exit 1 \
			;; \
	esac

# Superset commands - using subcommands
superset:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		restart) \
			echo "Restarting Superset..."; \
			$(DC) restart superset; \
			echo "✓ Superset restarted" \
			;; \
		logs) \
			$(DC) logs -f superset \
			;; \
		sync|sync-datasources) \
			echo "Syncing Superset datasources (refresh dataset metadata)..."; \
			$(DC) exec superset python3 /usr/local/bin/provision/sync-datasources.py || true; \
			echo "✓ Datasources sync completed" \
			;; \
		help|"") \
			echo "Superset:"; \
			echo "  make superset restart          Restart Superset (reload configs)"; \
			echo "  make superset logs            View Superset logs"; \
			echo "  make superset sync-datasources Refresh dataset metadata from databases"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make superset [restart|logs|sync-datasources|help]"; \
			exit 1 \
			;; \
	esac

# API commands - using subcommands
api:
	@cd api && unset VIRTUAL_ENV && \
	SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		dev) \
			echo "Starting API service in dev mode (with auto-reload)..."; \
			$(UV) uvicorn main:app --host 0.0.0.0 --port 8080 --reload \
			;; \
		help|"") \
			echo "API:"; \
			echo "  make api dev              Start API service in dev mode (with auto-reload)"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make api [dev|help]"; \
			exit 1 \
			;; \
	esac

# Dependencies
deps-check: ## Check for outdated dependencies
	@echo "Checking for outdated dependencies..."
	@echo ""
	@echo "Currently installed packages:"
	@uv pip list
	@echo ""
	@echo "Checking for available updates..."
	@uv pip list --outdated 2>/dev/null || (echo "No outdated packages found (or command not available)" && echo "")
	@echo ""
	@echo "To update dependencies:"
	@echo "  1. Edit pyproject.toml with desired version constraints"
	@echo "  2. Run: uv lock --upgrade"
	@echo "  3. Run: uv sync"

# Setup commands
setup: ## Initial setup (copy .env, install deps)
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✓ Created .env file - please edit it with your credentials"; \
	else \
		echo "✓ .env file already exists"; \
	fi
	@echo "Installing dependencies for each application..."
	@cd dlt && uv sync || echo "Warning: dlt dependencies not installed"
	@cd dbt && uv sync || echo "Warning: dbt dependencies not installed"
	@cd api && uv sync || echo "Warning: api dependencies not installed"
	@echo "✓ Dependencies installed"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit .env with your credentials"
	@echo "  2. make infra start"
	@echo "  3. make dbt run"

dev: ## Full development workflow (setup, start, run dbt)
	@$(MAKE) -s setup
	@$(MAKE) -s infra start
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@$(MAKE) -s dbt run
	@echo ""
	@echo "✓ Development environment ready!"
	@$(MAKE) -s _print-urls

# Shared utility targets
_print-urls:
	@echo ""
	@echo "Access services:"
	echo "  - API: http://localhost:8080/docs" && \
	echo "  - Superset: http://localhost:8088" && \
	echo "  - Traefik Dashboard: http://localhost:8080" && \
	echo "  - ClickHouse: http://localhost:8123 ($${CLICKHOUSE_USER:-default}/$${CLICKHOUSE_PASSWORD:-<set in .env>})" && \
	echo "  - Grafana: http://localhost:3000 ($${GRAFANA_ADMIN_USER:-admin}/$${GRAFANA_ADMIN_PASSWORD:-admin})" && \
	echo "  - Prometheus: http://localhost:9090 (no auth)" && \
	echo "  - RustFS: http://localhost:9001 ($${RUSTFS_ACCESS_KEY:-admin}/$${RUSTFS_SECRET_KEY:-admin})"

# Catch-all target to prevent "No rule to make target" errors
# This allows arguments to be passed to targets without make complaining
%:
	@:

