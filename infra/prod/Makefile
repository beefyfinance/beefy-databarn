# Makefile for Docker Compose operations
# This Makefile provides commands for building images and managing the Docker Compose stack
# Uses docker rollout plugin for no-downtime deployments

.PHONY: help build ps logs deploy ensure-dirs restart dbt dlt clickhouse run test client github_files tvls beefy_db beefy_api loop docker
.DEFAULT_GOAL := help

# Paths and common commands
ROOT_DIR := $(abspath ../..)
DC := docker compose -f $(ROOT_DIR)/infra/prod/docker-compose.yml --env-file $(ROOT_DIR)/.env
UV := uv run --env-file $(ROOT_DIR)/.env

# Directories that need to exist for bind mounts (override via .env if present)
STORAGE_DIR ?= /mnt/data
DATA_DIRS := $(STORAGE_DIR)/clickhouse \
             $(STORAGE_DIR)/prometheus \
             $(STORAGE_DIR)/grafana \
             $(STORAGE_DIR)/rustfs \
             $(STORAGE_DIR)/postgres \
             $(STORAGE_DIR)/superset \
             $(STORAGE_DIR)/redis \
             $(STORAGE_DIR)/traefik_certs \
             $(STORAGE_DIR)/dbt \
             $(STORAGE_DIR)/dlt

# Default target
.DEFAULT_GOAL := help

# Show help message
help:
	@echo "Available commands:"
	@echo ""
	@$(MAKE) -s --no-print-directory build
	@$(MAKE) -s --no-print-directory dbt
	@$(MAKE) -s --no-print-directory dlt
	@$(MAKE) -s --no-print-directory clickhouse
	@$(MAKE) -s --no-print-directory infra
	@$(MAKE) -s --no-print-directory docker


infra: ensure-dirs
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		build) \
			SERVICE="$(word 3,$(MAKECMDGOALS))" && \
			if [ -n "$$SERVICE" ]; then \
				echo "Rebuilding infrastructure image: $$SERVICE..."; \
				$(DC) build $$SERVICE; \
				echo "✓ Infrastructure image $$SERVICE rebuilt"; \
			else \
				echo "Rebuilding infrastructure images..."; \
				$(DC) build; \
				echo "✓ Infrastructure images rebuilt"; \
			fi; \
			;; \
		start|up) \
			echo "Starting infrastructure services (rebuilding images if needed)..."; \
			$(DC) up -d; \
			echo "✓ Infrastructure services started"; \
			$(MAKE) -s _print-urls \
			;; \
		stop) \
			echo "Stopping infrastructure services..."; \
			$(DC) down; \
			echo "✓ Infrastructure services stopped"; \
			;; \
		restart) \
			echo "Restarting infrastructure services..."; \
			$(DC) restart; \
			echo "✓ Infrastructure services restarted"; \
			;; \
		logs) \
			$(DC) logs -f; \
			;; \
		ps) \
			$(DC) ps; \
			;; \
		help|"") \
			echo "infra:"; \
			echo "  make infra build [<svc>] Build all images (or only <svc>, e.g. dlt)"; \
			echo "  make infra start         Start infrastructure services (rebuilds if needed)"; \
			echo "  make infra stop          Stop infrastructure services"; \
			echo "  make infra restart       Restart infrastructure services"; \
			echo "  make infra logs          View infrastructure logs"; \
			echo "  make infra ps            Show service status"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make infra [build [<svc>]|start|stop|restart|logs|ps|help]"; \
			exit 1; \
		;; \
	esac

# Run dbt: make dbt run or make dbt test
# Usage: make dbt run or make dbt test
dbt:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		logs) \
			echo "$$SUBCMD dbt..."; \
			$(DC) logs -f dbt; \
		;; \
		restart) \
			echo "Restarting dbt..."; \
			$(DC) up -d --force-recreate --build dbt; \
		;; \
		refresh) \
			MODEL="$(word 3,$(MAKECMDGOALS))" && \
			if [ -n "$$MODEL" ]; then \
				echo "Refreshing dbt model: $$MODEL..."; \
				$(DC) exec dbt /app/run_dbt_with_lock.sh run --full-refresh --select $$MODEL; \
			else \
				echo "Refreshing all dbt models (full-refresh)..."; \
				$(DC) exec dbt /app/run_dbt_with_lock.sh run --full-refresh; \
			fi; \
			echo "dbt refresh completed successfully"; \
		;; \
		run) \
			echo "Running dbt..."; \
			MODEL="$(word 3,$(MAKECMDGOALS))" && \
			if [ -n "$$MODEL" ]; then \
				echo "Running dbt model: $$MODEL..."; \
				$(DC) exec dbt /app/run_dbt_with_lock.sh run --select $$MODEL; \
			else \
				echo "Running all dbt models..."; \
				$(DC) exec dbt /app/run_dbt_with_lock.sh run; \
			fi; \
			echo "dbt run completed successfully"; \
		;; \
		test) \
			echo "Testing dbt..."; \
			MODEL="$(word 3,$(MAKECMDGOALS))" && \
			if [ -n "$$MODEL" ]; then \
				echo "Testing dbt model: $$MODEL..."; \
				$(DC) exec dbt /app/run_dbt_with_lock.sh test --select $$MODEL; \
			else \
				echo "Testing all dbt models..."; \
				$(DC) exec dbt /app/run_dbt_with_lock.sh test; \
			fi; \
			echo "dbt test completed successfully"; \
		;; \
		help|"") \
			echo "dbt:"; \
			echo "  make dbt run [<model>]     Run dbt models"; \
			echo "  make dbt refresh [<model>] Full refresh all dbt models (or a specific model)"; \
			echo "  make dbt test [<model>]    Run dbt tests"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make dbt [run [<model>]|refresh [<model>]|test [<model>]|help]"; \
			exit 1; \
		;; \
	esac

# Run dlt: make dlt run
# Usage: make dlt run
dlt:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		logs) \
			echo "Showing dlt logs..."; \
			$(DC) logs -f dlt; \
		;; \
		restart) \
			echo "Restarting dlt..."; \
			$(DC) up -d --force-recreate --build dlt; \
		;; \
		ps) \
			echo "Showing dlt service status..."; \
			$(DC) ps dlt; \
		;; \
		run) \
			echo "Running dlt..."; \
			SOURCE="$(word 3,$(MAKECMDGOALS))" && \
			RESOURCE="$(word 4,$(MAKECMDGOALS))" && \
			if [ -n "$$RESOURCE" ] && [ -n "$$SOURCE" ]; then \
				echo "Running dlt source: $$SOURCE, resource: $$RESOURCE..."; \
				$(DC) exec dlt /app/infra/dlt/entrypoint.sh uv run /app/dlt/$${SOURCE}_pipeline.py $$RESOURCE; \
			elif [ -n "$$SOURCE" ]; then \
				echo "Running dlt source: $$SOURCE..."; \
				$(DC) exec dlt /app/infra/dlt/entrypoint.sh uv run /app/dlt/$${SOURCE}_pipeline.py; \
			else \
				echo "Usage: make dlt run <source> [resource]"; \
				exit 1; \
			fi; \
		;; \
		loop) \
			echo "Looping dlt..."; \
			SOURCE="$(word 3,$(MAKECMDGOALS))" && \
			RESOURCE="$(word 4,$(MAKECMDGOALS))" && \
			if [ -n "$$RESOURCE" ] && [ -n "$$SOURCE" ]; then \
				echo "Looping dlt source: $$SOURCE, resource: $$RESOURCE..."; \
				$(DC) exec dlt /app/infra/dlt/entrypoint.sh uv run /app/dlt/$${SOURCE}_pipeline.py $$RESOURCE --loop; \
			fi; \
			echo "dlt loop completed successfully"; \
		;; \
		help|"") \
			echo "dlt:"; \
			echo "  make dlt run [<pipeline>]   Run dlt pipeline"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make dlt [run [<pipeline>]|help]"; \
			exit 1; \
		;; \
	esac

# ClickHouse client: make clickhouse client
# Usage: make clickhouse client
ch: clickhouse # alias for clickhouse
clickhouse:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		logs) \
			echo "Showing ClickHouse logs..."; \
			$(DC) logs -f clickhouse; \
		;; \
		restart) \
			echo "Restarting ClickHouse..."; \
			$(DC) up -d --force-recreate --build clickhouse; \
		;; \
		ps) \
			echo "Showing ClickHouse service status..."; \
			$(DC) ps clickhouse; \
		;; \
		client|cli) \
			echo "Opening ClickHouse client..."; \
			$(DC) exec clickhouse clickhouse-client; \
		;; \
		help|"") \
			echo "clickhouse:"; \
			echo "  make clickhouse client   Open ClickHouse client"; \
			echo "" \
			;; \
		*) \
			echo "Usage: make clickhouse [client|help]"; \
			exit 1; \
		;; \
	esac

postgres:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		client|cli) \
			echo "Opening Postgres client..."; \
			$(DC) exec postgres psql"; \
		;; \
		logs) \
			echo "Showing Postgres logs..."; \
			$(DC) logs -f postgres; \
		;; \
		restart) \
			echo "Restarting Postgres..."; \
			$(DC) up -d --force-recreate --build postgres; \
		;; \
		ps) \
			echo "Showing Postgres service status..."; \
			$(DC) ps postgres; \
		;; \
		help|"") \
		;; \
		*) \
			echo "Usage: make postgres [logs|restart|ps|help]"; \
			exit 1; \
		;; \
	esac

# Docker: prune (clean up old Docker images/containers; runs only if all services are up)
docker:
	@SUBCMD="$(word 2,$(MAKECMDGOALS))" && \
	case "$$SUBCMD" in \
		prune) \
			EXPECTED=$$($(DC) config --services | wc -l); \
			echo "Checking all $$EXPECTED services are running..."; \
			RUNNING=$$($(DC) ps -q 2>/dev/null | wc -l); \
			if [ "$$RUNNING" -ne "$$EXPECTED" ]; then \
				echo "Error: expected $$EXPECTED running services, found $$RUNNING. Refusing to prune."; \
				$(DC) ps -a; \
				exit 1; \
			fi; \
			NOT_UP=$$($(DC) ps -a --format '{{.Status}}' | grep -v '^Up' || true); \
			if [ -n "$$NOT_UP" ]; then \
				echo "Error: some containers are not up. Refusing to prune."; \
				$(DC) ps -a; \
				exit 1; \
			fi; \
			echo "✓ All services running. Pruning unused Docker resources..."; \
			docker system prune -af; \
			docker container prune -f; \
			docker image prune -af; \
			echo "✓ Prune completed"; \
		;; \
		help|"") \
			echo "docker:"; \
			echo "  make docker prune   Remove unused Docker containers and dangling images (only if all services are running)"; \
			echo "" \
		;; \
		*) \
			echo "Usage: make docker [prune|help]"; \
			exit 1; \
		;; \
	esac

# Prevent make from treating command arguments as targets
run test client github_files tvls beefy_db beefy_api logs restart ps:
	@:

# Catch-all for any other resource/source names
%:
	@:

# Ensure required directories exist for bind mounts
ensure-dirs:
	@echo "Creating required data directories..."
	@for dir in $(DATA_DIRS); do \
		if [ ! -d "$$dir" ]; then \
			echo "Creating $$dir..."; \
			sudo mkdir -p "$$dir"; \
			sudo chmod 755 "$$dir"; \
		else \
			echo "Directory $$dir already exists"; \
		fi; \
	done
	@echo "✓ All required directories created"

