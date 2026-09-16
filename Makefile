COMPOSE := docker compose -f infra/compose/docker-compose.yml
UV := uv

.DEFAULT_GOAL := help

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## --- local development (no containers needed) -------------------------------

install: ## Sync the uv workspace
	$(UV) sync

test: ## Fast tests: loader, CDC, collector, replication, golden (embedded Postgres, no Docker)
	$(UV) run pytest -m "not dbt"

test-all: ## Every test, including the dbt builds (slower: real dbt runs per test)
	$(UV) run pytest

test-slowest: ## Show which tests dominate the runtime
	$(UV) run pytest --durations=15 -q

test-alerts: ## Alert rule unit tests (promtool) + Alertmanager config check
	cd infra/monitoring && promtool test rules rules_test.yml
	amtool check-config infra/monitoring/alertmanager.yml
	promtool check config infra/monitoring/prometheus.yml

lint: ## ruff check + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt: ## Apply formatting
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

## --- stack ------------------------------------------------------------------

up: ## Start the core stack (~4 GB): broker, warehouse, Airflow, collector, simulator
	$(COMPOSE) up -d --build

up-full: ## Core stack plus console/monitoring extras
	$(COMPOSE) --profile full up -d --build

down: ## Stop the stack, keep data
	$(COMPOSE) down

nuke: ## Stop the stack and delete all volumes
	$(COMPOSE) down -v

logs: ## Tail logs (S=service to filter)
	$(COMPOSE) logs -f $(S)

ps: ## Show container status
	$(COMPOSE) ps

ddl: ## Re-apply warehouse DDL (idempotent)
	$(COMPOSE) run --rm warehouse-init

## --- history and golden snapshots ---

history: ## Seed 12 months of history into the backfill topics (see --estimate-only first)
	$(UV) run simulator-history --days 365

history-estimate: ## Report how many events a history seed would produce, and produce none
	$(UV) run simulator-history --days 365 --estimate-only

golden: ## Take a cold golden snapshot of every volume
	./infra/golden/golden.sh snapshot

restore: ## Restore the golden snapshot, then catch up the gap
	./infra/golden/golden.sh restore

## --- pipeline ---------------------------------------------------------------

load-once: ## Run one micro-batch load outside Airflow, against the running stack
	WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse \
	REDPANDA_BOOTSTRAP=localhost:19092 \
	$(UV) run python -m loader.cli load --topic product.feature_usage

dbt: ## Build the dbt models against the running warehouse
	cd dbt && $(UV) run --with dbt-postgres dbt build --profiles-dir .

.PHONY: help install test test-all test-slowest test-alerts lint fmt up up-full down nuke logs ps ddl load-once dbt \
	history history-estimate golden restore
