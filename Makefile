COMPOSE := docker compose -f infra/compose/docker-compose.yml
UV := uv

.DEFAULT_GOAL := help

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## --- local development (no containers needed) -------------------------------

install: ## Sync the uv workspace
	$(UV) sync

test: ## Run the test suite (embedded Postgres, no Docker required)
	$(UV) run pytest

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

## --- pipeline ---------------------------------------------------------------

load-once: ## Run one micro-batch load outside Airflow, against the running stack
	WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse \
	REDPANDA_BOOTSTRAP=localhost:19092 \
	$(UV) run python -m loader.cli load --topic product.feature_usage

dbt: ## Build the dbt models against the running warehouse
	cd dbt && $(UV) run --with dbt-postgres dbt build --profiles-dir .

.PHONY: help install test lint fmt up up-full down nuke logs ps ddl load-once dbt
