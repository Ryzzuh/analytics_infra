# docker compose reads `.env` from the *project directory* — the directory holding the compose
# file — not from the repo root, where `.env.example` tells you to put it. So every knob in that
# file (SIM_ACCOUNTS, SIM_RATE, DOMAIN, LOADER_MAX_RECORDS...) was silently ignored: compose fell
# back to the defaults baked into docker-compose.yml and nothing said otherwise. Found by setting
# DOMAIN and watching Caddy carry on serving `localhost`.
#
# --env-file rather than --project-directory: the latter also re-bases every relative path, so
# the `../../` build contexts resolve two levels too high and every service builds from the
# wrong directory. Only passed when the file exists, since compose errors on a missing one.
ENV_FILE := $(wildcard .env)
COMPOSE := docker compose $(if $(ENV_FILE),--env-file $(ENV_FILE)) -f infra/compose/docker-compose.yml
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

test-db-stop: ## Stop the embedded Postgres servers the tests reuse between runs
	@# The fixtures reuse one data directory per role and stop the servers on teardown, so this
	@# is only needed after a run is killed outright (teardown goes through atexit, which
	@# SIGKILL skips). Each abandoned server holds a System V shared memory id; macOS allows 32
	@# in total, and past that initdb fails with "No space left on device".
	@pkill -TERM -f "pgserver/pginstall/bin/postgres -D" 2>/dev/null || true
	@sleep 2
	@echo "embedded postgres servers running: $$(pgrep -f 'pgserver/pginstall/bin/postgres -D' | wc -l | tr -d ' ')"
	@echo "shared memory segments:            $$(ipcs -m 2>/dev/null | grep -c '^m' || echo 0)"

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

## --- console ----------------------------------------------------------------

console-install: ## Install the Console's dependencies
	cd console && npm install

console-build: ## Build the Console (static bundle served by Caddy)
	cd console && npm run build

console-dev: ## Run the Console against a local control plane on :8006
	cd console && npm run dev

## --- infrastructure ---------------------------------------------------------

TF := terraform -chdir=infra/terraform

infra-check: ## Validate everything that describes the live instance (no cloud calls)
	$(TF) init -backend=false -input=false >/dev/null
	$(TF) validate
	$(TF) fmt -check -recursive
	DOMAIN=example.com ACME_EMAIL=ops@example.com DEMO_PASSCODE_HASH='$$2a$$14$$x' \
		caddy validate --config infra/caddy/Caddyfile --adapter caddyfile
	actionlint .github/workflows/*.yml
	./scripts/check-secrets.sh

plan: ## Show what would change on the live instance (reads cloud state; creates nothing)
	$(TF) init -backend-config=backend.hcl -input=false
	$(TF) plan

apply: ## CREATES BILLABLE RESOURCES (~EUR 21/month). Asks for confirmation.
	$(TF) apply

destroy: ## Destroys the live instance and its volume. The data goes with it.
	$(TF) destroy

cost: ## Print the monthly cost estimate for the current configuration
	$(TF) output monthly_cost_estimate_eur

## --- secrets ----------------------------------------------------------------

secrets-edit: ## Decrypt, open in $$EDITOR, re-encrypt on save
	sops secrets/prod.yaml

secrets-encrypt: ## Encrypt a freshly written secrets/prod.yaml in place
	sops -e -i secrets/prod.yaml

secrets-check: ## Fail if any plaintext secret is about to be committed
	./scripts/check-secrets.sh

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

.PHONY: help install test test-all test-slowest test-db-stop test-alerts console-install console-build console-dev infra-check plan apply destroy cost \
	secrets-edit secrets-encrypt secrets-check lint fmt up up-full down nuke logs ps ddl load-once dbt \
	history history-estimate golden restore
