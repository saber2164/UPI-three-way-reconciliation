# UPI three-way reconciliation - development entry points.
#
# The stack is a single Docker Compose project ("upi-recon").  Everything here
# works with either `docker compose` (Docker Desktop v2) or the legacy
# `docker-compose` binary; the detection below picks whichever is installed.

SHELL := /bin/bash

COMPOSE := $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; else echo "docker-compose"; fi)
PYTEST := uv run --with pytest pytest

.DEFAULT_GOAL := help

.PHONY: help up down clean logs status submit dbt test integration-test verify ports images

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## build and start the whole stack (Kafka, Postgres, MinIO, Iceberg, Flink, generator, dashboard)
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Dashboard   http://localhost:8503"
	@echo "Flink UI    http://localhost:8087"
	@echo "SQL Gateway http://localhost:8086"
	@echo ""
	@echo "The two Flink jobs are submitted automatically on first start."
	@echo "Use 'make status' to watch the pipeline and 'make dbt' for the gold layer."

down: ## stop the stack, keep the data
	$(COMPOSE) down

clean: ## stop the stack and delete the volumes (Kafka topics, Postgres, Iceberg, checkpoints)
	$(COMPOSE) down -v --remove-orphans

logs: ## tail the logs (SERVICE=flink-taskmanager make logs)
	$(COMPOSE) logs -f --tail=150 $(SERVICE)

status: ## Flink jobs, decision counts, late corrections
	@echo "--- Flink jobs ---"
	@curl -s localhost:8087/jobs/overview | python3 -m json.tool | head -60 || true
	@echo "--- decisions ---"
	@$(COMPOSE) exec -T postgres psql -U app -d fintech -c \
		"select match_status, count(*) from recon.match_results group by 1 order by 2 desc;" || true
	@echo "--- late corrections (RRNs that went MISSING_SETTLEMENT -> MATCHED) ---"
	@$(COMPOSE) exec -T postgres psql -U app -d fintech -c \
		"select count(*) from (select rrn from recon.decision_audit group by rrn \
		 having bool_or(match_status='MISSING_SETTLEMENT') and bool_or(match_status='MATCHED')) x;" || true

submit: ## cancel and resubmit both Flink jobs
	$(COMPOSE) run --rm -e FORCE_RESUBMIT=1 flink-sql-submit

dbt: ## build the gold layer (dbt/DuckDB) against the pinned Iceberg snapshot
	$(COMPOSE) --profile batch run --rm dbt

test: ## unit tests, no Docker required
	$(PYTEST) tests -q -m "not integration"

integration-test: ## needs the stack up and ~25 min of traffic
	POSTGRES_DSN="postgresql://app:app@localhost:5432/fintech" \
		uv run --with pytest --with psycopg2-binary pytest tests -q -m integration

verify: ## unit tests + integration tests (the stack must be up)
	$(MAKE) test
	$(MAKE) integration-test

ports: ## print the published ports
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Ports}}'
