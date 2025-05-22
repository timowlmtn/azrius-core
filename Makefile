# Makefile for azrius-core

# ─── Variables ───────────────────────────────────────────────────────────────
CORE_PATH       ?= .
PYTHON          ?= python3

PYTHONPATH  := $(abspath $(CORE_PATH)/python/src):$(PYTHONPATH)
export PYTHONPATH

PG_CONN         ?= $(error Please set PG_CONN, e.g. export PG_CONN="postgresql://user:pass@host:port/db")
MIGRATION_FILE  := sql/migrations/V0001__create_iowa_liquor_sales.sql
LOAD_SCRIPT     := web/app/core/load_cvs_to_db.py
LOAD_DIR        := data/lincoln_liquors
SCHEMA          := lincoln_liquors
DATE_FMT        := MM/dd/yyyy
TABLE_MAP       := '{"Iowa_Liquor_Sales.csv":"iowa_liquor_sales"}'
DBT_DIR         := dbt_ragtime
DBT_TARGET      := dev_postgres
DBT_MODEL       := weekly_sales

# ─── Meta ────────────────────────────────────────────────────────────────────
.PHONY: help deps install lint format test docs clean migrate load-data dbt-run data

help:
	@echo "Available targets:"
	@echo "  deps         Install Python dependencies"
	@echo "  install      Install azrius-core package in editable mode"
	@echo "  lint         Run flake8 on Python source"
	@echo "  format       Format Python source with black"
	@echo "  test         Run pytest suite"
	@echo "  docs         Build documentation site"
	@echo "  clean        Remove build/artifact caches"
	@echo "  migrate      Apply SQL migration to create iowa_liquor_sales table"
	@echo "  load-data    Load CSV into Postgres via Spark"
	@echo "  dbt-run      Run dbt model '$(DBT_MODEL)'"
	@echo "  data         Run migrate → load-data → dbt-run"

# ─── Python / Lint / Tests ───────────────────────────────────────────────────
deps:
	@$(PYTHON) -m pip install --upgrade pip
	@$(PYTHON) -m pip install -r python/requirements.txt

install:
	@$(PYTHON) -m pip install -e python/src

lint:
	@$(PYTHON) -m flake8 python/src

format:
	@$(PYTHON) -m black python/src

test:
	@$(PYTHON) -m pytest python/tests

# ─── Documentation ───────────────────────────────────────────────────────────
docs:
	@mkdocs build -f docs/mkdocs.yml -d docs/site

clean:
	@rm -rf .pytest_cache .mypy_cache python/src/*.egg-info docs/site

# ─── Data Pipeline ───────────────────────────────────────────────────────────
migrate:
	@echo "⏳ Applying SQL migration: $(MIGRATION_FILE)"
	@psql $(PG_CONN) -f $(MIGRATION_FILE)

load-data:
	@echo "⏳ Loading CSV into Postgres via Spark"
	@$(PYTHON) $(LOAD_SCRIPT) \
		--db_type postgres \
		--use-spark \
		--load_dir $(LOAD_DIR) \
		--schema $(SCHEMA) \
		--date_format "$(DATE_FMT)" \
		--truncate \
		--table_name '$(TABLE_MAP)'

dbt-run:
	@echo "⏳ Running dbt model: $(DBT_MODEL)"
	@cd $(DBT_DIR) && dbt run --select $(DBT_MODEL) --target $(DBT_TARGET)

data: migrate load-data dbt-run
	@echo "✅ Data pipeline complete!"

run-ui:
	streamlit run python/src/ui/streamlit_forecast_ui.py