.PHONY: clean-pyc clean-test clean-all help docker-up docker-down docker-restart docker-rebuild docker-clean docker-logs lint format sync venv-recreate lock

help:
	@echo "Usage: make <target>"
	@echo "  clean-pyc    - Remove python cache files"
	@echo "  clean-test   - Remove test and mypy caches"
	@echo "  clean-all    - Remove all caches and temporary files"
	@echo "  lint         - Run ruff check and format check"
	@echo "  format       - Run ruff fix and format"
	@echo "  sync         - Sync virtual environment using uv"
	@echo "  venv-recreate - Delete and recreate the virtual environment"
	@echo "  lock         - Update the uv.lock file"
	@echo "  typecheck    - Run type checking"

clean-pyc:
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.pyc" -delete

clean-test:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache

clean-all: clean-pyc clean-test
	find . -name ".DS_Store" -delete
	@echo "All temporary caches cleared."

typecheck:
	ty  # Or 'pyright' if ty isn't aliased in your CI

run:
	uv run -m ingestion

lint: ## Run ruff check and format check
	uv run ruff check .
	uv run ruff format --check .

format: ## Run ruff fix and format
	uv run ruff check --fix .
	uv run ruff format .

# --- Environment Management ---

sync: ## Sync virtual environment with current lockfile
	uv sync

venv-recreate: ## Force delete and recreate the .venv directory
	rm -rf .venv
	uv sync

lock: ## Update the uv.lock file
	uv lock
	
# --- Docker Management ---

DOCKER_COMPOSE_FILE := apps/ingestion/infra/environments/local/local.docker-compose.yaml

docker-up: ## Start the Docker containers in detached mode
	ls -ld apps/ingestion/infra/environments/local/garage/garage.toml \
		&& docker compose -f $(DOCKER_COMPOSE_FILE) up -d --remove-orphans \

docker-down: ## Stop and remove the Docker containers
	docker compose -f $(DOCKER_COMPOSE_FILE) down

docker-restart: ## Stop and then start the Docker containers again
	docker compose -f $(DOCKER_COMPOSE_FILE) restart

docker-reset: ## Force a wipe of all local data and restart
	make docker-clean && make docker-up

docker-rebuild: ## Rebuild Docker images and restart containers
	docker compose -f $(DOCKER_COMPOSE_FILE) build --no-cache
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d --force-recreate

ch-img: ## Build only the custom ClickHouse image
	docker compose -f $(DOCKER_COMPOSE_FILE) build clickhouse

docker-clean: ## Stop, remove containers, volumes, and images
	docker compose -f $(DOCKER_COMPOSE_FILE) down --volumes --rmi all
	rm -rf apps/ingestion/infra/environments/local/tmp/clickhouse
	rm -rf apps/ingestion/infra/environments/local/tmp/localstack

docker-logs: ## View logs for all Docker services
	docker compose -f $(DOCKER_COMPOSE_FILE) logs -f

docker-logs-ch-init: ## View logs for the ClickHouse initialization container
	docker compose -f $(DOCKER_COMPOSE_FILE) logs -f clickhouse-init

docker-exec-ch: ## Execute a command inside the ClickHouse container
	docker compose -f $(DOCKER_COMPOSE_FILE) exec clickhouse bash