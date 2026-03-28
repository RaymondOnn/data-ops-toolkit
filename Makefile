.PHONY: clean-pyc clean-test clean-all help docker-up docker-down docker-rebuild docker-clean docker-logs lint format sync venv-recreate lock

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
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d

docker-down: ## Stop and remove the Docker containers
	docker-compose -f $(DOCKER_COMPOSE_FILE) down

docker-rebuild: ## Rebuild Docker images and restart containers
	docker-compose -f $(DOCKER_COMPOSE_FILE) build --no-cache
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d --force-recreate

docker-clean: ## Stop, remove containers, volumes, and images
	docker-compose -f $(DOCKER_COMPOSE_FILE) down --volumes --rmi all

docker-logs: ## View logs for all Docker services
	docker-compose -f $(DOCKER_COMPOSE_FILE) logs -f

docker-exec-ch: ## Execute a command inside the ClickHouse container
	docker-compose -f $(DOCKER_COMPOSE_FILE) exec clickhouse bash