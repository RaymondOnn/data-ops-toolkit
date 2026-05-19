set shell := ["bash", "-cu"]

# Load environment variables from .env if it exists
import? ".env"

# Resolve the path to the just executable to allow calling recipes from within recipes
just := just_executable()

# Path to the local infrastructure stack
docker_compose_file := "apps/ingestion/infra/environments/local/local.docker-compose.yaml"

# Base docker compose command to reduce repetition and ensure project isolation
dc := "docker compose -p " + project_name + " -f " + docker_compose_file

# Unique project name to prevent worktree collisions
project_name := "toolkit-" + file_name(justfile_directory())

# Display all available recipes
help:
    @just --list

# --- Cleanup ---

# Completely reset the local development environment
reset-dev: docker-clean clean-workspace bootstrap
    @echo "♻️  Development environment fully reset."

# Tail the orchestrator's JSONL execution stream in the terminal
watch-stream:
    tail -f .workspace/state/execution_stream.jsonl | uv run python -m json.tool

# Remove python cache files
clean-pyc:
    find . -name "__pycache__" -type d -exec rm -rf {} +
    find . -name "*.pyc" -delete

# Remove test and mypy caches
clean-test:
    rm -rf .pytest_cache .mypy_cache .ruff_cache

# Remove all caches and temporary files
clean-all: clean-pyc clean-test
    find . -name ".DS_Store" -delete
    rm -rf .workspace
    @echo "All temporary caches cleared."

# Remove the local execution workspace (logs, state, and active runs)
clean-workspace:
    @rm -rf .workspace
    @echo "🗑️  Local workspace cleared."

# --- Validation & Linting ---

# Run type checking (using ty/pyright via uv)
typecheck:
    uv run ty

# Run ruff check and format check
lint:
    uv run ruff check .
    uv run ruff format --check .

# Run ruff fix and format
format:
    uv run ruff check --fix .
    uv run ruff format .

# --- Environment Management ---

# Run all tests using pytest
test *args:
    @echo "🧪 Running test suite..."
    uv run pytest {{args}}

# Sync virtual environment with current lockfile
sync:
    @uv sync --quiet || (echo "⚠️  Dependency issues detected (possibly yanked packages). Self-healing lockfile..." && uv lock --upgrade && uv sync)
    @echo "✅ Environment synced and healthy."

# Force delete and recreate the .venv directory
venv-recreate:
    rm -rf .venv
    uv sync

# Update the uv.lock file
lock:
    uv lock

# --- Docker Management ---

# Check if Docker daemon is running and attempt to start it if not
ensure-docker:
    @if ! docker info > /dev/null 2>&1; then \
        echo "🐳 Docker daemon is not running. Attempting to start..."; \
        if [[ "$OSTYPE" == "darwin"* ]]; then \
            open --background -a Docker; \
        elif command -v systemctl > /dev/null; then \
            sudo systemctl start docker; \
        else \
            echo "❌ Auto-start not supported for this OS. Please start Docker manually."; \
            exit 1; \
        fi; \
        echo -n "⏳ Waiting for Docker to initialize..."; \
        for i in {1..30}; do \
            if docker info > /dev/null 2>&1; then \
                echo " Done! ✅"; \
                exit 0; \
            fi; \
            echo -n "."; \
            sleep 2; \
        done; \
        echo " ❌ Timed out waiting for Docker."; \
        exit 1; \
    fi

# Start the Docker containers in detached mode
docker-up: ensure-docker
    {{dc}} up -d --remove-orphans

# Stop and remove the Docker containers
docker-down:
    {{dc}} down

# Stop and then start the Docker containers again
docker-restart:
    {{dc}} restart

# Force a wipe of all local data and restart
docker-reset: docker-clean docker-up

# Rebuild Docker images and restart containers
docker-rebuild:
    {{dc}} build --no-cache
    {{dc}} up -d --force-recreate

# Build only the custom ClickHouse image
ch-img: ensure-docker
    {{dc}} build clickhouse

# Stop, remove containers, volumes, and images, and wipe local data
docker-clean:
    @echo -n "Wiping Docker environment and named volumes..."
    {{dc}} down --volumes --rmi all || true
    @echo -n "Cleaning up local workspace and legacy temp folders..."
    rm -rf .workspace
    rm -rf apps/ingestion/infra/environments/local/.tmp/localstack
    @echo -n "Data volumes wiped."

# View logs for all Docker services
docker-logs:
    {{dc}} logs -f

# Wait for ClickHouse HTTP interface to be ready
wait-for-ch:
    @echo -n "⏳ Waiting for ClickHouse health check..."
    @until curl -s "http://localhost:8123/ping" | grep -q "Ok"; do \
        printf "."; \
        sleep 1; \
    done
    @echo " ✅ ClickHouse is healthy."

# Enter the ClickHouse SQL client
ch-sql:
    {{dc}} exec clickhouse clickhouse-client -u default --password password

# Force-refresh ClickHouse metadata (Tables, Views, UDFs) without wiping data
ch-init:
    @echo "🔄 Refreshing ClickHouse metadata..."
    {{dc}} exec clickhouse bash /docker-entrypoint-initdb.d/init_databases.sh

# Tail the ClickHouse execution log for real-time audit visibility
ch-logs:
    {{dc}} exec clickhouse clickhouse-client -u default --password password -q "SELECT * FROM META.EXECUTION_LOG ORDER BY LAST_UPDATED_AT_TS_LC DESC LIMIT 50"

# --- Ray Management ---

# Show Ray cluster status and active resources
ray-status:
    uv run ray status

# Open the Ray Dashboard in the default browser
ray-dashboard:
    open http://localhost:8265

# --- Tooling Setup ---

# Initialize the Garage S3 buckets and API keys
init-garage:
    @echo "🏗️  Initializing Garage S3 storage..."
    bash apps/ingestion/infra/environments/local/garage/init-garage.sh

# Bootstrap the local environment (Sync deps + Infrastructure)
bootstrap: sync docker-up wait-for-ch ch-init
    @echo "✨ Local development environment is ready!"

# Start infrastructure and run the application in one command
# Usage: just local <job_id> <dataset_id> <partition_date> [clean=true]
# Example: just local test_job orders 2026-04-24 clean=true
local-run job_id dataset_id partition_date="" clean="false":
    if [ "{{clean}}" != "false" ]; then {{just}} clean-workspace; fi
    just docker-up
    just wait-for-ch
    @echo "🚀 Launching application..."
    uv run python -m apps.ingestion.src.main run --job-id {{job_id}} --dataset {{dataset_id}} {{partition_date}}

# Start infrastructure and run the Orchestrator in daemon (Always-On) mode
# Usage: just serve [clean=true]
local-serve clean="false":
    clear
    if [ "{{clean}}" != "false" ]; then {{just}} clean-workspace; fi
    just docker-up
    just wait-for-ch
    @echo "🤖 Starting Orchestrator in ALWAYS-ON mode..."
    uv run python -m apps.ingestion start --debug