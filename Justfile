set shell := ["bash", "-cu"]

# Path to the local infrastructure stack
docker_compose_file := "apps/ingestion/infra/environments/local/local.docker-compose.yaml"

# Display all available recipes
help:
    @just --list

# --- Cleanup ---

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
    @echo "All temporary caches cleared."

# --- Validation & Linting ---

# Run type checking (using Pyright via uv)
typecheck:
    uv run pyright

# Run ruff check and format check
lint:
    uv run ruff check .
    uv run ruff format --check .

# Run ruff fix and format
format:
    uv run ruff check --fix .
    uv run ruff format .

# --- Environment Management ---

# Sync virtual environment with current lockfile
sync:
    uv sync

# Force delete and recreate the .venv directory
venv-recreate:
    rm -rf .venv
    uv sync

# Update the uv.lock file
lock:
    uv lock

# --- Docker Management ---

# Start the Docker containers in detached mode
docker-up:
    @ls -ld apps/ingestion/infra/environments/local/garage/garage.toml > /dev/null
    docker compose -f {{docker_compose_file}} up -d --remove-orphans

# Stop and remove the Docker containers
docker-down:
    docker compose -f {{docker_compose_file}} down

# Stop and then start the Docker containers again
docker-restart:
    docker compose -f {{docker_compose_file}} restart

# Force a wipe of all local data and restart
docker-reset: docker-clean docker-up

# Rebuild Docker images and restart containers
docker-rebuild:
    docker compose -f {{docker_compose_file}} build --no-cache
    docker compose -f {{docker_compose_file}} up -d --force-recreate

# Build only the custom ClickHouse image
ch-img:
    docker compose -f {{docker_compose_file}} build clickhouse

# Stop, remove containers, volumes, and images, and wipe local data
docker-clean:
    docker compose -f {{docker_compose_file}} down --volumes --rmi all
    rm -rf apps/ingestion/infra/environments/local/tmp/clickhouse
    rm -rf apps/ingestion/infra/environments/local/tmp/localstack

# View logs for all Docker services
docker-logs:
    docker compose -f {{docker_compose_file}} logs -f

# --- Tooling Setup ---

# Install direnv to local bin (Corporate/Restricted server friendly)
setup-direnv:
    curl -sfL https://direnv.net/install.sh | bash
    @echo "✅ direnv installed to $HOME/.local/bin"
    @echo "⚠️  Action Required: Add the following to your ~/.bashrc:"
    @echo '   export PATH="$HOME/.local/bin:$PATH"'
    @echo '   eval "$(direnv hook bash)"'