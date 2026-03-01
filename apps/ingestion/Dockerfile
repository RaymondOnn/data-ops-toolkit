# STAGE 1: Build the PEX executable
FROM python:3.11-slim as builder

# Install uv and pex
RUN pip install uv pex

WORKDIR /monorepo

# Copy the entire monorepo context so uv can resolve local workspace links
# (In a real production setup, you might optimize this copy to cache layers better)
COPY . .

# 1. Generate a requirements.txt using uv
# We point to the specific app's pyproject.toml
RUN uv pip compile apps/ingestion/pyproject.toml -o requirements.txt

# 2. Build the PEX file
# -r: requirements file
# -c: script entry point (defined in pyproject.toml [project.scripts])
# -o: output file
# --python-shebang: ensures it runs with the system python
RUN pex -r requirements.txt \
    --sources-directory apps/ingestion/src \
    --sources-directory libs/clients \
    -c ingestion \
    -o /app.pex \
    --python-shebang="/usr/bin/env python3"

# STAGE 2: Runtime
FROM python:3.11-slim

WORKDIR /app

# Copy only the single executable file
COPY --from=builder /app.pex ./app.pex

# Expose port
EXPOSE 8000

# Run the PEX file directly
ENTRYPOINT ["./app.pex"]
