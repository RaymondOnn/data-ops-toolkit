# Gemini Developer Context: Data-Ops Toolkit

This document defines the architectural guardrails, coding standards, and monorepo constraints for the `data-ops-toolkit`. Gemini should strictly adhere to these rules when generating code or suggesting refactors.

## 1. Monorepo Architecture

### Structure
- **`apps/`**: Independent, deployable units (e.g., `ingestion`). Each app should have its own `src/` and entry points.
- **`libs/`**: Shared internal libraries (e.g., `libs/file`, `libs/utils`). Libs must remain generic and should not import from `apps/`.
- **Dependency Management**: We use `uv` for all package management.
    - Workspace-wide dependencies are defined in the root.
    - Apps and Libs use `pyproject.toml` for local definitions.
    - Always suggest `uv run ...` or `uv pip install ...` for environment actions.

### Cross-Package Imports
- Use absolute imports: `from libs.file.file import FileClient`.
- Avoid circular dependencies between `libs`.

## 2. Core Tech Stack

- **Python**: 3.11+ (Strict type hinting required).
- **Orchestration**: Ray for distributed task execution.
- **Data Handling**: Polars for high-performance DataFrames (prefer `LazyFrame` for large datasets).
- **Storage I/O**: `fsspec` and `universal-pathlib` (UPath) for protocol-agnostic paths (S3, Local, Azure).
- **Database**: ClickHouse is the primary analytical sink.
- **CLI**: Typer for all command-line interfaces.

## 3. Architecture & Design Patterns

### Design Guardrails
- **Atomic Operations**: State transitions must be atomic (e.g., write temp file -> `fs.mv` to final path).
- **Content-Addressable Storage (CAS)**: Use the `CASArchiveMixin` for long-term archiving to ensure deduplication via SHA-256 hashes.
- **Observer Pattern**: Prefer observers over hard-coding side effects in the core engine (e.g., `self.drive_engine` logic).
- **Guard Clauses**: Use early returns/raises to reduce nesting.

## 4. Coding Standards & Style

### Error Handling
- **No Blank Excepts**: Never use `except:`. Always catch specific exceptions.
- **Contextual Wrapping**: Use `raise NewException(...) from e` to preserve stack traces.
- **Logging**: Use standard logging or Loguru. Use `logger.exception()` in catch blocks to capture full tracebacks.

### Paths & Time
- **UPath**: Never use `os.path` or `pathlib.Path` for data paths. Use `upath.UPath` to ensure compatibility with `s3://` and local paths.
- **UTC Only**: All timestamps must be UTC. Use `libs.utils.dates.get_current_timestamp()`.

### Concurrency
- When using Ray, ensure tasks are idempotently retryable.
- Handle "Zombie" tasks by verifying if the `run_id` directory still exists in `active/` but the Ray worker is dead.

## 5. Testing & Reliability

### Regression Testing
- We use side-by-side comparison (Baseline vs. Candidate).
- Ensure new features include a simulation scenario in `apps/ingestion/src/cli/test.py`.

### Resilience Testing (Chaos)
- Support "Chaos" simulations: worker kills, network latency (`tc`), and disk pressure.
- Always consider `DISK_THRESHOLD_HALT` when writing logic that produces large local files.

## 6. Deployment (PEX)

- Our deployment unit is a **PEX** binary.
- **Layering**: We build `deps.pex` (heavy dependencies) and `app.pex` (lightweight source) separately to optimize CI caching.
- **Atomic Symlinks**: Deployments switch symlinks (`ln -sfn`) to ensure zero-downtime atomic transitions.

---
*When in doubt, prioritize reliability and disk-based state recovery over in-memory speed.*
