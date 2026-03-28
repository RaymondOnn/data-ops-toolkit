# Project Structure

This repository is organized as a monorepo containing multiple data applications and shared libraries.

## 📂 Repository Layout

```text
data-ops-toolkit/
├── apps/                       # Data Applications
│   ├── ingestion/              # ⚡ 50M Row Ingestion Engine (Ray + Polars)
│   │   ├── config/             # Task-specific YAML configurations
│   │   ├── src/
│   │   │   ├── core/           # Orchestrator, Engine, and Models
│   │   │   ├── services/       # External service adapters (DB, S3, etc.)
│   │   │   └── utils/          # CLI and core utilities
│   │   ├── __main__.py         # CLI Entry point
│   │   └── pyproject.toml
│   ├── rag/                    # RAG pipelines (LLM based)
│   ├── transform/              # Specialized transformation services
│   └── validation/             # Data quality & audit application
│
├── libs/                       # Shared Internal Libraries
│   ├── auth/                   # Centralized Secret Management (Vault/AWS)
│   ├── clients/                # Optimized DB/API clients (Postgres, Oracle, S3)
│   ├── file/                   # High-performance Parquet & CSV handlers
│   ├── resilience/             # Circuit Breakers & Heartbeat logic
│   └── utils/                  # Common logging (structlog) and date helpers
│
├── specs/                      # Architecture ADRs and technical specs
├── pyproject.toml              # Global tool configurations
└── venv/                       # Shared virtual environment (optional)
```

## 🚀 App: Ingestion Engine Deep Dive

The Ingestion Engine (`apps/ingestion/`) is the heart of the toolkit, featuring:
- **`src/core/orchestrator/`**: Polling control loop and Ray actor management.
- **`src/core/models/stages/`**: Pipeline stage definitions (`Raw`, `Transform`, `Audit`, `Write`).
- **`src/core/strategies/`**: Pluggable business logic for ingestion and transformation.

## 🛠️ Shared Libraries

These libraries are designed to be imported by any app in the monorepo:
- **`libs/resilience/`**: Provides the `@protect_service` decorator used across all DB clients.
- **`libs/auth/`**: Injects `Secret` objects into service configurations to prevent plain-text leakages during distributed execution.
- **`libs/file/`**: Abstracts Polars streaming sinks for consistent Parquet handling.
