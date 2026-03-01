# Ingestion Engine

## Rationale & Architecture

This module implements a robust, state-driven data ingestion workflow designed for scalability, resilience, and auditability.

### Core Design Principles

1.  **Checkpoint-Driven State Machine**:
    The workflow moves data through distinct states: `Start -> Raw -> Transform -> Audit -> Load -> Complete`.
    Between each state, data is persisted as **Parquet** files in a structured folder hierarchy.
    *   **Why?** This ensures resilience. If the `Load` step fails, we do not need to re-ingest or re-transform; we simply resume from the `Audit` checkpoint. It also provides full observability into the data lineage and allows for easy debugging by inspecting the artifacts of any stage.

2.  **Decoupled Compute (Ray)**:
    We use Ray to orchestrate and execute compute tasks.
    *   **Why?** This allows the exact same code to run on a single laptop (for development) or scale to a Kubernetes cluster with hundreds of nodes (for production) without code changes. It handles the distribution of the "Transform" and "Audit" workloads transparently.

3.  **High-Performance Data Ops (Polars)**:
    Polars is used for all data manipulation.
    *   **Why?** Its Rust-based query engine is significantly faster and more memory-efficient than Pandas, which is critical for batch ETL jobs processing large datasets.

4.  **Structured Persistence**:
    Artifacts are stored using a hierarchical structure for easy partitioning and lookup:
    `{state}/{job_id}/{run_id}/{dataset}/proc_{worker_id}.parquet`

### Technology Stack

*   **Orchestration**: Ray
*   **Data Processing**: Polars
*   **Validation**: Pandera (integrated in Audit step)
*   **Storage I/O**: FSSpec (supports S3, GCS, Azure, Local)
*   **Configuration**: Dynaconf
*   **Caching**: Diskcache (for persistent memoization of config/schema)

## Workflow States

| State | Description | Artifact |
| :--- | :--- | :--- |
| **Start** | Initialization, config validation, run ID generation. | `metadata.json` |
| **Raw** | Data acquisition from source (API/DB/File). | `proc_*.parquet` |
| **Transform** | Standardization, cleaning, and enrichment. | `proc_*.parquet` |
| **Audit** | Data quality checks using Pandera schemas. | `proc_*.parquet` |
| **Load** | Persistence to the target destination (DW/Lake). | `receipt.json` |
| **Complete** | Final success marker. | `_SUCCESS` |

## Usage

```bash
python apps/ingestion/src/main.py ingest --source "s3://my-bucket/data.csv" --dataset "sales_data"
```