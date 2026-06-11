# Job Configuration Guide

The Ingestion Engine uses a hierarchical configuration system. Every run is driven by a `TaskContext` which can be defined in YAML and validated via `python -m ingestion test config`.

## Sample Configuration (`job.yaml`)

```yaml
job_id: "sales_sync"
dataset_id: "daily_orders"

# Optional: Stop execution after a specific stage (e.g., stop after 'write')
# to_stage: "write"

extract:
  type: "s3"
  resource: "s3://production-lake/sales/"
  config:
    region: "us-east-1"
    # Secrets are resolved JIT on the worker (ADR 011)
    aws_access_key_id: "${S3_ACCESS_KEY}"

transform:
  transform_type: "sql_standard"
  transform_params:
    query: "SELECT * FROM self WHERE status = 'COMPLETED'"

load:
  sink_type: "clickhouse"
  destination: "analytics.orders_fact"
  partition_by: "order_date"
  config:
    host: "clickhouse.internal"

archive:
  enabled: true
  type: "s3"
  base_path: "s3://archive-vault/ingestion/"
  config:
    region: "us-east-1"
    aws_access_key_id: "${S3_ARCHIVE_KEY}"
  retention_days: 365
```

## Validation Decisions
1. **Type Strictness:** `msgspec` is used to enforce that `retention_days` is an integer and `enabled` is a boolean.
2. **JIT Secrets:** Any value starting with `${VAR}` is treated as a secret and is not resolved until the task reaches the Ray Worker node.
3. **Path Determinism:** The `job_id` and `dataset_id` are used to build the immutable vault path: `data/{job_id}/{dataset_id}/{date}/{run_id}`.
