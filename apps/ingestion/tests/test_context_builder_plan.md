# Unit Test Specification: TaskContext & ContextBuilder

This document defines unit test cases, verification targets, and assertions for `TaskContext`, `TaskContextBuilder`, and `TemplateContext`.

---

## 1. TaskContext Validation (`apps/ingestion/src/core/contexts/task.py`)

### Test Case 1.1: Reject Empty Steps on Initialization
- **Target**: `TaskContext.__post_init__`
- **Setup**: Instantiate a `TaskContext` with `steps=[]`.
- **Expected Outcome**: Raises `ValueError` with message indicating that `TaskContext for dataset '<id>' in job '<id>' has no steps configured. Pipeline execution cannot proceed.`
- **Verify**:
  - Ensures pipeline execution fails fast before writing state files, task queues, or dispatching `StartStage`.

### Test Case 1.2: Valid Steps Initialization
- **Target**: `TaskContext.__post_init__`
- **Setup**: Instantiate a `TaskContext` with at least one `StepContext(id="extract_orders", stage="extract")`.
- **Expected Outcome**:
  - `from_step` defaults to `"start"`.
  - `to_step` defaults to the last step ID (`"extract_orders"`).
  - `get_step_ids()` returns `["start", "extract_orders"]`.
  - `get_next_step_id("start")` returns `"extract_orders"`.

---

## 2. Template Scope Construction (`apps/ingestion/src/core/contexts/builder/template.py`)

### Test Case 2.1: Scoping Job and Dataset Configurations
- **Target**: `build_template_scope`
- **Input**:
  - `job_id="test_job"`
  - `dataset_id="orders"`
  - `run_id="20260905-120000-abcd"`
  - `partition_date="2026-09-05"`
  - `dataset_cfg={"mode": "snapshot", "destination": "TEST.ORDERS", "steps": [...]}`
  - `job_cfg={"source": {"service_ref": "mock_data_folder"}, "mode": "append", "calls": {...}}`
- **Expected Outcome**:
  - `scope["job"]["source"]["service_ref"] == "mock_data_folder"`
  - `scope["job"]` does not leak the `"calls"` key.
  - `scope["dataset"]["destination"] == "TEST.ORDERS"`
  - `scope["dataset"]` does not leak `"steps"` or `"calls"`.
  - Expressions like `{job.source.service_ref}` evaluate to `"mock_data_folder"`.

---

## 3. Two-Phase Template & Service Resolution (`apps/ingestion/src/core/contexts/builder/builder.py`)

### Test Case 3.1: Pre-Service Resolution String Interpolation
- **Target**: `TaskContextBuilder._build_dataset_context`
- **Input YAML Configuration**:
  ```yaml
  job:
    source:
      service_ref: mock_data_folder
    calls:
      extract:
        hooks:
          post:
            - id: archive_src_files
              type: copy
              from:
                service_ref: "{job.source.service_ref}"
  ```
- **Expected Outcome**:
  - In Phase 1 string resolution, `"{job.source.service_ref}"` is interpolated to `"mock_data_folder"`.
  - In Phase 2 service resolution, `_resolve_service_refs` detects `service_ref: "mock_data_folder"` as a string and maps it to `connection: {...}` from `services.yaml`.
  - Does NOT log: `Invalid service_ref type at calls.extract.hooks.post[0].from.service_ref: <class 'dict'>`.

### Test Case 3.2: Post-Service Dynamic String Interpolation
- **Target**: `TaskContextBuilder._build_dataset_context`
- **Input**: Service URL or hook path containing dynamic parameters like `{job_id}` or `{partition_date}`:
  ```yaml
  path: "s3://archive-vault/{job_id}/{partition_date}/{run_id}/raw"
  ```
- **Expected Outcome**:
  - After service reference injection, the second interpolation pass renders all dynamic tokens into concrete strings:
    `s3://archive-vault/test_job/2026-09-05/20260905-120000-abcd/raw`.

---

## 4. Environment-Aware Configuration Loading (`apps/ingestion/src/core/contexts/builder/builder.py`)

### Test Case 4.1: Inherit Defaults under Dynaconf Environment Mode
- **Target**: `TaskContextBuilder.build`
- **Input**: `test_job/config.yaml` structured with `default:` and optional environment overrides (e.g., `dev:`, `local:`).
- **Expected Outcome**:
  - `job_settings.get("datasets")` correctly returns `{"orders": ...}`.
  - `job_settings.get("job")` correctly returns default job configuration.
  - `ctx.steps` contains all configured steps (e.g., extract, transform, write).
  - `ctx.mode` is correctly resolved (e.g. `"snapshot"`).
