# MetadataRepository Test Verification Plan

This document outlines the test verification strategy and test scenarios for `MetadataRepository` in `apps/ingestion/src/repositories/metadata.py`.

---

## 1. Scope & Objectives
- **Unit Isolation**: Verify that `MetadataRepository` correctly forwards configuration to `ServiceFactory.get(**connection_kwargs)` and exposes domain methods.
- **Domain Operations Verification**: Test all core metadata DB operations without needing raw SQL in domain modules:
  1. Task State Store (`get_task_state`, `update_task_status`)
  2. Timeout Monitor (`record_heartbeat`, `fetch_stale_tasks`)
  3. Checkpoints (`save_checkpoint`, `get_latest_checkpoint`)
  4. Schema Evolution (`get_schema_version`, `record_schema_change`)

---

## 2. Test Verification Scenarios

### A. Repository Instantiation & Lazy Service Loading
- **Scenario**: Initialize `repo = MetadataRepository(type="postgres_db", host="localhost", port=5432)`.
- **Verification**:
  - `_db_service` is `None` upon `__init__`.
  - Accessing `repo.db` triggers `ServiceFactory.get(**connection_kwargs)`.
  - Multiple calls to `repo.db` reuse the cached `_db_service` instance.

### B. State Store Operations
1. `get_task_state(task_id)`:
   - **Verification**: Executes `SELECT task_id, run_id, status, updated_at, metadata FROM task_states WHERE task_id = %s`.
   - Returns a `TaskStateRecord` instance when a record exists, or `None` if not found.
2. `update_task_status(task_id, run_id, status, metadata)`:
   - **Verification**: Executes `INSERT ... ON CONFLICT (task_id) DO UPDATE ...` with current UTC timestamp.

### C. Timeout Monitor Operations
1. `record_heartbeat(task_id)`:
   - **Verification**: Executes `UPDATE task_heartbeats SET last_heartbeat = %s WHERE task_id = %s`.
2. `fetch_stale_tasks(timeout_duration)`:
   - **Verification**: Computes cutoff time `utcnow() - timeout_duration` and executes `SELECT task_id FROM task_heartbeats WHERE last_heartbeat < %s AND status = 'RUNNING'`.
   - Returns list of task ID strings.

### D. Checkpoint Operations
1. `save_checkpoint(pipeline_id, partition_key, offset)`:
   - **Verification**: Executes `INSERT INTO checkpoints (pipeline_id, partition_key, checkpoint_offset, created_at) ...`.
2. `get_latest_checkpoint(pipeline_id, partition_key)`:
   - **Verification**: Executes `SELECT checkpoint_offset FROM checkpoints WHERE pipeline_id = %s AND partition_key = %s ORDER BY created_at DESC LIMIT 1`.
   - Returns integer offset or `None`.

### E. Schema Evolution Operations
1. `get_schema_version(dataset_id)`:
   - **Verification**: Executes `SELECT max(version) FROM schema_versions WHERE dataset_id = %s`.
   - Returns version integer or `0` if no record exists.
2. `record_schema_change(dataset_id, version, schema_json)`:
   - **Verification**: Executes `INSERT INTO schema_versions (dataset_id, version, schema_json, applied_at) ...`.

### F. Database Object Mutation Notifications (Observer Pattern)
- **Scenario**: Register a custom listener callback via `repo.subscribe(listener_fn)`.
- **Verification**:
  - When `update_task_status`, `record_heartbeat`, `save_checkpoint`, or `record_schema_change` is invoked, `_notify()` logs the change and dispatches a `ChangeEvent` object.
  - `listener_fn` receives `ChangeEvent` containing `entity_type`, `action`, `entity_id`, `payload`, and `timestamp`.
  - Unsubscribing via `repo.unsubscribe(listener_fn)` prevents further notifications to that callback.

### G. Operational Digest Notification Service & CLI Command
- **Scenario**: Invoke `OperationalDigestService.send_digest()` or `uv run ingestion notify digest -r dev@company.com`.
- **Verification**:
  - Queries `MetadataRepository.get_unnotified_schema_changes()` and `get_unnotified_empty_result_sets()`.
  - Formats HTML & plain-text email sections via `DigestSection` (`msgspec.Struct`).
  - Formats `📭 Empty Result Sets Processed (SUCCESS)` digest section for tasks where `is_empty_result_set=True` in `FULL_MANIFEST`.
  - Sends email via `EmailClient`.
  - Updates notification timestamps via `mark_schema_changes_notified()` and `mark_empty_result_sets_notified()`.
  - Subsequent executions return `False` / report no pending events.

### H. 0-Row Verification & `TaskManifest.is_empty_result_set` Propagation
- **Scenario**: Execute pipeline stage with 0 rows.
- **Verification**:
  - First attempt: `ExecutionStage.validate_row_count()` raises `RollbackRequired` to force re-attempt.
  - Re-attempt: `ExecutionStage.validate_row_count()` sets `task.manifest.is_empty_result_set = True` and returns `True`.
  - `PublishStage`: Detects `task.manifest.is_empty_result_set == True`, logs warning, and safely skips `sink.promote()` / `TRUNCATE TABLE`.

---

## 3. How to Run Unit & CLI Commands

```bash
# Trigger scheduled central digest email
uv run ingestion notify digest -r ops@company.local

# Run pytest for repository & digest tests (when implemented)
uv run pytest apps/ingestion/tests/unit/
```
