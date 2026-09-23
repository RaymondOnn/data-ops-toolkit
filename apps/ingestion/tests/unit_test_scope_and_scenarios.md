# Ingestion Engine: Unit Testing Scope & Test Scenarios

This document outlines the required unit test suite coverage, edge cases, and testing strategies for `apps/ingestion`. It serves as the test specification for both current functionality and the object decoupling refactor.

---

## 1. Orchestration & Outcome Management (`src/core/orchestrator/common/task/`)

### A. Outcome Evaluation & State Transitions (`outcome.py`)
Each outcome type must be tested in isolation using pure/mock inputs without requiring a live `TaskManager` or Ray instance:

- **`SuccessOutcome`**
  - [ ] Validates pipeline completion when all configured steps in `TaskContext` match completed steps in `TaskManifest`.
  - [ ] Verifies signal sent is `TaskSignal.DONE`.
  - [ ] Verifies manifest status transitions to `ExecutionStatus.SUCCESS`.
  - [ ] Verifies hot-cache release and dataset lock release via `TimeoutMonitor`.

- **`ProgressOutcome`**
  - [ ] Standard sequential progression (`step_1 -> step_2 -> step_3`).
  - [ ] Boundary enforcement: raises appropriate error if current step is beyond `to_step`.
  - [ ] Non-linear rollback returns:
    - Single-step rollback stack pop (`E -> D -> E`).
    - Multi-step nested rollback stack pop (`E -> C -> B -> C -> E`).
    - Stack preservation when intermediate forward steps execute.
  - [ ] Emits `TaskSignal.SYNC` with updated `current_step_id`.
  - [ ] Pushes updated `TaskMetadata` to task priority queue with correct priority score.

- **`RetryOutcome`**
  - [ ] Transient exception categorization:
    - Retries for `TransientError`, `HostUnreachable`, `ClientCantConnect`, `CircuitOpen`, `TryAgainLater`, `OutOfDiskSpace`.
    - Non-retryable for generic runtime errors or domain validation exceptions.
  - [ ] Retry count increment: verifies `manifest.retry_count` increases by 1.
  - [ ] Exponential backoff calculation: verifies `wait_seconds = min(600, (2 ** retry_count) * 30)`.
  - [ ] Midnight refusal: rejects retries if current UTC timestamp is >= midnight of the partition date.
  - [ ] Marker creation: writes `.retrying` marker JSON to workspace and cleans up `.blocked`.

- **`BlockedOutcome`**
  - [ ] Disk pressure block: verifies `blocked_by="DISK_PRESSURE"` when caused by `OutOfDiskSpace`.
  - [ ] Service dependency block: verifies `blocked_by=<service_name>` when caused by `TryAgainLater`.
  - [ ] Workspace markers: creates `.blocked` marker and removes `.retrying` marker.
  - [ ] Manifest update: sets status to `ExecutionStatus.BLOCKED` with detailed error and metrics.

- **`FailedOutcome`**
  - [ ] Terminal failure recording: sets status to `ExecutionStatus.FAILED`.
  - [ ] Traceback extraction: populates `manifest.error` with error type, message, step ID, and formatted stack trace.
  - [ ] Eviction: purges entry from active cache and releases dataset concurrency locks.
  - [ ] Emits `TaskSignal.FAIL`.

- **`RollbackOutcome`**
  - [ ] Appends caller step to `rollback_stack`.
  - [ ] Records timestamped rollback history for target step ID.
  - [ ] Infinite loop prevention: drops to `FailedOutcome` if target step already exists in `rollback_history`.
  - [ ] Recursion depth limit: drops to `FailedOutcome` if `len(rollback_stack) >= 20` or stack count >= 5.

---

## 2. Stage Execution & Contracts (`src/core/stages/`)

### A. Stage Base Contract (`contracts/stage.py`)
- **Stage Registry Discovery**:
  - [ ] Auto-discovery of all standard stages (`start`, `extract`, `transform`, `write`, `publish`, `archive`) via package scan without hardcoded list.
  - [ ] Custom stage registration via `@ExecutionStage.register(key=...)`.
- **Pre-flight Checks**:
  - [ ] Disk space gate: raises `OutOfDiskSpace` when `SystemMonitor.is_disk_blocked()` is true and stage has `requires_disk_space = True`.
  - [ ] Bypasses disk check when stage has `requires_disk_space = False` (e.g., `archive`, `write`).
  - [ ] Strict config binding: validates `step.config` matches expected `config_class`.
- **Placeholder Rendering**:
  - [ ] Resolves Jinja / template expressions referencing `steps.<step_id>.<property>`.
  - [ ] Resolves `upstream.<property>` relative to the immediate previous step in the manifest.
- **Hook Lifecycle**:
  - [ ] Executes `pre` stage hooks before `_execute()`.
  - [ ] Executes `post` stage hooks after `_execute()`.
- **Zero-Row Validation (`validate_row_count`)**:
  - [ ] First attempt with 0 rows triggers `RollbackRequired`.
  - [ ] Subsequent attempt with 0 rows flags `manifest.is_empty_result_set = True` and allows pipeline to proceed.

### B. Concrete Stages
- **Extract Stage (`extract/stage.py`)**:
  - [ ] Watermark / Checkpoint loading and resolution of missing partition dates.
  - [ ] Filter generation: produces incremental SQL filters for CDC/delta modes, omits filter for full refresh.
- **Write Stage (`write/stage.py`)**:
  - [ ] Case conversion and styling (`format_column_name` for camel, snake, kebab, pascal, etc.).
  - [ ] Staging artifact symlink creation in workspace data directory.
- **Publish Stage (`publish/stage.py`)**:
  - [ ] Missing staging artifact triggers `RollbackRequired(Stage.WRITE)`.
  - [ ] Promotion execution delegating to Sink client.
- **Archive Stage (`archive/stage.py`)**:
  - [ ] Respects `config.enabled = False` and skips without error.
  - [ ] Calculates retention expiry dates (default 7 years or task-configured days).

---

## 3. Session & Context Lifecycle (`src/core/orchestrator/common/task/session.py`)

- [ ] Context manager `__enter__`:
  - Sets up dedicated log handler isolated to `run_id`.
  - Asserts workspace directory exists on disk, fails fast with `FileNotFoundError` if missing.
  - Removes `.retrying` and `.blocked` markers from previous executions.
  - Calls `task.check_in(step_id)`.
- [ ] Context manager `__exit__`:
  - Logs execution duration with precision.
  - Cleans up and detaches log handlers without memory leaks or file descriptor leaks.
  - Properly propagates unhandled exceptions while logging full traceback.

---

## 4. Signal Processing & Notifications (`src/core/orchestrator/common/signals.py`)

- [ ] Signal file pattern matching: parses `{job_id}:{dataset_id}:{partition_date}:{run_id}.{signal}`.
- [ ] Event routing:
  - Dispatches `TaskSignal.DONE` to registered success callbacks.
  - Dispatches `TaskSignal.FAIL` to registered failure callbacks.
  - Dispatches `TaskSignal.SYNC` to manifest state sync callbacks.
- [ ] Atomic file cleanup: consumes and deletes signal files after dispatch to prevent duplicate event loops.

---

## 5. Domain Models & State (`src/core/models/`)

- **`Task` Entity**:
  - [ ] Rehydration from disk via `Task.from_path(folder, exec_ctx)`.
  - [ ] Manifest atomic updates using `deep_merge`.
  - [ ] Step configuration lookup from `TaskContext`.
- **`TaskContext`**:
  - [ ] Boundary validation: rejects invalid `from_step` or `to_step`.
  - [ ] Boundary ordering: rejects when `from_step` is after `to_step`.
  - [ ] Serializable for Ray task arguments (passes `verify_serializable`).

---

## 6. Recommended Test Fixtures & Mocking Utilities

To keep tests decoupled and fast:
1. **`MockWorkspace` Fixture**: In-memory or `tmp_path`-backed workspace with pre-populated `config.json` and `manifest.json`.
2. **`MockHealthChecker`**: Controllable protocol implementation that can simulate healthy, degraded, or out-of-disk states without touching `psutil`.
3. **`MockMetadataRepo`**: Dict-backed in-memory store implementing the metadata repository protocol for testing checkpoints without ClickHouse or SQLite.
4. **`MockServiceResolver`**: Fixture returning fake Sources/Sinks without network sockets or credential providers.
