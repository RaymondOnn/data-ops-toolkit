# Distributed Task Lifecycle Architecture

This document defines the authoritative lifecycle of an ingestion task within the toolkit. It details the handshakes between the Orchestrator, Ray Workers, and the Telemetry subsystem.

## 1. The Four Pillars of State

To ensure 100% reliability and observability, the engine synchronizes state across four layers:

1.  **The Physical Layer (`active/`)**: The source of truth on disk. Contains `manifest.json` (progress) and `config.json` (parameters).
2.  **The Hot Cache (`StateStore`)**: An in-memory RLock-protected registry in the Orchestrator for sub-millisecond scheduling decisions.
3.  **The Event Bus (`signals/`)**: Zero-byte files (`.sync`, `.done`, `.fail`) that facilitate asynchronous IPC between workers and the controller.
4.  **The Telemetry Tier (`ClickHouse`)**: Persistent SQL-based audit logs for long-term reporting and dashboarding.

---

## 2. Phase 1: Provisioning & Discovery

### 2.1 Triggering
Jobs are initiated either via **Ad-hoc CLI** (`TriggerRuntime`) or the **Daemon Scheduler** (`DaemonRuntime`).
- **Daemon Mode**: Polls the metadata database for scheduled runs or misfired tasks.
- **Staggered Flush**: In Daemon mode, the state is flushed to the DB 15 seconds before every poll to ensure the engine sees the latest telemetry.

### 2.2 Workspace Seeding
The `Orchestrator` generates a deterministic `run_id` and creates the physical workspace:
1.  **Config Persistence**: The `TaskContext` is serialized to `active/{identity}/{run_id}_config.json`.
2.  **Manifest Creation**: An initial `manifest.json` is created with status `PROVISIONED`.
3.  **State Registration**: The `StateHub` is notified, adding the task to the `StateStore` and appending a record to the `StateSink`.

---

## 3. Phase 2: Compute & Dispatch

### 3.1 Resource Costing
The `Compute` manager assigns a `WorkloadClass` to the stage:
- **EXTRACT/LOAD**: `IO_INTENSIVE` (High IO slots, low CPU).
- **TRANSFORM**: `CPU_INTENSIVE` (1.0 CPU, high Memory).

### 3.2 The Admission Heuristic
Before spawning a worker, the engine checks:
1.  **Logical Capping**: Does the projected usage fit within the logical limits (e.g., 80% of total cores)?
2.  **Physical Vitals**: Is the system CPU < 90% and Memory < 85%?
3.  **Backpressure**: If limits are exceeded, the task remains in `WAITING` until resources are reclaimed.

---

## 4. Phase 3: Distributed Execution

### 4.1 Ray Worker Initialization
When a worker is spawned, it performs a **Local Initialization**:
- **AWS Client**: Sanitizes environment variables (removes `"null"` strings) and establishes fresh credential handles.
- **Service Factory**: Re-creates database/file clients locally so that socket handles are not shared across network boundaries.

### 4.2 Workload Sharding Heuristics
- **Database Slicing**: Uses a **Cell-Budget** target of 20M cells per worker to keep memory usage under the 2GB Ray worker limit.
- **File Sharding**:
    - **Greedy Bin-Packing (LPT)**: Distributes files based on size (largest first) to minimize tail latency.
    - **Intra-file Slicing**: If workers exceed file count, splittable files (Parquet) are sliced by row-offsets.
    - **Density Floor**: Coalesces workloads smaller than 100MB into a single worker to avoid the "Ray Tax" (setup overhead).

---

## 5. Phase 4: Termination & Signaling

### 5.1 The Terminal Handshake
As a stage completes, the `Executor` concludes the task:
1.  **Disk Update**: The `manifest.json` is updated with row counts and metrics.
2.  **Signal Drop**: A zero-byte signal (e.g., `run_id.done`) is dropped in `signals/`.
3.  **Local Cleanup**: Ray resources are reclaimed locally.

### 5.2 Signal Processing
The Orchestrator's `SignalScanner` detects the file:
1.  **Deep Sync**: The Orchestrator re-reads the physical manifest and updates the `StateHub`.
2.  **Post-Mortem**: If a `.fail` signal is found, the Orchestrator extracts the `ERRORS` block and prints a "Pipeline Post-Mortem" summary.
3.  **Quarantine**: Failed tasks are moved to `FAILED/` for surgical resume.

---

## 6. Resilience & Recovery

### 6.1 Zombie Recovery (ADR 010)
If a worker process is killed (`SIGKILL`) and fails to drop a `.fail` signal:
1.  The `MaintenancePolicy` compares `StateStore` (RUNNING) tasks against Ray's `active_tasks`.
2.  Discrepancies are declared "Zombies".
3.  The Janitor moves the task back to `active/`, clears the `RUNNING` status, and re-queues it for execution.

### 6.2 Circuit Breakers (ADR 006)
If a service (e.g., Snowflake) is down:
1.  The first worker to fail "trips" the global breaker in the shared cache.
2.  The Orchestrator detects the `BLOCKED` status and moves pending tasks to the `HOLD/` vault.

---

## 7. Lifecycle Flowchart

```mermaid
graph TD
    subgraph Orchestrator (Control Plane)
        T[Trigger: Daemon/Ad-hoc] --> P[Provision: active/folder]
        P --> S[StateHub: Hot Cache]
        S --> C[Compute: Check Limits]
        C -- "Wait" --> C
        C -- "Spawn" --> W[Ray Worker]
    end

    subgraph Ray Worker (Data Plane)
        W --> Init[Local Service Init]
        Init --> Shard[Heuristic Sharding]
        Shard --> Exec[Stage Execution]
        Exec --> Manifest[Update manifest.json]
        Manifest --> Signal[Drop .done / .fail signal]
    end

    subgraph Telemetry (Audit)
        Signal --> Scan[SignalScanner]
        Scan --> Sync[StateHub Deep Sync]
        Sync --> DB[(ClickHouse: EXECUTION_LOG)]
    end

    DB --> T
```

---

## 8. Summary of Stage Transitions

| Stage Status | Filesystem Location | State Registry | Next Action |
| :--- | :--- | :--- | :--- |
| **PROVISIONED** | `active/` | `PENDING` | Orchestrator Dispatch |
| **RUNNING** | `active/` | `RUNNING` | Ray Worker execution |
| **SUCCESS** | `active/` | `WAITING` | Move to next Stage |
| **FAILED** | `FAILED/` | `FAILED` | Manual Resume or Post-Mortem |
| **BLOCKED** | `HOLD/` | `BLOCKED` | Wait for Circuit Breaker reset |
| **COMPLETED** | `active/` (TTL) | `SUCCESS` | Janitor Cleanup |

---
*Note: This architecture prioritizes "Disk over Memory". In the event of a total system crash, the Orchestrator can fully rehydrate its state by scanning the `active/` folder and re-reading manifests.*
