# Architectural Decision Records (ADR)

## ADR 001: Deterministic Data Vault & Symlinking

**Context:**
In multi-stage pipelines, passing massive datasets between stages often leads to redundant IO or complex "latest folder" lookups.

**Decision:**
We implement a "Data Vault" structure (`data/{job}/{dataset}/{run}/{stage}`) combined with an "Active Path" symlink.

**Consequences:**

* **Pros:** Stages (e.g., `Transform`) don't need to know the physical timestamp of the `Extract` stage; they simply read from the `extract/` symlink.
* **Cons:** Requires the runtime to handle relative symlink creation to ensure portability across Pods/Nodes.

## ADR 002: Bitmask-Driven State Machine

**Context:**
We need to track partial completion of a multi-stage pipeline that can be resumed or rewound.

**Decision:**
Each stage is assigned a bitmask (1, 2, 4, 8...). The `TaskManifest` stores a cumulative integer.

**Consequences:**

* **Pros:** Checking if a job is "All Done" is a single bitwise comparison. Rewinding to a specific stage is a simple bit-clearing operation.
* **Cons:** Limits the total number of stages to the bit-width of the integer (though 64 stages is plenty for this scope).

## ADR 003: Process Isolation via Ray Actors

**Context:**
Python's Global Interpreter Lock (GIL) and memory management make multi-threaded 50M row ingestion unstable.

**Decision:**
Use Ray to spawn stages as isolated processes with logical resource constraints (e.g., 2GB RAM per worker).

**Consequences:**

* **Pros:** A crash in a worker (OOM) does not kill the Orchestrator. Memory pressure is managed by Ray's scheduler.
* **Cons:** Adds overhead for serializing the `ExecutionContext`.


## ADR 004: Filesystem as Primary Signal Bus (IPC)

**Context:** Traditional message brokers (RabbitMQ/Redis) add infrastructure complexity and can lose state if the broker crashes during a task completion.

**Decision:** Use zero-byte signal files (`.done`, `.fail`, `.sync`) dropped into a `signals/` directory.

**Consequences:**
*   **Pros:** Perfectly observable by humans (`ls -la signals/`). Inherently persistent and survives orchestrator reboots.
*   **Cons:** Requires polling (The "Tick" loop), which introduces a sub-second latency in task transitions.

## ADR 005: Multi-Worker Schema Unioning

**Context:** In distributed extraction, workers might encounter different schemas (e.g., API added a field halfway through a batch).

**Decision:** The `Extract` stage performs a post-flight schema merge, favoring `String` types on conflicts to ensure data "wins" over type-strictness during ingestion.

**Consequences:**
*   **Pros:** Downstream `Transform` stages receive a "Master Contract" that accounts for all discovered columns.

## ADR 006: Shared Circuit Breaker Registry

**Context:** If an Oracle DB is down, 50 parallel Ray workers will all attempt to connect, potentially worsening the outage (thundering herd).

**Decision:** Use `Diskcache` as a shared registry for `ServiceMonitor`.

**Consequences:**
*   **Pros:** The first worker to fail "trips" the breaker globally. Remaining 49 workers fail-fast without hitting the network.

## ADR 007: Resource-Aware Scale Calculation

**Context:** Static worker counts lead to OOMs for "Wide" datasets (many columns) or under-utilization for "Narrow" datasets.

**Decision:** The `ExtractStage` calculates a "Width Factor" (Columns vs Rows) to dynamically determine the optimal `rows_per_worker`.

**Consequences:**
*   **Pros:** Automatically scales Ray actor counts based on data volume, ensuring memory usage stays under the 2GB worker ceiling.
*   **Cons:** Requires a pre-flight row count check (`count_units`), adding slight overhead to the driver node.

## ADR 008: The "Zero-Footprint" Local Disk Protocol

**Context:** Large pipelines quickly saturate high-speed local NVMe storage if intermediate artifacts are not managed.

**Decision:** The `ArchiveStage` reclaims local disk space by moving physical artifacts to S3/Cloud storage immediately after the `Write` stage confirms persistence.

**Consequences:**
*   **Pros:** Allows the engine to process datasets significantly larger than the local disk capacity.

## ADR 009: Side-by-Side PEX Execution (Shadow Runs)

**Context:** Regression testing requires running "Candidate" code against "Baseline" data without cross-contaminating the environment.

**Decision:** We use PEX (Python Executable) files to package the entire runtime. The `Executor` can dynamically point to different `.pex` files per task.

**Consequences:**
*   **Pros:** Enables true "Shadow Mode" where the same worker can run two different versions of the logic simultaneously.

## ADR 010: Zombie Task Reclamation Logic

**Context:** In distributed systems, workers can die silently (SIGKILL) without updating the manifest, leaving tasks "stuck" in a RUNNING state.

**Decision:** The `MaintenancePolicy` performs a reconciliation loop. It compares the `Hot Cache` (which says a task is RUNNING) against Ray's internal `active_tasks` list.

**Consequences:**
*   **Pros:** Automatically re-queues tasks that lost their heartbeat, ensuring 100% job completion rates without manual intervention.

## ADR 011: Just-In-Time (JIT) Secret Resolution

**Context:** Passing raw credentials from the Orchestrator to Ray Workers via task arguments creates a security risk (secrets in logs/Ray Dashboard).

**Decision:** Workers are provided with a `SecretProvider` configuration. They resolve credentials (S3 keys, DB passwords) locally on the worker node immediately before execution.

**Consequences:**
*   **Pros:** Secrets never touch the network or the central Orchestrator’s memory.

## ADR 012: Pluggable Service Discovery via Factory Pattern

**Context:**
Data ecosystems are heterogeneous. Forcing every developer to modify the core orchestrator logic to add a new database sink or source type violates the Open-Closed Principle.

**Decision:**
We utilize a Factory Pattern combined with a standard Interface (Protocol) for all Sources, Sinks, and Transformers. Configuration is resolved via `TaskContext` and passed to the specific service at runtime.

**Consequences:**
*   **Pros:** New business logic (Transformers) or infrastructure (Sinks) can be added by simply registering a new class in the `ServiceFactory` without touching the `Executor` or `Orchestrator` core.
*   **Cons:** Requires strict adherence to the interface; breaking changes in the base service class propagate to all implementations.

## ADR 013: DB-Driven Scheduling with EC2-Contained Execution

**Context:**
Scheduling needs to be globally aware and durable, but high-performance data execution should avoid the latency of constant network-based state updates.


**Decision:**
We utilize a central database to drive the authoritative scheduling and task queueing logic, while the physical execution is localized and contained within EC2 instances. Real-time IPC is handled via a local "Hot Cache" and signal files to maximize throughput.


**Consequences:**
*   **Pros:** High reliability for the "Control Plane" via DB persistence, with "Data Plane" speed via localized execution.
