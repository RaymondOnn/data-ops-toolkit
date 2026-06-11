# Resilience Playbook: Operational Recovery & Incident Response

This document defines how the Ingestion Engine handles infrastructure failures, silent process deaths, and temporal boundaries. It serves as a guide for SREs and developers to understand the automated self-healing mechanisms and manual intervention points.

---

## 🧠 Failure Philosophy

1. **Infrastructure vs. Data:** We distinguish between infrastructure outages (Service Outage) and data-specific bugs (Failed).
2. **Zero-Touch Recovery:** Wherever possible, the engine should self-heal without manual intervention.
3. **Forensic Integrity:** Terminal failures (`FAILED/`) must preserve their artifacts (manifests, logs) for root-cause analysis.
4. **Temporal Boundaries:** We enforce a "Midnight Kill" policy to prevent yesterday's backlog from starving today's critical path.

---

## 🛠️ Detailed State Scenarios

### 1. Zombie Tasks (Silent Process Death)

**Scenario:** A Ray worker is killed by the OOM killer or a hardware fault without updating the manifest.

* **Detection:** The `MaintenancePolicy` (via `Janitor`) reconciles the `RUNNING` status in the cache against the active tasks in the Ray GCS. If no worker is found and the logical heartbeat has drifted > 5 minutes, the task is declared a Zombie.
* **Action:** The task is moved back to `PENDING` in the `active/` directory.
* **Recovery:** The `TaskManager` re-queues the task for a fresh dispatch.

### 2. Service Outages (Circuit Breakers)

**Scenario:** An external database (Oracle) or storage layer (S3) becomes unreachable.

* **Detection:** The `Executor` catches a `TransientError` and increments the failure count in the global `ServiceMonitor`.
* **Action:** Once the threshold (3 failures) is hit, the breaker trips to `OPEN`. Affected tasks are transitioned to `BLOCKED`.
* **Marker:** A physical `.blocked` file is created in the task workspace.
* **Behavior (Daemon):** The `Orchestrator` periodically calls `ServiceMonitor.probe()`. Once the service is reachable, the breaker closes, and tasks are automatically resumed.
* **Behavior (Trigger):** The run finishes other healthy tasks and exits with a summary of blocked runs.

### 3. Task Retries (Exponential Backoff)

**Scenario:** A transient network blip or file lock prevents a stage from completing.

* **Detection:** The `Executor` identifies a retriable exception.
* **Action:** The task is moved to `RETRY` state. A `.retrying` marker is written to disk containing the `retry_at` timestamp.
* **Backoff:** Uses exponential backoff ($2^{n} \times 30s$), maxing at 10 minutes.
* **Midnight Kill:** If the next retry falls after 23:59:59, the task is automatically transitioned to `FAILED`.

### 4. Task Expiry (TTL Management)

**Scenario:** A triggered snapshot job has been waiting in the queue too long.

* **Detection:** `ExpiredState` checks the `EXPIRATION_THRESHOLD` in the database.
* **Sunk Cost Protection:** If the `EXTRACT` stage is already complete (`EXT+` in bitmask), the task **will not expire**. We protect the expensive data acquisition work even if the TTL is breached.
* **Action:** Stale jobs are moved to `EXPIRED` status and their local workspace is purged by the `Janitor`.

---

## 🚀 Runtime Mode Variations

The engine behaves differently depending on its deployment mode:

| Feature | Trigger Mode (`run`) | Daemon Mode (`start`) |
| :--- | :--- | :--- |
| **Goal** | Run-to-completion for specific IDs. | Persistent orchestration for the enterprise. |
| **Blocked Tasks** | Exit with a summary and `resume` command. | Active Probing: Auto-resumes when service heals. |
| **Recovery** | Manual intervention via `run resume`. | Automated `MaintenancePolicy` runs every "Tick". |
| **Termination** | Exits when the specific queue is empty. | Remains active, polling for signals and triggers. |

---

## 🚑 Incident Response Workflow

### Stage 1: Triage

If a pipeline is degraded, use the **Doctor CLI** to check global health:

```bash
python -m ingestion doctor
```

Check for tripped circuit breakers:

* Inspect `.workspace/signals/*.outage` files.
* Presence of `.outage` indicates the global breaker is **OPEN**.

### Stage 2: Forensic Analysis

For `FAILED` jobs, inspect the terminal manifest:

```bash
# View the error and traceback
python -m ingestion test peek .workspace/FAILED/{run_id}/manifest.json --rows 100

# Inspect data at the point of failure
python -m ingestion test peek .workspace/FAILED/{run_id}/extract/ --sql "SELECT count(*) FROM self"
```

### Stage 3: Manual Recovery

If the issue was a configuration error (e.g., bad credentials) that is now fixed:

```bash
# Resume from the point of failure
python -m ingestion run resume {run_id}

# Force a rewind to a specific stage
python -m ingestion run resume {run_id} --from extract
```

### Stage 4: Service Restoration

If a service outage has ended, you can force the orchestrator to check connectivity:

```bash
# Daemon mode will eventually probe, but you can trip it manually
python -m ingestion doctor connect {service_name}
```

---
<!--
## 🛡️ Self-Healing Configuration

Key settings in `app.yaml` that control these behaviors:

```yaml
resilience:
  max_retries: 3
  midnight_kill_enabled: true
  zombie_heartbeat_threshold_sec: 300
  circuit_breaker:
    failure_threshold: 3
    timeout_secs_sec: 300

janitor:
  default_ttl_days: 7
  incremental_expiry_protection: true
``` -->
