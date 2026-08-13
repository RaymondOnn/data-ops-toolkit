incorporate data retention policies i.e. can keep data for 1 year
[x] disable self-healing
error_handling / exception hook /.
feature flags
- database mode: truncate / incremental (update_key) / snapshot / cdc
- primary key field
work on CD process.
- Can't proceed to K8S without this
- regression testing
bug: Why constantly evict expired run?
bug: check updates
qn: change fallback in TaskCache to build from manifest
qn: daemon mode + overrides
idea: saving to filesystem -> data lakes
idea: bash pipe input and output
idea: using column info in insert using select query
idea: new stages: download, iterate, parse for ai pipeline
idea: two extract steps -> merge the two datasets
idea: dry run mode
idea: quarantine process (after write / publish)
  - Write the problematic rows into a qurantine file in jsonl format, saved to the data folder.
  - {
    "_quarantine_reason": "INVALID_NULL_PRIMARY_KEY",
    "_quarantine_step": "normalize_transform",
    "_quarantine_timestamp": "2026-08-03T18:04:12",
    "order_id": null,
    "amount": 150.00
  }
  - Have a threshold to decide whether to go ahead with the load process. The threshold is to be discussed with the stakeholder.
  - The task folder is moved to FAILED/ and either the quarantine file or symlink to the file will be available in the task folder.


refine custom transform logic.
rollback
zomie recovery
check if metadata folder is removed on success

### Planned Enhancements & s

- [ ] **Status Integrity**: Add automated validation in `TaskMetadata` ensuring the status in the key matches the serialized object state.
- [ ] **Queue Monitoring**: Implement 'queue age' tracking to issue `WARNING` logs for tasks stuck in `PENDING` for > 30 minutes.
- [ ] **Priority Eviction**: Design priority-based eviction to reclaim resources (e.g., stopping an IO worker for a high-priority TRANSFORM task).
- [ ] **PEX Integrity**: Implement `app doctor pex` to verify MD5 checksums against the S3 master code.
- [ ] **Recovery Simulation**: Implement a 'dry-run' for the recovery command to preview resume stages without moving data.

### Resilience & Limit Testing (Roadmap)

- [ ] **Chaos Monkey**: Implement a utility to randomly `ray.cancel()` active tasks to verify Zombie recovery.
- [ ] **Connectivity Interruption**: Simulate database "flapping" to verify that Circuit Breakers correctly move tasks to `BLOCKED`.
- [ ] **Memory Pressure**: Run a 100M row ingestion with a 2GB container limit to verify Polars LazyFrame streaming.
- [ ] **Backpressure Validation**: Queue 500 tasks simultaneously to verify the `Compute` logical capping and system-level CPU/MEM throttling.
- [ ] **Disk Full Scenario**: Physically fill the workspace partition to verify the `DISK_THRESHOLD_HALT` logic.

### K8S Deployment Readiness (Revisit during actual deployment)

- [ ] **Cgroup Memory Reconciliation**: Refine `check_k8s_vitals` to ensure `Compute` uses pod limits instead of node memory.
- [ ] **Storage Latency Benchmarking**: Tune `check_storage_performance` thresholds based on chosen cloud storage (EFS vs EBS).
- [ ] **Downward API Integration**: Ensure Pod Name/Namespace env vars are correctly mapped in Helm charts.
- [ ] **Ray-on-K8S Lifecycle**: Validate `ray.shutdown()` behavior during Pod pre-stop hooks.

1. Artifact Verification (The "MD5 Handshake")
Currently, you upload to S3 and then pull to the server. If a network hiccup occurs during the upload, you might end up with a corrupted PEX on your server.

The Improvement: Generate an MD5 checksum of the PEX on your Mac runner and save it as an S3 object (e.g., app.pex.md5).

Why: After the App Server pulls the PEX, it should run md5sum -c app.pex.md5. If they don't match, the deployment should abort immediately before switching the symlink. This prevents running "broken" binaries.

1. The "Pre-Switch" Validation (Dry Run)
Your health check currently happens after the symlink is switched. If the new version is broken, the symlink is already pointing to it.

The Improvement: Run the health check on the new folder before running the ln -sfn command.

Bash

# Inside the deployment script

/opt/deploy/ingestion/$RELEASE_ID/app.pex --version || exit 1

# ONLY IF ABOVE PASSES, then switch the link

ln -sfn /opt/deploy/ingestion/$RELEASE_ID/app.pex /opt/deploy/current_app.pex
Why: This turns your deployment into a "Blue-Green" style switch at the directory level, ensuring the "Current" pointer only ever points to something that can actually boot up.

1. Multi-Container Deployment (Parallelism)
Your current script handles one app server. If you have both a DEV and a PROD container running:

The Improvement: Use the GitHub environment feature to map the SSH ports dynamically.

YAML

# Example snippet

host: ${{ secrets.SSH_HOST }}
port: ${{ github.event.inputs.environment == 'prod' && 2223 || 2222 }}
Why: This allows the same workflow to target different containers based on your input, proving you can manage multi-stage environments from a single pipeline.

1. Core Orchestrator Features & State Management
Data Retention Policies: Incorporate explicit data retention policies (e.g., for archived data, intermediate data) to manage storage lifecycle.
Self-Healing Control: Implement mechanisms to selectively disable or configure self-healing behaviors.
Masking Types: Add functionality for data masking (e.g., PII masking).
Enhanced Error Handling: Implement more robust error handling and exception hooks across the pipeline.
Dry Run Mode: Fully integrate and refine the dry run capabilities across all stages.

2. Resilience & Limit Testing (Chaos Engineering)
The test CLI app is currently commented out in apps/ingestion/**main**.py, so the first step is to enable it. Once enabled, the following roadmap items are pending:

Enable test CLI: Uncomment app.add_typer(test_app, name="test") in apps/ingestion/**main**.py.
Chaos Monkey: Implement a utility to randomly ray.cancel() active tasks to verify Zombie recovery.
Connectivity Interruption: Simulate database "flapping" to verify that Circuit Breakers correctly move tasks to BLOCKED.
Memory Pressure Testing: Run a 100M row ingestion with a 2GB container limit to verify Polars LazyFrame streaming and memory safety.
Backpressure Validation: Queue 500 tasks simultaneously to verify the Compute logical capping and system-level CPU/MEM throttling.
Disk Full Scenario: Physically fill the workspace partition to verify the DISK_THRESHOLD_HALT logic.
3. Deployment & CI/CD Readiness
These items are crucial for robust deployment and continuous integration.

Artifact Verification (MD5 Handshake):
Generate an MD5 checksum of the PEX on the build runner.
Save this checksum as an S3 object (e.g., app.pex.md5).
After pulling the PEX, verify its integrity using md5sum -c app.pex.md5 before deployment.
Pre-Switch Validation:
Run health checks on the new deployment folder before switching the symlink (ln -sfn).
Only switch the symlink if the health check passes.
S3 Cleanup Strategy:
Add a Lifecycle Policy to the LocalStack S3 bucket (or a step in cd.yaml) to delete old PEX versions.
PEX Layering Strategy:
Implement logic to only build and upload deps.pex if pyproject.toml or requirements.txt has changed.
Multi-Container Deployment:
Use GitHub environment features to dynamically map SSH ports for targeting different containers (e.g., DEV vs. PROD).
K8S Deployment Readiness (Revisit during actual deployment):
Refine check_k8s_vitals to ensure Compute uses pod limits instead of node memory.
Tune check_storage_performance thresholds based on chosen cloud storage (EFS vs EBS).
Ensure Pod Name/Namespace environment variables are correctly mapped via Downward API integration in Helm charts.
Validate ray.shutdown() behavior during Pod pre-stop hooks.
4. Data Handling & Quality
Schema Merging Type Conflicts: In ExtractStage._merge_schemas, add logic to handle type conflicts (e.g., Float vs Int) when merging schemas from multiple files.
CSV Row Count Optimization: For StorageSource.get_work_units, if dealing with thousands of small CSV files, optimize the row count check to avoid full file scans.
File-based is_equal Robustness: Enhance StorageSink.is_equal to perform deeper data comparison for non-Parquet files, beyond just metadata.
Quarantine & Repair Logic (from libs/file/file.py - currently commented out):
Re-integrate and activate the repair_json, repair_xml, repair_csv functions (currently some repair logic exists in handlers, but the commented file suggests a more centralized approach).
Implement fetch_by_hash for Content Addressable Storage (CAS) archive retrieval.
Implement quarantine functionality to move offending files and generate error reports.
Implement update_manifest to maintain a manifest.jsonl for archival.
Implement garbage_collect to purge orphaned files not registered in the manifest.
Implement rebuild_metadata_from_manifest to reconstruct metadata from the manifest.
5. Regression Testing Enhancements
Data Reuse: Implement a mechanism to reuse extracted data from a baseline job for test jobs when the ExtractStep has not changed.
Multi-Dataset Regression: Ensure the regression testing framework fully supports scenarios involving one or more datasets.
Affected Dataset Discovery: Enhance the ability to figure out which datasets are affected by changes (the test impact command exists, but the requirements suggest further refinement).
Comparison without Primary Key: Explore hash-based bucketing or other strategies for data comparison when datasets lack a primary or natural join key.

You need to test the "handshakes" between the Orchestrator, the Disk, and the Ray Workers.

Here is how I recommend splitting the testing to cover all components and processes:

Tier 1: Atomic Component Testing (Logic & Rules)
These tests ensure individual classes behave correctly without requiring Ray or a Database.

Context Builder: Verify hierarchical lookup (Dataset > Job > App) and ${VAR} expansion.
Format Handlers: Test ParquetHandler, CSVHandler, and JSONHandler with small edge-case files (BOMs, trailing commas, empty files).
Admission Policies: Test DenyDuplicateAdmission by mocking a cache and ensuring it blocks duplicate runs.
Path Formulas: Verify TaskWorkspace.get_data_path returns the exact deterministic hierarchy you designed.
Tier 2: Stateful Integration Testing (The "Workbench")
These tests verify that the TaskWorkspace and Task objects manage the filesystem correctly.

Lifecycle Markers: Provision a task, "check-in" to EXTRACT, and verify manifest.json reflects the change.
Surgical Cleanup: Execute create_symlink and reset_data_dir, then verify the symlinks are relative and portable.
Relocation: Mock a failure, call relocate("FAILED"), and verify the folder moves from active/ to FAILED/ while keeping its internal structure.
Tier 3: Workflow Orchestration Testing (The "Scenario" Suite)
This is where your ScenarioType in test.py comes into play. Instead of just manual CLI commands, these should be structured as automated tests.

Happy Path: A mock source to a mock sink. Verify the bitmask is all.
Recovery Workflow:
Trigger a job.
Simulate a failure at TRANSFORM (manually delete the folder or corrupt the manifest).
Run the recover_task_by_path logic.
Verify bits are unset and markers are purged.
Cleanup/TTL Workflow: Set a very short TTL (1 second), wait, run the Janitor sweep, and verify the data/ vault and active/ folders are gone.
Tier 4: Resilience & Chaos (Distributed Stress)
Zombie Detection: Start a task, kill the Ray worker process, run recover_zombie_tasks, and verify it re-queues.
Backpressure: Spoil the CPU usage and verify Compute._can_fit returns False, holding tasks in WAITING.

## 1. The Trigger & Command Handshake

**Boundary**: `CLI` ➔ `Daemon`
**Goal**: Verify signal files are correctly prioritized and parsed.

- **Test Case**: Ad-hoc run injection.
- **Test Case**: Priority command processing (STOP vs RESUME).

## 2. The Resource & Dispatch Handshake

**Boundary**: `TaskManager` ➔ `Compute` ➔ `Ray`
**Goal**: Ensure 2GB RAM limits and stage priorities are enforced.

- **Test Case**: Backpressure (holding tasks in WAITING).
- **Test Case**: Priority Dispatch (ARCHIVE tasks leapfrog EXTRACT tasks).

## 3. The Physical State Handshake

**Boundary**: `Ray Worker` ➔ `TaskWorkspace` ➔ `Filesystem`
**Goal**: Verify deterministic pathing and symlink portability.

- **Test Case**: Hierarchical data vault creation (`data/job/ds/date/run`).
- **Test Case**: Relative symlink verification (portable across Pods).

## 4. The Event Feedback Loop

**Boundary**: `TaskWorkspace` ➔ `SignalProcessor` ➔ `Orchestrator`
**Goal**: Ensure the "Tick" triggers only when work is physically complete.

- **Test Case**: Zero-byte signal detection (.done, .fail).
- **Test Case**: Signal coalescing (handling multiple completions in one tick).

## 5. The Maintenance & Recovery Handshake

**Boundary**: `Janitor` ➔ `StateStore` ➔ `Filesystem`
**Goal**: Ensure surgical recovery doesn't leave "ghost" markers.

- **Test Case**: Rewind integrity (purging markers forward of the resume point).
- **Test Case**: TTL Expiry (Recursive folder cleanup in data vault).

**Boundary**: `CLI` ➔ `Daemon`
**Goal**: Verify signal files are correctly prioritized and parsed.

- **Test Case**: Ad-hoc run injection.
- **Test Case**: Priority command processing (STOP vs RESUME).
- **Test Case**: Backpressure (holding tasks in WAITING).
- **Test Case**: Priority Dispatch (ARCHIVE tasks leapfrog EXTRACT tasks).
- **Test Case**: Hierarchical data vault creation (`data/job/ds/date/run`).
- **Test Case**: Relative symlink verification (portable across Pods).
- **Test Case**: Zero-byte signal detection (.done, .fail).
- **Test Case**: Signal coalescing (handling multiple completions in one tick).
- **Test Case**: Rewind integrity (purging markers forward of the resume point).
- **Test Case**: TTL Expiry (Recursive folder cleanup in data vault).

## 2. The Resource & Dispatch Handshake

**Boundary**: `TaskManager` ➔ `Compute` ➔ `Ray`
**Goal**: Ensure 2GB RAM limits and stage priorities are enforced.

## 3. The Physical State Handshake

**Boundary**: `Ray Worker` ➔ `TaskWorkspace` ➔ `Filesystem`
**Goal**: Verify deterministic pathing and symlink portability.

## 4. The Event Feedback Loop

**Boundary**: `TaskWorkspace` ➔ `SignalProcessor` ➔ `Orchestrator`
**Goal**: Ensure the "Tick" triggers only when work is physically complete.

## 5. The Maintenance & Recovery Handshake

**Boundary**: `Janitor` ➔ `StateStore` ➔ `Filesystem`
**Goal**: Ensure surgical recovery doesn't leave "ghost" markers.

Here's what we need to do:

1. Add google style docstrings to each method / function

- For short simple functions, a one line docstring is fine
- For complex functions, provide the full docstring (including args, returns, examples).
- Especially for architectural decisions, includes notes on the decisions made.

1. Create unit tests for each module.

- Please avoid writing brittle tests.
- Only for unit test docstrings, use the GIVEN-THEN-WHEN pattern.
- Keep to the char per line limit.

1. Could you help to improve / simplify / reduce this code? Feel free to rename the functions if deem appropriate. I think the names can be better
For Context:

- Here's the regression testing process:
  - We need two tables, hence the need for the clone table, one for each run.
  - Baseline run loads into clone table
  - Candidate run loads into the other table
  - Then we compare datasets from the two table and generate a datacompy style report
  - Then we drop the table for the candidate run so that for subsequent testing, we only need to do the candidate run and we can compare the dataset.
  - NOTE: I want a datacompy style report but not use datacompy for the comparision!!
- Let's pass the test date via run().
- The environments are isloated so baseline_env and candidate_env will always be the same.
- For cleanup, we will need the option to drop both cloned tables, just the candidate table or just the baseline table.

1. Would you recommend a final cleanup method that we can trigger separately once we are done with regression testing? Or perhaps a generic drop table cli command that drops any specified table?
