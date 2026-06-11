"""Resilience and chaos simulation scenarios for testing."""

import multiprocessing
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import msgspec
import polars as pl
import psutil
import ray
import typer
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskManifest,
)
from apps.ingestion.src.core.orchestrator.enums import TaskRecord
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.core.orchestrator.modes.daemon import DaemonRuntime
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    CONFIG_FILENAME,
    DISK_THRESHOLD_HALT,
    MANIFEST_FILENAME,
)
from libs.utils.dates import current_timestamp
from libs.utils.system import get_disk_usage

from .harness import ScenarioType


class SimulationRunner:
    """Executes resilience and chaos scenarios."""

    def __init__(self, runtime: Any, env: str = "local"):
        """Initializes the runner with a runtime orchestrator.

        Args:
            runtime: The Runtime instance (Trigger or Daemon).
            env: The environment context for config resolution.
                Defaults to "local".
        """
        self.runtime = runtime
        self.orchestrator = runtime.orchestrator
        self.env = env

    def run(
        self,
        scenario: ScenarioType,
        job_id: str | None,
        dataset: str | None,
        auto_input: bool = False,
    ) -> tuple[bool | None, str | None]:
        """Dispatches a simulation scenario to its internal handler.

        Args:
            scenario: The type of resilience test to execute.
            job_id: The target job ID.
            dataset: The target dataset identifier.
            auto_input: If True, suppresses interactive prompts.

        Returns:
            tuple: (success_status, failure_reason).

        Decision: Dynamic Handler Mapping.
        Using getattr allows for adding new chaos scenarios by simply
        defining a private method, keeping the run logic clean and open/closed.
        """
        standalone = {
            ScenarioType.STRESS,
            ScenarioType.RECOVERY,
            ScenarioType.KILL_DAEMON,
            ScenarioType.RETENTION,
        }

        if scenario not in standalone and (not job_id or not dataset):
            typer.secho(
                f"❌ Scenario '{scenario}' requires --job-id and --dataset", fg="red"
            )
            return None, "Missing required arguments"

        handler = getattr(self, f"_{scenario.value}", None)
        if not handler:
            return False, f"No handler for scenario: {scenario}"

        try:
            result = handler(job_id, dataset, auto_input)
            return result if isinstance(result, tuple) else (True, None)
        except Exception as e:
            typer.secho(f"💥 Scenario failed: {e}", fg="red")
            return False, str(e)

    # =========================================================================
    # Service Failure Scenarios
    # =========================================================================

    def _block(self, *args) -> tuple[bool | None, str | None]:
        """Simulates a service outage by creating a filesystem outage signal.

        Decision: Signal Injection.
        Injecting a '.outage' file tests the TaskManager's ability to react
        to environment signals and transition tasks to the BLOCKED state.
        """
        target = "clickhouse_db"
        typer.echo(f"🚧 Simulating outage for: {target}")
        signal = self.orchestrator.exec_ctx.signal_path / f"{target}.outage"
        signal.touch()
        typer.secho(f"✅ Created signal: {signal.name}", fg="yellow")
        return True, None

    def _flapping(
        self, job_id: str, dataset: str, *args
    ) -> tuple[bool | None, str | None]:
        """Toggles service availability to test circuit breaker recovery.

        Decision: Intermittent Recovery.
        Verifies that tasks can move between BLOCKED and WAITING states
        without losing their logical progress or metadata.
        """
        target = "clickhouse_db"
        signal = self.orchestrator.exec_ctx.signal_path / f"{target}.outage"

        self.orchestrator.start_job(job_id, dataset, partition_date="2024-02-01")

        for i in range(3):
            typer.echo(f"Iteration {i+1}: Dropping service...")
            signal.touch()
            time.sleep(5)
            typer.echo(f"Iteration {i+1}: Restoring service...")
            signal.unlink(missing_ok=True)
            time.sleep(5)

        typer.secho("✅ Flapping simulation complete", fg="green")
        return True, None

    # =========================================================================
    # Load & Stress Scenarios
    # =========================================================================

    def _concurrency(
        self, job_id: str, dataset: str, *args
    ) -> tuple[bool | None, str | None]:
        """Floods the task manager with multiple partition requests.

        Decision: Throttling Verification.
        Rapidly injecting tasks ensures the TaskManager correctly enforces
        resource caps and prevents worker explosion on the Ray cluster.
        """
        count = 15
        typer.echo(f"🌀 Stressing with {count} concurrent requests...")

        for i in range(count):
            date = f"2024-01-{i+1:02d}"
            self.orchestrator.start_job(job_id, dataset, partition_date=date)

        dash = getattr(ray.get_runtime_context(), "dashboard_url", "localhost:8265")
        typer.secho(f"✅ {count} tasks queued. Monitor: {dash}", fg="green")
        return True, None

    def _stress(self, *args) -> tuple[bool | None, str | None]:
        """Trigger all configured jobs simultaneously."""
        typer.echo("🔥 Executing cross-job contention stress test...")

        jobs = [
            d.name
            for d in APP_CONFIG_ROOT.iterdir()
            if d.is_dir() and (d / "config.yaml").exists()
        ]

        if not jobs:
            return None, "No jobs found"

        for jid in jobs:
            self.orchestrator.start_job(jid, partition_date="2024-03-15")

        typer.secho(f"✅ Triggered {len(jobs)} jobs", fg="green")
        return True, None

    def _throttling(
        self, job_id: str, dataset: str, *args
    ) -> tuple[bool | None, str | None]:
        """Consumes system CPU to trigger orchestrator backpressure.

        Decision: System-Level Awareness.
        Spawning CPU burners tests if the Compute manager correctly
        identifies system-wide saturation and halts new worker dispatches.
        """
        typer.echo("📉 Testing adaptive backpressure...")

        def burner():
            while True:
                _ = 1000 * 1000

        burners = [
            multiprocessing.Process(target=burner, daemon=True)
            for _ in range(multiprocessing.cpu_count())
        ]

        try:
            for b in burners:
                b.start()
            time.sleep(5)
            self.orchestrator.start_job(job_id, dataset, partition_date="2024-04-01")
            typer.secho("✅ Burners active. Check logs for throttling.", fg="yellow")
            time.sleep(15)
        finally:
            for b in burners:
                b.terminate()

        return True, None

    # =========================================================================
    # Failure & Recovery Scenarios
    # =========================================================================

    def _zombie(
        self, job_id: str, dataset: str, *args
    ) -> tuple[bool | None, str | None]:
        """Kills a running Ray task to verify zombie detection.

        Decision: Worker SIGKILL Simulation.
        By manually cancelling the Ray task, we test the Janitor's ability
        to reconcile 'ghost' tasks that are RUNNING in cache but gone in GCS.
        """
        run_ids = self.orchestrator.start_job(job_id, dataset)
        target = next(iter(run_ids))
        typer.echo(f"Waiting for {target} to start...")

        found = None
        for _ in range(30):
            self.orchestrator.process_signals()
            self.orchestrator.process_queue()

            for ref, key in self.orchestrator.scheduler._active_tasks.items():
                if target in key:
                    found = ref
                    break
            if found:
                break
            time.sleep(1)

        if not found:
            return False, "Timed out waiting for task"

        typer.secho(f"💥 Terminating Ray task: {target}", fg="red", bold=True)
        ray.cancel(found, force=True)
        return True, None

    def _recovery(self, *args) -> tuple[bool | None, str | None]:
        """Seeds multiple failure folders to test the mass-recovery logic.

        Decision: Cold-Start Recovery.
        Mocking failed directories ensures that the RECOVER_ALL command
        can re-hydrate the state registry purely from physical files."""
        typer.echo("🚑 Creating mock failures for recovery test...")

        for i in range(5):
            run_id = f"fail-test-{i}-{int(time.time())}"
            path = self.orchestrator.exec_ctx.failed_path / f"sim:job:{i}" / run_id
            path.mkdir(parents=True, exist_ok=True)

            manifest = TaskManifest(
                job_id=f"sim_job_{i}",
                run_id=run_id,
                dataset_id="sim_ds",
                status=ExecutionStatus.FAILED,
                current_stage="extract",
                bitmask=0,
            )
            (path / MANIFEST_FILENAME).write_bytes(msgspec.json.encode(manifest))

            record = TaskRecord(
                JOB_ID=f"sim_job_{i}",
                DATASET_ID="sim_ds",
                RUN_ID=run_id,
                JOB_STATUS=ExecutionStatus.FAILED.value,
                SCHEDULED_TIMESTAMP_LC=current_timestamp(naive=True),
            )
            (path / CONFIG_FILENAME).write_bytes(
                msgspec.json.encode(
                    {
                        "job_id": record.JOB_ID,
                        "dataset_id": record.DATASET_ID,
                        "partition_date": record.PARTITION_DATE,
                        "run_id": record.RUN_ID,
                        "overrides": {},
                    }
                )
            )
            typer.echo(f"   Created: {run_id}")

        if self.orchestrator.exec_ctx.always_on:
            (self.orchestrator.exec_ctx.signal_path / "RECOVER_ALL.cmd").touch()
            typer.secho("🚀 RECOVER_ALL signal sent", fg="green")

        return True, None

    def _kill_daemon(self, *args) -> tuple[bool | None, str | None]:
        """Kill daemon to test lock recovery."""
        typer.echo("💀 Testing daemon crash recovery...")

        def find_daemon():
            for p in psutil.process_iter(["pid", "cmdline"]):
                try:
                    cmd = p.info["cmdline"] or []
                    if (
                        any("ingestion" in part for part in cmd)
                        and "start" in cmd
                        and p.pid != os.getpid()
                    ):
                        return p
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return None

        daemon = find_daemon()

        if not daemon:
            typer.echo("No daemon found, starting one...")
            subprocess.Popen(
                [sys.executable, "-m", "apps.ingestion", "start"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            for _ in range(10):
                if self.orchestrator.exec_ctx.lock_file.exists():
                    break
                time.sleep(1)
            daemon = find_daemon()

        if not daemon:
            return None, "Could not find or start daemon"

        typer.secho(f"🔥 Killing daemon (PID: {daemon.pid})", fg="red", bold=True)
        daemon.kill()
        time.sleep(2)

        builder = TaskContextBuilder(env=self.env)
        exec_ctx = builder.build_execution_context()
        exec_ctx.always_on = True
        runtime = assemble_runtime(exec_ctx, builder)

        if isinstance(runtime, DaemonRuntime):
            typer.secho("✅ Replacement orchestrator acquired lock", fg="green")
            runtime._recovery_sweep()

        return True, None

    # =========================================================================
    # Resource & Data Scenarios
    # =========================================================================

    def _memory(
        self, job_id: str, dataset: str, *args
    ) -> tuple[bool | None, str | None]:
        """Restricts memory resources to trigger OOM failure handling.

        Decision: Resource Capping.
        By setting a 100MB limit, we verify that Ray correctly enforces
        container-level limits and the orchestrator captures the resulting crash.
        """
        typer.echo("🧠 Testing memory limits (100MB)...")
        overrides = {"_global": {"compute": {"memory_gb": 0.1}}}
        self.orchestrator.start_job(job_id, dataset, overrides=overrides)
        typer.secho("✅ Job triggered with restrictive memory", fg="green")
        return True, None

    def _disk_full(
        self, job_id: str, dataset: str, auto_input: bool
    ) -> tuple[bool | None, str | None]:
        """Fills the workspace partition to verify safety interlocks.

        Decision: Storage Guarding.
        Verifies that DISK_THRESHOLD_HALT prevents any new work from
        starting when storage is near saturation."""
        usage = get_disk_usage(self.orchestrator.exec_ctx.workspace_dir)
        target = DISK_THRESHOLD_HALT + 2
        needed = int((usage.total * target / 100) - usage.used)

        dummy = self.orchestrator.exec_ctx.workspace_dir / ".disk_pressure_sim"
        try:
            if platform.system() == "Linux":
                subprocess.run(["fallocate", "-l", str(needed), str(dummy)], check=True)
            else:
                dummy.write_bytes(b"\0" * needed)

            typer.secho(f"✅ Disk at {target}% (simulated)", fg="red", bold=True)

            if auto_input:
                time.sleep(5)
            else:
                input("\nPress Enter to restore space...")
        finally:
            dummy.unlink(missing_ok=True)

        return True, None

    def _latency(
        self, job_id: str, dataset: str, auto_input: bool
    ) -> tuple[bool | None, str | None]:
        """Simulate network latency using tc."""
        if platform.system() != "Linux":
            return None, "Latency test requires Linux 'tc'"

        interface = "lo" if self.env == "local" else "eth0"
        delay = "500ms"

        try:
            subprocess.run(
                [
                    "sudo",
                    "tc",
                    "qdisc",
                    "add",
                    "dev",
                    interface,
                    "root",
                    "netem",
                    "delay",
                    delay,
                ],
                check=True,
            )
            typer.secho(f"✅ Network throttled on {interface}", fg="yellow")
            self.orchestrator.start_job(job_id, dataset)

            if auto_input:
                time.sleep(5)
            else:
                input("\nScenario active. Press Enter to restore...")
        finally:
            subprocess.run(
                ["sudo", "tc", "qdisc", "del", "dev", interface, "root"],
                capture_output=True,
                check=False,
            )

        typer.secho("✅ Network restored", fg="green")
        return True, None

    def _schema_drift(
        self, job_id: str, dataset: str, *args
    ) -> tuple[bool | None, str | None]:
        """Injects conflicting Parquet schemas to test the Extract stage.

        Decision: Schema Strictness.
        Tests that the ExtractStage correctly fails when merging files
        with incompatible column types (e.g. Int vs String).
        """
        source = Path("sim_data") / "drift_test"
        source.mkdir(parents=True, exist_ok=True)

        pl.DataFrame({"id": [1], "val": [100]}).write_parquet(source / "part1.parquet")
        pl.DataFrame({"id": [2], "val": ["error"]}).write_parquet(
            source / "part2.parquet"
        )

        overrides = {"extract": {"type": "flat_file", "resource": str(source)}}
        self.orchestrator.start_job(job_id, dataset, overrides=overrides)
        typer.secho("✅ Drift job triggered", fg="green")
        return True, None

    def _data_loss(
        self, job_id: str, dataset: str, *args
    ) -> tuple[bool | None, str | None]:
        """Corrupt task manifest metadata."""
        run_ids = self.orchestrator.start_job(job_id, dataset)
        target = next(iter(run_ids))

        self.orchestrator.process_queue()

        task_path = self.orchestrator.state.find_task_path(target)
        if not task_path:
            return None, f"Cannot find path for {target}"

        task = Task.from_path(task_path, self.orchestrator.exec_ctx)
        task.update_manifest(
            {
                "write": {
                    "staging_artifact": "mock_table",
                    "rows_inserted": 1000000,
                    "sink_type": "clickhouse_db",
                    "destination": "prod.table",
                },
                "bitmask": 15,
            }
        )

        typer.secho(f"✅ Patched {target} with corrupted manifest", fg="yellow")
        return True, None

    def _retention(self, *args) -> tuple[bool | None, str | None]:
        """Seeds old runs to test the Janitor's retention reaper.

        Decision: Lifecycle Verification.
        Seeding tasks with a 1-year-old timestamp verifies that the
        'clean --expired' logic correctly identifies and purges stale data."""
        typer.echo("📦 Creating expired tasks for retention test...")
        expired_ts = time.time() - (365 * 24 * 3600)

        for i in range(3):
            run_id = f"expired-run-{i}"
            path = self.orchestrator.exec_ctx.active_path / "sim:job:exp" / run_id
            path.mkdir(parents=True, exist_ok=True)

            (path / CONFIG_FILENAME).write_bytes(
                msgspec.json.encode(
                    {
                        "job_id": "exp",
                        "dataset_id": "ds",
                        "partition_date": "2023-01-01",
                        "expires_at": expired_ts,
                    }
                )
            )
            (path / MANIFEST_FILENAME).write_bytes(
                msgspec.json.encode({"status": "success"})
            )
            typer.echo(f"   Created: {run_id}")

        typer.secho(
            "✅ Expired tasks created. Run 'clean --expired' to purge.", fg="green"
        )
        return True, None
