import multiprocessing
import os
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import msgspec
import polars as pl
import psutil
import ray
import typer
from apps.ingestion.src.core.contexts import TaskContext, TaskContextBuilder
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskManifest,
)
from apps.ingestion.src.core.orchestrator.enums import JobRecord
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.core.orchestrator.modes.daemon import DaemonRuntime
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    CONFIG_FILENAME,
    DISK_THRESHOLD_HALT,
    MANIFEST_FILENAME,
)
from libs.utils.system import get_disk_usage

from .harness import ScenarioType


class SimulationEngine:
    """
    Encapsulates the execution logic for all resilience and chaos scenarios.
    """

    def __init__(self, runtime: Any, env: str = "local"):
        self.runtime = runtime
        self.orchestrator = runtime.orchestrator
        self.env = env

    def run(
        self,
        scenario: ScenarioType,
        job_id: str | None,
        dataset: str | None,
        automated_input: bool = False,
    ) -> tuple[bool | None, str | None]:
        """Dispatches to the specific simulation logic based on type."""
        # Scenarios that do NOT require a specific job/dataset target
        standalone_scenarios = {
            ScenarioType.STRESS,
            ScenarioType.RECOVERY,
            ScenarioType.KILL_DAEMON,
            ScenarioType.RETENTION,
        }

        # Most simulations (including SCHEMA_DRIFT) require a target unit
        # to resolve paths or trigger a specific pipeline run.
        if scenario not in standalone_scenarios:
            if not job_id or not dataset:
                typer.secho(
                    f"❌ Error: Scenario '{scenario}' requires --job-id and --dataset.",
                    fg="red",
                )
                return None, "Missing --job-id or --dataset"

        method_name = f"_sim_{scenario.value}"
        handler = getattr(self, method_name, None)

        if not handler:
            typer.secho(f"❌ No handler implemented for scenario: {scenario}", fg="red")
            return False, "Not Implemented"

        try:
            # Fix: Use types instead of strings in cast() for static analysis compliance
            # For standalone scenarios, job_id/dataset can safely be passed as None
            result = handler(
                cast("str", job_id) if job_id else None,
                cast("str", dataset) if dataset else None,
                automated_input,
            )
            # Ensure we return a tuple even if the handler returns None
            return result if isinstance(result, tuple) else (True, None)
        except Exception as e:
            typer.secho(f"💥 Scenario {scenario} failed: {e}", fg="red")
            return False, str(e)

    def _sim_block(self, job_id: str, dataset: str, *args):
        target_service = "clickhouse_db"
        typer.echo(f"🚧 Simulating outage for service: {target_service}")
        signal_file = (
            self.orchestrator.exec_ctx.signal_path / f"{target_service}.source_down"
        )
        signal_file.touch()
        typer.secho(f"✅ Created signal: {signal_file.name}", fg="yellow")
        typer.echo(f"New tasks for '{target_service}' will now transition to BLOCKED.")
        return True, None

    def _sim_concurrency(self, job_id: str, dataset: str, *args):
        count = 15
        typer.echo(f"🌀 Stressing TaskManager with {count} concurrent requests...")
        for i in range(count):
            date_str = f"2024-01-{i+1:02d}"
            self.orchestrator._trigger_job(job_id, dataset, partition_date_str=date_str)

        # Resolve Dashboard URL using compute.py logic
        try:
            dash_url = ray.get_runtime_context().dashboard_url
        except (AttributeError, Exception):
            dash_url = "localhost:8265 (Local Mode)"

        typer.secho(
            f"✅ {count} Tasks queued. Monitor limits at: {dash_url}",
            fg="green",
        )
        return True, None

    def _sim_stress(self, *args):
        typer.echo("🔥 Executing STRESS simulation: Cross-job contention...")
        job_dirs = [
            d.name
            for d in APP_CONFIG_ROOT.iterdir()
            if d.is_dir() and (d / "config.yaml").exists()
        ]
        if not job_dirs:
            return None, "No jobs found in configs/ to stress test"

        for j_id in job_dirs:
            self.orchestrator._trigger_job(
                j_id, dataset_id=None, partition_date_str="2024-03-15"
            )
        typer.secho(f"✅ Triggered {len(job_dirs)} jobs simultaneously.", fg="green")
        return True, None

    def _sim_zombie(self, job_id: str, dataset: str, *args):
        run_ids = self.orchestrator._trigger_job(job_id, dataset)
        target_run = next(iter(run_ids))
        typer.echo(f"Waiting for {target_run} to enter RUNNING state...")

        found_ref = None
        for _ in range(30):
            # SELF-DRIVING: Manually tick the engine to move the task forward
            self.orchestrator.process_task_events()
            self.orchestrator._drive_engine()

            for ref, key in self.orchestrator.tasks._active_tasks.items():
                if target_run in key:
                    found_ref = ref
                    break
            if found_ref:
                break
            time.sleep(1)

        if found_ref:
            typer.secho(f"💥 Terminating Ray task: {target_run}", fg="red", bold=True)
            ray.cancel(found_ref, force=True)
        else:
            return False, "Timed out waiting for task to start"
        return True, None

    def _sim_throttling(self, job_id: str, dataset: str, *args):
        typer.echo("📉 Executing THROTTLING simulation: Adaptive Backpressure...")

        def cpu_burner():
            while True:
                _ = 1000 * 1000

        burners = [
            multiprocessing.Process(target=cpu_burner, daemon=True)
            for _ in range(multiprocessing.cpu_count())
        ]
        for b in burners:
            b.start()
        try:
            time.sleep(5)
            self.orchestrator._trigger_job(
                job_id, dataset, partition_date_str="2024-04-01"
            )
            typer.secho(
                "✅ Burners active. Verify 'Adaptive throttling engaged' in logs.",
                fg="yellow",
            )
            time.sleep(15)
        finally:
            for b in burners:
                b.terminate()
            typer.secho("✅ System CPU cooling down.", fg="green")
        return True, None

    def _sim_flapping(self, job_id: str, dataset: str, *args):
        target_service = "clickhouse_db"
        typer.echo(f"📳 Simulating flapping connectivity for: {target_service}")
        signal_file = (
            self.orchestrator.exec_ctx.signal_path / f"{target_service}.source_down"
        )
        self.orchestrator._trigger_job(job_id, dataset, partition_date_str="2024-02-01")
        for i in range(3):
            typer.echo(f"Iteration {i+1}: Dropping service...")
            signal_file.touch()
            time.sleep(5)
            typer.echo(f"Iteration {i+1}: Restoring service...")
            signal_file.unlink(missing_ok=True)
            time.sleep(5)
        typer.secho("✅ Flapping simulation complete.", fg="green")
        return True, None

    def _sim_disk_full(self, job_id: str, dataset: str, automated_input: bool):
        usage = get_disk_usage(self.orchestrator.exec_ctx.workspace_dir)
        target_pct = DISK_THRESHOLD_HALT + 2
        bytes_needed = int((usage.total * (target_pct / 100)) - usage.used)

        dummy_file = self.orchestrator.exec_ctx.workspace_dir / ".disk_pressure_sim"
        try:
            if platform.system() == "Linux":
                subprocess.run(
                    ["fallocate", "-l", str(bytes_needed), str(dummy_file)], check=True
                )
            else:
                with dummy_file.open("wb") as f:
                    f.truncate(bytes_needed)

            typer.secho(
                f"✅ Disk pressure applied ({target_pct}%).", fg="red", bold=True
            )
            if automated_input:
                time.sleep(5)
            else:
                input("\nPress Enter to restore space...")
        finally:
            if dummy_file.exists():
                dummy_file.unlink()
        return True, None

    def _sim_schema_drift(self, job_id: str, dataset: str, *args):
        source_dir = Path("sim_data") / "drift_test"
        source_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"id": [1], "val": [100]}).write_parquet(
            source_dir / "part1.parquet"
        )
        pl.DataFrame({"id": [2], "val": ["error"]}).write_parquet(
            source_dir / "part2.parquet"
        )

        overrides = {
            "extract": {
                "source_type": "flat_file",
                "source_identifier": str(source_dir),
            }
        }
        self.orchestrator._trigger_job(job_id, dataset, overrides=overrides)
        typer.secho("✅ Drift job triggered.", fg="green")
        return True, None

    def _sim_data_loss(self, job_id: str, dataset: str, *args):
        run_ids = self.orchestrator._trigger_job(job_id, dataset)
        target_run = next(iter(run_ids))

        # Tick engine to ensure the task moves from PENDING to PROVISIONED
        # so the physical folder is created on disk.
        self.orchestrator._drive_engine()

        task_path = self.orchestrator.state_store.resolve_task_path(target_run)
        if not task_path:
            return None, f"Could not resolve path for run {target_run}"

        task = Task.from_folder(cast("Path", task_path), self.orchestrator.exec_ctx)

        task.update_manifest(
            {
                "write": {
                    "staging_artifact": "mock_table",
                    "rows_inserted": 1000000,
                    "sink_type": "clickhouse_db",
                    "sink_identifier": "prod.table",
                },
                "bitmask": 15,
            }
        )
        typer.secho(f"✅ Patched {target_run} with invalid row count.", fg="yellow")
        return True, None

    def _sim_memory(self, job_id: str, dataset: str, *args):
        typer.echo("🧠 Simulating memory pressure (100MB ceiling)...")
        overrides = {"_global": {"compute": {"memory_gb": 0.1}}}
        self.orchestrator._trigger_job(job_id, dataset, overrides=overrides)
        typer.secho("✅ Job triggered with restrictive RAM budget.", fg="green")
        return True, None

    def _sim_recovery(self, *args):
        typer.echo("🚑 Simulating mass-failure recovery event...")
        for i in range(5):
            mock_run_id = f"fail-test-{i}-{int(time.time())}"
            mock_path = (
                self.orchestrator.exec_ctx.failed_path / f"sim:job:{i}" / mock_run_id
            )
            mock_path.mkdir(parents=True, exist_ok=True)
            manifest = TaskManifest(
                job_id=f"sim_job_{i}",
                run_id=mock_run_id,
                dataset_id="sim_ds",
                status=ExecutionStatus.FAILED,
                current_stage="extract",
                bitmask=0,
            )
            (mock_path / MANIFEST_FILENAME).write_bytes(msgspec.json.encode(manifest))
            record = JobRecord(
                JOB_ID=f"sim_job_{i}",
                DATASET_ID="sim_ds",
                RUN_ID=mock_run_id,
                JOB_STATUS=ExecutionStatus.FAILED.value,
                SCHEDULED_TIMESTAMP_LC=datetime.now(),
            )
            placeholder_ctx = TaskContext.create_placeholder(record)
            (mock_path / CONFIG_FILENAME).write_bytes(
                msgspec.json.encode(placeholder_ctx)
            )
            typer.echo(f"   Created mock failure: {mock_run_id}")

        if self.orchestrator.exec_ctx.always_on:
            (self.orchestrator.exec_ctx.signal_path / "RECOVER_ALL.cmd").touch()
            typer.secho("🚀 RECOVER_ALL signal dropped.", fg="green")
        return True, None

    def _sim_kill_daemon(self, *args):
        typer.echo("💀 Executing KILL_DAEMON simulation...")

        def find_daemon():
            return next(
                (
                    p
                    for p in psutil.process_iter(["pid", "cmdline"])
                    if any("ingestion" in part for part in (p.info["cmdline"] or []))
                    and "start" in p.info["cmdline"]
                    and p.pid != os.getpid()
                ),
                None,
            )

        daemon_proc = find_daemon()

        if not daemon_proc:
            typer.echo("🚀 No active daemon found. Spawning one for the test...")
            # Start the daemon in a new session to ensure it lives as a background process
            subprocess.Popen(
                [sys.executable, "-m", "apps.ingestion", "start"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            # Wait for initialization (the lock file is our signal of readiness)
            for _ in range(10):
                if self.orchestrator.exec_ctx.lock_file.exists():
                    break
                time.sleep(1)

            daemon_proc = find_daemon()

        if not daemon_proc:
            return None, "No active daemon found or failed to start one"

        typer.secho(
            f"🔥 SIGKILL sent to Daemon (PID: {daemon_proc.pid})", fg="red", bold=True
        )
        daemon_proc.kill()
        time.sleep(2)

        builder_rep = TaskContextBuilder(env=self.env)
        exec_ctx_rep = builder_rep.get_execution_context()
        exec_ctx_rep.always_on = True
        runtime_rep = assemble_runtime(exec_ctx_rep, builder_rep)

        if isinstance(runtime_rep, DaemonRuntime):
            typer.secho(
                "✅ Replacement Orchestrator acquired the stale lock.", fg="green"
            )
            runtime_rep._perform_maintenance()
            typer.secho(
                "✅ Takeover logic completed successfully.", fg="green", bold=True
            )
        return True, None

    def _sim_latency(self, job_id: str, dataset: str, automated_input: bool):
        if platform.system() != "Linux":
            return None, "The 'latency' scenario requires Linux 'tc'"

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
            typer.secho(f"✅ Network throttled on {interface}.", fg="yellow")
            self.orchestrator._trigger_job(job_id, dataset)
            if automated_input:
                time.sleep(5)
            else:
                input("\nScenario active. Press Enter to restore network speed...")
        finally:
            subprocess.run(
                ["sudo", "tc", "qdisc", "del", "dev", interface, "root"],
                capture_output=True,
            )
            typer.secho("✅ Network latency removed.", fg="green")
        return True, None

    def _sim_retention(self, *args):
        typer.echo("扫 Simulating RETENTION: Seeding expired tasks...")
        for i in range(3):
            mock_run_id = f"expired-run-{i}"
            mock_path = (
                self.orchestrator.exec_ctx.active_path / "sim:job:exp" / mock_run_id
            )
            mock_path.mkdir(parents=True, exist_ok=True)
            expired_ts = time.time() - (365 * 24 * 3600)
            (mock_path / CONFIG_FILENAME).write_bytes(
                msgspec.json.encode(
                    {"job_id": "exp", "dataset_id": "ds", "expires_at": expired_ts}
                )
            )
            (mock_path / MANIFEST_FILENAME).write_bytes(
                msgspec.json.encode({"status": "success"})
            )
        typer.secho(
            "✅ Expired mock tasks seeded. Run 'python -m ingestion clean --expired' to test reaper.",
            fg="green",
        )
        return True, None
