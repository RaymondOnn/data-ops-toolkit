import multiprocessing
import os
import platform
import subprocess
import time
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import psutil
import ray
import typer
from apps.ingestion.src.core.orchestrator import create_orchestrator
from apps.ingestion.src.extras.regression.regression import (
    find_affected_peers,
    run_comparison,
)
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    DISK_THRESHOLD_HALT,
)
from libs.utils.system import get_disk_usage

test_app = typer.Typer(help="Testing and validation utilities.")


class ScenarioType(StrEnum):
    ZOMBIE = "zombie"  # Test worker death recovery
    BLOCK = "block"  # Test circuit breaker behavior
    FLAPPING = "flapping"  # Test intermittent service availability
    CONCURRENCY = "concurrency"  # Test high-volume task dispatch
    MEMORY = "memory"  # Test 2GB RAM ceiling/streaming
    RECOVERY = "recovery"  # Test mass-recovery of failed tasks
    STRESS = "stress"  # Test cross-job resource contention
    THROTTLING = "throttling"  # Test adaptive system backpressure
    KILL_DAEMON = "kill_daemon"  # Test master failure and lock recovery
    LATENCY = "latency"  # Test network degradation (Linux tc)
    DISK_FULL = "disk_full"  # Test emergency halt logic


@test_app.command("compare")
def test_compare(
    job_id: Annotated[str, typer.Argument(help="The job ID containing the dataset")],
    dataset: Annotated[
        str, typer.Option("--dataset", "-d", help="Specific dataset to compare")
    ],
    env: Annotated[
        str, typer.Option("--env", help="Environment to resolve sinks from")
    ] = "local",
    suffix: Annotated[
        str, typer.Option("--suffix", help="Suffix used for the shadow table")
    ] = "_shadow",
    ignore_cols: Annotated[
        str, typer.Option("--ignore", help="Comma-separated columns to skip")
    ] = "",
    detailed: Annotated[
        bool, typer.Option("--detailed", help="Use analytical reporting")
    ] = False,
):
    """
    Performs a side-by-side equality check between production and shadow sinks.
    """
    ignore_set = {c.strip() for c in ignore_cols.split(",") if c.strip()}

    typer.echo(f"🔍 Comparing {dataset} against shadow copy...")

    success = run_comparison(
        job_id=job_id,
        dataset_id=dataset,
        env=env,
        suffix=suffix,
        ignore_cols=ignore_set,
        detailed=detailed,
    )

    if not success:
        typer.secho("❌ Regression detected! Data mismatch found.", fg="red", bold=True)
        raise typer.Exit(1)

    typer.secho("✅ Regression test passed. Data is identical.", fg="green")


@test_app.command("impact")
def test_impact(job_id: str, dataset: str, env: str = "local"):
    """
    Discovery tool to find other datasets sharing the same transformation logic.
    """
    peers = find_affected_peers(job_id, dataset, env=env)
    if not peers:
        typer.echo("No affected peer datasets found.")
        return

    typer.echo(f"👥 Found {len(peers)} datasets sharing this transformation logic:")
    for p in peers:
        typer.echo(f" - {p['job_id']}.{p['dataset_id']}")


@test_app.command("scenario")
def test_scenario(
    scenario: Annotated[
        ScenarioType, typer.Argument(help="The simulation scenario to execute")
    ],
    job_id: Annotated[
        str | None, typer.Option("--job-id", "-j", help="Job ID to use")
    ] = None,
    dataset: Annotated[
        str | None, typer.Option("--dataset", "-d", help="Dataset ID to use")
    ] = None,
    env: str = "local",
):
    """
    Runs complex multi-stage simulations to test the limits of the engine.
    """
    typer.secho(f"🧪 Executing Simulation: {scenario.upper()}", fg="magenta", bold=True)
    orchestrator = create_orchestrator(env=env)

    # Resolve Defaults from app.yaml via TaskContextBuilder
    job_id = job_id or orchestrator.builder.app_settings.get("test.default_job")
    dataset = dataset or orchestrator.builder.app_settings.get("test.default_dataset")

    # Auto-generate if missing
    if job_id and not (APP_CONFIG_ROOT / job_id).exists():
        typer.echo(f"⚠️ Simulation job '{job_id}' missing. Generating artifacts...")
        from libs.utils.mock_data import generate_simulation_artifacts

        # For memory scenarios, we force a high row count to test limits
        rows = 10_000_000 if scenario == ScenarioType.MEMORY else 1000
        generate_simulation_artifacts(
            job_id=job_id,
            dataset=dataset or "sim_data",
            config_root=APP_CONFIG_ROOT,
            target_root=Path("sim_data"),
            rows=rows,
        )
        typer.secho("✅ Test artifacts generated.", fg="green")

    # Scenarios that strictly require a target unit
    resource_intensive = {
        ScenarioType.ZOMBIE,
        ScenarioType.MEMORY,
        ScenarioType.CONCURRENCY,
        ScenarioType.LATENCY,
    }

    if scenario in resource_intensive:
        if not job_id or not dataset:
            typer.secho(
                "❌ Error: This scenario requires --job-id and --dataset "
                "OR 'test.default_job/dataset' configured in app.yaml",
                fg="red",
            )
            raise typer.Exit(1)

    if scenario == ScenarioType.CONCURRENCY:
        count = 15
        typer.echo(
            f"🌀 Stressing TaskManager with {count} concurrent partition requests..."
        )
        # Triggering many partitions simultaneously to test logical capping and queueing
        for i in range(count):
            date_str = f"2024-01-{i+1:02d}"
            orchestrator._trigger_job(job_id, dataset, partition_date_str=date_str)
        typer.secho(
            f"✅ {count} Tasks queued. Monitor Ray Dashboard to verify concurrency limits.",
            fg="green",
        )

    elif scenario == ScenarioType.ZOMBIE:
        typer.echo("🧟 Injecting worker failure (Chaos Simulation)...")
        # We trigger a job and wait for the Ray ObjectRef to be tracked in TaskManager
        run_ids = orchestrator._trigger_job(job_id, dataset)
        target_run = next(iter(run_ids))

        typer.echo(f"Waiting for {target_run} to enter RUNNING state...")
        found_ref = None
        for _ in range(30):  # 30 second timeout
            for ref, key in orchestrator.tasks._active_tasks.items():
                if target_run in key:
                    found_ref = ref
                    break
            if found_ref:
                break
            time.sleep(1)

        if found_ref:
            typer.secho(
                f"💥 Terminating Ray task for Run ID: {target_run}", fg="red", bold=True
            )
            ray.cancel(found_ref, force=True)
            typer.echo(
                "Monitor orchestrator logs for 'Zombie task detected' and auto-recovery."
            )
        else:
            typer.error("Timed out waiting for task to start. Is the daemon running?")

    elif scenario == ScenarioType.BLOCK:
        # Simulate a downed Oracle DB or S3 bucket
        target_service = "clickhouse_db"
        typer.echo(f"🚧 Simulating outage for service: {target_service}")
        signal_file = (
            orchestrator.exec_ctx.signal_path / f"{target_service}.source_down"
        )
        signal_file.touch()
        typer.secho(f"✅ Created signal: {signal_file.name}", fg="yellow")
        typer.echo(f"New tasks for '{target_service}' will now transition to BLOCKED.")

    elif scenario == ScenarioType.STRESS:
        typer.echo("🔥 Executing STRESS simulation: Cross-job contention...")
        # 1. Discover all jobs
        job_dirs = [
            d.name
            for d in APP_CONFIG_ROOT.iterdir()
            if d.is_dir() and (d / "config.yaml").exists()
        ]

        if not job_dirs:
            typer.error("No jobs found in configs/ to stress test.")
            raise typer.Exit(1)

        # 2. Trigger all datasets for all jobs for a specific date
        test_date = "2024-03-15"
        for j_id in job_dirs:
            typer.echo(f"   Queueing all datasets for job: {j_id}")
            orchestrator._trigger_job(
                j_id, dataset_id=None, partition_date_str=test_date
            )

        typer.secho(
            f"✅ Triggered {len(job_dirs)} jobs simultaneously. Monitor budget in Ray Dashboard.",
            fg="green",
        )

    elif scenario == ScenarioType.THROTTLING:
        typer.echo("📉 Executing THROTTLING simulation: Adaptive Backpressure...")

        def cpu_burner():
            """Consumes CPU to trigger the 90% threshold."""
            while True:
                _ = 1000 * 1000

        # 1. Start Burners (Number of cores)
        typer.echo("🔥 Spiking system CPU usage to trigger threshold...")
        burners = [
            multiprocessing.Process(target=cpu_burner, daemon=True)
            for _ in range(multiprocessing.cpu_count())
        ]
        for b in burners:
            b.start()

        try:
            # Give CPU a moment to ramp up
            time.sleep(5)

            # 2. Try to trigger a job
            typer.echo("Attempting to trigger job while system is hot...")
            orchestrator._trigger_job(job_id, dataset, partition_date_str="2024-04-01")

            typer.secho(
                "✅ Burners active. Check logs: Orchestrator should log "
                "'Adaptive throttling engaged' and keep task in WAITING.",
                fg="yellow",
            )

            typer.echo("Waiting 15 seconds for verification...")
            time.sleep(15)

        finally:
            # 3. Cleanup Burners
            typer.echo("🛑 Stopping burners...")
            for b in burners:
                b.terminate()
            typer.secho("✅ System CPU cooling down.", fg="green")

    elif scenario == ScenarioType.FLAPPING:
        target_service = "clickhouse_db"
        typer.echo(f"📳 Simulating flapping connectivity for: {target_service}")
        signal_file = (
            orchestrator.exec_ctx.signal_path / f"{target_service}.source_down"
        )

        # Trigger a few tasks first
        orchestrator._trigger_job(job_id, dataset, partition_date_str="2024-02-01")

        for i in range(3):
            typer.echo(f"Iteration {i+1}: Dropping service...")
            signal_file.touch()
            time.sleep(5)
            typer.echo(f"Iteration {i+1}: Restoring service...")
            if signal_file.exists():
                signal_file.unlink()
            time.sleep(5)
        typer.secho("✅ Flapping simulation complete.", fg="green")

    elif scenario == ScenarioType.MEMORY:
        typer.echo("🧠 Simulating memory pressure (100MB ceiling)...")
        overrides = {"_global": {"compute": {"memory_gb": 0.1}}}
        orchestrator._trigger_job(job_id, dataset, overrides=overrides)
        typer.secho(
            "✅ Job triggered with restrictive RAM budget. Monitor for queueing.",
            fg="green",
        )

    elif scenario == ScenarioType.RECOVERY:
        typer.echo("🚑 Simulating mass-failure recovery event...")

        # 1. Create mock failed tasks (metadata only)
        for i in range(5):
            mock_run_id = f"fail-test-{i}-{int(time.time())}"
            # standard structure: {workspace}/FAILED/{identifier}/{run_id}
            mock_path = orchestrator.exec_ctx.failed_path / f"sim:job:{i}" / mock_run_id
            mock_path.mkdir(parents=True, exist_ok=True)

            # Drop a dummy manifest so the Janitor recognizes it
            (mock_path / "manifest.json").write_text(
                '{"status": "failed", "current_stage": "extract"}'
            )
            typer.echo(f"   Created mock failure: {mock_run_id}")

        # 2. Drop the recovery signal
        if orchestrator.exec_ctx.always_on:
            signal_path = orchestrator.exec_ctx.signal_path / "RECOVER_ALL.cmd"
            signal_path.touch()
            typer.secho("🚀 RECOVER_ALL signal dropped.", fg="green")
        else:
            typer.secho(
                "⚠️ Orchestrator not in Always-On mode. Use 'python -m ingestion recover' to process manually.",
                fg="yellow",
            )

    elif scenario == ScenarioType.KILL_DAEMON:
        typer.echo("💀 Executing KILL_DAEMON simulation: Master failure recovery...")
        # 1. Identify Daemon Process
        daemon_proc = None
        for proc in psutil.process_iter(["pid", "cmdline"]):
            cmd = proc.info.get("cmdline") or []
            if any("ingestion" in part for part in cmd) and "start" in cmd:
                if proc.pid != os.getpid():
                    daemon_proc = proc
                    break

        if not daemon_proc:
            typer.error(
                "No active Orchestrator daemon found. Run 'python -m ingestion start' first."
            )
            raise typer.Exit(1)

        pid = daemon_proc.pid
        typer.secho(f"🔥 SIGKILL sent to Daemon (PID: {pid})", fg="red", bold=True)
        daemon_proc.kill()

        # 2. Wait for OS cleanup
        time.sleep(2)

        # 3. Simulate replacement takeover
        typer.echo("Initializing replacement Orchestrator to verify lock takeover...")
        try:
            replacement = create_orchestrator(env=env)
            typer.secho(
                "✅ Replacement Orchestrator acquired the stale lock.", fg="green"
            )

            typer.echo("Running maintenance sweep to re-register orphaned tasks...")
            replacement._perform_maintenance()
            typer.secho(
                "✅ Takeover logic completed successfully.", fg="green", bold=True
            )
        except Exception as e:
            typer.error(f"Takeover failed: {e}")

    elif scenario == ScenarioType.LATENCY:
        if platform.system() != "Linux":
            typer.error(
                "The 'latency' scenario requires Linux 'tc' (Traffic Control) utility."
            )
            raise typer.Exit(1)

        # Interface selection logic: lo for localstack, eth0 for real cloud
        interface = "lo" if env == "local" else "eth0"
        delay = "500ms"
        typer.echo(
            f"🕸️ Executing LATENCY simulation: Injecting {delay} delay on {interface}..."
        )

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
            typer.secho(
                f"✅ Network throttled. Calls to {interface} will be significantly delayed.",
                fg="yellow",
            )

            if job_id and dataset:
                typer.echo(f"Triggering {dataset} to observe performance impact...")
                orchestrator._trigger_job(job_id, dataset)

            typer.echo("\nScenario active. Press Enter to restore network speed...")
            input()
        except KeyboardInterrupt:
            pass
        except subprocess.CalledProcessError as e:
            typer.error(
                f"Failed to apply latency: {e}. Ensure you have sudo privileges for 'tc'."
            )
        finally:
            typer.echo("🛑 Restoring network...")
            subprocess.run(
                ["sudo", "tc", "qdisc", "del", "dev", interface, "root"],
                capture_output=True,
            )
            typer.secho("✅ Network latency removed.", fg="green")

    elif scenario == ScenarioType.DISK_FULL:
        typer.echo("💾 Executing DISK_FULL simulation: Triggering safety halt...")
        usage = get_disk_usage(orchestrator.exec_ctx.workspace_dir)

        # Target slightly above the halt threshold (e.g., 92%)
        target_pct = DISK_THRESHOLD_HALT + 2
        bytes_needed = int((usage.total * (target_pct / 100)) - usage.used)

        if bytes_needed <= 0:
            typer.secho(
                "⚠️ Disk is already above threshold. Daemon should trigger immediate halt.",
                fg="yellow",
            )
        else:
            dummy_file = orchestrator.exec_ctx.workspace_dir / ".disk_pressure_sim"
            typer.echo(
                f"Creating {bytes_needed / (1024**3):.2f}GB dummy file to reach {target_pct}%..."
            )

            try:
                # Use fallocate on Linux (instant allocation), fallback to truncate on other OS
                if platform.system() == "Linux":
                    subprocess.run(
                        ["fallocate", "-l", str(bytes_needed), str(dummy_file)],
                        check=True,
                    )
                else:
                    # Creates a sparse file that reports the target size
                    with dummy_file.open("wb") as f:
                        f.truncate(bytes_needed)

                new_usage = get_disk_usage(orchestrator.exec_ctx.workspace_dir)
                typer.secho(
                    f"✅ Disk pressure applied. Current Usage: {new_usage.percent}%",
                    fg="red",
                    bold=True,
                )
                typer.echo(
                    "Run 'python -m ingestion start' and verify it exits with 'Disk full'."
                )
                typer.echo("\nPress Enter to delete dummy file and restore space...")
                input()
            except Exception as e:
                typer.error(f"Failed to create dummy file: {e}")
            finally:
                if dummy_file.exists():
                    dummy_file.unlink()
                    typer.secho("✅ Disk space restored.", fg="green")

    typer.secho(
        "\n🏁 Scenario initiated. View results in ClickHouse or Logs.",
        fg="cyan",
        bold=True,
    )
