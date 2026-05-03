import traceback
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

# from apps.ingestion.src.cli.test import test_app
from apps.ingestion.src.core.contexts import ExecutionMode, parse_set_options
from apps.ingestion.src.core.contexts.execution import RayMode
from apps.ingestion.src.core.models.task import Task
from apps.ingestion.src.core.orchestrator import create_orchestrator
from apps.ingestion.src.utils.common import setup_logger

app = typer.Typer(help="50M Row Ingest Pipeline")
# app.add_typer(test_app, name="test")


state = {"dry_run": False, "debug": False, "ray_mode": RayMode.CLUSTER}


def _configure_runtime(debug: bool, dry_run: bool, ray_mode: str = "cluster") -> None:
    """Helper to apply runtime configurations (Logging, Dry Run)."""
    state["dry_run"] = dry_run
    state["debug"] = debug
    state["ray_mode"] = RayMode(ray_mode.lower())

    if debug:
        typer.secho("🔧 DEBUG MODE: ON", fg=typer.colors.CYAN)


@app.callback()
def global_options(
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate execution"),
    debug: bool = typer.Option(False, "--debug", help="Global debug logging"),
    ray_mode: str = typer.Option(
        "cluster", "--ray_mode", help="Ray execution mode (local/cluster)"
    ),
):
    _configure_runtime(debug, dry_run, ray_mode)


@app.command()
def run(
    # Main Argument - '...' indicates it is required
    partition_date: Annotated[
        datetime,
        typer.Argument(
            formats=["%Y-%m-%d"], help="The target date for processing (YYYY-MM-DD)"
        ),
    ],
    # Required Options - '...' indicates it is required
    job_id: Annotated[
        str, typer.Option("--job-id", "-j", help="The unique UUID for this run")
    ],
    dataset: Annotated[
        str,
        typer.Option("--dataset", "-d", help="Dataset identifier (e.g., 'sales_data')"),
    ],
    stage: Annotated[
        str | None, typer.Option("--stage", help="Execute a specific stage only")
    ],
    # Allow debug to be passed here to override the global setting
    debug: Annotated[
        bool, typer.Option("--debug", help="Enable verbose logging (overrides global)")
    ] = False,
    # Optional List - Default is None
    settings: Annotated[
        list[str] | None,
        typer.Option(
            "--set", "-s", help="Override config: key=value (e.g. -s batch_size=5000)"
        ),
    ] = None,
) -> None:
    """
    Execute the ingestion pipeline for a specific date and dataset.
    """
    # Allow local debug override if the global callback didn't catch it
    if debug:
        _configure_runtime(
            debug=True, dry_run=state["dry_run"], ray_mode=state["ray_mode"]
        )

    # 1. Configuration & Overrides
    overrides = parse_set_options(settings)

    # 2. Initialize Unified Logging as early as possible
    # Note: We hardcode a temporary log path or resolve it from env if orchestrator isn't ready
    # For now, let's keep the logic but move it before orchestrator init to catch builder logs.
    setup_logger(
        log_dir=Path("./.workspace/logs"),
        is_prod=not state["debug"],
        is_debug=state["debug"],
        filename=f"{job_id}.jsonl",
        enqueue=True,
    )

    # 3. Resolve Execution Mode
    exec_mode = ExecutionMode.DEBUG if state["debug"] else ExecutionMode.NORMAL

    # 4. Initialize Orchestrator with runtime context
    orchestrator = create_orchestrator()
    orchestrator.exec_ctx.execution_mode = exec_mode
    orchestrator.exec_ctx.ray_mode = state["ray_mode"]

    # 5. Isolated Stage Execution (Subprocess Entry Point)
    if stage:
        typer.echo(f"🛠️  Executing isolated stage: {stage}")
        task = Task(
            run_id=job_id,  # Simplified for isolated run
            composite_key=f"{job_id}:{dataset}",
            partition_date=partition_date.strftime("%Y-%m-%d"),
            worker_id="pex-subprocess",
            exec_ctx=orchestrator.exec_ctx,
            target_stage=stage,
        )
        task.execute()
        return

    typer.echo(f"🚀 Initializing {dataset} for {partition_date.date()} (ID: {job_id})")

    # 3. Hand off to Orchestrator
    def _execute_orchestrator():
        orchestrator.run(
            job_id=job_id,
            dataset_id=dataset,
            partition_date_str=partition_date.strftime("%Y-%m-%d"),
            overrides=overrides,
        )

    _execute_orchestrator()

    try:
        pass  # The _execute_orchestrator() call is now outside the try-except
    except Exception as e:
        typer.secho(f"💥 Critical Failure: {e}", fg=typer.colors.RED)
        if state["debug"]:
            traceback.print_exc()
        raise typer.Exit(code=1) from e


@app.command()
def start(
    debug: Annotated[
        bool, typer.Option("--debug", help="Enable verbose logging")
    ] = False,
) -> None:
    """
    Start the Ingestion Orchestrator in ALWAYS-ON mode (Daemon).
    In this mode, it polls for database triggers and filesystem signals.
    """

    orchestrator = create_orchestrator()
    orchestrator.exec_ctx.always_on = True

    # 2. Setup Local-Friendly Logging
    setup_logger(
        log_dir=Path("./.workspace/logs"),
        is_prod=orchestrator.exec_ctx.is_prod,
        is_debug=debug,
        filename="orchestrator_daemon.jsonl",
        # enqueue=True,
    )

    typer.secho(
        "🐝 Orchestrator starting in ALWAYS-ON mode...",
        fg=typer.colors.MAGENTA,
        bold=True,
    )
    orchestrator.run(job_id="daemon", dataset_id="daemon")


@app.command()
def recover(
    job_id: Annotated[
        str | None, typer.Option("--job-id", "-j", help="Specific Job ID to recover")
    ] = None,
    dataset: Annotated[
        str | None, typer.Option("--dataset", "-d", help="Specific dataset to recover")
    ] = None,
    partition_date: Annotated[
        str | None, typer.Option("--run-date", help="Specific run date to recover")
    ] = None,
    run_id: Annotated[
        str | None, typer.Option("--run-id", help="Specific run ID to recover")
    ] = None,
) -> None:
    """
    Recover jobs from a failed state (HOLD or FAILED).
    If no specific IDs are provided, triggers a global recovery sweep.
    """
    orchestrator = create_orchestrator()

    # 1. Search for candidate folders in terminal states
    target_folders = []
    for category in ["HOLD", "FAILED"]:
        base_path = orchestrator.exec_ctx.workspace_dir / category
        if not base_path.exists():
            continue

        # rglob ensures we find manifests regardless of directory depth
        for manifest_path in base_path.rglob("manifest.json"):
            folder = manifest_path.parent
            r_id = folder.name
            ident = folder.parent.name

            try:
                # Extract components to verify against filters
                p_job, p_ds, p_date, _ = orchestrator.exec_ctx.parse_identifier(
                    f"{ident}:{r_id}"
                )

                # Apply filters: if a value is provided, it must match
                if job_id and p_job != job_id:
                    continue
                if dataset and p_ds != dataset:
                    continue
                if partition_date and p_date != partition_date:
                    continue
                if run_id and r_id != run_id:
                    continue

                target_folders.append(folder)
            except ValueError:
                continue

    if not target_folders:
        typer.secho(
            "No matching failed or held jobs found to recover.",
            fg=typer.colors.YELLOW,
        )
        return

    typer.echo(f"🔧 Found {len(target_folders)} matching tasks in HOLD/FAILED.")

    # 2. Trigger Recovery mechanism based on loop mode
    if orchestrator.exec_ctx.always_on:
        # Signal trigger: Communicates with the background Orchestrator loop
        signal_path = orchestrator.exec_ctx.signal_path / "RECOVER_ALL.cmd"
        signal_path.touch()
        typer.secho(
            "🚀 Signal dropped. ALWAYS_ON Orchestrator will process the recovery.",
            fg=typer.colors.GREEN,
        )
    else:
        # Dumb trigger mode: CLI executes recovery directly in the foreground
        with typer.progressbar(target_folders, label="Recovering jobs") as progress:
            for folder in progress:
                orchestrator.lifecycle.recover_task_by_path(folder)
        typer.secho(
            f"✅ Successfully recovered {len(target_folders)} tasks.",
            fg=typer.colors.GREEN,
        )


if __name__ == "__main__":
    app()
