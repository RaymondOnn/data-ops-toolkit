import traceback
from datetime import datetime
from pathlib import Path
from typing import Annotated

import structlog
import typer

# from apps.ingestion.src.cli.test import test_app
from apps.ingestion.src.core.contexts import ExecutionMode, parse_set_options
from apps.ingestion.src.core.contexts.execution import RayMode
from apps.ingestion.src.core.orchestrator import create_orchestrator
from libs.utils.log import setup_logging

app = typer.Typer(help="50M Row Ingest Pipeline")
# app.add_typer(test_app, name="test")


state = {"dry_run": False, "debug": False, "ray_mode": RayMode.CLUSTER}


def _configure_runtime(debug: bool, dry_run: bool, ray_mode: str = "cluster") -> None:
    """Helper to apply runtime configurations (Logging, Dry Run)."""
    state["dry_run"] = dry_run
    state["debug"] = debug
    state["ray_mode"] = RayMode(ray_mode.lower())

    if debug:
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(10))
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
    run_date: Annotated[
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
    setup_logging(
        log_dir=Path("./.workspace/logs"),
        is_prod=not state["debug"],
        is_debug=state["debug"],
        filename=f"{job_id}.jsonl",
    )

    # 3. Resolve Execution Mode
    exec_mode = ExecutionMode.DEBUG if state["debug"] else ExecutionMode.NORMAL

    # 4. Initialize Orchestrator with runtime context
    orchestrator = create_orchestrator()
    orchestrator.exec_ctx.execution_mode = exec_mode
    orchestrator.exec_ctx.ray_mode = state["ray_mode"]

    typer.echo(f"🚀 Initializing {dataset} for {run_date.date()} (ID: {job_id})")

    # 3. Hand off to Orchestrator
    def _execute_orchestrator():
        orchestrator.run(
            job_id=job_id,
            dataset_id=dataset,
            run_date_str=run_date.strftime("%Y-%m-%d"),
            overrides=overrides,
        )

    # If in debug mode and Ray is local, one wrapper covers the whole process
    if state["debug"] and state["ray_mode"] == RayMode.LOCAL:
        try:
            from moto import mock_aws

            with mock_aws():
                _execute_orchestrator()
        except ImportError:
            _execute_orchestrator()
    else:
        _execute_orchestrator()

    try:
        pass  # The _execute_orchestrator() call is now outside the try-except
    except Exception as e:
        typer.secho(f"💥 Critical Failure: {e}", fg=typer.colors.RED)
        if state["debug"]:
            traceback.print_exc()
        raise typer.Exit(code=1) from e


@app.command()
def recover():
    """Recover jobs from a failed state."""
    # ... placeholder ...
    pass


if __name__ == "__main__":
    app()
