import traceback
from datetime import datetime
from typing import Annotated

import structlog
import typer

# from apps.ingestion.src.cli.test import test_app
from apps.ingestion.src.core.contexts import parse_set_options
from apps.ingestion.src.core.orchestrator import create_orchestrator
from libs.utils.log import setup_logging

app = typer.Typer(help="50M Row Ingest Pipeline")
# app.add_typer(test_app, name="test")


state = {"dry_run": False, "debug": False}


def _configure_runtime(debug: bool, dry_run: bool) -> None:
    """Helper to apply runtime configurations (Logging, Dry Run)."""
    state["dry_run"] = dry_run
    state["debug"] = debug

    if debug:
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(10))
        typer.secho("🔧 DEBUG MODE: ON", fg=typer.colors.CYAN)


@app.callback()
def global_options(
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate execution"),
    debug: bool = typer.Option(False, "--debug", help="Global debug logging"),
):
    _configure_runtime(debug, dry_run)


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
        _configure_runtime(debug=True, dry_run=state["dry_run"])

    # 1. Configuration & Overrides
    overrides = parse_set_options(settings)

    # 2. Initialize Orchestrator
    orchestrator = create_orchestrator()

    # 2.5 Initialize Unified Logging in the Workspace
    # We name the file after the job_id to fulfill the 'one log per run' request
    setup_logging(
        log_dir=orchestrator.exec_ctx.workspace_dir / "logs",
        is_prod=not state["debug"],
        filename=f"{job_id}.jsonl",
    )

    typer.echo(f"🚀 Initializing {dataset} for {run_date.date()} (ID: {job_id})")

    # 3. Hand off to Orchestrator
    try:
        orchestrator.run(
            job_id=job_id,
            dataset_id=dataset,
            run_date_str=run_date.strftime("%Y-%m-%d"),
            overrides=overrides,
        )
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
