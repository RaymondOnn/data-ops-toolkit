from datetime import datetime
from typing import Optional

import structlog
import typer
from src.core.models.steps import JobSteps

from src.core.contexts import parse_set_options
from src.core.orchestrator import create_orchestrator

app = typer.Typer(help="50M Row Ingest Pipeline")
app.add_typer(test_app, name="test")


state = {"dry_run": False, "debug": False}


@app.callback()
def global_options(
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate execution"),
    debug: bool = typer.Option(False, "--debug", help="Enable verbose logging"),
):
    state["dry_run"] = dry_run
    state["debug"] = debug

    if debug:
        # Assuming structlog is used as per your orchestrator
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(10))
        typer.secho("🔧 DEBUG MODE: ON", fg=typer.colors.CYAN)


@app.command()
def run(
    # Main Argument
    run_date: datetime = typer.Argument(
        ..., formats=["%Y-%m-%d"], help="The target date for processing (YYYY-MM-DD)"
    ),
    # Required/Common Options
    job_id: str = typer.Option(
        ..., "--job-id", "-j", help="The unique UUID for this run"
    ),
    dataset: str = typer.Option(
        ..., "--dataset", "-d", help="Dataset identifier (e.g., 'sales_data')"
    ),
    settings: list[str] | None = typer.Option(
        None, "--set", "-s", help="Override config: key=value (e.g. -s batch_size=5000)"
    ),
) -> None:
    """
    Execute the ingestion pipeline for a specific date and dataset.
    """

    typer.echo(f"🚀 Initializing {dataset} for {run_date.date()} (ID: {job_id})")

    # 1. Configuration & Overrides
    overrides = parse_set_options(settings)

    # 2. Initialize Orchestrator
    orchestrator = create_orchestrator()

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
            import traceback

            traceback.print_exc()
        raise typer.Exit(code=1)


@test_app.command(name="run")
def test(
    # Main Argument
    run_date: datetime = typer.Argument(
        ..., formats=["%Y-%m-%d"], help="The target date for processing (YYYY-MM-DD)"
    ),
    # Required/Common Options
    job_id: str = typer.Option(
        ..., "--job-id", "-j", help="The unique UUID for this run"
    ),
    dataset: str = typer.Option(
        ..., "--dataset", "-d", help="Dataset identifier (e.g., 'sales_data')"
    ),
    from_step: Optional[JobSteps] = typer.Option(
        None, "--from", help="Force start from this step"
    ),
    to_step: Optional[JobSteps] = typer.Option(
        None, "--to", help="Stop execution after this step"
    ),
    force: bool = typer.Option(True, "--force", help="Defaults to True for testing"),
) -> None:
    """Developer test mode. Allows slicing the pipeline."""
    _execute_pipeline(
        run_date, job_id, dataset, from_step=from_step, to_step=to_step, force=force
    )


@app.command()
def recover():
    """Recover jobs from a failed state."""
    # ... placeholder ...
    pass


if __name__ == "__main__":
    app()
