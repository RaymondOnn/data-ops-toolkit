from ty_extensions import Unknown
from datetime import datetime
from typing import Optional

import typer  # type: ignore
import structlog # type: ignore

from src.core.models.job import Job
from src.core.models.steps import JobSteps


app = typer.Typer(help="50M Row Ingest Pipeline")
state = {"dry_run": False, "debug": False}

@app.callback()
def global_options(
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate execution"),
    debug: bool = typer.Option(False, "--debug", help="Enable verbose logging")
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
    settings: Optional[list[str]] = typer.Option(
        None, "--set", "-s", help="Override config: key=value (e.g. -s batch_size=5000)"
    ),
) -> None:
    """
    Execute the ingestion pipeline for a specific date and dataset.
    """
    from src.services.factory import ServiceFactory
    from src.core.orchestrator import Orchestrator
    from src.core.contexts.job import parse_set_options
    
    typer.echo(f"Initializing {dataset} for {run_date.date()} (ID: {job_id})")
    
    # 1. Parse strings into a dictionary
    # These overrides will be used by JobContextBuilder in trigger_job
    overrides = parse_set_options(settings)

    # 2. Initialize Orchestrator
    # (Requires db_service, but in Trigger mode, we might mock it or pass None)
    db = ServiceFactory.get_service("db")  # Ensure this handles config-based init
    orchestrator = Orchestrator(db_service=db, )

    # 3. Hand off to Orchestrator (This triggers _run_synchronous_task)
    try:
        orchestrator.run(
            job_id=job_id,
            dataset_id=dataset,
            run_date_str=run_date.strftime("%Y-%m-%d"),
            overrides=overrides,
        )
    except Exception as e:
        typer.secho(f"💥 Critical Failure: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@app.command()
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
    # ... (Your 'recover' logic) ...
    pass


if __name__ == "__main__":
    # This makes 'python -m ingest' work
    app()
