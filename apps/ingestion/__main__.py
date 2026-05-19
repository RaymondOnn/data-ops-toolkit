from datetime import datetime
from typing import Annotated, Any

import typer
from apps.ingestion.src.cli import actions
from apps.ingestion.src.cli.dev import dev_app

# from apps.ingestion.src.cli.test import test_app
from apps.ingestion.src.cli.doctor import doctor_app
from apps.ingestion.src.core.contexts.execution import RayMode
from libs.utils.exceptions import install_exception_hooks
from loguru import logger

app = typer.Typer(help="50M Row Ingest Pipeline")
app.add_typer(doctor_app, name="doctor")
app.add_typer(dev_app, name="dev")
# app.add_typer(test_app, name="test")

state: dict[str, Any] = {"dry_run": False, "debug": False, "ray_mode": RayMode.CLUSTER}


def _configure_runtime(debug: bool, dry_run: bool, ray_mode: str = "cluster") -> None:
    """Helper to apply runtime configurations (Logging, Dry Run)."""
    state["dry_run"] = dry_run
    state["debug"] = debug
    state["ray_mode"] = RayMode(ray_mode.lower())

    if debug:
        typer.secho("🔧 DEBUG MODE: ON", fg="cyan")


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
    from_stage: Annotated[
        str | None, typer.Option("--from", help="Start execution from this stage")
    ] = None,
    to_stage: Annotated[
        str | None, typer.Option("--to", help="Stop execution after this stage")
    ] = None,
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

    try:
        actions.run_pipeline(
            partition_date=partition_date,
            job_id=job_id,
            dataset=dataset,
            from_stage=from_stage,
            to_stage=to_stage,
            debug=debug,
            settings=settings,
            state=state,
        )
    finally:
        logger.complete()


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
    try:
        actions.start_daemon(debug=debug)

    # 3. Handle specific ERROR CATEGORIES across the whole batch
    except* FileNotFoundError as eg:
        # eg.exceptions is a list of all FileNotFoundErrors that occurred
        for e in eg.exceptions:
            print(f"MISSING DATA: {e}")
        # Maybe send a specific alert to Slack about missing files

    except* ConnectionError as eg:
        # Handles network issues (like LocalStack being down)
        print(f"INFRA FAILURE: {len(eg.exceptions)} tasks failed to connect.")

    except* Exception as eg:
        # Catch-all for anything else in the group
        print(f"UNEXPECTED: {len(eg.exceptions)} miscellaneous failures.")


# Register Passthrough Commands directly from actions
app.command(name="recover")(actions.recover_tasks)
app.command(name="stop")(actions.stop_daemon)
app.command(name="clean")(actions.clean_workspace)


if __name__ == "__main__":
    install_exception_hooks()
    app()
