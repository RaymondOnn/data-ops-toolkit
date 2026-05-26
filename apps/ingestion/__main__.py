from typing import Annotated

import typer
from apps.ingestion.src.cli import actions
from apps.ingestion.src.cli.dev import dev_app
from apps.ingestion.src.cli.doctor import doctor_app
from apps.ingestion.src.cli.run import run_app
from apps.ingestion.src.cli.test import test_app
from libs.utils.exceptions import install_exception_hooks

app = typer.Typer(help="50M Row Ingest Pipeline")
app.add_typer(doctor_app, name="doctor")
app.add_typer(dev_app, name="dev")
app.add_typer(run_app, name="run")
app.add_typer(test_app, name="test")


@app.callback()
def global_options(
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate execution"),
    debug: bool = typer.Option(False, "--debug", help="Global debug logging"),
    ray_mode: str = typer.Option(
        "cluster", "--ray_mode", help="Ray execution mode (local/cluster)"
    ),
):
    actions.configure_runtime(debug, dry_run, ray_mode)


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
