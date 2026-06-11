"""
Workspace Maintenance Module.

Provides administrative commands for managing the local filesystem state,
including TTL-based expiry sweeps and surgical run purges.
"""

from typing import Annotated

import typer
from apps.ingestion.src.cli.state import app
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime


@app.command(name="clean")
def clean_workspace(
    run_id: Annotated[
        str | None, typer.Argument(help="Specific run ID to purge from the workspace")
    ] = None,
    older_than: Annotated[
        int | None,
        typer.Option("--days", help="Purge tasks older than X days (age-based sweep)"),
    ] = None,
    expired: Annotated[
        bool,
        typer.Option("--expired", help="Only purges folders beyond their TTL."),
    ] = True,
    all_data: Annotated[
        bool,
        typer.Option("--all", help="Wipe the entire local workspace."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List what would be deleted."),
    ] = False,
) -> None:
    """Cleans up the local workspace directory.

    Decision: Safety Interlocks (ADR 015).
    We implement a strict guard against 'clean --all' if a daemon is running.
    Deleting the active/ or signals/ directory while Ray workers are
    communicating state would lead to 'Ghost Tasks' and database
    desynchronization.

    Decision: Intentional Confirmation.
    Wiping the workspace is a destructive operation. We use Typer's native
    confirmation prompt for the '--all' flag to prevent accidental
    data loss in production environments.
    """
    builder = TaskContextBuilder()
    exec_ctx = builder.build_execution_context()
    runtime = assemble_runtime(exec_ctx, builder)

    if all_data:
        # Verify if daemon is holding the system lock
        if runtime.exec_ctx.lock_file.exists():
            typer.secho(
                "🚨 ERROR: Daemon is running. Stop it before 'clean --all'.",
                fg="red",
                bold=True,
            )
            raise typer.Exit(code=1)

        if not typer.confirm("DANGER: This will wipe the entire workspace. Proceed?"):
            typer.secho("Cleanup aborted.", fg="yellow")
            raise typer.Exit(code=0)

    typer.echo("🧹 Initializing Janitor sweep...")

    # Delegate physical logic to the Janitor component
    runtime.orchestrator.janitor.cleanup_workspace(
        expired=expired,
        all_data=all_data,
        run_id=run_id,
        older_than_days=older_than,
    )
