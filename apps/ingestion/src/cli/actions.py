from pathlib import Path
from typing import Annotated, Any

import typer
from apps.ingestion.src.core.contexts import (
    RayMode,
    TaskContextBuilder,
)
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.core.orchestrator.modes.daemon import DaemonRuntime
from apps.ingestion.src.utils.common import setup_logger

state: dict[str, Any] = {"dry_run": False, "debug": False, "ray_mode": RayMode.CLUSTER}


def configure_runtime(debug: bool, dry_run: bool, ray_mode: str = "cluster") -> None:
    """Helper to apply runtime configurations (Logging, Dry Run)."""
    state["dry_run"] = dry_run
    state["debug"] = debug
    state["ray_mode"] = RayMode(ray_mode.lower())

    if debug:
        typer.secho("🔧 DEBUG MODE: ON", fg="cyan")


def start_daemon(debug: bool) -> None:
    """Implementation logic for the 'start' (daemon) command."""
    builder = TaskContextBuilder()
    exec_ctx = builder.get_execution_context()
    exec_ctx.always_on = True
    runtime = assemble_runtime(exec_ctx, builder)

    if not isinstance(runtime, DaemonRuntime):
        typer.secho("❌ Error: 'start' command requires DaemonRuntime.", fg="red")
        raise typer.Exit(code=1)

    setup_logger(
        log_dir=Path("./.workspace/logs"),
        is_prod=runtime.exec_ctx.is_prod,
        is_debug=debug,
        filename="orchestrator_daemon.jsonl",
    )

    typer.secho(
        "🐝 Orchestrator starting in ALWAYS-ON mode...", fg="magenta", bold=True
    )
    runtime.run()


def recover_tasks(
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
    builder = TaskContextBuilder()
    exec_ctx = builder.get_execution_context()
    runtime = assemble_runtime(exec_ctx, builder)

    target_folders = []
    for category in ["HOLD", "FAILED"]:
        base_path = runtime.exec_ctx.workspace_dir / category
        if not base_path.exists():
            continue

        for manifest_path in base_path.rglob("manifest.json"):
            folder = manifest_path.parent
            r_id = folder.name
            ident = folder.parent.name

            try:
                p_job, p_ds, p_date, _ = runtime.exec_ctx.parse_identifier(
                    f"{ident}:{r_id}"
                )
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
        typer.secho("No matching failed or held jobs found to recover.", fg="yellow")
        return

    typer.echo(f"🔧 Found {len(target_folders)} matching tasks in HOLD/FAILED.")

    if runtime.exec_ctx.always_on:
        signal_path = runtime.exec_ctx.signal_path / "RECOVER_ALL.cmd"
        signal_path.touch()
        typer.secho("🚀 Signal dropped. Daemon will process recovery.", fg="green")
    else:
        with typer.progressbar(target_folders, label="Recovering jobs") as progress:
            for folder in progress:
                runtime.orchestrator.janitor.recover_task_by_path(folder)
        typer.secho(
            f"✅ Successfully recovered {len(target_folders)} tasks.", fg="green"
        )


def stop_daemon(
    force: Annotated[
        bool,
        typer.Option(
            "--force", help="Kill immediately without waiting for active tasks"
        ),
    ] = False,
) -> None:
    """Gracefully shuts down the Always-On Orchestrator (Drains by default)."""
    builder = TaskContextBuilder()
    exec_ctx = builder.get_execution_context()
    signal_path = exec_ctx.signal_path / "STOP.cmd"
    exec_ctx.signal_path.mkdir(parents=True, exist_ok=True)

    if not exec_ctx.lock_file.exists():
        typer.secho("⚠️ Orchestrator daemon is not running.", fg="yellow")
        return

    mode = "force" if force else "drain"
    signal_path.write_text(mode)
    typer.secho(f"🛑 Stop signal ({mode.upper()}) sent.", fg="yellow", bold=True)


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
    """
    Cleans up the local workspace directory.
    """
    builder = TaskContextBuilder()
    exec_ctx = builder.get_execution_context()
    runtime = assemble_runtime(exec_ctx, builder)

    if all_data:
        if runtime.exec_ctx.lock_file.exists():
            typer.secho(
                "🚨 ERROR: Daemon is running. Stop it before 'clean --all'.", fg="red"
            )
            raise typer.Exit(code=1)

        if not typer.confirm("DANGER: Wipe entire workspace?"):
            typer.secho("Cleanup aborted.", fg="yellow")
            raise typer.Exit(code=0)

    runtime.orchestrator.janitor.clean_workspace(
        expired=expired,
        all_data=all_data,
        dry_run=dry_run,
        run_id=run_id,
        older_than_days=older_than,
    )
