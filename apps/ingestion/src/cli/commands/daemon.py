"""
Operational Lifecycle Module.

Provides core engine control commands for starting the Always-On daemon
and performing graceful service terminations.
"""

import traceback
from pathlib import Path
from typing import Annotated

import typer
from apps.ingestion.src.cli.state import app, configure_runtime, state
from apps.ingestion.src.cli.utils import _write_signal_file
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.core.orchestrator.modes.daemon import DaemonRuntime
from apps.ingestion.src.utils.common import setup_logger


@app.command(name="start")
def start_orchestrator(
    verbose: Annotated[
        int, typer.Option("--verbose", "-v", count=True, help="Set verbosity level")
    ] = 0,
) -> None:
    """Start the Ingestion Orchestrator in ALWAYS-ON mode (Daemon).

    In this mode, the engine polls for database triggers and reactive
    filesystem signals.
    """

    configure_runtime(
        verbose=verbose or state["verbose_level"],
        dry_run=state["dry_run"],
        ray_mode=state["ray_mode"].value,
    )

    builder = TaskContextBuilder()
    try:
        exec_ctx = builder.build_execution_context()
        exec_ctx.always_on = True
        runtime = assemble_runtime(exec_ctx, builder)

        if not isinstance(runtime, DaemonRuntime):
            typer.secho("❌ Error: 'start' command requires DaemonRuntime.", fg="red")
            raise typer.Exit(code=1)

        setup_logger(
            log_dir=Path("./.workspace/logs"),
            verbose_level=state["verbose_level"],
            filename="orchestrator_daemon.jsonl",
        )

        typer.secho(
            "🐝 Orchestrator starting in ALWAYS-ON mode...", fg="magenta", bold=True
        )
        runtime.run()

    except* FileNotFoundError as eg:
        for e in eg.exceptions:
            typer.secho(f"MISSING DATA: {e}", fg="yellow")

    except* ConnectionError as eg:
        typer.secho(
            f"INFRA FAILURE: {len(eg.exceptions)} tasks failed to connect.", fg="red"
        )
        if state["verbose_level"] >= 2:
            for e in eg.exceptions:
                typer.echo(f"Details: {e}")

    except* Exception as eg:
        for e in eg.exceptions:
            typer.secho(
                f"❌ CRITICAL FAILURE ({type(e).__name__}):", fg="red", bold=True
            )
            typer.secho(f"  {e}", fg="white")
            if state["verbose_level"] >= 2:
                traceback.print_exception(type(e), e, e.__traceback__)


@app.command(name="stop")
def stop_daemon(
    force: Annotated[
        bool,
        typer.Option(
            "--force", help="Kill immediately without waiting for active tasks"
        ),
    ] = False,
) -> None:
    """Gracefully shuts down the Always-On Orchestrator (Drains by default).

    Args:
        force: If True, signals the daemon to kill active tasks immediately
            rather than waiting for them to finish (drain mode).

    Decision: Signal-Based Termination.
    We use a .cmd file to signal the daemon thread. This ensures the engine
    completes active Ray tasks before exiting, preventing state corruption.
    """
    exec_ctx = TaskContextBuilder().build_execution_context()

    if not exec_ctx.lock_file.exists():
        typer.secho("⚠️ Orchestrator daemon is not running.", fg="yellow")
        return

    mode = "force" if force else "drain"
    _write_signal_file(exec_ctx, "STOP.cmd", mode, overwrite=True)
    typer.secho(f"🛑 Stop signal ({mode.upper()}) sent.", fg="yellow", bold=True)
