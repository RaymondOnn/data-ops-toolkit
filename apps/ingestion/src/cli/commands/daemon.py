"""
Operational Lifecycle Module.

Provides core engine control commands for starting the Always-On daemon
and performing graceful service terminations.
"""

from pathlib import Path
from typing import Annotated

import typer
from apps.ingestion.src.cli.state import app
from apps.ingestion.src.cli.utils import _write_signal_file
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.core.orchestrator.modes.daemon import DaemonRuntime
from apps.ingestion.src.utils.common import setup_logger


@app.command(name="start")
def start_orchestrator(
    debug: Annotated[
        bool, typer.Option("--debug", help="Enable verbose logging")
    ] = False,
) -> None:
    """Start the Ingestion Orchestrator in ALWAYS-ON mode (Daemon).

    In this mode, the engine polls for database triggers and reactive
    filesystem signals.

    Args:
        debug: Enables verbose logging and diagnostics.

    Decision: Unified Exception Hooking (ADR 014).
    We use the 'except*' syntax to handle ExceptionGroups produced by the
    Daemon's multi-threaded scheduler and Ray workers. This allows us to
    report failures across multiple concurrent tasks without crashing the
    main monitoring thread.
    """
    builder = TaskContextBuilder()
    try:
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

    except* FileNotFoundError as eg:
        for e in eg.exceptions:
            typer.secho(f"MISSING DATA: {e}", fg="yellow")

    except* ConnectionError as eg:
        typer.secho(
            f"INFRA FAILURE: {len(eg.exceptions)} tasks failed to connect.", fg="red"
        )

    except* Exception as eg:
        typer.secho(
            f"UNEXPECTED: {len(eg.exceptions)} miscellaneous failures.", fg="red"
        )


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
    exec_ctx = TaskContextBuilder().get_execution_context()

    if not exec_ctx.lock_file.exists():
        typer.secho("⚠️ Orchestrator daemon is not running.", fg="yellow")
        return

    mode = "force" if force else "drain"
    _write_signal_file(exec_ctx, "STOP.cmd", mode, overwrite=True)
    typer.secho(f"🛑 Stop signal ({mode.upper()}) sent.", fg="yellow", bold=True)
