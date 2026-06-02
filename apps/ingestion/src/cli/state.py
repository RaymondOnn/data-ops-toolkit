from typing import Any

import typer
from apps.ingestion.src.core.contexts import RayMode

"""
Global CLI State Management.

Decision: Configuration Decoupling.
By isolating the CLI's mutable state and setup logic, we allow other
modules to inspect runtime flags (like dry_run) without importing the
heavy Typer action implementations.
"""

state: dict[str, Any] = {"dry_run": False, "debug": False, "ray_mode": RayMode.CLUSTER}

# Decision: Centralized App Instance.
# We instantiate the Typer app here so that implementation modules can use
# the @app.command decorator without inducing circular imports with the
# package entry point.
app = typer.Typer(help="50M Row Ingest Pipeline")


def configure_runtime(debug: bool, dry_run: bool, ray_mode: str = "cluster") -> None:
    """Applies global runtime configurations to the internal state.

    Args:
        debug: Enables verbose logging and diagnostic hooks.
        dry_run: Prevents physical side-effects (DB writes, file moves).
        ray_mode: Determines the compute backend (local vs cluster).

    Decision: Single Source of Truth.
    We store the CLI flags in a mutable dictionary within this module to
    allow the Orchestrator and Ray workers to inspect global settings
    without re-parsing CLI arguments or inducing circular imports between
    actions and the entry point.
    """
    state["dry_run"] = dry_run
    state["debug"] = debug
    state["ray_mode"] = RayMode(ray_mode.lower())

    if debug:
        typer.secho("🔧 DEBUG MODE: ON", fg="cyan")


def apply_global_options(
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate execution"),
    debug: bool = typer.Option(False, "--debug", help="Global debug logging"),
    ray_mode: str = typer.Option(
        "cluster", "--ray_mode", help="Ray execution mode (local/cluster)"
    ),
) -> None:
    """Typer callback to capture and apply runtime flags across all commands.

    Args:
        dry_run: Flag to simulate data processing.
        debug: Flag to enable verbose platform logging.
        ray_mode: Mode string for the Ray initialization logic.

    Decision: Centralized Injection.
    By hosting the global options here, we ensure that every command suite
    automatically inherits the same configuration behavior and state logic,
    maintaining a consistent developer experience across the CLI.
    """
    configure_runtime(debug, dry_run, ray_mode)
