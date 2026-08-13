from typing import Any

import typer

from src.core.contexts.execution import RayMode

"""
Global CLI State Management.

Decision: Configuration Decoupling.
By isolating the CLI's mutable state and setup logic, we allow other
modules to inspect runtime flags (like dry_run) without importing the
heavy Typer action implementations.
"""

state: dict[str, Any] = {
    "dry_run": False,
    "verbose_level": 0,
    "ray_mode": RayMode.CLUSTER,
}

# Decision: Centralized App Instance.
# We instantiate the Typer app here so that implementation modules can use
# the @app.command decorator without inducing circular imports with the
# package entry point.
app = typer.Typer(help="50M Row Ingest Pipeline")


def configure_runtime(verbose: int, dry_run: bool, ray_mode: str = "cluster") -> None:
    """Applies global runtime configurations to the internal state.

    Args:
        verbose: Countable verbosity level (0-5).
        dry_run: Prevents physical side-effects (DB writes, file moves).
        ray_mode: Determines the compute backend (local vs cluster).

    Decision: Single Source of Truth.
    We store the CLI flags in a mutable dictionary within this module to
    allow the Orchestrator and Ray workers to inspect global settings
    without re-parsing CLI arguments or inducing circular imports between
    actions and the entry point.
    """
    state["dry_run"] = dry_run
    state["verbose_level"] = verbose
    state["ray_mode"] = RayMode(ray_mode.lower())

    if verbose > 0:
        typer.secho(f"📢 VERBOSITY LEVEL: {verbose}", fg="cyan")


def apply_global_options(
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate execution"),
    verbose: int = typer.Option(
        0, "--verbose", "-v", count=True, help="Set verbosity level"
    ),
    ray_mode: str = typer.Option(
        "cluster", "--ray_mode", help="Ray execution mode (local/cluster)"
    ),
) -> None:
    """Typer callback to capture and apply runtime flags across all commands.

    Args:
        dry_run: Flag to simulate data processing.
        verbose: Countable verbosity level.
        ray_mode: Mode string for the Ray initialization logic.

    Decision: Centralized Injection.
    By hosting the global options here, we ensure that every command suite
    automatically inherits the same configuration behavior and state logic,
    maintaining a consistent developer experience across the CLI.
    """
    configure_runtime(verbose, dry_run, ray_mode)
