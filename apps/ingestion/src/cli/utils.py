from typing import Any

import msgspec
import typer
from loguru import logger


def _write_signal_file(
    exec_ctx: Any, filename: str, payload: dict | str, overwrite: bool = False
) -> None:
    """Internal helper to safely write command signals to the workspace.

    Args:
        exec_ctx: The execution context.
        filename: The target .cmd filename.
        payload: Dictionary (JSON) or string content.
        overwrite: Whether to overwrite existing signals.

    Raises:
        typer.Exit: If a command is already pending or writing fails.
    """
    path = exec_ctx.signal_path / filename
    exec_ctx.signal_path.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite:
        typer.secho(f"⚠️ A command '{filename}' is already pending.", fg="yellow")
        raise typer.Exit(code=0)

    try:
        if isinstance(payload, dict):
            path.write_bytes(msgspec.json.encode(payload))
        else:
            path.write_text(payload)
        logger.success(f"Command signal created: {filename}")
    except Exception as e:
        logger.error(f"Failed to write signal {filename}: {e}")
        raise typer.Exit(code=1) from e
