"""General utility functions for the ingestion pipeline."""

from pathlib import Path

from libs.utils.log import setup_logging
from nanoid import generate

from .constants import LOG_HIGHLIGHT_KEYS


def find_path(search_dir: Path, pattern: str) -> Path | None:
    """Find first directory matching glob pattern."""
    for path in search_dir.rglob(pattern):
        if path.is_dir():
            return path
    return None


def short_hash(length: int = 8) -> str:
    """Generate random hex string."""
    return generate(alphabet="0123456789abcdef", size=length)


def setup_logger(*args, **kwargs):
    """Setup application logging with silenced third-party loggers."""
    # Extract parameters
    log_dir = kwargs.get("log_dir")
    verbose_level = kwargs.get("verbose_level", 0)
    silence_packages = kwargs.get("silence_packages")
    filename = kwargs.get("filename", "platform.jsonl")
    enqueue = kwargs.get("enqueue", False)

    if not log_dir:
        # Fallback to default
        from pathlib import Path

        log_dir = Path("./logs")

    setup_logging(
        log_dir=log_dir,
        verbose_level=verbose_level,
        filename=filename,
        silence_packages=silence_packages,
        enqueue=enqueue,
        highlight_keys=LOG_HIGHLIGHT_KEYS,
    )
