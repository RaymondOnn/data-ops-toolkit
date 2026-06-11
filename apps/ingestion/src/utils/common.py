"""General utility functions for the ingestion pipeline."""

import logging
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
    # Silence noisy loggers
    for name in ["botocore", "boto3", "urllib3", "ray", "apscheduler", "filelock"]:
        logging.getLogger(name).setLevel(logging.WARNING)

    # Extract parameters
    log_dir = kwargs.get("log_dir")
    is_debug = kwargs.get("is_debug", True)  # Force debug for now
    filename = kwargs.get("filename", "platform.jsonl")
    enqueue = kwargs.get("enqueue", False)

    if not log_dir:
        # Fallback to default
        from pathlib import Path

        log_dir = Path("./logs")

    setup_logging(
        log_dir=log_dir,
        is_debug=is_debug,
        filename=filename,
        enqueue=enqueue,
        highlight_keys=LOG_HIGHLIGHT_KEYS,
    )

    import sys

    from loguru import logger

    print(
        f"Number of handlers after setup: {len(logger._core.handlers)}", file=sys.stderr
    )
    for handler_id, handler in logger._core.handlers.items():
        print(f"Handler {handler_id}: {handler}", file=sys.stderr)
