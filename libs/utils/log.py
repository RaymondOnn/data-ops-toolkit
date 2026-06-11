"""Logging setup with masking and dual sinks."""

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import FrameType

    from loguru import Record

from loguru import logger

# Global registry for sensitive strings to mask
_MASKED_STRINGS: set[str] = set()


def _mask_sensitive(record: "Record") -> None:
    """Mask registered sensitive strings in log messages."""
    msg = str(record["message"])
    for secret in _MASKED_STRINGS:
        if secret and secret in msg:
            msg = msg.replace(secret, "[MASKED]")
    record["message"] = msg


def console_formatter(highlight_keys: set[str]) -> Any:
    """Create console formatter with highlighted keys and SQL truncation."""

    def formatter(record: "Record") -> str:
        # Escape problematic characters
        name = str(record["name"]).replace("<", "\\<").replace(">", "\\>")
        func = str(record["function"]).replace("<", "\\<").replace(">", "\\>")

        # Build prefix with highlighted keys
        prefixes = []
        extras = []
        for k, v in record["extra"].items():
            val = (
                str(v)
                .replace("<", "\\<")
                .replace(">", "\\>")
                .replace("{", "{{")
                .replace("}", "}}")
            )
            if k in highlight_keys:
                prefixes.append(f" <magenta>[{val}]</magenta>")
            else:
                if k == "query" and len(val) > 100:
                    val = val[:97] + "..."
                extras.append(f"{k}={val}")

        prefix = "".join(prefixes)
        extra = (
            f" <light-magenta>({', '.join(extras)})</light-magenta>" if extras else ""
        )

        fmt = (
            f"<green>{{time:YYYY-MM-DD HH:mm:ss}}</green> | "
            f"<level>{{level: <8}}</level> |"
            f"{prefix} "
            f"<cyan>{name}</cyan>:<cyan>{func}</cyan>:<cyan>{{line}}</cyan> - "
            f"<level>{{message}}</level>"
        )

        line = f"{fmt}{extra}\n"
        if record["exception"]:
            line += "{exception}\n"
        return line

    return formatter


class LogInterceptor(logging.Handler):
    """Redirect standard logging to Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller
        frame: FrameType | None = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # Extract extra fields
        std_extra = {
            k: v
            for k, v in record.__dict__.items()
            if k not in logging.makeLogRecord({}).__dict__
        }

        logger.opt(depth=depth, exception=record.exc_info).bind(**std_extra).log(
            level, record.getMessage()
        )


def setup_logging(
    log_dir: Path,
    is_debug: bool = False,
    filename: str = "platform.jsonl",
    enqueue: bool = False,
    highlight_keys: set[str] | None = None,
) -> None:
    """Initialize logging with console and JSON file sinks."""
    print(
        f"!!! setup_logging called with log_dir={log_dir}, is_debug={is_debug} !!!",
        file=sys.stderr,
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / filename
    log_file.touch(exist_ok=True)

    logger.remove()
    logger.configure(patcher=_mask_sensitive)

    # Console sink
    logger.add(
        sys.stderr,
        level="DEBUG" if is_debug else "INFO",
        format=console_formatter(highlight_keys or set()),
        colorize=True,
        serialize=False,
        backtrace=True,
        diagnose=is_debug,
        enqueue=enqueue,
    )

    # JSON file sink
    logger.add(
        str(log_file),
        level="DEBUG",
        serialize=True,
        rotation="00:00",
        retention="7 days",
        compression="zip",
        enqueue=enqueue,
    )

    # Redirect standard logging
    logging.basicConfig(handlers=[LogInterceptor()], level=0, force=True)

    # Silence noisy loggers
    for name in ["filelock", "apscheduler", "ray"]:
        logging.getLogger(name).setLevel(logging.WARNING)


def mask_secrets(secrets: str | list[str]) -> None:
    """Register sensitive strings to be masked in logs."""
    items = [secrets] if isinstance(secrets, str) else secrets
    _MASKED_STRINGS.update(str(s) for s in items if s)


def is_masked(secret: str) -> bool:
    """Check if a string is registered for masking."""
    return secret in _MASKED_STRINGS
