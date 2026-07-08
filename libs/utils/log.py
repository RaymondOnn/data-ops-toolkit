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
    """Mask registered sensitive strings in log messages.

    Args:
        record: Log record to mask.

    Returns:
        None
    """
    msg = record["message"]
    for secret in _MASKED_STRINGS:
        if secret and secret in msg:
            msg = msg.replace(secret, "[MASKED]")
    record["message"] = msg


def console_formatter(highlight_keys: set[str]) -> Any:
    """Create console formatter with highlighted keys and SQL truncation.

    Args:
        highlight_keys: Keys to highlight in the console output.

    Returns:
        Any: Formatter for console output.
    """

    def formatter(record: "Record") -> str:
        """Format a log record for console output.

        Args:
            record: Log record to format.

        Returns:
            str: Formatted log message.
        """
        # Escape problematic characters
        name = str(record["name"]).replace("<", "\\<").replace(">", "\\>")
        func = record["function"].replace("<", "\\<").replace(">", "\\>")

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
    """Redirect standard logging to Loguru.

    Attributes:
        _logger: Loguru logger instance.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record.

        Args:
            record: Log record to emit.

        Returns:
            None
        """
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
    verbose_level: int = 0,
    filename: str = "platform.jsonl",
    enqueue: bool = False,
    highlight_keys: set[str] | None = None,
    silence_packages: list[str] | None = None,
) -> None:
    """Initialize logging with console and JSON file sinks.

    Args:
        log_dir: Directory to store log files.
        verbose_level: Verbosity level (0-5).
        filename: Name of the log file.
        enqueue: Whether to use enqueue for log rotation.
        highlight_keys: Keys to highlight in the console output.
        silence_packages: Packages to silence in the console output.

    Returns:
        None
    """

    # Verbosity Mapping Logic
    # 0: App=WARNING, Silenced=WARNING
    # 1: App=INFO,    Silenced=WARNING
    # 2: App=DEBUG,   Silenced=WARNING
    # 3: App=TRACE,   Silenced=WARNING
    # 4: App=TRACE,   Silenced=INFO
    # 5: App=TRACE,   Silenced=DEBUG

    app_level_map = {0: "WARNING", 1: "INFO", 2: "DEBUG"}
    app_level = app_level_map.get(verbose_level, "TRACE")

    silenced_level = "WARNING"
    if verbose_level == 4:
        silenced_level = "INFO"
    elif verbose_level >= 5:
        silenced_level = "DEBUG"

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / filename
    log_file.touch(exist_ok=True)

    logger.remove()
    logger.configure(patcher=_mask_sensitive)

    # Console sink
    logger.add(
        sys.stderr,
        level=app_level,
        format=console_formatter(highlight_keys or set()),
        colorize=True,
        serialize=False,
        backtrace=True,
        diagnose=verbose_level >= 2,
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
    packages_to_silence = silence_packages or [
        "filelock",
        "apscheduler",
        "ray",
        "botocore",
        "boto3",
        "urllib3",
    ]

    for name in packages_to_silence:
        logging.getLogger(name).setLevel(silenced_level)


def mask_secrets(secrets: str | list[str]) -> None:
    """Register sensitive strings to be masked in logs."""
    items = [secrets] if isinstance(secrets, str) else secrets
    _MASKED_STRINGS.update(s for s in items if s)


def is_masked(secret: str) -> bool:
    """Check if a string is registered for masking."""
    return secret in _MASKED_STRINGS
