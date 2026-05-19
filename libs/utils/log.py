import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from loguru import logger

# Internal state to track strings that must be redacted
_MASK_STRINGS: set[str] = set()


def _mask_sensitive_data(record):
    msg = record["message"]
    for secret in _MASK_STRINGS:
        if secret and secret in msg:
            msg = msg.replace(secret, "[MASKED_SECRET]")
    record["message"] = msg


def create_console_formatter(highlight_keys: set[str]) -> Callable[[Any], str]:
    """
    Creates a Loguru formatter that highlights specific keys in the prefix
    and appends others as context extras.
    """

    def formatter(record: Any) -> str:
        # Escape name and function to prevent loguru parsing errors (e.g. <module>)
        name = record["name"].replace("<", "\\<").replace(">", "\\>")
        function = record["function"].replace("<", "\\<").replace(">", "\\>")

        fmt = f"<green>{{time:YYYY-MM-DD HH:mm:ss}}</green> | <level>{{level: <8}}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{{line}}</cyan> - <level>{{message}}</level>"

        prefixes = []
        extras_list = []

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
                # Truncate long SQL queries for cleaner console output
                if k == "query" and len(val) > 100:
                    val = val[:97] + "..."
                extras_list.append(f"{k}={val}")

        prefix_str = "".join(prefixes)
        extras_str = (
            f" <light-magenta>({', '.join(extras_list)})</light-magenta>"
            if extras_list
            else ""
        )

        line = f"{fmt}{prefix_str}{extras_str}\n"

        if record["exception"] is not None:
            line += "{exception}\n"
        return line

    return formatter


class InterceptHandler(logging.Handler):
    """
    Standard python logging handler interceptor to redirect
    library logs (apscheduler, ray, etc) to loguru.
    """

    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # Extract 'extra' from standard logging record
        # Standard logging merges 'extra' into the record's __dict__
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
    is_prod: bool = False,
    is_debug: bool = False,
    filename: str = "platform.jsonl",
    enqueue: bool = False,
    highlight_keys: set[str] | None = None,
):
    # Ensure the log directory exists before initializing handlers
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / filename

    # Defensive touch to ensure the file exists before Loguru's background thread
    # attempts to perform rotation checks.
    log_file.touch(exist_ok=True)

    # 1. Clear default handlers
    logger.remove()

    # 2. Configure global patcher for redacting
    logger.configure(patcher=_mask_sensitive_data)

    # 2. Add Console Handler
    highlight_keys = highlight_keys or set()
    logger.add(
        # sys.stdout,
        sys.stderr,
        level="DEBUG" if is_debug else "INFO",
        format=create_console_formatter(highlight_keys),
        colorize=True,
        # We disable serialization for the console. This ensures CLI users
        # always see the formatted output instead of JSON blobs.
        serialize=False,
        backtrace=True,  # stops the repetitive stack nesting
        diagnose=is_debug,  # hides the local variable values which clutter the group
        enqueue=enqueue,
    )

    # 3. Add JSON File Handler with Rotation
    logger.add(
        str(log_file),
        level="DEBUG",
        serialize=True,  # Always JSON in files
        rotation="00:00",
        retention="7 days",
        compression="zip",
        enqueue=enqueue,
    )

    # 4. Intercept standard logging calls
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Silent noisy libraries by setting their levels at the source
    # Standard logging levels are respected before reaching the Interceptor
    logging.getLogger("filelock").setLevel(logging.WARNING)
    # APScheduler is very noisy at INFO level; lock to WARNING even in debug mode
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    # logging.getLogger("ray").setLevel(logging.WARNING)


def register_log_masking(secrets: str | list[str]) -> None:
    """
    Public API to add new sensitive values to the global redact list.
    """
    if isinstance(secrets, str):
        secrets = [secrets]

    for s in secrets:
        if s:
            _MASK_STRINGS.add(str(s))


def is_masked(secret: str) -> bool:
    """
    Returns True if the given string is registered in the global redact list.
    """
    return secret in _MASK_STRINGS
