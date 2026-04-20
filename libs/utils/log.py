import logging
import sys
from pathlib import Path

from loguru import logger

# Internal state to track strings that must be redacted
_MASK_STRINGS: set[str] = set()


def _mask_sensitive_data(record):
    msg = record["message"]
    for secret in _MASK_STRINGS:
        if secret and secret in msg:
            msg = msg.replace(secret, "[MASKED_SECRET]")
    record["message"] = msg


def _console_formatter(record):
    """
    Dynamic formatter that appends extra context to the end of the line
    only if extra data exists.
    """
    fmt = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

    # Check for extra context (excluding internal loguru keys if any)
    extras = {k: v for k, v in record["extra"].items() if k not in ["run_id"]}

    # We handle run_id separately to highlight it
    run_id = record["extra"].get("run_id")
    prefix = f" <magenta>[{run_id}]</magenta>" if run_id else ""

    # Format extras for console display without using braces
    # (which would trigger Loguru's internal format_map KeyError)
    if extras:
        display_parts = []
        for k, v in extras.items():
            val = str(v)
            # Truncate long SQL queries for cleaner console output
            if k == "query" and len(val) > 100:
                val = val[:97] + "..."

            # Escape curly braces to prevent Loguru from interpreting them as placeholders
            val = val.replace("{", "{{").replace("}", "}}")
            display_parts.append(f"{k}={val}")

        extras_str = ", ".join(display_parts)
        return f"{fmt}{prefix} <light-magenta>({extras_str})</light-magenta>\n"
    return f"{fmt}{prefix}\n"


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
        while frame.f_code.co_filename == logging.__file__:
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
):
    # Ensure the log directory exists before initializing handlers
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / filename

    # 1. Clear default handlers
    logger.remove()

    # 2. Configure global patcher for redacting
    logger.configure(patcher=_mask_sensitive_data)

    # 2. Add Console Handler
    logger.add(
        # sys.stdout,
        sys.stderr,
        level="DEBUG" if is_debug else "INFO",
        format=_console_formatter,
        colorize=True,
        serialize=is_prod,
        backtrace=True,
        diagnose=is_debug,
    )

    # 3. Add JSON File Handler with Rotation
    logger.add(
        str(log_file),
        level="DEBUG",
        serialize=True,  # Always JSON in files
        rotation="00:00",
        retention="7 days",
        compression="zip",
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
