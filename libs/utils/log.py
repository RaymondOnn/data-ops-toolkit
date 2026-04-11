import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from collections.abc import Callable


def setup_logging(
    log_dir: Path,
    is_prod: bool = False,
    is_debug: bool = False,
    filename: str = "platform.jsonl",
):
    # Ensure the log directory exists before initializing handlers
    log_dir.mkdir(parents=True, exist_ok=True)

    # 1. The JSON File Handler
    # We use a standard Formatter that just outputs the message
    # (which structlog will provide as JSON)
    file_handler = TimedRotatingFileHandler(
        filename=log_dir / filename, when="midnight", backupCount=7
    )
    file_handler.setLevel(logging.DEBUG)

    # 2. The Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if is_debug else logging.INFO)

    # 3. Define the Shared Processors
    # These run for BOTH the console and the file
    shared_processors: list[Callable] = [
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.add_log_level,
        # Adds the name of the logger (e.g. apps.ingestion.src.services.file)
        structlog.stdlib.add_logger_name,
        # Adds the filename, function name, and line number
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.MODULE,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
        structlog.contextvars.merge_contextvars,
        structlog.processors.dict_tracebacks,
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            # This is the bridge: it sends the log to the standard logging module
            structlog.stdlib.filter_by_level,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # 4. Use ProcessorFormatter to split the rendering styles
    # FILE gets JSON
    file_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )
    file_handler.setFormatter(file_formatter)

    # CONSOLE gets Pretty (if not prod) or JSON
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processor=(
            structlog.processors.JSONRenderer()
            if is_prod
            else structlog.dev.ConsoleRenderer(colors=True, event_key="event")
        ),
        foreign_pre_chain=shared_processors,
    )
    console_handler.setFormatter(console_formatter)

    root = logging.getLogger()

    # Clear existing handlers to prevent duplicate logs in Ray workers
    if root.hasHandlers():
        root.handlers.clear()

    root.handlers = [file_handler, console_handler]
    root.setLevel(logging.DEBUG)

    # 5. Suppress noisy third-party libraries
    logging.getLogger("filelock").setLevel(logging.WARNING)
    # Suppress APScheduler info logs unless explicitly in debug mode
    logging.getLogger("apscheduler").setLevel(
        logging.DEBUG if is_debug else logging.WARNING
    )
