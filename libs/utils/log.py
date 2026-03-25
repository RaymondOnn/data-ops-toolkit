import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import structlog


def setup_logging(log_dir: Path, is_prod: bool = False):
    # 1. The Detailed File Handler (DEBUG)
    file_handler = TimedRotatingFileHandler(
        filename=f"{log_dir}/platform.log", when="midnight", backupCount=7
    )
    file_handler.setLevel(logging.DEBUG)

    # 2. The Clean Console Handler (INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # 3. Configure Structlog to use the Standard Lib bridge
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.dict_tracebacks,
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            # In Dev, ConsoleRenderer makes the Console pretty,
            # while JSON goes to the file if configured.
            (
                structlog.dev.ConsoleRenderer()
                if not is_prod
                else structlog.processors.JSONRenderer()
            ),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )

    root = logging.getLogger()
    root.setLevel(
        logging.DEBUG
    )  # Root must be DEBUG to allow file_handler to see everything
    root.addHandler(file_handler)
    root.addHandler(console_handler)
