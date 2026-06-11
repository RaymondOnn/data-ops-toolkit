from functools import wraps

from loguru import logger

LOG = logger


def log_dry_run(func):
    """Decorator to log dry run mode."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.exec_ctx.is_dry_run:
            LOG.info(f"[DRY RUN] Would execute: {func.__name__}")
            return None
        return func(self, *args, **kwargs)

    return wrapper
