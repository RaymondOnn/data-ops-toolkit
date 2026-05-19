import functools
import logging
import sys
import threading

LOG = logging.getLogger(__name__)


class Error(Exception):
    """Base class for exceptions in this module."""

    pass


class TerminalError(Error):
    """Base class for terminal exceptions that should move the job to a final state."""

    pass


class TransientError(Error):
    """Base class for transient exceptions that can be retried."""

    pass


class AuthFailure(TerminalError):
    """Terminal: Invalid credentials. Retrying will not help."""

    pass


class HostUnreachable(TransientError):
    """Transient: Network or DNS issues. Service might come back."""

    pass


class ResourceNotFound(TerminalError):
    """Terminal:"""

    pass


def install_exception_hooks():
    """
    Installs global handlers for unhandled exceptions in the main thread
    and background threads.
    """

    def global_handler(exctype, value, tb):
        if issubclass(exctype, KeyboardInterrupt):
            sys.__excepthook__(exctype, value, tb)
            return
        LOG.critical("Unhandled top-level exception", exc_info=(exctype, value, tb))

    # Process-level hook
    sys.excepthook = global_handler
    # Thread-level hook (requires Python 3.8+)
    threading.excepthook = lambda args: global_handler(
        args.exc_type, args.exc_value, args.exc_traceback
    )


def catch_exception(func):
    """
    Generic decorator to wrap logic blocks.
    Identifies the failing 'section' dynamically via the function name.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Dynamically determine the section name (e.g., 'ArchiveStage.execute')
        section = func.__qualname__

        try:
            return func(*args, **kwargs)
        except Exception:
            # Capture the context but don't silence the error
            LOG.error("Execution failed in [%s]", section)
            # We re-raise to ensure the Orchestrator/Ray knows the task failed
            raise

    return wrapper
