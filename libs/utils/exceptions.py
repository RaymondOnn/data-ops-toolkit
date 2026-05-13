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
    """Terminal: """
    pass



import functools
from loguru import logger

def track_execution(func):
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
        except Exception as e:
            # Capture the context but don't silence the error
            logger.bind(section=section).error(f"Execution failed in [{section}]")
            # We re-raise to ensure the Orchestrator/Ray knows the task failed
            raise
    return wrapper