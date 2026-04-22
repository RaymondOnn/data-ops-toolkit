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
    """Raised when authentication to an external service fails due to invalid credentials."""

    pass


class HostUnreachable(TransientError):
    """Raised when the target host is unreachable, e.g., due to network issues or service downtime."""

    pass

class AppWarning(Warning):
    """Base class for non-critical issues that should be logged but do not require job failure."""

    pass