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
