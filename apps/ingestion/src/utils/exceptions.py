class Error(Exception):
    """Base class for exceptions in this module."""

    pass


class TaskDeferred(Error):
    """Exception raised when a non-critical task fails."""

    pass


class TaskBlocked(Error):
    """Exception raised when a critical task fails."""

    pass


class TaskFailed(Error):
    """Exception raised when data fails validation and must be isolated."""

    pass
