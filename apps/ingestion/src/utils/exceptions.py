class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class JobDeferred(Error):
    """Exception raised when a non-critical task fails."""
    pass

class JobBlocked(Error):
    """Exception raised when a critical task fails."""
    pass

class JobFailed(Error):
    """Exception raised when data fails validation and must be isolated."""
    pass
