class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class DeferredError(Error):
    """Exception raised when a non-critical task fails."""
    pass

class BlockedError(Error):
    """Exception raised when a critical task fails."""
    pass

class QuarantineError(Error):
    """Exception raised when data fails validation and must be isolated."""
    pass
