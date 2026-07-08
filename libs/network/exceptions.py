"""Custom exceptions for network diagnostics."""


class NetworkDiagnosticError(Exception):
    """Base exception for network diagnostic errors."""

    pass


class CheckDependencyError(NetworkDiagnosticError):
    """Raised when a diagnostic check dependency fails."""

    pass


class CheckExecutionError(NetworkDiagnosticError):
    """Raised when a diagnostic check fails to execute."""

    pass


class ReporterError(NetworkDiagnosticError):
    """Raised when a reporter fails to output results."""

    pass
