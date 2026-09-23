class SQLCompilationError(Exception):
    """Raised when compilation fails due to mismatched syntax, missing keys, or type errors."""


class ConfigurationValidationError(Exception): ...
