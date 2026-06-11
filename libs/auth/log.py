"""Log masking utilities for sensitive data."""

import logging

# Global registry of values to mask
_masked_values: set[str] = set()


def mask_in_logs(value: str) -> None:
    """Register a value to be masked in all log output."""
    _masked_values.add(value)


def is_masked(value: str) -> bool:
    """Check if a value is registered for masking."""
    return value in _masked_values


class SecretMasker(logging.Filter):
    """Log filter that masks registered secret values."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for secret in _masked_values:
            if secret in msg:
                record.msg = msg.replace(secret, "[MASKED]")
        return True
