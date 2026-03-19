import logging

from libs.auth.models import Secret


class SecretMasker(logging.Filter):
    """
    A global guardrail that scans every log message and replaces
    known sensitive values with [MASKED].
    """

    def __init__(self, secret_instances: list[Secret]) -> None:
        super().__init__()
        self.secret_instances = secret_instances

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for secret in self.secret_instances:
            val = getattr(secret, "_value", None)
            if val and val in message:
                record.msg = message.replace(val, "[MASKED_SECRET]")
        return True
