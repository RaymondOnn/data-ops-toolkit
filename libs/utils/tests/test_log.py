from unittest.mock import MagicMock

from libs.utils.log import (
    _MASK_STRINGS,
    create_console_formatter,
    is_masked,
    register_log_masking,
    setup_logging,
)
from loguru import logger


class TestLoggingUtils:
    """Unit tests for the platform logging and masking utilities."""

    def setup_method(self):
        """Clear the global mask list before each test."""
        _MASK_STRINGS.clear()

    def test_register_and_check_masking(self):
        """
        GIVEN a sensitive string 'secret_password'
        THEN it should be found in the mask registry
        WHEN register_log_masking is called
        """
        secret = "secret_password"
        register_log_masking(secret)

        assert is_masked(secret) is True
        assert is_masked("public_info") is False

    def test_mask_list_input(self):
        """
        GIVEN a list of sensitive strings
        THEN all items should be registered in the global set
        WHEN register_log_masking is called with a list
        """
        secrets = ["pass1", "token2"]
        register_log_masking(secrets)

        assert is_masked("pass1") is True
        assert is_masked("token2") is True

    def test_redaction_logic(self, capsys):
        """
        GIVEN a registered secret and a log sink with patching enabled
        THEN the output log should replace the secret with [MASKED_SECRET]
        WHEN a message containing the secret is logged
        """
        register_log_masking("super-secret-key")

        # Temporary sink to capture output
        def sink_handler(message):
            print(message, end="")

        from libs.utils.log import _mask_sensitive_data

        # Loguru's .add() does not accept a 'patcher' argument.
        # We use .patch() to create a logger instance that applies the redaction.
        redacting_logger = logger.patch(_mask_sensitive_data)
        handler_id = redacting_logger.add(sink_handler, format="{message}")

        try:
            # Log through the patched instance to trigger the masking logic
            redacting_logger.info("My key is super-secret-key")
            captured = capsys.readouterr()
            assert "My key is [MASKED_SECRET]" in captured.out
            assert "super-secret-key" not in captured.out
        finally:
            logger.remove(handler_id)

    def test_console_formatter_highlights(self):
        """
        GIVEN a set of highlight keys
        THEN the formatter should wrap those keys in magenta tags
        WHEN a log record is processed
        """
        formatter = create_console_formatter(highlight_keys={"run_id"})

        record = {
            "name": "test_mod",
            "function": "test_func",
            "line": 10,
            "time": MagicMock(strftime=lambda x: "2024-01-01"),
            "level": MagicMock(name="INFO"),
            "message": "hello",
            "extra": {"run_id": "uuid-123", "other": "val"},
            "exception": None,
        }

        # Mocking complex Loguru objects is heavy, so we test the result string
        # contains the expected markup components
        result = formatter(record)
        assert "<magenta>[uuid-123]</magenta>" in result
        assert "other=val" in result

    def test_setup_logging_creates_files(self, tmp_path):
        """
        GIVEN a temporary log directory
        THEN the log file should be touched and initialized
        WHEN setup_logging is called
        """
        log_dir = tmp_path / "logs"
        setup_logging(log_dir=log_dir, filename="test.jsonl")

        assert (log_dir / "test.jsonl").exists()
