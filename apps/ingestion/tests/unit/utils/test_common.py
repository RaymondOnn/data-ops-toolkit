from pathlib import Path
from unittest.mock import MagicMock, patch

from apps.ingestion.src.utils.common import (
    find_path,
    make_short_hash,
    recursive_merge,
    setup_logger,
)


class TestCommonUtils:
    """Unit tests for the application utility functions."""

    def test_find_path_success(self, tmp_path):
        """
        GIVEN a directory structure with a target subdirectory
        THEN find_path should return the Path of that subdirectory
        WHEN searched with a matching glob pattern
        """
        target = tmp_path / "subdir" / "target_dir"
        target.mkdir(parents=True)

        result = find_path(tmp_path, "target_dir")
        assert result == target

    def test_find_path_not_found(self, tmp_path):
        """
        GIVEN a search directory
        THEN find_path should return None
        WHEN the pattern does not match any existing directory
        """
        result = find_path(tmp_path, "missing")
        assert result is None

    def test_make_short_hash_format(self):
        """
        GIVEN a request for a short hash
        THEN return a string of requested length containing only hex chars
        WHEN make_short_hash is called
        """
        h = make_short_hash(length=12)
        assert len(h) == 12
        assert all(c in "0123456789abcdef" for c in h)

    def test_recursive_merge_logic(self):
        """
        GIVEN a base dictionary and an update dictionary with nested keys
        THEN the base dictionary should be updated in-place with deep merging
        WHEN recursive_merge is invoked
        """
        base = {"a": 1, "meta": {"status": "init", "count": 0}, "list": [1]}
        upd = {"b": 2, "meta": {"status": "updated"}, "list": [1, 2]}

        recursive_merge(base, upd)

        assert base["a"] == 1
        assert base["b"] == 2
        assert base["meta"]["status"] == "updated"
        assert base["meta"]["count"] == 0  # preserved
        assert base["list"] == [1, 2]  # list is replaced, not merged

    @patch("logging.getLogger")
    @patch("apps.ingestion.src.utils.common.setup_logging")
    def test_setup_logger_delegation(self, mock_setup, mock_get_logger):
        """
        GIVEN a log directory and debug flag
        THEN it should silence specific libraries and delegate to libs.utils.log
        WHEN setup_logger is called
        """
        mock_log_obj = MagicMock()
        mock_get_logger.return_value = mock_log_obj
        log_path = Path("/tmp/logs")

        setup_logger(log_dir=log_path, is_debug=True)

        # Verify library silencing (logging.WARNING = 30)
        assert mock_get_logger.call_count >= 5
        mock_log_obj.setLevel.assert_called_with(30)

        # Verify delegation to libs.utils.log.setup_logging
        mock_setup.assert_called_once()
        _, kwargs = mock_setup.call_args
        assert kwargs["log_dir"] == log_path
        assert "highlight_keys" in kwargs
