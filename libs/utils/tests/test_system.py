from pathlib import Path
from unittest.mock import MagicMock, patch

from libs.utils.system import get_disk_usage, get_system_vitals


class TestSystemUtils:
    """Unit tests for system monitoring utilities."""

    @patch("psutil.disk_usage")
    def test_get_disk_usage_success(self, mock_usage):
        """
        GIVEN a valid directory path
        THEN return a DiskUsage object with correctly mapped bytes and percent
        WHEN get_disk_usage is called
        """
        # Mock the psutil.sdiskusage namedtuple
        mock_usage.return_value = MagicMock(
            total=1000, used=400, free=600, percent=40.0
        )

        usage = get_disk_usage("/")

        assert usage.total == 1000
        assert usage.free_gb == 600 / (1024**3)
        assert usage.percent == 40.0

    def test_get_disk_usage_path_fallback(self):
        """GIVEN a non-existent child path
        THEN the function should recurse up to find an existing parent
        WHEN get_disk_usage is called
        """
        # We test that it doesn't crash when pointing to a non-existent path
        # because it resolves to the existing parent (like /tmp or /)
        path = Path("/tmp/non_existent_data_ops_dir_123")

        with patch("psutil.disk_usage") as mock_usage:
            mock_usage.return_value = MagicMock(
                total=100, used=10, free=90, percent=10.0
            )
            usage = get_disk_usage(path)
            assert usage.total == 100
            # Verify it called disk_usage on an existing parent path
            args, _ = mock_usage.call_args
            assert Path(args[0]).exists()

    @patch("psutil.cpu_percent")
    @patch("psutil.virtual_memory")
    def test_get_system_vitals(self, mock_mem, mock_cpu):
        """GIVEN mocked system resource stats
        THEN return a SystemVitals object aggregating the snapshots
        WHEN get_system_vitals is called
        """
        mock_cpu.return_value = 25.5
        mock_mem.return_value = MagicMock(total=16000, available=8000, percent=50.0)

        vitals = get_system_vitals()

        assert vitals.cpu_pct == 25.5
        assert vitals.mem_total == 16000
        assert vitals.mem_pct == 50.0

    def test_disk_usage_gb_conversion(self):
        """
        GIVEN a DiskUsage instance with 1GB of free space
        THEN the free_gb property should return exactly 1.0
        WHEN free_gb is accessed
        """
        from libs.utils.system import DiskUsage

        usage = DiskUsage(total=0, used=0, free=1024**3, percent=0)
        assert usage.free_gb == 1.0
