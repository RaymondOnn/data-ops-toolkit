import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from libs.file.mixins.cas import CASArchiveMixin, calculate_sha256


class MockCASClient(CASArchiveMixin):
    """Concrete class to test the CAS Mixin."""

    def __init__(self, fs, url):
        self.fs = fs
        self.url = url
        self.options = {}

    def get_reader_context(self, path, file_pattern=None):
        """Mock implementation of the required crawler helper."""
        return self.fs, []


class TestCASArchiveMixin:
    """Unit tests for the CASArchiveMixin."""

    @pytest.fixture
    def mock_fs(self):
        """Returns a MagicMock for fsspec.AbstractFileSystem."""
        return MagicMock(spec=AbstractFileSystem)

    @pytest.fixture
    def client(self, mock_fs):
        """Returns a MockCASClient instance."""
        return MockCASClient(fs=mock_fs, url="s3://cas-bucket")

    @patch("upath.UPath.open")
    def test_calculate_sha256(self, mock_open):
        """
        GIVEN a local file with known content
        THEN calculate_sha256 should return the correct hex digest
        WHEN invoked
        """
        mock_file = MagicMock()
        mock_file.read.side_effect = [b"hello world", b""]
        mock_open.return_value.__enter__.return_value = mock_file

        # Expected hash for "hello world"
        expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        assert calculate_sha256("/tmp/test.txt") == expected

    @patch("libs.file.mixins.cas.calculate_sha256")
    def test_archive_to_cas_new_file(self, mock_hash, client, mock_fs):
        """
        GIVEN a new file and a job ID
        THEN it should perform an atomic upload to a sharded path
        WHEN archive_to_cas is called
        """
        file_hash = "aabbccddeeff"
        mock_hash.return_value = file_hash
        mock_fs.exists.return_value = False  # Not in vault

        vault_path, manifest_path = client.archive_to_cas(
            "/tmp/data.csv", "job_123", metadata={"rows": 100}
        )

        # Verify sharding: /aa/bb/
        assert "vault/aa/bb/aabbccddeeff/data.csv" in vault_path
        assert "jobs/job_123" in manifest_path

        # Verify atomic upload steps
        mock_fs.put.assert_called_once()
        mock_fs.mv.assert_called_once()

    @patch("libs.file.mixins.cas.calculate_sha256")
    def test_archive_to_cas_deduplication(self, mock_hash, client, mock_fs):
        """
        GIVEN a file hash that already exists in the vault
        THEN it should skip the upload and only write the manifest
        WHEN archive_to_cas is called
        """
        mock_hash.return_value = "12345"
        mock_fs.exists.return_value = True  # Already in vault

        client.archive_to_cas("/tmp/duplicate.txt", "job_456")

        mock_fs.put.assert_not_called()
        mock_fs.mv.assert_not_called()

    def test_retrieve_file_success(self, client, mock_fs):
        """
        GIVEN a valid job and date
        THEN return the physical path stored in the logical manifest
        WHEN retrieve_file is called
        """
        manifest_data = {"physical_path": "s3://vault/file.csv"}
        mock_fs.exists.return_value = True
        mock_fs.open.return_value.__enter__.return_value = MagicMock(
            read=lambda: json.dumps(manifest_data).encode()
        )

        path = client.retrieve_file("job1", "2024/01/01")
        assert path == "s3://vault/file.csv"

    def test_garbage_collect_dry_run(self, client, mock_fs):
        """
        GIVEN a list of vault files, some of which are orphans
        THEN it should identify orphans but not call fs.rm
        WHEN garbage_collect is called with dry_run=True
        """
        # 1. Mock crawl_manifests result (Logical Tier)
        active_df = pl.DataFrame({"content_hash": ["active_hash"]})

        with patch.object(client, "crawl_manifests", return_value=active_df):
            # 2. Mock physical vault files
            mock_fs.find.return_value = [
                "s3://vault/ac/ti/active_hash/file.csv",
                "s3://vault/or/ph/orphan_hash/file.csv",
            ]

            deleted_count = client.garbage_collect(dry_run=True)

            assert deleted_count == 1
            mock_fs.rm.assert_not_called()

    def test_garbage_collect_execution(self, client, mock_fs):
        """
        GIVEN orphaned files in the vault
        THEN it should remove the specific orphan paths
        WHEN garbage_collect is called with dry_run=False
        """
        active_df = pl.DataFrame({"content_hash": ["hash1"]})

        with patch.object(client, "crawl_manifests", return_value=active_df):
            mock_fs.find.return_value = ["s3://vault/or/ph/orphan/data.csv"]

            client.garbage_collect(dry_run=False)

            mock_fs.rm.assert_called_with("s3://vault/or/ph/orphan/data.csv")

    def test_garbage_collect_safety_halt(self, client):
        """
        GIVEN no manifests are found in the system
        THEN return None and abort to prevent accidental mass deletion
        WHEN garbage_collect is called
        """
        with patch.object(client, "crawl_manifests", return_value=pl.DataFrame()):
            result = client.garbage_collect()
            assert result is None
