from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from apps.ingestion.src.services.file import (
    BaseStorageService,
    StorageSink,
    StorageSource,
)
from libs.file import FileSystemSkills


class TestBaseStorageService:
    """Unit tests for the base storage service logic."""

    @pytest.fixture
    def service(self):
        """Returns a BaseStorageService for testing."""
        return BaseStorageService(
            name="test-fs",
            url="s3://bucket",
            capabilities={FileSystemSkills.FILE},
            storage_options={},
        )

    @patch("libs.file.base.create_fs_client")
    def test_client_lazy_initialization(self, mock_create, service):
        """
        GIVEN a BaseStorageService instance
        THEN the client should be created once and cached
        WHEN the client property is accessed
        """
        mock_client = MagicMock()
        mock_create.return_value = mock_client

        c1 = service.client
        c2 = service.client

        assert c1 is c2
        mock_create.assert_called_once()

    def test_reset_client(self, service):
        """
        GIVEN an initialized client
        THEN the cache should be cleared
        WHEN reset_client is called
        """
        with patch("libs.file.base.create_fs_client", return_value=MagicMock()):
            _ = service.client
            assert "client" in service.__dict__

            service.reset_client()
            assert "client" not in service.__dict__


class TestStorageSource:
    """Unit tests for source-specific filesystem logic."""

    @pytest.fixture
    def source(self):
        """Returns a StorageSource for testing."""
        return StorageSource(
            name="source-fs",
            url="s3://source",
            capabilities={FileSystemSkills.FILE},
            storage_options={},
        )

    def test_parallelize_small_dataset_coalescing(self, source):
        """
        GIVEN a list of small files totaling less than 50MB
        THEN return a single work unit containing all files
        WHEN parallelize is called
        """
        mock_client = MagicMock()
        mock_handler = MagicMock()

        # Total size: 10MB
        mock_client.fs.size.return_value = 2 * 1024 * 1024
        mock_handler.discover.return_value = [
            "f1.csv",
            "f2.csv",
            "f3.csv",
            "f4.csv",
            "f5.csv",
        ]

        with (
            patch.object(source, "client", mock_client),
            patch.object(source, "_get_handler", return_value=mock_handler),
        ):
            units = source.parallelize("data/", num_workers=5)

            assert len(units) == 1
            assert len(units[0]["files"]) == 5

    def test_parallelize_file_level_parallelism(self, source):
        """
        GIVEN a dataset with many files and a splittable format
        THEN return work units distributed across workers
        WHEN parallelize is called
        """
        mock_client = MagicMock()
        mock_handler = MagicMock()

        # Total size: 500MB (Exceeds coalescing limit)
        mock_client.fs.size.return_value = 100 * 1024 * 1024
        mock_handler.discover.return_value = [f"f{i}.parquet" for i in range(10)]
        mock_handler.is_splittable = True

        with (
            patch.object(source, "client", mock_client),
            patch.object(source, "_get_handler", return_value=mock_handler),
        ):
            # 10 files, 2 workers -> 5 files per worker
            units = source.parallelize("data/", num_workers=2)

            assert len(units) == 2
            assert len(units[0]["files"]) == 5

    def test_pull_concatenation(self, source):
        """
        GIVEN a work unit with multiple files
        THEN fetch each and return a concatenated LazyFrame
        WHEN pull is called
        """
        mock_handler = MagicMock()
        # Each to_df returns a LazyFrame with 1 row
        mock_handler.to_df.side_effect = [
            pl.LazyFrame({"id": [1]}),
            pl.LazyFrame({"id": [2]}),
        ]

        with (
            patch.object(source, "_get_handler", return_value=mock_handler),
            patch.object(source, "client", MagicMock()),
        ):
            result = source.pull({"files": ["a.csv", "b.csv"]})

            assert isinstance(result, pl.LazyFrame)
            df = result.collect()
            assert df.height == 2


class TestStorageSink:
    """Unit tests for sink-specific filesystem logic."""

    @pytest.fixture
    def sink(self):
        """Returns a StorageSink for testing."""
        return StorageSink(
            name="sink-fs",
            url="s3://sink",
            capabilities={FileSystemSkills.FILE},
            storage_options={},
        )

    def test_promote_idempotency(self, sink):
        """
        GIVEN a target partition that already exists
        THEN remove the existing partition before moving the staged data
        WHEN promote is called
        """
        mock_client = MagicMock()
        mock_client.exists.return_value = True

        with patch.object(sink, "client", mock_client):
            sink.promote(
                staging_location="tmp/stage_123",
                target_location="prod/orders",
                partition_by="dt",
                partition_val="2024-01-01",
            )

            expected_final = "prod/orders/dt=2024-01-01"
            mock_client.rm.assert_called_once_with(expected_final)
            mock_client.mv.assert_called_once_with("tmp/stage_123", expected_final)
