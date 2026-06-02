from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fsspec import AbstractFileSystem
from libs.file.base import (
    FileSystemClient,
    FileSystemProtocol,
    FileSystemSkills,
    create_fs_client,
)


class MockFileSystemClient(FileSystemClient):
    """Concrete implementation for testing the abstract FileSystemClient."""

    _fs_mock: MagicMock

    @property
    def fs(self) -> MagicMock:
        """Returns the internal mock filesystem."""
        return self._fs_mock


class TestFileSystemClient:
    """Unit tests for the base FileSystemClient logic."""

    @pytest.fixture
    def mock_fs(self):
        """Fresh mock filesystem for each test."""
        return MagicMock(spec=AbstractFileSystem)

    @pytest.fixture
    def client(self, mock_fs):
        """Base client instance with injected mock filesystem."""
        c = MockFileSystemClient(url="s3://base-bucket")
        c._fs_mock = mock_fs
        return c

    def test_resolve_path_cloud_protocol(self, client):
        """
        GIVEN an S3 protocol URL
        THEN resolve_path should return it as-is without local resolution
        WHEN resolve_path is called
        """
        path = "s3://other-bucket/data.csv"
        resolved = client.resolve_path(path)
        assert resolved == path

    def test_resolve_path_local_absolute(self, client):
        """
        GIVEN a local absolute path
        THEN resolve_path should return the absolute string representation
        WHEN resolve_path is called
        """
        path = "/tmp/data.json"
        resolved = client.resolve_path(path)
        assert resolved == str(Path(path).resolve())

    def test_resolve_path_relative_joining(self, client):
        """
        GIVEN a naked relative path
        THEN resolve_path should join it to the client's base URL
        WHEN resolve_path is called
        """
        # Our client base is s3://base-bucket
        path = "subfolder/file.txt"
        resolved = client.resolve_path(path)
        assert resolved == "s3://base-bucket/subfolder/file.txt"

    def test_walk_paths_file_discovery(self, client, mock_fs):
        """
        GIVEN a path that points to a specific file
        THEN walk_paths should yield only that file
        WHEN walk_paths is called
        """
        target = "s3://base-bucket/file.csv"
        mock_fs.exists.return_value = True
        mock_fs.isfile.return_value = True

        results = list(client.walk_paths(target))
        assert results == [target]

    def test_walk_paths_directory_discovery(self, client, mock_fs):
        """
        GIVEN a path that points to a directory
        THEN walk_paths should yield all files found by the filesystem
        WHEN walk_paths is called
        """
        target = "s3://base-bucket/dir"
        mock_fs.exists.return_value = True
        mock_fs.isfile.return_value = False
        mock_fs.find.return_value = [
            "base-bucket/dir/a.csv",
            "base-bucket/dir/b.csv",
        ]
        mock_fs.unstrip_protocol.side_effect = lambda x: f"s3://{x}"

        results = list(client.walk_paths(target))
        assert "s3://base-bucket/dir/a.csv" in results
        assert "s3://base-bucket/dir/b.csv" in results
        assert len(results) == 2


class TestFileSystemFactory:
    """Tests for the dynamic client factory."""

    def test_create_fs_client_with_skills(self):
        """
        GIVEN a local URL and the CAS skill
        THEN the factory should return a dynamic class inheriting from both
        WHEN create_fs_client is invoked
        """
        client = create_fs_client(url="/tmp/data", capabilities={FileSystemSkills.CAS})

        # Verify that the instance has methods from both base and mixin
        # (Using __class__.__name__ to verify ManagedLocalClient structure)
        assert "ManagedLocalClient" in client.__class__.__name__

        # Verify protocol compliance
        assert isinstance(client, FileSystemProtocol)

        # Verify structural subtyping for the mixin (CASArchiveMixin has archive_to_cas)
        assert hasattr(client, "archive_to_cas")


if __name__ == "__main__":
    pytest.main([__file__])
