from unittest.mock import MagicMock, patch

import pytest
from libs.file.clients.s3 import S3Client


class TestS3Client:
    """Unit tests for the S3 filesystem client."""

    @pytest.fixture
    def mock_opts(self):
        """Standard storage options for S3 tests."""
        return {
            "client": {
                "region": "us-east-1",
                "sts_endpoint_url": "http://sts",
                "role_arn": "arn:role",
                "aws_access_key_id": "key",
            },
            "s3_endpoint_url": "http://localhost:4566",
            "password": "secret_password",
        }

    @patch("libs.file.clients.s3.S3FileSystem")
    @patch("libs.file.clients.s3.AWSClient")
    def test_fs_property_initialization(self, mock_aws_cls, mock_s3fs_cls, mock_opts):
        """
        GIVEN valid S3 storage options and credentials
        THEN it should instantiate S3FileSystem with correct parameters
        WHEN the fs property is first accessed
        """
        # Setup mock credentials
        mock_aws = mock_aws_cls.return_value
        mock_aws.get_current_credentials.return_value = MagicMock(
            access_key="ak", secret_key="sk", token="tok"
        )
        mock_aws.config.region = "us-east-1"

        client = S3Client("s3://bucket", storage_options=mock_opts)
        fs = client.fs

        assert fs is not None
        mock_s3fs_cls.assert_called_once()
        # Verify endpoint was passed to s3fs
        _, kwargs = mock_s3fs_cls.call_args
        assert kwargs["client_kwargs"]["endpoint_url"] == "http://localhost:4566"

    @patch("libs.file.clients.s3.S3FileSystem")
    @patch("libs.file.clients.s3.AWSClient")
    def test_exists_bucket_root(self, mock_aws, mock_s3fs_cls, mock_opts):
        """
        GIVEN a path pointing to a bucket root
        THEN it should use list_buckets for validation (LocalStack workaround)
        WHEN exists() is called
        """
        mock_fs = mock_s3fs_cls.return_value
        mock_fs.call_s3.return_value = {"Buckets": [{"Name": "my-bucket"}]}

        client = S3Client("s3://my-bucket", storage_options=mock_opts)
        client._fs = mock_fs  # Inject mock

        assert client.exists("s3://my-bucket") is True
        mock_fs.call_s3.assert_called_with("list_buckets")

    @patch("libs.file.clients.s3.S3FileSystem")
    @patch("libs.file.clients.s3.AWSClient")
    def test_exists_file_not_found(self, mock_aws, mock_s3fs_cls, mock_opts):
        """
        GIVEN a file path that does not exist
        THEN it should return False and handle NoSuchKey exceptions
        WHEN exists() is called
        """
        mock_fs = mock_s3fs_cls.return_value
        mock_fs.exists.side_effect = Exception("NoSuchKey")

        client = S3Client("s3://bucket", storage_options=mock_opts)
        client._fs = mock_fs

        assert client.exists("s3://bucket/missing.txt") is False

    @patch("libs.file.clients.s3.S3FileSystem")
    @patch("libs.file.clients.s3.AWSClient")
    def test_cp_local_to_s3(self, mock_aws, mock_s3fs_cls, mock_opts):
        """
        GIVEN a local source path and an S3 destination
        THEN it should invoke the put() method for upload
        WHEN cp() is called
        """
        mock_fs = mock_s3fs_cls.return_value
        client = S3Client("s3://bucket", storage_options=mock_opts)
        client._fs = mock_fs

        client.cp("/tmp/local_file.txt", "s3://bucket/remote.txt")

        mock_fs.put.assert_called_once_with(
            "/tmp/local_file.txt", "s3://bucket/remote.txt", recursive=True
        )

    @patch("libs.file.clients.s3.S3FileSystem")
    @patch("libs.file.clients.s3.AWSClient")
    def test_find_with_pattern(self, mock_aws, mock_s3fs_cls, mock_opts):
        """
        GIVEN a list of files in S3
        THEN it should yield only those matching the glob pattern
        WHEN find() is called
        """
        mock_fs = mock_s3fs_cls.return_value
        mock_fs.find.return_value = ["bucket/data.csv", "bucket/data.json"]
        mock_fs.unstrip_protocol.side_effect = lambda x: f"s3://{x}"

        client = S3Client("s3://bucket", storage_options=mock_opts)
        client._fs = mock_fs

        results = list(client.find("s3://bucket", pattern="*.csv"))

        assert len(results) == 1
        assert results[0] == "s3://bucket/data.csv"

    @patch("libs.file.clients.s3.S3FileSystem")
    @patch("libs.file.clients.s3.AWSClient")
    def test_ensure_bucket_exists(self, mock_aws, mock_s3fs_cls, mock_opts):
        """
        GIVEN a path to a non-existent bucket
        THEN it should call mkdir to create the bucket
        WHEN _ensure_bucket_exists is invoked
        """
        mock_fs = mock_s3fs_cls.return_value
        # Mock list_buckets to return empty
        mock_fs.call_s3.return_value = {"Buckets": []}

        client = S3Client("s3://new-bucket", storage_options=mock_opts)
        client._fs = mock_fs

        client._ensure_bucket_exists("s3://new-bucket/data/")

        mock_fs.mkdir.assert_called_once_with("new-bucket")
