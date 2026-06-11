from unittest.mock import MagicMock, patch

import pytest
from apps.ingestion.src.core.models.stages.archive import ArchiveStage
from apps.ingestion.src.core.models.stages.enums import Stage


@pytest.fixture
def archive_stage():
    return ArchiveStage(Stage.ARCHIVE)


@pytest.fixture
def mock_task(tmp_path):
    task = MagicMock()
    task.workspace.path = tmp_path
    task.run_id = "run-arch-999"
    task.job_id = "nightly_sync"
    task.dataset_id = "logs"

    # Workspace mock
    task.workspace.get_data_path.return_value = tmp_path / "data" / "extract"
    (tmp_path / "data" / "extract").mkdir(parents=True)

    # Context
    task.context.archive.enabled = True
    task.context.archive.archive_type = "s3_archive"
    task.context.archive.archive_params = {"retention_days": 365}

    # Previous payloads
    task.manifest.publish = MagicMock()
    task.manifest.publish.final_destination = "prod.logs"
    return task


def test_archive_execute_skipped_when_disabled(archive_stage, mock_task):
    """
    GIVEN a task where archiving is disabled in config
    WHEN execute is called
    THEN it should return the terminal sentinel without calling the archive service
    """
    mock_task.context.archive.enabled = False

    with patch(
        "apps.ingestion.src.services.factory.ServiceFactory.get_archive"
    ) as mock_factory:
        result = archive_stage.execute(mock_task)
        assert result == "FINISH"
        mock_factory.assert_not_called()


def test_archive_execute_success(archive_stage, mock_task, mock_archive):
    """
    GIVEN an enabled archive config
    WHEN execute is called
    THEN it should move artifacts to the vault and mark cleanup as verified
    """
    with (
        patch(
            "apps.ingestion.src.services.factory.ServiceFactory.get_archive",
            return_value=mock_archive,
        ),
        patch.object(archive_stage, "_transit", return_value="FINISH"),
    ):
        result = archive_stage.execute(mock_task)

        # Check archival call
        mock_archive.archive.assert_called_once()

        assert result == "FINISH"
        mock_task.checkpoint.assert_called_once()

        # Verify payload
        args, kwargs = mock_task.checkpoint.call_args
        res = kwargs["results"]
        assert res["cleanup_verified"] is True
        assert "nightly_sync" in res["archival_path"]


def test_archive_execute_handles_failure(archive_stage, mock_task):
    """
    GIVEN a failure in the archival service (e.g. S3 Timeout)
    WHEN execute is called
    THEN it should checkpoint with the exception so the job can be retried
    """
    with patch(
        "apps.ingestion.src.services.factory.ServiceFactory.get_archive",
        side_effect=RuntimeError("S3 Down"),
    ):
        with pytest.raises(RuntimeError, match="S3 Down"):
            archive_stage.execute(mock_task)

        mock_task.checkpoint.assert_called_once()
        assert "exception" in mock_task.checkpoint.call_args[1]
