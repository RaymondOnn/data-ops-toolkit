from unittest.mock import patch

from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.stages.start import StartStage


def test_start_stage_execution(mock_task):
    """
    GIVEN a task in the START stage
    WHEN execute is called
    THEN it should capture the git commit hash and finalize with a BasePayload
    """
    stage = StartStage(StageName.START)
    mock_task.run_id = "run-123"
    mock_task.worker_id = "worker-1"

    with patch.object(StartStage, "_get_commit_hash", return_value="abc1234"):
        stage.execute(mock_task)

    # Verify finalize was called with the correct metadata
    mock_task.finalize.assert_called_once()
    _, kwargs = mock_task.finalize.call_args
    results = kwargs["results"]
    assert results["commit_hash"] == "abc1234"
    assert results["worker_id"] == "worker-1"


def test_get_commit_hash_fallback():
    """
    GIVEN a StartStage environment where git is not available
    WHEN _get_commit_hash is called
    THEN it should return 'unknown' rather than crashing
    """
    stage = StartStage(StageName.START)
    with patch("subprocess.check_output", side_effect=Exception()):
        assert stage._get_commit_hash() == "unknown"
