from unittest.mock import MagicMock, patch

import pytest
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.stages.write import WriteStage
from apps.ingestion.src.utils.exceptions import RewindTask


@pytest.fixture
def write_stage():
    return WriteStage(StageName.WRITE)


def test_write_pre_flight_missing_transform_metadata(write_stage, mock_task, mock_sink):
    """
    GIVEN a task where transformation metadata is missing in the manifest
    WHEN pre_flight is called
    THEN it should raise a RewindTask to the TRANSFORM stage
    """
    mock_task.manifest.transform = None

    with patch(
        "apps.ingestion.src.services.factory.ServiceFactory.get_sink",
        return_value=mock_sink,
    ):
        with pytest.raises(RewindTask) as exc:
            write_stage.pre_flight(mock_task)
        assert exc.value.target_stage == StageName.TRANSFORM.label


def test_write_pre_flight_missing_marker(write_stage, mock_task, mock_sink):
    """
    GIVEN transformation metadata exists but the 'transform/' directory marker is missing
    WHEN pre_flight is called
    THEN it should raise a RewindTask to the TRANSFORM stage
    """
    # folder / 'transform' does not exist in tmp_path
    with patch(
        "apps.ingestion.src.services.factory.ServiceFactory.get_sink",
        return_value=mock_sink,
    ):
        with pytest.raises(RewindTask, match="Transformation data marker missing"):
            write_stage.pre_flight(mock_task)


def test_write_pre_flight_empty_artifacts(write_stage, mock_task, mock_sink):
    """
    GIVEN a 'transform/' marker exists but contains no parquet files
    WHEN pre_flight is called and output_row_count > 0
    THEN it should raise a RewindTask
    """
    transform_dir = mock_task.folder / StageName.TRANSFORM.label
    transform_dir.mkdir()
    mock_task.manifest.transform.output_row_count = 100

    with patch(
        "apps.ingestion.src.services.factory.ServiceFactory.get_sink",
        return_value=mock_sink,
    ):
        with pytest.raises(RewindTask, match="Transformed physical artifacts missing"):
            write_stage.pre_flight(mock_task)


def test_write_execute_success(write_stage, mock_task):
    """
    GIVEN a valid transformation artifact and a working sink
    WHEN execute is called
    THEN it should stage the data and return the label for the next stage (PUBLISH)
    """
    # Setup physical directory
    transform_dir = mock_task.folder / StageName.TRANSFORM.label
    transform_dir.mkdir()
    (transform_dir / "part_000.parquet").write_text("data")

    mock_task.manifest.transform.output_row_count = 10
    mock_task.context.load.sink_type = "clickhouse"
    mock_task.context.load.sink_identifier = "db.table"

    # Mock the Loader behavioral strategy
    mock_loader = MagicMock()
    mock_loader.load.return_value = ("stg_table_123", 10)

    with (
        patch(
            "apps.ingestion.src.core.models.stages.write.Loader",
            return_value=mock_loader,
        ),
        patch.object(write_stage, "_transit", return_value=StageName.PUBLISH.label),
    ):
        result = write_stage.execute(mock_task)

        assert result == StageName.PUBLISH.label
        mock_task.finalize.assert_called_once()

        # Verify payload contains staging info
        args, kwargs = mock_task.finalize.call_args
        results = kwargs["results"]
        assert results["staging_artifact"] == "stg_table_123"
        assert results["rows_inserted"] == 10


def test_write_execute_failure(write_stage, mock_task):
    """
    GIVEN an error during the loading process
    WHEN execute is called
    THEN it should finalize the task with the exception and re-raise it
    """
    transform_dir = mock_task.folder / StageName.TRANSFORM.label
    transform_dir.mkdir()
    (transform_dir / "part_000.parquet").write_text("data")

    mock_loader = MagicMock()
    mock_loader.load.side_effect = RuntimeError("Sink Connection Lost")

    with patch(
        "apps.ingestion.src.core.models.stages.write.Loader", return_value=mock_loader
    ):
        with pytest.raises(RuntimeError, match="Sink Connection Lost"):
            write_stage.execute(mock_task)

        # Verify finalize was called with the exception to update manifest error block
        args, kwargs = mock_task.finalize.call_args
        assert isinstance(kwargs["exception"], RuntimeError)
