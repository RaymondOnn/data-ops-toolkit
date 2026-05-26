from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.stages.transform import TransformStage
from apps.ingestion.src.utils.exceptions import RewindTask


@pytest.fixture
def transform_stage():
    return TransformStage(StageName.TRANSFORM)


@pytest.fixture
def mock_transform_task(mock_task):
    """
    GIVEN a base task
    THEN return a task configured for transformation testing
    """
    mock_task.run_id = "run-transform-001"
    mock_task.context.transform.transform_type = "base_transformer"
    mock_task.context.transform.transform_params = {"mode": "strict"}

    # Setup extract manifest
    mock_task.manifest.extract = MagicMock()
    mock_task.manifest.extract.file_count = 1

    return mock_task


def test_transform_pre_flight_missing_extract_metadata(
    transform_stage, mock_transform_task
):
    """
    GIVEN a task where the extract payload is missing from the manifest
    WHEN pre_flight is called
    THEN it should raise a RewindTask to the EXTRACT stage
    """
    mock_transform_task.manifest.extract = None

    with pytest.raises(RewindTask) as exc:
        transform_stage.pre_flight(mock_transform_task)
    assert exc.value.target_stage == StageName.EXTRACT.label


def test_transform_pre_flight_missing_data_marker(transform_stage, mock_transform_task):
    """
    GIVEN extract metadata exists but the 'extract/' marker folder is missing from disk
    WHEN pre_flight is called
    THEN it should raise a RewindTask to the EXTRACT stage
    """
    # folder / 'extract' does not exist in the temporary workspace
    with pytest.raises(RewindTask, match="Extraction data marker missing"):
        transform_stage.pre_flight(mock_transform_task)


def test_transform_pre_flight_empty_extract_dir(transform_stage, mock_transform_task):
    """
    GIVEN an 'extract/' marker exists but contains no parquet files
    WHEN pre_flight is called
    THEN it should raise a RewindTask
    """
    extract_dir = mock_transform_task.folder / StageName.EXTRACT.label
    extract_dir.mkdir(parents=True)

    with pytest.raises(RewindTask, match="Physical artifacts missing or empty"):
        transform_stage.pre_flight(mock_transform_task)


def test_transform_execute_success(transform_stage, mock_transform_task):
    """
    GIVEN a valid extraction output
    WHEN execute is called
    THEN it should read data via Ray, apply transformations, and finalize the manifest
    """
    # 1. Setup physical environment
    extract_dir = mock_transform_task.folder / StageName.EXTRACT.label
    extract_dir.mkdir(parents=True)
    (extract_dir / "part_000.parquet").write_text("data")

    # 2. Mock Ray Dataset behaviors
    mock_ds = MagicMock()
    mock_ds.map_batches.return_value = mock_ds

    # 3. Mock post-execution stats (Polars scan of the output)
    mock_stats = pl.DataFrame({"count": [100]})

    with (
        patch("ray.data.read_parquet", return_value=mock_ds),
        patch("polars.scan_parquet") as mock_scan,
        patch(
            "polars.read_parquet_schema", return_value={"id": "Int64", "val": "String"}
        ),
        patch.object(transform_stage, "_transit", return_value=StageName.WRITE.label),
    ):
        mock_scan.return_value.select.return_value.collect.return_value = mock_stats

        result = transform_stage.execute(mock_transform_task)

        # Assertions
        assert result == StageName.WRITE.label
        mock_transform_task.finalize.assert_called_once()

        # Verify payload contains row counts and logic version
        _, kwargs = mock_transform_task.finalize.call_args
        payload = kwargs["results"]
        assert payload["output_row_count"] == 100
        assert payload["transform_type"] == "base_transformer"
        assert "refined_schema" in payload


def test_transform_execute_skip_no_data(transform_stage, mock_transform_task):
    """
    GIVEN an extract manifest indicating 0 files were produced
    WHEN execute is called
    THEN it should skip the Ray processing and move to the next stage immediately
    """
    mock_transform_task.manifest.extract.file_count = 0

    with patch("ray.data.read_parquet") as mock_ray:
        result = transform_stage.execute(mock_transform_task)

        assert result == str(StageName.WRITE.label)
        mock_ray.assert_not_called()

        # Verify payload indicates zero rows
        _, kwargs = mock_transform_task.finalize.call_args
        assert kwargs["results"]["output_row_count"] == 0


def test_transform_execute_invalid_type(transform_stage, mock_transform_task):
    """
    GIVEN a transform type that is missing from the context
    WHEN execute is called
    THEN it should raise a ValueError
    """
    mock_transform_task.context.transform.transform_type = ""

    # Ensure data exists so it doesn't skip
    extract_dir = mock_transform_task.folder / StageName.EXTRACT.label
    extract_dir.mkdir(parents=True)
    (extract_dir / "part_000.parquet").write_text("data")

    # We trigger the ErrorPayload logic by failing the execute
    with pytest.raises(ValueError, match="Transform type is not defined"):
        transform_stage.execute(mock_transform_task)

    # Verify it attempted to finalize with the exception
    assert mock_transform_task.finalize.called
