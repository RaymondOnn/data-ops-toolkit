from unittest.mock import MagicMock, patch

import pytest
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.stages.publish import PublishStage
from apps.ingestion.src.utils.exceptions import RewindTask


@pytest.fixture
def publish_stage():
    return PublishStage(StageName.PUBLISH)


@pytest.fixture
def mock_publish_task(mock_task):
    mock_task.run_id = "run-publish-123"
    mock_task.dataset_id = "orders"
    mock_task.partition_date = "2024-01-01"

    # Setup context
    mock_task.context.load.sink_type = "clickhouse"
    mock_task.context.load.sink_identifier = "prod.orders"
    mock_task.context.load.partition_col = "dt"
    mock_task.context.load.partition_value = "2024-01-01"

    # Mock manifest
    mock_task.manifest.write = MagicMock()
    mock_task.manifest.write.staging_artifact = "stg_orders_123"
    mock_task.manifest.write.rows_inserted = 500
    return mock_task


def test_publish_pre_flight_missing_write_metadata(publish_stage, mock_publish_task):
    """
    GIVEN a task where the WRITE stage failed to produce metadata
    WHEN pre_flight is called for PUBLISH
    THEN it should raise a RewindTask to the WRITE stage
    """
    mock_publish_task.manifest.write = None

    with patch("apps.ingestion.src.services.factory.ServiceFactory.get_sink"):
        with pytest.raises(RewindTask) as exc:
            publish_stage.pre_flight(mock_publish_task)
        assert exc.value.target_stage == StageName.WRITE.label


def test_publish_execute_success(publish_stage, mock_task, mock_sink):
    """
    GIVEN valid staging metadata and a working sink
    WHEN execute is called
    THEN it should promote the data via atomic swap and finalize with success
    """
    # Mock the count check after promotion
    mock_sink.get_total_count.return_value = 500

    with (
        patch(
            "apps.ingestion.src.services.factory.ServiceFactory.get_sink",
            return_value=mock_sink,
        ),
        patch.object(publish_stage, "_transit", return_value=StageName.ARCHIVE.label),
    ):
        result = publish_stage.execute(mock_task)

        # Verify promote_data was called with correct parameters
        mock_sink.promote_data.assert_called_once_with(
            staging_table="stg_orders_123",
            target_table="prod.orders",
            partition_col="dt",
            partition_val="2024-01-01",
            expected_count=500,
        )

        assert result == StageName.ARCHIVE.label
        mock_task.finalize.assert_called_once()

        # Verify payload results
        args, kwargs = mock_task.finalize.call_args
        res = kwargs["results"]
        assert res["final_count"] == 500
        assert res["final_destination"] == "prod.orders"


def test_publish_execute_data_loss_detection(publish_stage, mock_task, mock_sink):
    """
    GIVEN the promotion succeeds but the final count is lower than expected
    WHEN execute is called
    THEN it should raise a ValueError to prevent silent data loss
    """
    mock_sink.get_total_count.return_value = 450  # Mismatch! (Expected 500)

    with (
        patch(
            "apps.ingestion.src.services.factory.ServiceFactory.get_sink",
            return_value=mock_sink,
        ),
        pytest.raises(ValueError, match="Row count mismatch after promotion"),
    ):
        publish_stage.execute(mock_task)
