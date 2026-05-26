import pytest
from apps.ingestion.src.core.models.task.enums import TaskRef
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE


@pytest.fixture
def sample_ref():
    return TaskRef(
        namespace="ingestion",
        status="RUNNING",
        stage="extract",
        job_id="daily_sales",
        dataset_id="orders",
        partition_date="2024-01-01",
        run_id="abc-123",
    )


def test_task_ref_from_str_success():
    """
    GIVEN a valid 7-part colon-delimited string
    WHEN TaskRef.from_str is called
    THEN it should return a correctly populated TaskRef instance
    """
    raw = "task:PENDING:start:my_job:my_ds:2023-10-27:run_999"
    ref = TaskRef.from_str(raw)

    assert ref.namespace == "task"
    assert ref.status == "PENDING"
    assert ref.job_id == "my_job"
    assert ref.run_id == "run_999"


def test_task_ref_from_str_malformed():
    """
    GIVEN a string with incorrect number of parts
    WHEN TaskRef.from_str is called
    THEN it should raise a ValueError
    """
    with pytest.raises(ValueError, match="Invalid TaskRef format"):
        TaskRef.from_str("invalid:format:too_short")


def test_task_ref_from_signal_stem():
    """
    GIVEN a 4-part signal file stem (job:ds:date:run)
    WHEN TaskRef.from_signal_stem is called
    THEN it should return a Ref with UNKNOWN status/stage and 'task' namespace
    """
    stem = "j1:d1:2024-01-01:r1"
    ref = TaskRef.from_signal_stem(stem)

    assert ref.job_id == "j1"
    assert ref.run_id == "r1"
    assert ref.status == "UNKNOWN"
    assert ref.namespace == "task"


def test_task_ref_properties(sample_ref):
    """
    GIVEN a TaskRef
    WHEN identifier or composite_key is accessed
    THEN it should return the correctly formatted subsets of the identity
    """
    assert sample_ref.identifier == "daily_sales:orders:2024-01-01"
    assert sample_ref.composite_key == "daily_sales:orders"


def test_task_ref_build(sample_ref):
    """
    GIVEN a TaskRef
    WHEN build is called with optional overrides
    THEN it should return a full cache key string with the global namespace
    """
    # Default build
    key = sample_ref.build()
    expected = (
        f"{CACHE_TASK_NAMESPACE}:RUNNING:extract:daily_sales:orders:2024-01-01:abc-123"
    )
    assert key == expected

    # Override build
    key_updated = sample_ref.build(status="SUCCESS", stage="transform")
    expected_updated = f"{CACHE_TASK_NAMESPACE}:SUCCESS:transform:daily_sales:orders:2024-01-01:abc-123"
    assert key_updated == expected_updated


def test_task_ref_with_updates_immutability(sample_ref):
    """
    GIVEN a frozen TaskRef
    WHEN with_updates is called
    THEN it should return a NEW instance with updated fields, leaving the original unchanged
    """
    new_ref = sample_ref.with_updates(status="FAILED")

    assert new_ref.status == "FAILED"
    assert sample_ref.status == "RUNNING"  # Original preserved
    assert new_ref.run_id == sample_ref.run_id  # Other fields preserved
    assert new_ref is not sample_ref  # Memory address check


def test_task_ref_frozen():
    """GIVEN a TaskRef WHEN an attribute is modified THEN it should raise an error."""
    ref = TaskRef("n", "st", "sg", "j", "d", "p", "r")
    with pytest.raises(AttributeError):
        ref.status = "NEW"  # type: ignore
