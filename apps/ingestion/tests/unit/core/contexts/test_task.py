from datetime import datetime

import pytest

from src.core.contexts.task import (
    ExtractConfig,
    TaskContext,
    load_context,
)
from src.core.orchestrator.enums import TaskRecord


def test_placeholder():
    """
    GIVEN a TaskRecord from the database
    WHEN placeholder is called
    THEN it should return a valid TaskContext with 'N/A' markers for audit logging
    """
    record = TaskRecord(
        JOB_ID="job1",
        DATASET_ID="ds1",
        PARTITION_DATE="2024-01-01",
        RUN_ID="run1",
        JOB_STATUS="PENDING",
        SCHEDULED_TIMESTAMP_LC=datetime(2024, 1, 1, 0, 0, 0),
    )

    ctx = TaskContext.placeholder(record)
    assert ctx.job_id == "job1"
    assert ctx.extract.source_type == "N/A"
    assert ctx.archive.enabled is False


def test_load_context_missing_file(tmp_path):
    """
    GIVEN a path to a workspace folder
    WHEN load_context is called but config.json is missing
    THEN it should raise a FileNotFoundError
    """
    with pytest.raises(FileNotFoundError, match=r"Missing config.json"):
        load_context(tmp_path)


def test_extract_config_defaults():
    """
    GIVEN an ExtractConfig initialization
    WHEN no num_workers is provided
    THEN it should default to 10
    """
    cfg = ExtractConfig(source_type="s3", resource="bucket/path")
    assert cfg.num_workers == 10
