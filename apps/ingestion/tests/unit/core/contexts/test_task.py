from datetime import datetime

import pytest
from apps.ingestion.src.core.contexts.task import (
    ExtractConfig,
    TaskContext,
    load_task_context,
)
from apps.ingestion.src.core.orchestrator.enums import JobRecord


def test_create_placeholder():
    """
    GIVEN a JobRecord from the database
    WHEN create_placeholder is called
    THEN it should return a valid TaskContext with 'N/A' markers for audit logging
    """
    record = JobRecord(
        JOB_ID="job1",
        DATASET_ID="ds1",
        PARTITION_DATE="2024-01-01",
        RUN_ID="run1",
        JOB_STATUS="PENDING",
        SCHEDULED_TIMESTAMP_LC=datetime(2024, 1, 1, 0, 0, 0),
    )

    ctx = TaskContext.create_placeholder(record)
    assert ctx.job_id == "job1"
    assert ctx.extract.source_type == "N/A"
    assert ctx.archive.enabled is False


def test_load_task_context_missing_file(tmp_path):
    """
    GIVEN a path to a workspace folder
    WHEN load_task_context is called but config.json is missing
    THEN it should raise a FileNotFoundError
    """
    with pytest.raises(FileNotFoundError, match=r"Missing config.json"):
        load_task_context(tmp_path)


def test_extract_config_defaults():
    """
    GIVEN an ExtractConfig initialization
    WHEN no num_workers is provided
    THEN it should default to 10
    """
    cfg = ExtractConfig(source_type="s3", source_identifier="bucket/path")
    assert cfg.num_workers == 10
