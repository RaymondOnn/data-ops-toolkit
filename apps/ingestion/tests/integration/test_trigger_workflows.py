from datetime import datetime

import pytest
from apps.ingestion.src.core.orchestrator.enums import JobRecord
from apps.ingestion.src.core.orchestrator.modes.daemon.trigger import TriggerManager


@pytest.fixture
def trigger_mgr(exec_ctx):
    """GIVEN a TriggerManager instance."""
    return TriggerManager(exec_ctx)


def test_file_arrival_trigger_with_globs(trigger_mgr, exec_ctx, tmp_path):
    """
    GIVEN a job configured with a WATCH_FILE_PATH using a glob pattern
    WHEN the matching files are created in the landing zone
    THEN the TriggerManager should return a 'trigger' decision.
    """
    # 1. Setup: Define a job watching for parquet files
    watch_pattern = str(tmp_path / "landing/*.parquet")
    record = JobRecord(
        JOB_ID="file_job",
        DATASET_ID="ds1",
        PARTITION_DATE="2024-01-01",
        RUN_ID="run_file_001",
        JOB_STATUS="PENDING",
        TRIGGER_TYPE="FILE",
        WATCH_FILE_PATH=watch_pattern,
        SCHEDULED_TIMESTAMP_LC=datetime.now(),
    )

    # 2. Execution: Evaluate while folder is empty
    decisions = trigger_mgr.evaluate([record])
    assert len(decisions) == 0, "Job triggered despite missing files"

    # 3. Action: Create the landing directory and a matching file
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir()
    (landing_dir / "data_part1.parquet").write_text("dummy")

    # 4. Execution: Re-evaluate
    decisions = trigger_mgr.evaluate([record])

    assert len(decisions) == 1
    assert decisions[0].action == "trigger"
    assert decisions[0].rule_name == "READY:FILE"


def test_trigger_security_traversal_block(trigger_mgr, exec_ctx):
    """
    GIVEN a malicious JobRecord with a path traversal in WATCH_FILE_PATH
    WHEN evaluate is called
    THEN the manager should block the check and log a warning.
    """
    malicious_record = JobRecord(
        JOB_ID="hack_job",
        DATASET_ID="ds1",
        PARTITION_DATE="2024-01-01",
        RUN_ID="run_hack",
        JOB_STATUS="PENDING",
        WATCH_FILE_PATH="../../../../etc/passwd",
        SCHEDULED_TIMESTAMP_LC=datetime.now(),
    )

    decisions = trigger_mgr.evaluate([malicious_record])

    # Rule logic in trigger.py should return False for '..'
    assert len(decisions) == 0


def test_ghost_task_auto_purge(trigger_mgr, exec_ctx, tmp_path):
    """
    GIVEN a Run ID that exists in the database but has no config.json on disk
    WHEN the TriggerManager evaluates the record
    THEN it should identify it as a 'Ghost Task' and issue a 'purge' decision.
    """
    record = JobRecord(
        JOB_ID="ghost_job",
        DATASET_ID="ds1",
        PARTITION_DATE="2024-01-01",
        RUN_ID="ghost_run_999",
        JOB_STATUS="PENDING",
        SCHEDULED_TIMESTAMP_LC=datetime.now(),
    )

    # No files created in tmp_path/active
    decisions = trigger_mgr.evaluate([record])

    assert len(decisions) == 1
    assert decisions[0].action == "purge"
    assert decisions[0].rule_name == "GHOST_RECOVERY"
