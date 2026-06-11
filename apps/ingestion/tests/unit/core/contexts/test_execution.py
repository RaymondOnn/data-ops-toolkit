import pytest
from apps.ingestion.src.core.contexts.execution import (
    ExecutionContext,
    ExecutionMode,
    RayMode,
)


@pytest.fixture
def exec_ctx(tmp_path):
    return ExecutionContext(
        workspace_dir=tmp_path,
        env="local",
        execution_mode=ExecutionMode.NORMAL,
        ray_mode=RayMode.LOCAL,
    )


def test_managed_directories(exec_ctx):
    """
    GIVEN an ExecutionContext
    WHEN get_managed_directories is called
    THEN it should yield all required system paths including active, signals, and FAILED
    """
    dirs = list(exec_ctx.get_managed_directories())
    assert exec_ctx.active_path in dirs
    assert exec_ctx.signal_path in dirs
    assert exec_ctx.failed_path in dirs
    assert (exec_ctx.workspace_dir / "HOLD") in dirs


def test_get_run_path(exec_ctx):
    """
    GIVEN job metadata and a run_id
    WHEN get_run_path is called for the 'active' category
    THEN it should return a path following the
        {workspace}/active/{identifier}/{run_id} pattern
    """
    path = exec_ctx.get_run_path(
        job_id="test_job",
        dataset_id="test_ds",
        partition_date="2024-01-01",
        run_id="run_123",
    )

    expected = (
        exec_ctx.workspace_dir / "active" / "test_job:test_ds:2024-01-01" / "run_123"
    )
    assert path == expected


def test_parse_identifier(exec_ctx):
    """
    GIVEN a standard colon-delimited identifier string
    WHEN parse_identifier is called
    THEN it should correctly unpack the job_id, dataset_id, partition_date, and run_id
    """
    ident = "my_job:my_dataset:2023-10-27:abc-123"
    job, ds, dt, run = exec_ctx.parse_identifier(ident)

    assert job == "my_job"
    assert ds == "my_dataset"
    assert dt == "2023-10-27"
    assert run == "abc-123"


def test_parse_identifier_malformed(exec_ctx):
    """
    GIVEN a malformed identifier string
    WHEN parse_identifier is called
    THEN it should raise a ValueError
    """
    with pytest.raises(ValueError, match="Malformed identifier string"):
        exec_ctx.parse_identifier("invalid:string")


def test_check_serializability(exec_ctx):
    """
    GIVEN a standard ExecutionContext
    WHEN check_serializability is invoked
    THEN it should return True, confirming the object can be sent to Ray workers
    """
    assert exec_ctx.check_serializability() is True


def test_is_prod_logic(exec_ctx):
    """
    GIVEN an ExecutionContext with env='prod'
    WHEN is_prod is checked
    THEN it should return True
    """
    exec_ctx.env = "prod"
    assert exec_ctx.is_prod is True
