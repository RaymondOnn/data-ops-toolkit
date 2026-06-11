from pathlib import Path

import pytest
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.core.models.task.workspace import TaskWorkspace


@pytest.fixture
def workspace(exec_ctx):
    """Returns a TaskWorkspace instance pointing to the test execution context."""
    return TaskWorkspace(
        job_id="unit_job",
        dataset_id="unit_ds",
        partition_date="2024-01-01",
        run_id="run_test_001",
        exec_ctx=exec_ctx,
    )


def test_workspace_paths(workspace, exec_ctx):
    """
    GIVEN a TaskWorkspace initialization
    WHEN path properties are accessed
    THEN they should follow the colon-delimited and hierarchical patterns
    """
    expected_run = (
        exec_ctx.workspace_dir
        / "active"
        / "unit_job:unit_ds:2024-01-01"
        / "run_test_001"
    )
    assert workspace.run_path == expected_run
    assert workspace.manifest_path == expected_run / "manifest.json"
    assert workspace.config_path == expected_run / "config.json"


def test_get_data_path_formula(workspace, exec_ctx):
    """
    GIVEN a stage name
    WHEN get_data_path is called
    THEN it should return the deterministic hierarchical path in the data vault
    """
    path = workspace.get_data_path("extract")
    expected = (
        exec_ctx.data_path
        / "unit_job"
        / "unit_ds"
        / "2024-01-01"
        / "run_test_001"
        / "extract"
    )
    assert path == expected


def test_provision_logic(workspace, tmp_path):
    """
    GIVEN a source configuration file
    WHEN provision is called
    THEN it should create the workspace folder and move the config file into place
    """
    source_cfg = tmp_path / "pending_config.json"
    source_cfg.write_text("{}")

    workspace.provision(source_cfg)

    assert workspace.run_path.is_dir()
    assert workspace.config_path.exists()
    assert not source_cfg.exists()  # Should have been moved


def test_read_manifest_default(workspace):
    """
    GIVEN a workspace with no physical manifest file
    WHEN read_manifest is called
    THEN it should return a default TaskManifest with UNKNOWN status
    """
    manifest = workspace.read_manifest()
    assert manifest.status == ExecutionStatus.UNKNOWN
    assert manifest.job_id == "unit_job"


def test_write_manifest_atomic_swap(workspace):
    """
    GIVEN a data dictionary
    WHEN write_manifest is called
    THEN it should perform a safe write using a .tmp file and atomic replacement
    """
    data = {"job_id": "unit_job", "status": "running", "bitmask": 1}
    workspace.write_manifest(data)

    assert workspace.manifest_path.exists()
    # Verify we can read it back
    manifest = workspace.read_manifest()
    assert manifest.job_id == "unit_job"


def test_relocate_workspace(workspace, exec_ctx):
    """
    GIVEN an active workspace
    WHEN relocate is called with 'FAILED'
    THEN it should move the folder to the FAILED root and update the category
    """
    workspace.run_path.mkdir(parents=True)
    old_path = workspace.run_path

    new_path_str = workspace.relocate("FAILED")

    assert workspace.category == "FAILED"
    assert (
        Path(new_path_str)
        == exec_ctx.failed_path / "unit_job:unit_ds:2024-01-01" / "run_test_001"
    )
    assert not old_path.exists()
    assert Path(new_path_str).exists()


def test_purge_recursive_vault(workspace, exec_ctx):
    """
    GIVEN a workspace with metadata and data artifacts
    WHEN purge is called with include_vaults=True
    THEN it should remove the run folders and recursively clean empty parent
    directories in data/
    """
    # Setup: Create hierarchy data/job/ds/date/run/extract
    data_path = workspace.get_data_path("extract")
    data_path.mkdir(parents=True)
    workspace.run_path.mkdir(parents=True)

    workspace.purge(include_vaults=True)

    assert not workspace.run_path.exists()
    assert not data_path.exists()
    # Recursive Check: Because we were the only run, the whole tree up
    # to 'data/' should be gone
    assert not (exec_ctx.data_path / "unit_job").exists()
    assert exec_ctx.data_path.exists()  # Root data dir should remain


def test_create_symlink_relative(workspace):
    """
    GIVEN a physical data folder
    WHEN create_symlink is called
    THEN it should create a relative symlink that is portable
    """
    workspace.run_path.mkdir(parents=True)
    data_folder = workspace.get_data_path("extract")
    data_folder.mkdir(parents=True)

    workspace.create_symlink("extract", data_folder)

    marker = workspace.run_path / "extract"
    assert marker.is_symlink()
    # Check relativity: active/job/run/extract -> ../../../data/job/ds/date/run/extract
    link_target = Path.readlink(marker)
    assert ".." in link_target


def test_reset_data_dir_idempotency(workspace):
    """
    GIVEN an existing stage directory with stale files
    WHEN reset_data_dir is called
    THEN it should purge existing content and return a clean directory
    """
    path = workspace.get_data_path("transform")
    path.mkdir(parents=True)
    (path / "stale.parquet").write_text("garbage")

    new_path = workspace.reset_data_dir("transform")

    assert new_path == path
    assert new_path.is_dir()
    assert not (path / "stale.parquet").exists()


def test_remove_marker_robustness(workspace):
    """
    GIVEN a directory or a file marker
    WHEN remove_marker is called
    THEN it should handle both unlinking files and removing directories
    """
    workspace.run_path.mkdir(parents=True)

    # Case 1: File
    f_marker = workspace.run_path / ".test_file"
    f_marker.touch()
    workspace.remove_marker(".test_file")
    assert not f_marker.exists()

    # Case 2: Directory
    d_marker = workspace.run_path / "test_dir"
    d_marker.mkdir()
    workspace.remove_marker("test_dir")
    assert not d_marker.exists()
