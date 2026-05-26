from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

# Make fixtures available project-wide
pytest_plugins = [
    "tests.fixtures.database",
    "tests.fixtures.api",
]


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent


@pytest.fixture(scope="session")
def test_data_dir(project_root):
    """Return the test data directory."""
    return project_root / "tests" / "data"


@pytest.fixture
def sample_user_data():
    """Sample user data for tests."""
    return {
        "email": "test@example.com",
        "username": "testuser",
        "password": "securepassword123",
    }


@pytest.fixture
def exec_ctx(tmp_path):
    """
    GIVEN a testing environment
    WHEN a test requires an ExecutionContext
    THEN return a real instance pointing to a temporary workspace
    """
    from apps.ingestion.src.core.contexts.execution import (
        ExecutionContext,
        ExecutionMode,
        RayMode,
    )

    return ExecutionContext(
        workspace_dir=tmp_path,
        env="local",
        execution_mode=ExecutionMode.TEST,
        ray_mode=RayMode.LOCAL,
    )


@pytest.fixture
def mock_task(exec_ctx, tmp_path):
    """
    GIVEN multiple stage unit tests
    WHEN a standardized Task object is required
    THEN return a MagicMock that mimics the folder structure and manifest requirements
    """
    task = MagicMock()
    task.run_id = "test-run-uuid"
    task.job_id = "test-job"
    task.dataset_id = "test-dataset"
    task.partition_date = "2024-01-01"
    task.folder = tmp_path / "active" / "test-run-uuid"
    task.folder.mkdir(parents=True)
    task.exec_ctx = exec_ctx

    # Initialize manifest mock with basic attributes
    task.manifest = MagicMock()
    task.manifest.bitmask = 0
    return task


@pytest.fixture(autouse=True)
def clear_service_singletons():
    """
    GIVEN a suite of unit tests
    WHEN a test finishes
    THEN clear the ServiceFactory and ServiceRegistry to prevent state leakage
    """
    yield
    from apps.ingestion.src.services.factory import ServiceFactory

    ServiceFactory._INSTANCES.clear()


@pytest.fixture(autouse=True)
def mock_ray_runtime():
    """
    GIVEN unit tests that might trigger Ray calls
    WHEN the test suite runs
    THEN mock ray APIs to keep tests fast and isolated from distributed compute.
    """
    with (
        patch("ray.init"),
        patch("ray.data.read_parquet"),
        patch("ray.get_runtime_context"),
        patch("ray.put"),
    ):
        yield


@pytest.fixture
def mock_source():
    """
    GIVEN a requirement for a Source service
    THEN return a MagicMock adhering to the Source interface.
    """
    from apps.ingestion.src.services.base import Source

    source = MagicMock(spec=Source)
    source.get_total_count.return_value = 1000
    return source


@pytest.fixture
def mock_sink():
    """
    GIVEN a requirement for a Sink service
    THEN return a MagicMock adhering to the Sink interface.
    """
    from apps.ingestion.src.services.base import Sink

    sink = MagicMock(spec=Sink)
    sink.get_total_count.return_value = 1000
    sink.stage_data.return_value = ("stg_mock_table", 1000)
    return sink


@pytest.fixture
def mock_archive():
    """Returns a MagicMock adhering to the Archive interface."""
    from apps.ingestion.src.services.base import Archive

    return MagicMock(spec=Archive)


@pytest.fixture
def mock_db_client():
    """
    GIVEN a unit test for a DatabaseService (like ClickHouseService)
    WHEN the underlying DB client is invoked
    THEN return a MagicMock configured with standard database behaviors.
    """
    client = MagicMock()
    # Default return for sql() calls (usually list of tuples)
    client.sql.return_value = []
    # Default for exists() checks
    client.exists.return_value = True
    return client


@pytest.fixture
def ch_service(mock_db_client):
    """
    GIVEN a requirement to test ClickHouseService logic
    WHEN the service is instantiated
    THEN return an instance with the 'client' property patched to use mock_db_client.
    """
    from apps.ingestion.src.services.database.clickhouse import ClickHouseService

    service = ClickHouseService(
        name="test_clickhouse", host="localhost", database="default"
    )

    # We patch the cached_property 'client' to return our mock
    with patch.object(
        ClickHouseService, "client", new_callable=PropertyMock
    ) as mock_prop:
        mock_prop.return_value = mock_db_client
        yield service


@pytest.fixture
def runtime(exec_ctx):
    """
    GIVEN an execution context
    WHEN the runtime is assembled for testing
    THEN return a TriggerRuntime instance configured for the test workspace.
    """
    from apps.ingestion.src.core.contexts.builder import TaskContextBuilder
    from apps.ingestion.src.core.orchestrator.factory import assemble_runtime

    builder = TaskContextBuilder()
    return assemble_runtime(exec_ctx, builder)
