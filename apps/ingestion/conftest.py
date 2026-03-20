import pytest
import os
from pathlib import Path

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
