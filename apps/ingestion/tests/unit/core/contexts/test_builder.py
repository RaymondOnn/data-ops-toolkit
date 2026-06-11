import os

from apps.ingestion.src.core.contexts.builder import (
    expand_env_vars,
    parse_cli_overrides,
)


def test_expand_env_vars_with_defaults():
    """
    GIVEN a string with ${VAR:-DEFAULT} syntax
    WHEN expand_env_vars is called and the variable is missing from environment
    THEN it should resolve to the provided default value
    """
    val = expand_env_vars("s3://${BUCKET_NAME:-my-default-bucket}/data")
    assert val == "s3://my-default-bucket/data"


def test_expand_env_vars_from_os(monkeypatch):
    """
    GIVEN a string with $VAR syntax
    WHEN expand_env_vars is called and the variable exists in environment
    THEN it should resolve to the environment value
    """
    monkeypatch.setenv("DB_USER", "admin")
    val = expand_env_vars("User is $DB_USER")
    assert val == "User is admin"


def test_expand_env_vars_recursive():
    """
    GIVEN a nested dictionary containing environment variable tokens
    WHEN expand_env_vars is called
    THEN it should recursively resolve all strings within the structure
    """
    data = {"level1": "val_${MISSING:-def}", "nested": {"key": "$PATH"}}
    resolved = expand_env_vars(data)
    assert resolved["level1"] == "val_def"
    assert os.environ["PATH"] in resolved["nested"]["key"]


def test_parse_cli_overrides_global():
    """
    GIVEN a list of CLI settings like ['batch_size=1000']
    WHEN parse_cli_overrides is called
    THEN it should return a dictionary with the value in the '_global' scope
    """
    settings = ["batch_size=1000", "debug=true"]
    result = parse_cli_overrides(settings)

    assert result["_global"]["batch_size"] == 1000
    assert result["_global"]["debug"] is True


def test_parse_cli_overrides_scoped():
    """
    GIVEN a list of CLI settings with dataset scopes like ['orders:batch_size=50']
    WHEN parse_cli_overrides is called
    THEN it should correctly group overrides by dataset key
    """
    settings = ["timeout=30", "orders:batch_size=50", "inventory:batch_size=10"]
    result = parse_cli_overrides(settings)

    assert result["_global"]["timeout"] == 30
    assert result["orders"]["batch_size"] == 50
    assert result["inventory"]["batch_size"] == 10
