from typing import Any

from msgspec import field

from src.core.stages.contracts.config import BaseStageConfig


class LoadConfig(BaseStageConfig, tag="write"):
    """Configuration for data loading."""

    type: str  # "clickhouse", "snowflake", etc.
    destination: str  # Target table or path
    partition_on: str
    partition_value: str
    connection: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)


def parse_load_config(config) -> LoadConfig:
    """Create LoadConfig from raw parameters."""
    params = config["params"].copy()

    # Resolve destination from params
    partition_on = config.get("partition_on") or params.pop("partition_on", "")
    partition_value = config.get("partition_value") or params.pop("partition_value", "")

    return LoadConfig(
        type=config["connection"]["type"].casefold(),
        destination=params["destination"],
        partition_on=partition_on,
        partition_value=partition_value,
        connection=config["connection"],
        params=params,
    )
