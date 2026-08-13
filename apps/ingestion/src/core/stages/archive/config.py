from typing import Any

from msgspec import field

from src.core.stages.contracts.config import BaseStageConfig


class ArchiveConfig(BaseStageConfig, tag="archive", kw_only=True, omit_defaults=True):
    enabled: bool = False
    type: str = ""  # Serializer drops key if it is ""
    base_path: str = ""  # Serializer drops key if it is ""
    connection: dict[str, Any] = field(default_factory=dict)
    retention_days: int = 2555


def parse_archive_config(config) -> ArchiveConfig:
    """Create ArchiveConfig from raw parameters."""

    if not config["archive_enabled"]:
        return ArchiveConfig(enabled=False)

    return ArchiveConfig(
        enabled=True,
        retention_days=config.get("retention_days", 2555),
        base_path=config["connection"]["url"],
        type=config["connection"]["type"].casefold(),
        connection=config["connection"],
    )
