from collections.abc import Callable
from enum import StrEnum
from typing import TYPE_CHECKING

import polars as pl
from libs.utils.dates import current_timestamp
from msgspec import Struct, field

from src.core.stages.contracts.payload import BasePayload

if TYPE_CHECKING:
    from src.core.models.task import TaskManifest


class PartitionStaged(Struct):
    partition_date: str
    rows_processed: int


class WritePayload(BasePayload, kw_only=True, tag="write"):
    """Write stage results."""

    staging_artifact: str
    # sink_type: str
    rows_processed: int
    # partition_on: str
    # partition_value: str
    destination: str
    partitions: list[PartitionStaged] = field(default_factory=list)
    # stage: Stage = Stage.WRITE


class WriteStrategy(StrEnum):
    DIRECT = "direct"
    CLONE = "clone"
    STAGING = "staging"


class CaseType(StrEnum):
    LOWER = "lower"
    UPPER = "upper"
    CAPS = "caps"
    TITLE = "title"
    NONE = "none"


# Note: Only CAPS or NONE for Camel
class CaseStyle(StrEnum):
    SNAKE = "snake"
    KEBAB = "kebab"
    NONE = "none"
    CAMEL = "camel"


class MetadataColumnSpec(Struct, frozen=True):
    key: str
    default_name: str
    is_enabled: bool = False
    name_override: str | None = None
    # Expression builder callable: (task, existing_cols) -> pl.Expr
    expr_builder: Callable[["TaskManifest", set[str]], pl.Expr] | None = None

    @property
    def target_name(self) -> str:
        """Determines the effective target column name."""
        return self.name_override or self.default_name

    def build_expr(self, manifest: "TaskManifest", existing_cols: set[str]) -> pl.Expr:
        """Returns the resolved Polars expression aliased to target_name."""
        if self.expr_builder:
            return self.expr_builder(manifest, existing_cols).alias(self.target_name)
        return pl.lit(None).alias(self.target_name)


def _build_source_expr(manifest: "TaskManifest", existing_cols: set[str]) -> pl.Expr:
    from src.core.models.task.manifest import TaskManifestView

    view = TaskManifestView(manifest)
    source_lookup = {
        k: v["source"]
        for k, v in view.get_partition_metadata_map().items()
        if v.get("source")
    }
    if "__partition" in existing_cols and source_lookup:
        return pl.col("__partition").cast(pl.Utf8).replace_strict(source_lookup)
    return pl.lit(None, dtype=pl.Utf8)


DEFAULT_METADATA_COLS: dict[str, MetadataColumnSpec] = {
    "run_id": MetadataColumnSpec(
        key="run_id",
        default_name="_run_id",
        expr_builder=lambda task, _: pl.lit(str(task.run_id)),
    ),
    "partition_date": MetadataColumnSpec(
        key="partition_date",
        default_name="_partition_date",
        expr_builder=lambda task, existing_cols: (pl.col("__partition").cast(pl.Utf8)),
    ),
    "source": MetadataColumnSpec(
        key="source",
        default_name="_source",
        expr_builder=_build_source_expr,
    ),
    "loaded_at": MetadataColumnSpec(
        key="loaded_at",
        default_name="_loaded_at",
        expr_builder=lambda task, _: pl.lit(
            current_timestamp(naive=True).isoformat(sep=" ")
        ),
    ),
    "deleted": MetadataColumnSpec(
        key="deleted",
        default_name="_is_deleted",
        expr_builder=lambda task, _: pl.lit(None, dtype=pl.Boolean),
    ),
}
