"""Base classes for extraction strategies."""

from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any, Generic, TypeVar

import msgspec
from libs.database.sql import SQLContext

# from src.core.contexts.task import ColumnMapping
from src.services.base import Source

T_Source = TypeVar("T_Source", bound=Source)


class ExtractContext(msgspec.Struct, frozen=True):
    """Serializable container for extraction parameters."""

    # kind: str
    resource: str
    src_connection: dict[str, Any]
    num_workers: int
    run_id: str
    partition_date: str
    job_id: str
    workspace: str | None = None
    monitor_params: dict[str, Any] = {}
    # select: list[str] = []
    # where: str | None = None
    # limit: int | None = None
    sql_context: SQLContext = msgspec.field(default_factory=SQLContext)
    columns: dict[str, str] = msgspec.field(default_factory=dict)
    null_if: list[str] = msgspec.field(default_factory=list)
    batch_size: int | None = None
    flatten: int = -1  # {-1: False, 0: True / All, Any other number: num_depth}
    temp_folder: str | None = None

    # ---- file only attributes ----
    # compression: str | None = None
    glob: str | None = None
    # header: bool = True
    # skip_blank_lines: bool = True
    # task_folder: str | None = None
    # tmp_cleanup: bool = True


class SQLContext(msgspec.Struct, frozen=True):
    """Encapsulates parameters for building dynamic and complex SQL queries safely."""

    resource: str  # The target table name
    select: list[str] = msgspec.field(
        default_factory=list
    )  # Specific columns to select
    where: str | None = None  # Filtering expression
    limit: int | None = None  # Row limitations
    sql: str | None = None  # Optional raw/complex query

    def compile(self) -> str:
        """
        Compiles the components into a single, unified ClickHouse-compatible SQL query
        using a CTE to cleanly combine 'sql' and 'select/where/limit'.
        """
        # 1. Resolve the base dataset (Either the custom SQL query or a standard SELECT *)
        if self.sql:
            # Strip trailing semicolons if present in the raw SQL
            base_query = self.sql.strip().rstrip(";")
        else:
            base_query = f"SELECT * FROM {self.resource}"

        # 2. Wrap the base dataset in a CTE so we can safely chain filters/limits
        cte_query = f"WITH __base_dataset AS ({base_query})"

        # 3. Determine target columns
        cols_str = ", ".join(self.select) if self.select else "*"

        # 4. Assemble the outer wrapper query
        final_query = f"{cte_query} SELECT {cols_str} FROM __base_dataset"

        if self.where:
            # Strip potential user-entered 'WHERE ' prefix for resilience
            clean_where = self.where.strip()
            if clean_where.lower().startswith("where"):
                clean_where = clean_where[5:].strip()
            final_query += f" WHERE {clean_where}"

        if self.limit is not None:
            final_query += f" LIMIT {int(self.limit)}"

        return final_query


class Extractor(ABC, Generic[T_Source]):
    """Abstract base class for all data extraction strategies."""

    @abstractmethod
    def extract(
        self, source: T_Source, context: ExtractContext, output_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Yield metadata for each Parquet chunk as it's written.

        Args:
            source: The source service instance.
            context: The extraction parameters.
            output_folder: The directory to write output files.

        Yields:
            Generator[dict[str, Any], None, None]: Metadata about the written chunks.
        """
