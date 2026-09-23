from enum import StrEnum
from pathlib import Path
from typing import Any

from msgspec import Struct, field


class Join(StrEnum):
    """Join types for SQL operations."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


class JoinConfig(Struct, kw_only=True):
    type: Join
    to_table: str
    on: str


class SelectQueryContext(Struct, kw_only=True):
    """Represents a single SELECT query block (either a CTE or a standalone query)."""

    name: str | None = None  # CTE alias/name (e.g. 'filtered_orders')
    from_table: str | None = None  # Source table, view, or CTE name
    select: list[str] | None = None  # Columns or expressions
    joins: list[JoinConfig] | None = None
    where: Any | None = None  # Predicate strings, dicts, or tuples
    group_by: list[str] | None = None
    having: Any | None = None  # HAVING clause predicates
    qualify: Any | None = (
        None  # QUALIFY clause predicates (e.g. ROW_NUMBER window filters)
    )
    order_by: list[str] | None = None
    limit: int | None = None
    sql: str | None = None  # Raw SQL block override for this specific CTE/query

    @property
    def has_clauses(self) -> bool:
        """Returns True if any structured query clause is populated."""
        return any(
            x is not None
            for x in (
                self.select,
                self.joins,
                self.where,
                self.group_by,
                self.having,
                self.qualify,
                self.order_by,
                self.limit,
            )
        )


class SQLContext(Struct, kw_only=True):
    """Top-level container holding CTE pipelines and the main query execution plan."""

    ctes: list[SelectQueryContext] = field(default_factory=list)
    main: SelectQueryContext | None = None
    sql: str | None = None  # Top-level full raw SQL override
    sql_file: str | None = None  # Path to external .sql file

    def __post_init__(self) -> None:
        if self.sql_file is not None:
            # 1. Mutual Exclusion: Cannot pass both `sql` string and `sql_file`
            if self.sql is not None:
                raise ValueError(
                    "Ambiguous SQL context: Cannot specify both 'sql' and 'sql_file'."
                )

            # 2. Structural Conflict: Warn/raise if combining raw file with CTEs/main
            if self.ctes or self.main is not None:
                raise ValueError(
                    "Ambiguous SQL context: Cannot combine 'sql_file' with structured 'ctes' or 'main'."
                )

            path = Path(self.sql_file)

            # 3. Path & Existence Checks
            if not path.exists():
                raise FileNotFoundError(f"SQL file not found at path: {path.resolve()}")

            if not path.is_file():
                raise ValueError(f"Specified path is not a file: {path.resolve()}")

            # 4. Read Content Safely
            try:
                content = path.read_text(encoding="utf-8").strip()
            except Exception as e:
                raise OSError(f"Failed to read SQL file '{path}': {e}") from e

            # 5. Empty Content Validation
            if not content:
                raise ValueError(f"SQL file at '{path}' is empty.")

            # 6. Populate `sql` attribute
            self.sql = content
