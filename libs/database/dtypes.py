from enum import StrEnum
from typing import Any, Final

import polars as pl


class TypeGroup(StrEnum):
    """Canonical type groups for cross-database compatibility."""

    NUMERIC = "NUMERIC"
    TEXT = "TEXT"
    TEMPORAL = "TEMPORAL"
    BOOLEAN = "BOOLEAN"
    BINARY = "BINARY"
    OBJECT = "OBJECT"
    NULL = "NULL"


# Type Registry: Database -> Native Type -> TypeGroup
_GLOBAL_TYPE_MAPPING: Final[dict[str, TypeGroup]] = {
    # Text types (Consolidating all database variations to TEXT)
    "varchar": TypeGroup.TEXT,
    "varchar2": TypeGroup.TEXT,
    "nvarchar2": TypeGroup.TEXT,
    "text": TypeGroup.TEXT,
    "string": TypeGroup.TEXT,
    "fixedstring": TypeGroup.TEXT,
    "clob": TypeGroup.TEXT,
    "uuid": TypeGroup.TEXT,
    "_text": TypeGroup.TEXT,
    # Numeric types
    "int4": TypeGroup.NUMERIC,
    "int8": TypeGroup.NUMERIC,
    "int32": TypeGroup.NUMERIC,
    "int64": TypeGroup.NUMERIC,
    "float64": TypeGroup.NUMERIC,
    "number": TypeGroup.NUMERIC,
    "binary_double": TypeGroup.NUMERIC,
    "_int4": TypeGroup.NUMERIC,
    # Boolean types
    "bool": TypeGroup.BOOLEAN,
    "boolean": TypeGroup.BOOLEAN,
    # Temporal types
    "date": TypeGroup.TEMPORAL,
    "datetime": TypeGroup.TEMPORAL,
    "timestamp": TypeGroup.TEMPORAL,
    "timestamptz": TypeGroup.TEMPORAL,
    # Object / Semi-structured types
    "jsonb": TypeGroup.OBJECT,
    "json": TypeGroup.OBJECT,
    # Binary types
    "bytea": TypeGroup.BINARY,
    "raw": TypeGroup.BINARY,
}

_PROVIDER_OVERRIDES: Final[dict[str, dict[str, TypeGroup]]] = {
    "oracle": {
        # Treat Oracle DATE as TEMPORAL instead of basic DATE
        "date": TypeGroup.TEMPORAL,
    },
    "postgres": {
        # Custom Postgres resolutions if any anomalies arise
    },
}

# Polars type mapping
_POLARS_MAP: Final = {
    TypeGroup.NUMERIC: pl.Float64,
    TypeGroup.TEXT: pl.Utf8,
    TypeGroup.TEMPORAL: pl.Datetime,
    TypeGroup.BOOLEAN: pl.Boolean,
    TypeGroup.BINARY: pl.Binary,
    TypeGroup.OBJECT: pl.Utf8,  # JSON stored as string in Parquet
    TypeGroup.NULL: pl.Null,
}

DEFAULT_TYPE_GROUP: Final = TypeGroup.TEXT
DEFAULT_POLARS_TYPE: Final = pl.Utf8


class TypeResolver:
    """Resolves database-specific types to canonical Polars types."""

    @classmethod
    def resolve_to_group(cls, db_type: str, raw_type: str) -> TypeGroup:
        """Convert DB-specific type string to canonical TypeGroup."""
        base_type = cls._normalize_type(raw_type)
        db_key = db_type.lower()

        # Check database-specific overrides first
        if (provider_map := _PROVIDER_OVERRIDES.get(db_key)) and (
            mapped_group := provider_map.get(base_type)
        ):
            return mapped_group

        # Fall back to the consolidated global multi-string map
        return _GLOBAL_TYPE_MAPPING.get(base_type, DEFAULT_TYPE_GROUP)

    @classmethod
    def group_to_polars(cls, group: TypeGroup) -> Any:
        """Map TypeGroup to canonical Polars data type."""
        return _POLARS_MAP.get(group, DEFAULT_POLARS_TYPE)

    @classmethod
    def resolve_to_polars(cls, db_type: str, raw_type: str) -> pl.DataType:
        """Complete resolution: DB type -> Polars type."""
        group = cls.resolve_to_group(db_type, raw_type)
        return cls.group_to_polars(group)

    @classmethod
    def _normalize_type(cls, raw_type: str) -> str:
        """Strip parameters, nullability wrappers, and whitespace from type strings."""
        if not raw_type:
            return ""

        raw = raw_type.strip().lower()

        # 1. Unpack Nullable or LowCardinality wrappers commonly used in ClickHouse
        # e.g. "nullable(string)" -> "string"
        while raw.startswith("nullable(") or raw.startswith("lowcardinality("):
            raw = raw[raw.find("(") + 1 : -1].strip()

        # 2. Handle PostgreSQL array notation (_int4, _text)
        if raw.startswith("_"):
            return raw

        # 3. Remove parameters (e.g., "varchar(255)" -> "varchar")
        if "(" in raw:
            raw = raw.split("(", maxsplit=1)[0]

        return raw

    # Convenience methods
    @classmethod
    def is_numeric(cls, db_type: str, raw_type: str) -> bool:
        """Check if a type belongs to the NUMERIC group."""
        return cls.resolve_to_group(db_type, raw_type) == TypeGroup.NUMERIC

    @classmethod
    def is_text(cls, db_type: str, raw_type: str) -> bool:
        """Check if a type belongs to the TEXT group."""
        return cls.resolve_to_group(db_type, raw_type) == TypeGroup.TEXT
