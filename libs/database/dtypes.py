from enum import StrEnum
from functools import lru_cache
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
_TYPE_REGISTRY: Final = {
    "postgres": {
        "int4": TypeGroup.NUMERIC,
        "int8": TypeGroup.NUMERIC,
        "varchar": TypeGroup.TEXT,
        "text": TypeGroup.TEXT,
        "bool": TypeGroup.BOOLEAN,
        "timestamp": TypeGroup.TEMPORAL,
        "timestamptz": TypeGroup.TEMPORAL,
        "date": TypeGroup.TEMPORAL,
        "jsonb": TypeGroup.OBJECT,
        "bytea": TypeGroup.BINARY,
        "_int4": TypeGroup.NUMERIC,  # Array types
        "_text": TypeGroup.TEXT,
    },
    "oracle": {
        "number": TypeGroup.NUMERIC,
        "binary_double": TypeGroup.NUMERIC,
        "varchar2": TypeGroup.TEXT,
        "nvarchar2": TypeGroup.TEXT,
        "clob": TypeGroup.TEXT,
        "date": TypeGroup.TEMPORAL,
        "timestamp": TypeGroup.TEMPORAL,
        "raw": TypeGroup.BINARY,
    },
    "clickhouse": {
        "int32": TypeGroup.NUMERIC,
        "int64": TypeGroup.NUMERIC,
        "float64": TypeGroup.NUMERIC,
        "string": TypeGroup.TEXT,
        "fixedstring": TypeGroup.TEXT,
        "bool": TypeGroup.BOOLEAN,
        "date": TypeGroup.TEMPORAL,
        "datetime": TypeGroup.TEMPORAL,
        "uuid": TypeGroup.TEXT,
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
    @lru_cache(maxsize=1000)
    def resolve_to_group(cls, db_type: str, raw_type: str) -> TypeGroup:
        """Convert DB-specific type string to canonical TypeGroup."""
        base_type = cls._normalize_type(raw_type)
        db_key = db_type.lower()

        provider_map = _TYPE_REGISTRY.get(db_key)
        if not provider_map:
            return DEFAULT_TYPE_GROUP

        return provider_map.get(base_type, DEFAULT_TYPE_GROUP)

    @classmethod
    @lru_cache(maxsize=100)
    def group_to_polars(cls, group: TypeGroup) -> Any:
        """Map TypeGroup to canonical Polars data type."""
        return _POLARS_MAP.get(group, DEFAULT_POLARS_TYPE)

    @classmethod
    @lru_cache(maxsize=1000)
    def resolve_to_polars(cls, db_type: str, raw_type: str) -> pl.DataType:
        """Complete resolution: DB type -> Polars type."""
        group = cls.resolve_to_group(db_type, raw_type)
        return cls.group_to_polars(group)

    @classmethod
    def _normalize_type(cls, raw_type: str) -> str:
        """Normalize type string (handle arrays, parameters, case)."""
        if not raw_type:
            return ""

        raw = raw_type.strip().lower()

        # Handle PostgreSQL array notation (_int4, _text)
        if raw.startswith("_"):
            return raw

        # Remove parameters (varchar(255) -> varchar)
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

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all LRU caches (useful for testing)."""
        cls.resolve_to_group.cache_clear()
        cls.group_to_polars.cache_clear()
        cls.resolve_to_polars.cache_clear()
