from enum import StrEnum
from typing import Any, Final

import polars as pl

from libs.database.sql import Dialect, DialectTemplate


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

GENERIC_TO_TYPE_GROUP: Final[dict[str, TypeGroup]] = {
    "string": TypeGroup.TEXT,
    "integer": TypeGroup.NUMERIC,
    "decimal": TypeGroup.NUMERIC,
    "float": TypeGroup.NUMERIC,
    "boolean": TypeGroup.BOOLEAN,
    "date": TypeGroup.TEMPORAL,
    "time": TypeGroup.TEMPORAL,
    "datetime": TypeGroup.TEMPORAL,
    "json": TypeGroup.OBJECT,
    "binary": TypeGroup.BINARY,
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
    def db_to_generic_type(cls, dialect: str | Dialect, raw_type: str) -> str:
        """Looks up the dialect's native_type_map from the YAML template."""
        dialect_enum = (
            Dialect(dialect.casefold()) if isinstance(dialect, str) else dialect
        )
        template = DialectTemplate.build(dialect_enum)

        cleaned_type = cls._normalize_type(raw_type)

        # 1. Check template native_type_map (e.g. "character varying" -> "string")
        generic_type = template.native_type_map.get(cleaned_type)
        if generic_type:
            return generic_type

        # 2. Fallback prefix check (e.g. "varchar(255)" -> "varchar")
        base_type = cleaned_type.split("(")[0].strip()
        return template.native_type_map.get(base_type, "string")

    @classmethod
    def resolve_to_group(cls, dialect: str | Dialect, raw_type: str) -> TypeGroup:
        """DB type -> Generic Type -> TypeGroup"""
        generic_type = cls.db_to_generic_type(dialect, raw_type)
        return GENERIC_TO_TYPE_GROUP.get(generic_type, TypeGroup.TEXT)

    @classmethod
    def resolve_to_polars(cls, dialect: str | Dialect, raw_type: str) -> pl.DataType:
        """DB type -> Generic Type -> TypeGroup -> Polars DataType"""
        group = cls.resolve_to_group(dialect, raw_type)
        return _POLARS_MAP.get(group, pl.Utf8)

    @classmethod
    def group_to_polars(cls, group: TypeGroup) -> Any:
        """Map TypeGroup to canonical Polars data type."""
        return _POLARS_MAP.get(group, DEFAULT_POLARS_TYPE)

    @classmethod
    def _normalize_type(cls, raw_type: str) -> str:
        if not raw_type:
            return ""

        raw = raw_type.strip().lower()

        # Unpack ClickHouse Nullable(...) or LowCardinality(...)
        while raw.startswith("nullable(") or raw.startswith("lowcardinality("):
            raw = raw[raw.find("(") + 1 : -1].strip()

        # Strip standard parameter lengths like varchar(255) if exact match fails
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
