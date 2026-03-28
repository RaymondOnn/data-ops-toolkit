from enum import Enum, auto
from typing import ClassVar

import polars as pl


class LogicalGroup(Enum):
    NUMERIC = auto()  # Ints, Decimals, Floats
    TEXT = auto()  # Strings, Enums
    TEMPORAL = auto()  # Dates, Times
    BOOLEAN = auto()  # Bits, Bools
    BINARY = auto()  # Bloads, Raw bytes (pl.Binary)
    OBJECT = auto()  # JSON, Lists, Maps (pl.Struct/pl.List)


# Mapping: Database Specific Type -> Canonical Type
POSTGRES_MAP = {
    "int4": LogicalGroup.NUMERIC,
    "int8": LogicalGroup.NUMERIC,
    "varchar": LogicalGroup.TEXT,
    "text": LogicalGroup.TEXT,
    "bool": LogicalGroup.BOOLEAN,
    "timestamp": LogicalGroup.TEMPORAL,
    "jsonb": LogicalGroup.OBJECT,
}

ORACLE_MAP = {
    "NUMBER": LogicalGroup.NUMERIC,  # Defaulting to BigInt for safety
    "BINARY_DOUBLE": LogicalGroup.NUMERIC,
    "VARCHAR2": LogicalGroup.TEXT,
    "CLOB": LogicalGroup.TEXT,
    "DATE": LogicalGroup.TEMPORAL,  # Oracle DATE includes time
    "TIMESTAMP": LogicalGroup.TEMPORAL,
    "RAW": LogicalGroup.BINARY,
}

# Clickhouse is very explicit about bit-width
CLICKHOUSE_MAP = {
    "Int32": LogicalGroup.NUMERIC,
    "Int64": LogicalGroup.NUMERIC,
    "Float64": LogicalGroup.NUMERIC,
    "String": LogicalGroup.TEXT,
    "FixedString": LogicalGroup.TEXT,
    "Bool": LogicalGroup.BOOLEAN,
    "Date": LogicalGroup.TEMPORAL,
    "DateTime": LogicalGroup.TEMPORAL,
    "UUID": LogicalGroup.TEXT,
}

# Mapping: Canonical Type -> Polars Type
POLARS_OUT_MAP = {
    LogicalGroup.NUMERIC: pl.Float64,  # Use Float64 if you have many Decimals
    LogicalGroup.TEXT: pl.Utf8,
    LogicalGroup.TEMPORAL: pl.Datetime,
    LogicalGroup.BOOLEAN: pl.Boolean,
    LogicalGroup.BINARY: pl.Binary,
    LogicalGroup.OBJECT: pl.Utf8,  # Safest to store JSON as String in Parquet
}


class TypeResolver:
    _REGISTRY: ClassVar[dict[str, dict[str, LogicalGroup]]] = {
        "postgres": POSTGRES_MAP,
        "oracle": ORACLE_MAP,
        "clickhouse": CLICKHOUSE_MAP,
    }

    _TO_POLARS = POLARS_OUT_MAP

    @classmethod
    def resolve(cls, source_system: str, db_type: str) -> pl.DataType:
        system_map = cls._REGISTRY.get(source_system.lower(), {})
        group = system_map.get(db_type)
        # Default to Utf8 if type is unknown
        return cls._TO_POLARS.get(group)
