from enum import Enum

import polars as pl


class TypeGroup(Enum):
    NUMERIC = "NUMERIC"  # Ints, Decimals, Floats
    TEXT = "TEXT"  # Strings, Enums
    TEMPORAL = "TEMPORAL"  # Dates, Times
    BOOLEAN = "BOOLEAN"  # Bits, Bools
    BINARY = "BINARY"  # Bloads, Raw bytes (pl.Binary)
    OBJECT = "OBJECT"  # JSON, Lists, Maps (pl.Struct/pl.List)
    NULL = "NULL"  # Special group for NULL-only columns


# Mapping: Database Specific Type -> Canonical Type
POSTGRES_MAP = {
    "int4": TypeGroup.NUMERIC,
    "int8": TypeGroup.NUMERIC,
    "varchar": TypeGroup.TEXT,
    "text": TypeGroup.TEXT,
    "bool": TypeGroup.BOOLEAN,
    "timestamp": TypeGroup.TEMPORAL,
    "jsonb": TypeGroup.OBJECT,
}

ORACLE_MAP = {
    "NUMBER": TypeGroup.NUMERIC,  # Defaulting to BigInt for safety
    "BINARY_DOUBLE": TypeGroup.NUMERIC,
    "VARCHAR2": TypeGroup.TEXT,
    "CLOB": TypeGroup.TEXT,
    "DATE": TypeGroup.TEMPORAL,  # Oracle DATE includes time
    "TIMESTAMP": TypeGroup.TEMPORAL,
    "RAW": TypeGroup.BINARY,
}

# Clickhouse is very explicit about bit-width
CLICKHOUSE_MAP = {
    "Int32": TypeGroup.NUMERIC,
    "Int64": TypeGroup.NUMERIC,
    "Float64": TypeGroup.NUMERIC,
    "String": TypeGroup.TEXT,
    "FixedString": TypeGroup.TEXT,
    "Bool": TypeGroup.BOOLEAN,
    "Date": TypeGroup.TEMPORAL,
    "DateTime": TypeGroup.TEMPORAL,
    "UUID": TypeGroup.TEXT,
}

# Mapping: Canonical Type -> Polars Type
POLARS_OUT_MAP = {
    TypeGroup.NUMERIC: pl.Float64,  # Use Float64 if you have many Decimals
    TypeGroup.TEXT: pl.Utf8,
    TypeGroup.TEMPORAL: pl.Datetime,
    TypeGroup.BOOLEAN: pl.Boolean,
    TypeGroup.BINARY: pl.Binary,
    TypeGroup.OBJECT: pl.Utf8,  # Safest to store JSON as String in Parquet
}


class TypeResolver:
    # Registries as defined in your dtypes.py
    _MAPS = {
        "postgres": POSTGRES_MAP,
        "oracle": ORACLE_MAP,
        "clickhouse": CLICKHOUSE_MAP,
    }

    @classmethod
    def resolve_to_group(cls, provider: str, raw_type: str) -> TypeGroup:
        """
        Factory method to convert a DB-specific string to a TypeGroup.
        """
        # Clean the input (e.g., 'varchar(255)' -> 'varchar')
        base_type = raw_type.split("(", maxsplit=1)[0].lower().strip()

        provider_map = cls._MAPS.get(provider.lower())
        if not provider_map:
            return TypeGroup.TEXT  # Default fallback

        return provider_map.get(base_type, TypeGroup.TEXT)

    @classmethod
    def group_to_polars(cls, group: TypeGroup) -> pl.DataType:
        """
        Maps a LogicalGroup to the canonical Polars type for normalization.
        """
        return POLARS_OUT_MAP.get(group, pl.Utf8)
