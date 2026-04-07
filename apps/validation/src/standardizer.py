from abc import ABC, abstractmethod

import polars as pl


class DialectStrategy(ABC):
    """Strategy interface for generating standardized SQL across DBs."""

    @abstractmethod
    def cast_bool(self, col: str) -> str:
        pass

    @abstractmethod
    def cast_numeric(self, col: str) -> str:
        pass

    @abstractmethod
    def cast_temporal(self, col: str) -> str:
        pass

    @abstractmethod
    def cast_string(self, col: str) -> str:
        pass


class PostgresStrategy(DialectStrategy):
    def cast_bool(self, col: str):
        return f"CASE WHEN {col} THEN '1' ELSE '0' END"

    def cast_numeric(self, col: str):
        return f"TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST({col}::DECIMAL(38,6) AS VARCHAR)))"

    def cast_temporal(self, col: str):
        return f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS.MS')"

    def cast_string(self, col: str):
        return f"UPPER(TRIM(CAST({col} AS VARCHAR)))"


class MSSQLStrategy(DialectStrategy):
    def cast_bool(self, col: str):
        return f"CAST(CAST({col} AS INT) AS VARCHAR)"

    def cast_numeric(self, col: str):
        # MSSQL STR function helps prevent scientific notation
        return f"UPPER(TRIM(STR({col}, 38, 6)))"

    def cast_temporal(self, col: str):
        return f"CONVERT(VARCHAR, {col}, 121)"

    def cast_string(self, col: str):
        return f"UPPER(TRIM(CAST({col} AS VARCHAR(MAX))))"


class SnowflakeStrategy(DialectStrategy):
    def cast_bool(self, col: str):
        return f"CASE WHEN {col} THEN '1' ELSE '0' END"

    def cast_numeric(self, col: str):
        return f"TO_VARCHAR({col}, '99999999999999999999999999999999.999999')"

    def cast_temporal(self, col: str):
        return f"TO_VARCHAR({col}, 'YYYY-MM-DD HH24:MI:SS.FF3')"

    def cast_string(self, col: str):
        return f"UPPER(TRIM(CAST({col} AS VARCHAR)))"


class ValidationStandardizer:
    """Orchestrates the selection and application of Dialect Strategies."""

    _STRATEGIES = {
        "postgres": PostgresStrategy(),
        "mssql": MSSQLStrategy(),
        "snowflake": SnowflakeStrategy(),
        "ansi": PostgresStrategy(),  # Default fallback
    }

    @classmethod
    def get_standardized_sql(
        cls, schema: pl.Schema, dialect: str, table_name: str
    ) -> str:
        strategy = cls._STRATEGIES.get(dialect.lower(), cls._STRATEGIES["ansi"])
        cols = []

        for name, dtype in schema.items():
            if dtype == pl.Boolean:
                expr = strategy.cast_bool(name)
            elif dtype.is_numeric():
                expr = strategy.cast_numeric(name)
            elif dtype.is_temporal():
                expr = strategy.cast_temporal(name)
            else:
                expr = strategy.cast_string(name)

            cols.append(f"COALESCE({expr}, '__NULL__') AS {name}")

        return f"SELECT {', '.join(cols)} FROM {table_name}"

    @staticmethod
    def apply_polars_standardization(lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Equivalent logic for non-SQL sources (Parquet, API results).
        Normalizes everything to String for byte-level hash parity.
        """
        exprs = []
        for name, dtype in lf.schema.items():
            if dtype == pl.Boolean:
                e = pl.when(pl.col(name)).then(pl.lit("1")).otherwise(pl.lit("0"))
            elif dtype.is_numeric():
                # Round to 6 decimal places and cast to string to fix representation
                e = pl.col(name).round(6).cast(pl.String)
            elif dtype.is_temporal():
                e = pl.col(name).dt.to_string("%Y-%m-%d %H:%M:%S.%3f")
            else:
                e = pl.col(name).cast(pl.String).str.strip_chars().str.to_uppercase()

            exprs.append(pl.coalesce(e, pl.lit("__NULL__")).alias(name))

        return lf.with_columns(exprs)
        return lf.with_columns(exprs)
