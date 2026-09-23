import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa

from libs.database.dtypes import TypeResolver

from .clients import DBClient
from .sql.compile import Predicate, SQLCompiler
from .sql.operations import SQLOperationType

LOG = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(self, dialect: str, **conn_kwargs: Any):
        self.dialect = dialect
        # 1. Instantiate the "Brain" (The SQL Compiler)
        self.sql = SQLCompiler(dialect=dialect)

        # 2. Instantiate the "Muscle" (The Database Client)
        # In a real engine, this would route to PgConnection, SnowflakeConnection, etc.
        db_type = str(conn_kwargs.pop("db_type"))
        self.db = DBClient.create(key=db_type, **conn_kwargs)

    def _connect(self):
        self.db.connect()

    # -------------------------------------------------------------------------
    # 1. Execution Engine (Only accepts SQL strings)
    # -------------------------------------------------------------------------

    def query(self, sql: str) -> Generator[Any, None, None]:
        """Executes any SQL query string and streams result batches."""
        LOG.debug(f"Executing query: {sql}")
        return self.db.query(sql)

    def fetch_df(self, sql: str) -> Generator[pl.DataFrame, None, None]:
        """Executes a SQL query string and streams Polars DataFrames."""
        for batch in self.query(sql):
            if isinstance(batch, pa.RecordBatch | pa.Table):
                df = pl.from_arrow(batch)
                yield df.to_frame() if isinstance(df, pl.Series) else df
            elif isinstance(batch, list):
                yield pl.DataFrame(batch)

    def command(self, sql: str) -> None:
        """Executes any DDL/DML statement string."""
        LOG.info(f"Executing DDL/DML on target ({self.db.db_type})")
        LOG.debug(f"Executing statement: {sql}")
        self.db.command(sql)

    def get_schema(self, fq_table: str) -> pl.DataFrame:
        """Compiles metadata query, executes via client, and builds Polars schema map."""
        sql = self.build_sql("columns", fq_table=fq_table)

        # 1. Collect batches safely
        dfs = list(self.fetch_df(sql))
        if not dfs:
            return pl.DataFrame()

        df = pl.concat(dfs, how="vertical")

        # 2. Normalize column names to lowercase to prevent casing mismatches
        df = df.rename({col: col.lower() for col in df.columns})
        if "data_type" not in df.columns or df.is_empty():
            if "data_type" not in df.columns:
                LOG.debug(f"Column 'data_type' not found: {df.columns}")
            return df

        # Standardize data types using TypeResolver
        return df.with_columns(
            pl.struct(["data_type"])
            .map_elements(
                lambda row: str(
                    TypeResolver.resolve_to_polars(self.dialect, row["data_type"])
                ),
                return_dtype=pl.Utf8,
            )
            .alias("canonical_type")
        )

    def copy_from_file(
        self, table: str, source_dir: str, file_format: str = "parquet", **kwargs
    ) -> None:
        """
        Multi-stage staging bulk load into ClickHouse:
        2. Streams binary Parquet files into the staging table via raw_insert().
        """

        if not isinstance(source_dir, list):
            sources = [source_dir]

        files: list[str] = []
        for src in sources:
            p = Path(src)
            if p.is_dir():
                files.extend(str(f) for f in p.glob(f"*.{file_format.lower()}"))
            elif p.exists():
                files.append(str(p))

        if not files:
            LOG.warning(f"No {file_format} files found across sources: {sources}")
            return

        self.db.copy(table=table, filepaths=files, file_format=file_format, **kwargs)

    # -------------------------------------------------------------------------
    # 2. SQL Builders (Return SQL strings)
    # -------------------------------------------------------------------------

    def build_sql(self, operation: str | SQLOperationType, **kwargs) -> str:
        """Explicit entry point to compile SQL operations via SQLCompiler."""
        if isinstance(operation, SQLOperationType):
            operation = operation.value
        return self.sql.compile(operation, **kwargs)

    def select(
        self, table: str, fields: str | list[str] = "*", where_cond: str | None = None
    ) -> str:
        return self.build_sql(
            "select", table=table, fields=fields, where_cond=where_cond
        )

    def where(self, conditions: Predicate | list[Predicate] | None) -> str:
        return self.sql.compile_conditions(conditions)

    def expr(self, expr_name, **kwargs) -> str:
        return self.sql.compile_expr(expr_name, **kwargs)

    def close(self):
        self.db.close()
