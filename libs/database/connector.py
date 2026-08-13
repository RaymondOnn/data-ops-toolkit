import logging
from collections.abc import Generator
from typing import Any

import polars as pl
import pyarrow as pa

from libs.database.dtypes import TypeResolver

from .clients.factory import DatabaseFactory
from .sql.compile import Predicate, SQLCompiler
from .sql.operations import SQLOperation

LOG = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(self, dialect: str, **conn_kwargs: Any):
        self.dialect = dialect
        # 1. Instantiate the "Brain" (The SQL Compiler)
        self.sql = SQLCompiler(dialect=dialect)

        # 2. Instantiate the "Muscle" (The Database Client)
        # In a real engine, this would route to PgConnection, SnowflakeConnection, etc.
        db_type = str(conn_kwargs.pop("db_type"))
        self.db = DatabaseFactory.get(db_type=db_type, **conn_kwargs)

    def _connect(self):
        self.db.connect()

    def command(self, operation: str | SQLOperation, **ops_kwargs) -> None:
        """
        Compiles DDL and executes it directly on the database.
        """
        # Step 1: Use compiler to generate dialect-safe SQL
        if isinstance(operation, str):
            operation = SQLOperation(operation.casefold())

        sql = self._build(operation, **ops_kwargs)
        LOG.info(f"Executing DDL '{operation.value}' on target ({self.db.type})")
        self.db.command(sql)

    def query(
        self, operation: str | SQLOperation, **ops_kwargs
    ) -> Generator[Any, None, None]:
        """
        Compiles DDL and executes it directly on the database.
        """
        # Step 1: Use compiler to generate dialect-safe SQL
        if isinstance(operation, str):
            operation = SQLOperation(operation.casefold())

        sql = self._build(operation, **ops_kwargs)
        LOG.info(f"Executing query '{operation.value}' on target ({self.db.type})")
        LOG.debug(f"Executing query: {sql}")
        return self.db.query(sql)

    def _build(self, operation: str, **kwargs) -> str:
        return self.sql.compile(operation, **kwargs)

    def where(self, conditions: Predicate | list[Predicate] | None) -> str:
        return self.sql.compile_conditions(conditions)

    def select(
        self, table: str, fields: str | list[str] = "*", where_cond: str | None = None
    ) -> str:
        return self._build("select", table=table, fields=fields, where_cond=where_cond)

    def expr(self, expr_name, **kwargs) -> str:
        return self.sql.compile_expr(expr_name, **kwargs)

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, None, None]:
        """Runs query via client and streams native Polars DataFrames."""
        for batch in self.db.query(query):
            if isinstance(batch, pa.RecordBatch | pa.Table):
                df = pl.from_arrow(batch)
                if isinstance(df, pl.Series):
                    df = df.to_frame()
                yield df
            elif isinstance(batch, list):
                yield pl.DataFrame(batch)

    def get_schema(self, fq_table: str) -> pl.DataFrame:
        """Compiles metadata query, executes via client, and builds Polars schema map."""
        sql = self._build("columns", fq_table=fq_table)

        # 1. Collect batches safely
        dfs = list(self.fetch_df(sql))
        if not dfs:
            return pl.DataFrame()

        df = pl.concat(dfs, how="vertical")

        # 2. Normalize column names to lowercase to prevent casing mismatches
        df = df.rename({col: col.lower() for col in df.columns})
        if "data_type" not in df.columns or df.is_empty():
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
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        """Delegates file bulk load to client's copy method."""
        self.db.copy(table, source_dir, file_ext=file_ext, audit_values=audit_values)

    # def load_data(
    #     self,
    #     table_name: str,
    #     schema_columns: dict[str, str],
    #     rows: list[dict[str, Any]],
    #     primary_key: str,
    # ):
    #     """
    #     Facade Method: Compiles write statements and streams data payload.
    #     """
    #     # Step 1: Compile the parameterized upsert statement
    #     sql = self._build_upsert(table_name, schema_columns, primary_key)
    #     LOG.info("Executing dynamic UPSERT statement.")

    #     # Step 2: Process transactional writes
    #     cursor = self.db.cursor()
    #     try:
    #         # We transform standard dictionary keys to match parameterized queries (e.g. :id)
    #         cursor.executemany(sql, rows)
    #         self.db.commit()
    #         LOG.info(f"Successfully committed {len(rows)} rows to {table_name}.")
    #     except Exception as e:
    #         self.db.rollback()
    #         raise RuntimeError(f"Data load transaction aborted: {e}") from e
    #     finally:
    #         cursor.close()

    def close(self):
        self.db.close()
