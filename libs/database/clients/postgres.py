import logging
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from libs.clients.base import ClientCantConnect
from libs.database.clients.base import DBClient

if TYPE_CHECKING:
    from adbc_driver_postgresql.dbapi import Connection

LOG = logging.getLogger(__name__)


class PostgresClient(DBClient):
    def __init__(self, **config: Any) -> None:
        super().__init__(**config)

    @property
    def type(self) -> str:
        return "postgres"

    def connect(self) -> "Connection":
        # INLINE IMPORT: Prevents pickling the driver across the network
        import adbc_driver_postgresql.dbapi as adbc_pg

        try:
            db_name = self.config.get("database", self.config.get("db_name"))
            uri = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}/{db_name}"
            conn = adbc_pg.connect(uri)
            self._ping(conn)
            return conn
        except Exception as e:
            raise ClientCantConnect("Failed to connect to Postgres") from e

    def _ping(self, conn: "Connection") -> None:
        # We don't use self.sql() here to avoid recursive reconnect logic
        with conn.cursor() as cur:
            cur.execute("SELECT 1")

    def get_load_strategy(
        self,
        table_name: str,
        num_workers: int = 10,
        filter_sql: str | None = None,
    ) -> set[str]:
        # Physical partitioning using Postgres hidden ctid column
        filter_sql = filter_sql.replace("WHERE", "") if filter_sql else ""
        return {
            f"""
            SELECT * FROM {table_name} 
            WHERE {filter_sql} 
            AND abs(hashint4(ctid::text::hashint4)) % {num_workers} = {i}
            """
            for i in range(num_workers)
        }

    def copy_from_file(
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        def pg_stream(dataset, target_columns, audit_values):
            for batch in dataset.to_batches():
                # Inject constants
                for col, val in audit_values.items():
                    batch = batch.append_column(col, pa.array([val] * batch.num_rows))

                # KEY STEP: Reorder columns to match the DB schema exactly
                # This prevents "column mismatch" errors if Parquet order differs from DB
                yield batch.select(target_columns)

        audit_values = audit_values or {}
        dataset = ds.dataset(source_dir, format=file_ext.casefold())
        with self.get_connection() as conn, conn.cursor() as cur:
            db_schema = conn.adbc_get_table_schema("target_table")
            target_columns = db_schema.names

            # Stream the dataset to the table
            # 'append' ensures we don't drop existing data
            stream = pg_stream(dataset, target_columns, audit_values={})
            cur.adbc_ingest(table, stream, mode="append")

    def sql(self, query: str) -> list[Sequence[Any]]:
        """
        Executes raw SQL using the package driver.
        Used for commands and small metadata fetches.
        """
        with self.get_connection() as conn, conn.cursor() as cur:
            LOG.debug("Executing SQL query", extra={"query": query})
            cur.execute(query)
            rows = cur.fetchall()
            return [tuple(row) for row in rows]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        with self.get_connection() as conn, conn.cursor() as cursor:
            LOG.debug("Executing SQL query", extra={"query": query})
            cursor.execute(query)
            # ADBC native streaming to Arrow, then to Polars
            reader = cursor.fetch_record_batch()
            for batch in reader:
                # ADBC to Arrow to Polars is zero-copy and very fast
                df: pl.DataFrame = pl.DataFrame(pl.from_arrow(batch))
                if isinstance(df, pl.DataFrame):
                    yield df

    def reconnect(self) -> None:
        super().reconnect()

    def get_schema(self, fq_table: str) -> pl.DataFrame:
        schema, table_name = fq_table.split(".")
        query = f"""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                character_maximum_length AS data_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = '{schema}' 
            AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """
        return pl.concat(self.fetch_df(query), how="vertical")

    def exists(self, fq_table: str) -> bool:
        """Checks information_schema for table existence."""
        schema, table = fq_table.split(".") if "." in fq_table else ("public", fq_table)
        query = f"""
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name = '{table}'
        """
        return len(self.sql(query)) > 0
