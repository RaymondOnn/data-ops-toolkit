import logging
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any

import polars as pl
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
                df: pl.DataFrame = pl.from_arrow(batch)
                if isinstance(df, pl.DataFrame):
                    yield df

    def reconnect(self) -> None:
        super().reconnect()

    # def write_table(self, lf: pl.LazyFrame, table_name: str) -> None:
    #     try:
    #         # We .collect() here, but because we use engine='adbc',
    #         # it streams the results rather than buffering everything if
    #         # the driver supports it, or handles the handoff in Arrow chunks.
    #         lf.collect().write_database(
    #             table_name=table_name,
    #             connection=self.connection,
    #             engine="adbc",
    #             if_table_exists="append",
    #         )
    #     except Exception as e:
    #         # If the network drops mid-50M-row-stream, try one reconnect
    #         print(f"Postgres Write failed: {e}. Attempting reconnect...")
    #         self.reconnect()
    #         lf.collect().write_database(
    #             table_name=table_name,
    #             connection=self.connection,
    #             engine="adbc",
    #             if_table_exists="append",
    #         )

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
