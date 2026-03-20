from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any

import polars as pl

from libs.clients.base import ClientCantConnect
from libs.clients.database.base import DBClient

if TYPE_CHECKING:
    from adbc_driver_postgresql.dbapi import Connection


class PostgresClient(DBClient):
    def __init__(self, **config: Any) -> None:
        super().__init__(**config)

    def connect(self) -> "Connection":
        # INLINE IMPORT: Prevents pickling the driver across the network
        import adbc_driver_postgresql.dbapi as adbc_pg

        if not self._connection:
            try:
                # Support both 'database' and 'db_name' for backward compatibility
                db_name = self.config.get("database", self.config.get("db_name"))
                self.uri = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}/{db_name}"
                self._connection = adbc_pg.connect(self.uri)
                self._ping(self._connection)
            except Exception as e:
                raise ClientCantConnect("Failed to connect to Postgres") from e
        return self._connection

    def _ping(self, conn: "Connection") -> None:
        # We don't use self.sql() here to avoid recursive reconnect logic
        with conn.cursor() as cur:
            cur.execute("SELECT 1")

    def get_load_strategy(
        self,
        table_name: str,
        num_partitions: int = 10,
        filter_sql: str | None = None,
    ) -> list[str]:
        # Physical partitioning using Postgres hidden ctid column
        filter_sql = filter_sql.replace("WHERE", "") if filter_sql else ""
        return [
            f"""
            SELECT * FROM {table_name} 
            WHERE {filter_sql} 
            AND abs(hashint4(ctid::text::hashint4)) % {num_partitions} = {i}
            """
            for i in range(num_partitions)
        ]

    def sql(self, query: str) -> list[Sequence[Any]]:
        """
        Executes raw SQL using the package driver.
        Used for commands and small metadata fetches.
        """
        with self.connect().cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            return [tuple(row) for row in rows]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, None, None]:
        with self.connect().cursor() as cursor:
            cursor.execute(query)
            # ADBC native streaming to Arrow, then to Pandas
            reader = cursor.fetch_record_batch()
            for batch in reader:
                # ADBC to Arrow to Polars is zero-copy and very fast
                yield pl.from_arrow(batch)

    def reconnect(self) -> None:
        super().reconnect()

    def write_table(self, lf: pl.LazyFrame, table_name: str) -> None:
        try:
            # We .collect() here, but because we use engine='adbc',
            # it streams the results rather than buffering everything if
            # the driver supports it, or handles the handoff in Arrow chunks.
            lf.collect().write_database(
                table_name=table_name,
                connection=self.connection,
                engine="adbc",
                if_table_exists="append",
            )
        except Exception as e:
            # If the network drops mid-50M-row-stream, try one reconnect
            print(f"Postgres Write failed: {e}. Attempting reconnect...")
            self.reconnect()
            lf.collect().write_database(
                table_name=table_name,
                connection=self.connection,
                engine="adbc",
                if_table_exists="append",
            )
