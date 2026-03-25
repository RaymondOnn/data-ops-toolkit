from collections.abc import Generator, Sequence
from typing import Any

import chdb
import polars as pl
from chdb import dbapi
from libs.database.clients.base import DBClient


class ChDBClient(DBClient):
    def connect(self) -> Any:
        """
        chDB is embedded, so 'connecting' just means pointing to 
        a persistence directory.
        If 'path' is not provided, it runs in-memory (ephemeral).
        """
        if self._connection:
            return self._connection

        # chDB DBAPI connect takes a 'path' argument for persistence
        db_path = self.config.get("path", "./.chdb_data")
        self._connection = dbapi.connect(path=db_path)
        self._ping(self._connection)
        return self._connection

    def _ping(self, conn: Any) -> None:
        """Pings the chDB engine to ensure it's responsive."""
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")

    def get_load_strategy(
        self,
        table_name: str,
        num_partitions: int = 10,
        filter_sql: str | None = None,
    ) -> list[str]:
        """
        Uses cityHash64 to create N virtual partitions for parallel reads,
        similar to ClickHouse.
        """
        base_query = f"SELECT * FROM {table_name}"
        # Clean up the filter to be safely combined
        filter_clause = filter_sql.replace("WHERE", "").strip() if filter_sql else ""

        return [
            f"{base_query} WHERE {' AND '.join(
                filter(
                    None, 
                    [filter_clause, f'cityHash64(*) % {num_partitions} = {i}']
                )
            )}"
            for i in range(num_partitions)
        ]

    def sql(self, query: str) -> list[Sequence[Any]]:
        """Executes SQL using the DBAPI cursor."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        # chDB returns a list of tuples
        return cursor.fetchall()

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """
        Uses chdb.query(..., 'Arrow') for zero-copy transfer to Polars.
        """
        db_path = self.config.get("path", "./.chdb_data")
        # Low-level chdb.query is often faster for bulk retrieval than DBAPI
        res = chdb.query(query, "Arrow", path=db_path)
        if res is not None:
            df = pl.from_arrow(res)
            if isinstance(df, pl.DataFrame):
                yield df

    def write_table(self, lf: pl.LazyFrame, table_name: str) -> None:
        """
        chDB does not support direct dataframe insertion via protocol.
        Data should be staged as Parquet and inserted via SQL (INSERT FROM file).
        """
        raise NotImplementedError("Use stage_data() pattern for chDB")
