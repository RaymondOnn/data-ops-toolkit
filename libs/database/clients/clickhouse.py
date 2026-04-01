import logging
from collections.abc import Generator, Sequence
from typing import Any

import polars as pl
from clickhouse_connect.driver.client import Client
from libs.database.clients.base import DBClient

LOG = logging.getLogger(__name__)

class ClickhouseClient(DBClient):
    def connect(self) -> Client:
        # Import inside so that Ray workers can import
        import clickhouse_connect
        from libs.clients.base import ClientCantConnect

        if self._connection:
            return self._connection

        try:
            self._connection: Client = clickhouse_connect.get_client(
                host=str(self.config.get("host", "localhost")),
                port=int(self.config.get("port", 8123)),
                username=str(self.config.get("user")),
                password=str(self.config.get("password")),
            )
            self._ping(self._connection)
        except Exception as e:
            raise ClientCantConnect("Failed to connect to ClickHouse") from e
        else:
            return self._connection

    def _ping(self, conn: Client) -> None:
        conn.ping()

    def get_load_strategy(
        self,
        table_name: str,
        num_partitions: int = 5,
        filter_sql: str | None = None,
    ) -> set[str]:
        filter_sql = filter_sql.replace("WHERE", "") if filter_sql else ""
        return {
            f"""
            SELECT * FROM {table_name} 
            WHERE {filter_sql} 
            AND cityHash64(*) % {num_partitions} = {i}
            """
            for i in range(num_partitions)
        }

    def sql(self, query: str) -> list[Sequence[Any]]:
        # Returns a list of tuples by default
        LOG.debug("Executing SQL query", extra={"query": query})
        result = self.connect().query(query)
        return list(result.result_rows)

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # clickhouse-connect supports native DataFrame streaming
        LOG.debug("Executing SQL query", extra={"query": query})
        result = self.connect().query_df_stream(
            query, settings={"max_block_size": 100_000}
        )
        with result:
            for pandas_df in result:
                yield pl.from_pandas(pandas_df)

    def write_table(self, lf: pl.LazyFrame, table_name: str) -> None:
        """
        Uses ClickHouse native client to insert data in optimized blocks.
        """
        # ClickHouse drivers are highly optimized for Polars/Pandas structures.
        # We stream the data to the insert method.
        df = lf.collect()

        self.connection.insert_df(table=table_name, df=df)
