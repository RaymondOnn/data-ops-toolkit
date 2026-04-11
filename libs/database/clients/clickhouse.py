import logging
from collections.abc import Generator, Sequence
from typing import Any

import polars as pl
from clickhouse_connect.driver.client import Client

from ..pool.queue import QueueConnectionPool
from .base import DBClient

LOG = logging.getLogger(__name__)


class ClickhouseClient(DBClient):
    def __init__(self, **config: Any):
        super().__init__(**config)

    @property
    def type(self) -> str:
        return "clickhouse"

    def _init_pool(self) -> ConnectionPool:
        pool_size = self.config.get("pool_size", 0)
        if pool_size > 1:
            LOG.info("Initializing ClickHouse Queue Pool", extra={"size": pool_size})
            return QueueConnectionPool(connector=self.connect, size=pool_size)
        return super()._init_pool()

    def connect(self) -> Client:
        # Import inside so that Ray workers can import
        import clickhouse_connect

        from libs.clients.base import ClientCantConnect

        try:
            conn = clickhouse_connect.get_client(
                host=str(self.config.get("host", "localhost")),
                port=int(self.config.get("port", 8123)),
                username=str(self.config.get("user")),
                password=str(self.config.get("password")),
            )
            self._ping(conn)
            return conn
        except Exception as e:
            raise ClientCantConnect("Failed to connect to ClickHouse") from e

    def _ping(self, conn: Client) -> None:
        conn.ping()

    def get_load_strategy(
        self,
        table_name: str,
        num_workers: int = 5,
        filter_sql: str | None = None,
    ) -> set[str]:
        filter_sql = filter_sql.replace("WHERE", "") if filter_sql else ""
        return {
            f"""
            SELECT * FROM {table_name} 
            WHERE {filter_sql} 
            AND cityHash64(*) % {num_workers} = {i}
            """
            for i in range(num_workers)
        }

    def sql(self, query: str) -> list[Sequence[Any]]:
        # Returns a list of tuples by default
        LOG.debug("Executing SQL query", extra={"query": query})
        with self.get_connection() as conn:
            result = conn.query(query)
            return list(result.result_rows)

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # clickhouse-connect supports native DataFrame streaming
        LOG.debug("Executing SQL query", extra={"query": query})
        with self.get_connection() as conn:
            result = conn.query_df_stream(query, settings={"max_block_size": 100_000})
            with result:
                for pandas_df in result:
                    yield pl.from_pandas(pandas_df)

    # def write_table(self, lf: pl.LazyFrame, table_name: str) -> None:
    #     """
    #     Uses ClickHouse native client to insert data in optimized blocks.
    #     """
    #     # ClickHouse drivers are highly optimized for Polars/Pandas structures.
    #     # We stream the data to the insert method.
    #     df = lf.collect()

    #     self.connection.insert_df(table=table_name, df=df)

    def get_schema(self, fq_table: str) -> pl.DataFrame:
        # ClickHouse has a system.columns table we can query for schema info
        database, table_name = fq_table.split(".")
        query = f"""
            SELECT 
                name AS column_name, 
                type AS data_type, 
                is_in_primary_key,
                -- ClickHouse doesn't use precision/scale for all types, 
                -- but it's available for Decimal types
                numeric_precision,
                numeric_scale
            FROM system.columns
            WHERE database = '{database}' 
            AND table = '{table_name}'
            ORDER BY position;
        """
        return pl.concat(self.fetch_df(query), how="vertical")
