

from typing import Any, Generator

from clickhouse_connect import Client
import polars as pl
from libs.clients.database.base import DBClient

class ClickhouseClient(DBClient):
    def connect(self) -> Client:
        import clickhouse_connect
        
        if not self._connection:
            self._connection = clickhouse_connect.get_client(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 8123),
                username=self.config.get('user'),
                password=self.config.get('password')
            )
        return self._connection

    def get_load_strategy(self, table_name: str, partitions: int = 5) -> list[str]:
        return [
            f"SELECT * FROM {table_name} WHERE cityHash64(*) % {partitions} = {i}"
            for i in range(partitions)
        ]

    def sql(self, query: str) -> list[tuple[Any, ...]]:
        # Returns a list of tuples by default
        result = self.connect().query(query)
        return list(result.result_rows)
    
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # clickhouse-connect supports native DataFrame streaming
        result = self.connect().query_df_stream(
            query, settings={'max_block_size': 100_000}
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
        
        self.connection.insert_df(
            table=table_name,
            df=df
        )