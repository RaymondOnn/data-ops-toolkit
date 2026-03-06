

from typing import Any, Generator

import clickhouse_connect
import polars as pl
from libs.clients.database.base import DBClient

class ClickhouseClient(DBClient):
    def connect(self) -> clickhouse_connect.Client:
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

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # clickhouse-connect supports native DataFrame streaming
        result = self.connect().query_df_stream(
            query, settings={'max_block_size': 100_000}
        )
        with result:
            for pandas_df in result:
                yield pl.from_pandas(pandas_df)