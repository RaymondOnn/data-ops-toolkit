

from typing import Any, Generator, Sequence

from clickhouse_connect.driver.client import Client
import polars as pl
from libs.clients.database.base import DBClient

class ClickhouseClient(DBClient):
    def connect(self) -> Client:
        import clickhouse_connect
        from libs.clients.base import ClientCantConnect
        
        if self._connection:
            return self._connection
        
        try:
            self._connection = clickhouse_connect.get_client(
                host=str(self.config.get('host', 'localhost')),
                port=int(self.config.get('port', 8123)),
                username=str(self.config.get('user')),
                password=str(self.config.get('password'))
            )
            self._ping(self._connection)
        except Exception as e:
            raise ClientCantConnect(str(e))
        else:
            return self._connection

    def _ping(self, conn: Client) -> None:
        conn.ping()

    def get_load_strategy(self, table_name: str, num_partitions: int = 5) -> list[str]:
        return [
            f"SELECT * FROM {table_name} WHERE cityHash64(*) % {num_partitions} = {i}"
            for i in range(num_partitions)
        ]

    def sql(self, query: str) -> list[Sequence[Any]]:
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