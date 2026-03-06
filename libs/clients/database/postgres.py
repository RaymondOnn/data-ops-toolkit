from typing import Any, Generator

import polars as pl
import adbc_driver_postgresql.dbapi as adbc_pg

from libs.clients.database.base import DBClient

class PostgresClient(DBClient):
    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
    
    def connect(self) -> adbc_pg.Connection:
        if not self._connection:
            self._connection = adbc_pg.connect(self.config['uri'])
        return self._connection

    def get_load_strategy(self, table_name: str, partitions: int = 10) -> list[str]:
        # Physical partitioning using Postgres hidden ctid column
        return [
            f"""
            SELECT * FROM {table_name} 
            WHERE abs(hashint4(ctid::text::hashint4)) % {partitions} = {i}
            """
            for i in range(partitions)
        ]

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, None, None]:
        with self.connect().cursor() as cursor:
            cursor.execute(query)
            # ADBC native streaming to Arrow, then to Pandas
            reader = cursor.fetch_record_batch_reader()
            for batch in reader:
                # ADBC to Arrow to Polars is zero-copy and very fast
                yield pl.from_arrow(batch)
