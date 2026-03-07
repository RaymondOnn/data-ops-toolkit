from typing import TYPE_CHECKING, Any, Generator

import polars as pl

from libs.clients.database.base import DBClient

if TYPE_CHECKING:
    from adbc_driver_postgresql.dbapi import Connection

class PostgresClient(DBClient):
    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
    
    def connect(self) -> Connection:
        # INLINE IMPORT: Prevents pickling the driver across the network
        import adbc_driver_postgresql.dbapi as adbc_pg
        
        if not self._connection:
            uri = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}/{self.config['database']}"
            self._connection = adbc_pg.connect(uri)
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

    def sql(self, query: str) -> list[tuple[Any, ...]]:
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
            reader = cursor.fetch_record_batch_reader()
            for batch in reader:
                # ADBC to Arrow to Polars is zero-copy and very fast
                yield pl.from_arrow(batch)
