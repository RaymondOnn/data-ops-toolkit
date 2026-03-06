from typing import Any, Generator


import oracledb
import polars as pl

from libs.clients.database.base import DBClient

class OracleClient(DBClient):
    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
    
    def connect(self) -> oracledb.Connection:
        # Thin mode: no instant client required
        if not self._connection:
            self._connection = oracledb.connect(
                user=self.config['user'],
                password=self.config['password'],
                dsn=self.config['dsn']
            )
        return self._connection
            
    def get_load_strategy(self, table_name: str, partitions: int = 10) -> list[str]:
        """
        Uses ORA_HASH to create N virtual partitions without needing a PK.
        """
        queries = []
        for i in range(partitions):
            # ORA_HASH(rowid, N) creates N buckets based on physical location
            sql = f"""
                SELECT * FROM {table_name} 
                WHERE ORA_HASH(rowid, {partitions-1}) = {i}
            """
            queries.append(sql)
        return queries

    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Fetched concurrently by Ray, but limited by the Manager's Session Lock."""
        # Note: In your specific case, we yield chunks to stay under 2GB
        cursor = self.connect().cursor()
        try:
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(50_000)
                if not rows: 
                    break
                yield pl.from_dicts(
                    [dict(
                        zip([c[0] for c in cursor.description], r)
                    ) for r in rows]
                )
        except Exception as e:
            # If connection died, reset to None so next call reconnects
            self._connection = None
            raise e