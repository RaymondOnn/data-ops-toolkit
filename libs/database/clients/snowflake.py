import polars as pl
from apps.ingestion.src.core.db.base import DBClient


class SnowflakeADBCClient(DBClient):
    """Snowflake client using ADBC (bypasses slow row-based drivers)."""

    def fetch_dataframe(self, query: str):
        # Snowflake-specific optimization or logging
        print("❄️ Snowflake Optimized Fetch")
        with self.pool.connection() as conn:
            return pl.read_database(query, connection=conn)

    def upload_to_stage(self, file_path, stage_name):
        """Custom behavior specific ONLY to Snowflake"""
        sql = f"PUT file://{file_path} @{stage_name}"
        self.execute_raw(sql)
