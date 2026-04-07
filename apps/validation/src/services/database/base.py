from abc import abstractmethod
from typing import TYPE_CHECKING, Any

import polars as pl
import structlog
from apps.validation.src.services.base import Service
from apps.validation.src.services.factory import ServiceFactory
from libs.database.clients.clickhouse import ClickhouseClient
from libs.database.clients.oracle import OracleClient
from libs.database.clients.postgres import PostgresClient

if TYPE_CHECKING:
    from libs.auth.models import Secret
    from libs.database.clients.base import DBClient


LOG = structlog.getLogger(__name__)


class DatabaseService(Service):
    """
    Intermediate layer for all SQL-based sources.
    """

    def __init__(self, name: str, **config: Any):
        super().__init__(name, **config)
        self.client: DBClient = self._init_client(**config)

    @abstractmethod
    def _init_client(self, **config: Any) -> Any:
        """Subclasses must initialize their specific DB client."""
        pass

    def scan(self, **kwargs: Any) -> pl.LazyFrame:
        """
        Service layer logic to handle partitioning and lazy streaming.
        """
        table_name = str(kwargs.get("table_name"))
        filter_sql = kwargs.get("filter_sql", "1=1")
        num_parts = self.config.get("partitions", 5)

        # 1. Use the client-specific load strategy (ORA_HASH, cityHash64, etc.)
        queries = self.client.get_load_strategy(
            table_name=table_name,
            num_partitions=num_parts,
            filter_sql=filter_sql
        )
        
        # 2. Map queries to LazyFrames using fetch_lazy (streaming via pl.from_generator)
        # This prevents materializing 10M rows in your 512MB RAM
        lazy_parts = [self.client.fetch_lazy(sql) for sql in queries]

        # 3. Concatenate vertically. Polars will parallelize these streams
        return pl.concat(lazy_parts, how="vertical")
    
    def get_actual_schema(self, fq_table: str) -> pl.DataFrame:
        """
        Fetches the actual schema of the table from the database.
        This is crucial for the Tier 0 Schema Check and for generating the correct expressions in the Mega-Scan.
        """
        return self.client.get_schema(fq_table)


@ServiceFactory.register("clickhouse_db")
class ClickHouseService(DatabaseService):
    def _init_client(self, **config: Any) -> ClickhouseClient:
        # 1. Resolve Password safely
        # If 'secret_key' was used, 'password' is a Secret object.
        # If 'password' was a string in YAML, it stays a string.
        raw_password = config.get("password", "")
        resolved_password = (
            raw_password.resolve(sanitize=True)
            if hasattr(raw_password, "resolve")
            else str(raw_password)
        )
        return ClickhouseClient(
            host=config.get("host", "localhost"),
            port=config.get("port", 8123),
            user=config.get("user", "default"),
            password=resolved_password,
        )


@ServiceFactory.register("postgres_db")
class PostgresService(DatabaseService):
    def _init_client(self, **config: Any) -> PostgresClient:
        secret: Secret = config["password"]
        return PostgresClient(
            host=config["host"],
            database=config["database"],
            user=config["user"],
            password=secret.resolve(sanitize=True),
            port=config.get("port", 5432),
        )


@ServiceFactory.register("oracle_db")
class OracleService(DatabaseService):
    def _init_client(self, **config: Any) -> Any:
        secret: Secret = config["password"]
        return OracleClient(
            user=config["user"],
            password=secret.resolve(sanitize=True),
            dsn=config["dsn"],
        )
        
