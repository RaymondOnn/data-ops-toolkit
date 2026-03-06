from typing import Any, Generator

import polars as pl



from src.services.factory import ServiceFactory
from src.services.registry import protect_service
from libs.clients.database.postgres import PostgresClient


@ServiceFactory.register("postgres")
class PostgresService:
    def __init__(self, name: str, **config: Any) -> None:
        self.name = name
        # The Client is internal to the Service
        self.client = PostgresClient(**config)

    @protect_service(threshold=3)
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        # Ray Workers call this. If it trips, Diskcache is updated.
        return self.client.fetch_df(query)

    def get_work_units(self, target: str, parallelism: int) -> list[str]:
        return self.client.get_load_strategy(target, parallelism)