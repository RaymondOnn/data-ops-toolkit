import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from libs.database.clients.duckdb import DuckDBClient
from libs.database.sql import SQLCompiler, SQLContext
from loguru import logger

from src.core.stages.transform.execution.base import (
    StandardTransformer,
    TransformContext,
    Transformer,
)

if TYPE_CHECKING:
    from src.core.stages.transform.config import TransformStep

LOG = logger


@Transformer.register("sql")
class SQLTransformer(StandardTransformer):
    """
    A simple wrapper for DuckDB SQL transformations. This processor takes a SQL query and applies it to the incoming Arrow RecordBatchReader.
    """

    duckdb: DuckDBClient

    def __init__(
        self,
        config: "TransformStep",
        sources: dict[str, str],
        output_dir: Path,
        file_format: str,
    ):
        super().__init__(config, sources, output_dir, file_format=file_format)
        self.duckdb = DuckDBClient()
        self.sql: SQLCompiler = SQLCompiler(dialect="duckdb")

    def setup(self, context: TransformContext) -> None:
        temp_dir = context.target / "_duckdb_temp"
        temp_dir.mkdir(parents=True, exist_ok=True)

        # 1. Enforce strict RAM limit & out-of-core disk spilling
        self.duckdb.command(f"SET temp_directory = '{temp_dir}'")
        # Preserve thread efficiency for disk spilling
        self.duckdb.command("SET max_temp_directory_size = '100GB'")
        # Config memory & temp directory for out-of-core join spilling if needed
        self.duckdb.command(
            f"SET temp_directory = '{self.output_dir / '_duckdb_temp'}'"
        )

    def execute(self, data: None = None) -> None:
        # 1. Register multiple input sources using wildcard parquet paths
        for alias, src_dir in self.sources.items():
            self.duckdb.register_source_view(alias, src_dir, ext=self.format)

        # 2. Compile query using SQLContext or raw SQL fallback
        sql_ctx = getattr(self.config, "sql_context", None)
        if isinstance(sql_ctx, SQLContext):
            final_query = self.sql.compile_context(sql_ctx)
        # elif getattr(self.config, "sql", None):
        #     final_query = self.config.sql_context
        else:
            raise TypeError(
                f"Sub-step '{getattr(self.config, 'id', 'unknown')}' missing valid SQLContext or SQL query string."
            )

        LOG.debug(f"Executing DuckDB transformation query:\n{final_query}")

        # 4. Stream query execution directly into partitioned Parquet files
        # PER_THREAD_OUTPUT writes multiple parquet files in parallel without holding all data in RAM
        self.duckdb.copy_to_parquet(final_query, self.output_dir, per_thread=True)

    def teardown(self) -> None:
        """Clean up any resources after processing."""
        self.duckdb.close()
        shutil.rmtree(self.output_dir / "_duckdb_temp", ignore_errors=True)
