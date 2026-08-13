"""Delta Lake table format handler."""

import io
import logging
from pathlib import Path
from typing import Any

import polars as pl

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class DeltaHandler(FormatHandler):
    """Handler for Delta Lake tables."""

    splittable = True

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        """Discover Delta table root paths (directories containing _delta_log).

        Args:
            path: The root path to search.
            pattern: Pattern to match table folders.

        Returns:
            set[str]: Set of discovered Delta table directory paths.
        """
        search_path = str(path)

        # If path is already a direct delta table folder containing _delta_log
        delta_log = f"{search_path.rstrip('/')}/_delta_log"
        if self.fs.exists(delta_log):
            return {search_path}

        # Otherwise glob subdirectories for _delta_log
        delta_logs = self.fs.glob(f"{search_path.rstrip('/')}/**/_delta_log")
        return {str(Path(p).parent) for p in delta_logs}

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """Convert Delta Lake table to Polars LazyFrame.

        Args:
            path: Path to the Delta table directory or URI.
            **kwargs: Extra parameters like version, timestamp, or storage_options.

        Returns:
            pl.LazyFrame: The LazyFrame scanning the Delta table.
        """
        tables = list(self.discover(path))
        if not tables:
            LOG.warning(f"No Delta Lake table found at: {path}")
            return pl.LazyFrame()

        # Handle time-travel versioning parameters if provided
        version = kwargs.get("version")

        lfs = []
        for table_path in tables:
            lf = pl.scan_delta(
                source=table_path,
                version=version,
                storage_options=self.options,
            )
            lfs.append(lf)

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(
        self, df: pl.LazyFrame | pl.DataFrame, path: Path | str, **kwargs: Any
    ) -> None:
        """Write LazyFrame or DataFrame to a Delta table.

        Args:
            df: The LazyFrame or DataFrame to write.
            path: Target Delta table directory path.
            **kwargs: Write mode ('append', 'overwrite', 'error') and delta options.
        """
        mode = kwargs.get("mode", "append")
        overwrite_schema = kwargs.get("overwrite_schema", False)

        # Polars write_delta requires an eager DataFrame
        eager_df = df.collect() if isinstance(df, pl.LazyFrame) else df

        eager_df.write_delta(
            target=str(path),
            mode=mode,
            overwrite_schema=overwrite_schema,
            storage_options=self.options,
        )

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Read raw bytes from the latest transaction log manifest file."""
        tables = self.discover(path)
        if not tables:
            return io.BytesIO(b"")

        # Pick first table's latest _delta_log JSON
        table_path = next(iter(tables))
        logs = sorted(self.fs.glob(f"{table_path}/_delta_log/*.json"))
        if not logs:
            return io.BytesIO(b"")

        with self.fs.open(logs[-1], "rb") as fp:
            return io.BytesIO(fp.read())

    def write_raw(self, data: bytes, path: str) -> None:
        """Raw byte writes are not supported directly on Delta table abstractions."""
        raise NotImplementedError(
            "Direct raw byte writing is not supported on Delta Lake tables."
        )
