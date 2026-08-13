"""Apache Iceberg table format handler."""

import io
import logging
from pathlib import Path
from typing import Any

import polars as pl

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class IcebergHandler(FormatHandler):
    """Handler for Apache Iceberg tables."""

    splittable = True

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        """Discover Iceberg table root paths or metadata.json files.

        Args:
            path: Path or pattern to search.

        Returns:
            set[str]: Set of discovered metadata.json file paths or table roots.
        """
        search_path = str(path)

        # 1. If pointing directly to a metadata.json file
        if search_path.endswith(".metadata.json") and self.fs.exists(search_path):
            return {search_path}

        # 2. Check for metadata folder inside table directory
        metadata_dir = f"{search_path.rstrip('/')}/metadata"
        if self.fs.exists(metadata_dir):
            meta_files = sorted(self.fs.glob(f"{metadata_dir}/*.metadata.json"))
            if meta_files:
                return {meta_files[-1]}  # Latest metadata file

        # 3. Recursive search for any metadata.json files
        meta_files = self.fs.glob(
            f"{search_path.rstrip('/')}/**/metadata/*.metadata.json"
        )
        return set(meta_files)

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """Convert Iceberg table to Polars LazyFrame.

        Args:
            path: Metadata JSON file path or Table object/URI.
            **kwargs: Extra parameters like snapshot_id or catalog settings.

        Returns:
            pl.LazyFrame: The LazyFrame scanning the Iceberg table.
        """
        snapshot_id = kwargs.get("snapshot_id")
        catalog = kwargs.get("catalog")  # Optional PyIceberg Catalog instance

        meta_files = list(self.discover(path))
        if not meta_files and not catalog:
            LOG.warning(f"No Iceberg metadata file found at: {path}")
            return pl.LazyFrame()

        lfs = []
        targets = meta_files if meta_files else [str(path)]

        for target in targets:
            lf = pl.scan_iceberg(
                source=target,
                snapshot_id=snapshot_id,
                catalog=catalog,
                storage_options=self.options,
            )
            lfs.append(lf)

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(
        self, df: pl.LazyFrame | pl.DataFrame, path: Path | str, **kwargs: Any
    ) -> None:
        """Write LazyFrame or DataFrame to an Iceberg table.

        Args:
            df: The LazyFrame or DataFrame to write.
            path: Target metadata path or PyIceberg Table object.
            **kwargs: Write mode ('append', 'overwrite') and Iceberg table properties.
        """
        mode = kwargs.get("mode", "append")
        target = str(path)

        # Use streaming sink if LazyFrame, else eager write
        if isinstance(df, pl.LazyFrame):
            if hasattr(df, "sink_iceberg"):
                df.sink_iceberg(target=target, mode=mode)
            else:
                df.collect().write_iceberg(target=target, mode=mode)
        else:
            df.write_iceberg(target=target, mode=mode)

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Read raw bytes from the latest metadata.json file."""
        meta_files = sorted(self.discover(path))
        if not meta_files:
            return io.BytesIO(b"")

        latest_metadata = meta_files[-1]
        with self.fs.open(latest_metadata, "rb") as fp:
            return io.BytesIO(fp.read())

    def write_raw(self, data: bytes, path: str) -> None:
        """Raw byte writes are not supported directly on Iceberg table abstractions."""
        raise NotImplementedError(
            "Direct raw byte writing is not supported on Iceberg tables."
        )
