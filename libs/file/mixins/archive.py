import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import fsspec
import polars as pl
from libs.utils.dates import current_timestamp

LOG = logging.getLogger(__name__)


class StandardArchiveMixin:
    """
    Handles structured archiving for Multi-Dataset Tasks.
    Structure: archive/{job_id}/{dataset_name}/{YYYY}/{MM}/{DD}/{category}/
    """

    fs: fsspec.AbstractFileSystem
    url: str
    options: dict[str, Any]

    def archive_snapshot(
        self,
        data: str | pl.LazyFrame,
        job_id: str,
        dataset_name: str,  # Added to support multiple tables per job
        category: str,  # 'source' or 'bronze'
        logical_date: datetime | None = None,
    ) -> str:
        """
        Archives raw source or normalized Bronze data for a specific dataset.

        Args:
            data: Either a path to a local file (str) or a Polars LazyFrame.
            job_id: The unique identifier for the orchestration job.
            dataset_name: The name of the specific table or dataset.
            category: The lifecycle stage of the data (e.g., 'source', 'bronze').
            logical_date: The business date the data belongs to. Defaults to now.

        Returns:
            str: The full path to the archived artifact.
        """
        ref_date = logical_date or current_timestamp()
        date_path = ref_date.strftime("%Y/%m/%d")

        # New Hierarchy: job_id -> dataset_name -> date -> category
        dest_dir = f"{self.url}/archive/{job_id}/{dataset_name}/{date_path}/{category}"
        self.fs.makedirs(dest_dir, exist_ok=True)

        if isinstance(data, str):
            filename = Path(data).name
            dest_path = f"{dest_dir}/{filename}"
            self.fs.cp(data, dest_path)
            return dest_path

        dest_path = f"{dest_dir}/{dataset_name}_bronze.parquet"
        # Memory-efficient sink for 50M rows
        data.sink_parquet(dest_path)
        return dest_path

    def restore_from_archive(
        self,
        job_id: str,
        dataset_name: str,
        logical_date: datetime,
        category: str = "source",
    ) -> str:
        """
        Retrieves the specific table's archived file for re-processing.

        Args:
            job_id: The unique identifier for the job.
            dataset_name: The name of the dataset to restore.
            logical_date: The specific business date to look up.
            category: The data category (default 'source').

        Returns:
            str: The path to the first file found in the archive directory.
        """
        date_path = logical_date.strftime("%Y/%m/%d")
        search_dir = (
            f"{self.url}/archive/{job_id}/{dataset_name}/{date_path}/{category}"
        )

        if not self.fs.exists(search_dir):
            raise FileNotFoundError(
                f"No archive: {job_id}/{dataset_name} on {date_path}"
            )

        files = self.fs.ls(search_dir)
        if not files:
            raise FileNotFoundError(f"Empty archive directory: {search_dir}")

        return str(files[0])

    def apply_retention_policy(
        self, job_id: str, dataset_name: str, days: int, dry_run: bool = True
    ):
        """
        Deletes expired archives for a specific dataset within a job.

        Args:
            job_id: The unique identifier for the job.
            dataset_name: The name of the dataset to clean.
            days: Number of days of data to retain.
            dry_run: If True, logs intended deletions without removing files.
        """
        cutoff_date = current_timestamp() - timedelta(days=days)
        dataset_root = f"{self.url}/archive/{job_id}/{dataset_name}"

        if not self.fs.exists(dataset_root):
            return

        # Navigate: YYYY -> MM -> DD
        for year_dir in self.fs.ls(dataset_root):
            for month_dir in self.fs.ls(year_dir):
                for day_dir in self.fs.ls(month_dir):
                    parts = day_dir.rstrip("/").split("/")[-3:]
                    try:
                        folder_date = datetime.strptime("/".join(parts), "%Y/%m/%d")
                        if folder_date < cutoff_date:
                            if dry_run:
                                LOG.info(f"[DRY-RUN] Retention: Deleting {day_dir}")
                            else:
                                self.fs.rm(day_dir, recursive=True)
                    except ValueError:
                        continue
