import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import diskcache
import polars as pl
import ray

from src.core.entities.job import Job, JobStage

LOG = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    source_url: str
    dataset_name: str
    output_path: str = field(default="./data_lake")
    worker_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])


class IngestionEngine:
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.job_id = datetime.now().strftime("%Y%m%d")
        self.cache = diskcache.Cache(".cache/ingestion")

        # Initialize Ray (connect to cluster or start local)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)


    def run(self) -> None:
        try:
            job = Job(self.job_id, self.config)
            LOG.info(f"Starting Ingestion Job: {job.id}/{job.run_id}")
            while job.step != JobStage.COMPLETE:
                job.execute()

        except Exception as e:
            LOG.error(f"Job Failed: {e}")
            raise

    


