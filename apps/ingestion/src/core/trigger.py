import argparse
import subprocess
import sys
import time
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

from src.core.engine import IngestionConfig, IngestionEngine
from watchfiles import Change, watch

from libs.registry import get_registry

LOG = logging.getLogger(__name__)

class BaseTrigger(ABC):
    """Base class for all triggers."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

    @abstractmethod
    def run(self) -> None:
        """Executes the trigger's action."""
        pass


class CommandLineTrigger(BaseTrigger):
    """Standard Batch Trigger."""

    def run(self) -> None:
        source = self.args.source
        dataset = self.args.dataset
        run_id = self.args.run_id or f"{dataset}-{int(time.time())}"
        output_path = self.args.output_path

        LOG.info(f"Starting Ingestion: {dataset} from {source} (Run ID: {run_id})")

        registry = get_registry()
        registry.update(f"job:{run_id}", "STARTING")

        try:
            config = IngestionConfig(
                source_url=source, dataset_name=dataset, output_path=output_path
            )
            engine = IngestionEngine(config, run_id=run_id)
            engine.run()

            registry.update(f"job:{run_id}", "COMPLETED")
            LOG.info(f"Job {run_id} finished successfully.")
        except Exception as e:
            LOG.error(f"Job {run_id} failed: {e}")
            registry.update(f"job:{run_id}", "FAILED")
            sys.exit(1)


class FileTrigger(BaseTrigger):
    """File Trigger."""

    def run(self) -> None:
        directory = self.args.directory
        dataset = self.args.dataset
        LOG.info(f"Starting Watcher on {directory} for dataset '{dataset}'")

        try:
            for changes in watch(directory):
                for change_type, path in changes:
                    if change_type == Change.added:
                        filepath = Path(path)
                        LOG.info(f"Watcher detected file: {filepath.name}")

                        cmd = [
                            sys.executable,
                            self.args.script_path,
                            "ingest",
                            "--source",
                            f"file://{filepath}",
                            "--dataset",
                            dataset,
                        ]
                        subprocess.Popen(cmd)
                        LOG.info(f"Triggered CLI command for {filepath.name}")
        except KeyboardInterrupt:
            LOG.info("Watcher stopped by user.")


class InternalTrigger(BaseTrigger):
    """Internal Trigger."""

    def run(self) -> None:
        job_id = self.args.job_id
        LOG.info(f"Attempting to resume job: {job_id}")

        # Placeholder Logic:
        LOG.info("Found checkpoint at state: 'Transform'. Resuming...")
        # In a real implementation, this would instantiate an IngestionEngine
        # with a specific 'resume_from' state and run it.
        LOG.info(f"Resumed job {job_id} completed.")


class SchedulerTrigger(BaseTrigger):
    """Always-On Scheduler Trigger."""

    def run(self) -> None:
        poll_interval = self.args.interval
        LOG.info(f"Starting Scheduler Daemon (Polling every {poll_interval}s)...")

        try:
            while True:
                job = self._poll_for_jobs()
                if job:
                    self._execute_workflow(job)

                time.sleep(poll_interval)
        except KeyboardInterrupt:
            LOG.info("Scheduler stopped by user.")

    def _poll_for_jobs(self) -> Optional[Dict[str, Any]]:
        """Mock DB polling logic."""
        # In a real system: SELECT * FROM scheduled_jobs WHERE status = 'PENDING'
        # For this portfolio demo, we'll trigger a job every 15 seconds to show it works.
        if int(time.time()) % 15 == 0:
            LOG.info("...found a scheduled job in the database...")
            return {
                "dataset": "scheduled_sales_data",
                "source": "s3://mock-bucket/daily_sales.csv",
                "run_id": f"sched-{int(time.time())}",
            }
        return None

    def _execute_workflow(self, job_details: dict[str, Any]) -> None:
        dataset = job_details["dataset"]
        run_id = job_details["run_id"]
        LOG.info(f"Triggering Scheduled Job: {dataset} (Run ID: {run_id})")

        # This block mirrors the CommandLineTrigger, but uses details from the polled job.
        try:
            config = IngestionConfig(
                source_url=job_details["source"],
                dataset_name=dataset,
                output_path=self.args.output_path,
            )
            engine = IngestionEngine(config, run_id=run_id)
            engine.run()
            LOG.info(f"Scheduled job {run_id} finished successfully.")
        except Exception as e:
            # In a real system, you'd update the job's status to FAILED in the DB.
            LOG.error(f"Scheduled job {run_id} failed: {e}")
