import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional
import uuid

import polars as pl

LOG = logging.getLogger(__name__)


class JobStep(ABC):
    """Base class for JobStage classes."""

    @property
    def bitmask(self) -> str:
        raise NotImplementedError("Subclasses must implement this property")

    @abstractmethod
    def execute(self, df: Optional[Any] = None) -> Any:
        """Execute the current JobStage with the given engine and dataframe.

        :param engine: The IngestionEngine instance.
        :type engine: IngestionEngine
        :param df: The dataframe to process in this stage.
        :type df: Optional[Any]
        """
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        """Transit the Job instance to the next stage."""
        raise NotImplementedError
    
    def _get_checkpoint_path(self, job: "Job", xtension: str = "parquet") -> str:
        """
        Generates the path: <state>/<job_id>/<run_id>/{dataset}/proc_{worker_id}.{ext}
        """
        # Note: In a real S3 scenario, we would use fsspec to handle 's3://' paths.
        base = Path(job.config.output_path)
        path = (
            base
            / self.__class__.__name__
            / job.id
            / job.run_id
            / job.dataset_name
            / f"proc_{self.config.worker_id}.{extension}"
        )
        # Ensure directory exists (for local fs)
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)
    
    def _checkpoint(self, df: pl.DataFrame, state: "JobStep"):
        """
        Persist the given Polars DataFrame to disk as a Parquet file,
        representing a checkpoint in the ingestion pipeline.

        :param df: The Polars DataFrame to checkpoint.
        :type df: pl.DataFrame
        :param state: The JobStage to checkpoint.
        :type state: JobStage
        """
        path = self._get_checkpoint_path(state)
        LOG.info(f"Checkpointing [{state.value.upper()}] -> {path}")
        df.write_parquet(path)

    def mark_success(self, df: Optional[Any] = None) -> str:
        """Optionally persist the dataframe for this stage (if df provided and supports write_parquet),
        then write a small success marker (timestamp) next to the checkpoint file and return path.

        :param engine: The IngestionEngine instance.
        :type engine: IngestionEngine
        :param df: The dataframe to process in this stage.
        :type df: Optional[Any]
        :return: The path of the success marker.
        :rtype: str
        """
        # write stage artifact if provided and supports write_parquet
        if df is not None and hasattr(df, "write_parquet"):
            path = self._get_checkpoint_path(self)
            df.write_parquet(path)

        # create a success marker alongside the checkpoint path
        marker_base = self._get_checkpoint_path(self, extension="json")
        marker_path = f"{marker_base}.success"
        with open(marker_path, "w") as f:
            f.write(datetime.now().isoformat())
        return marker_path

    @classmethod
    def from_step(cls, name: str) -> "JobStep":
        steps = {
            "start": StartStep(),
            "raw": RawStep(),
            "transform": TransformStep(),
            "audit": AuditStep(),
            "load": LoadStep(),
            "complete": CompleteStep(),
        }
        if name not in steps:
            raise ValueError(f"Unknown step name: {name}")
        return steps[name]


class StartStep(JobStep):
    def execute(self, df: Optional[Any] = None) -> None:
        # persist job-start metadata using engine helper
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        """Transit the Job instance to the next stage."""
        job.set_step(RawStep())


class RawStep(JobStep):
    def execute(self, df: Optional[Any] = None) -> Any:
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        job.set_step(TransformStep())


class TransformStep(JobStep):
    def execute(self, df: Any) -> Any:
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        job.set_step(AuditStep())


class AuditStep(JobStep):
    def execute(self, df: Any):
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        job.set_step(LoadStep())


class LoadStep(JobStep):
    def execute(self, df: Any) -> None:
        LOG.info(f"Loading {len(df)} rows to destination...")
        # Logic for adbc-driver to write to DB would go here
        pass
        return None

    def transit(self, job: "Job") -> None:
        job.set_step(CompleteStep())


class CompleteStep(JobStep):
    def execute(self, df: Optional[Any] = None) -> None:
        return None


class Job:
    def __init__(
        self, 
        job_id: str, 
        job_config: Any,
        step: str | None = None
    ) -> None:
        """
        Initialize a Job instance with the given step.

        :param step: The step name to start from (e.g. "raw", "transform", etc.). If None, the job will start from the beginning.
        :type step: str
        """
        self.history: list[str] = []
        self.id = job_id
        self.dataset_name = job_config.dataset_name
        self.run_id = str(uuid.uuid4())[:8]
        if step:
            self._step = JobStep.from_step(step)
        else:
            self._step = StartStep()

    @property
    def step(self) -> JobStep:
        return self._step

    def set_step(self, step: JobStep) -> None:
        prev_step = self._step
        self._step = step

        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{current_timestamp}] Transition: {prev_step.__class__.__name__} -> {step.__class__.__name__}"
        self.history.append(entry)

    def execute(self) -> None:
        if not self._step:
            raise ValueError("Job is not initialized.")

        self._step.execute()

    def show_history(self) -> None:
        print("Job History:")
        for record in self.history:
            print(record)
