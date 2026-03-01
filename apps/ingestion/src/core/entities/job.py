import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Type
from dataclasses import dataclass, field

import polars as pl

LOG = logging.getLogger(__name__)

@dataclass
class JobConfig:
    job_id: str
    source_url: str
    dataset_name: str
    worker_id: str
    output_path: str = field(default="./data_lake")

class JobBitmask:
    START = "000001"
    RAW = "000010"
    TRANSFORM = "000100"
    AUDIT = "001000"
    LOAD = "010000"
    COMPLETE = "100000"


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
        base = Path(job.job_config.output_path)
        path = (
            base
            / self.__class__.__name__.replace(
                "Step", ""
            ).lower()  # e.g., StartStep -> start
            / job.id
            / job.run_id
            / job.job_config.dataset_name
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
    def get_step_class_by_name(cls, name: str) -> Type["JobStep"]:
        steps = {
            "start": StartStep,
            "raw": RawStep,
            "transform": TransformStep,
            "audit": AuditStep,
            "load": LoadStep,
            "complete": CompleteStep,
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
    def execute(self, df: Optional[Any] = None) -> None:
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        job.set_step(AuditStep())


class AuditStep(JobStep):
    def execute(self, df: Optional[Any] = None) -> None:
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        job.set_step(LoadStep())


class LoadStep(JobStep):
    def execute(self, df: Optional[Any] = None) -> None:
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
    
    history: list[str] = []
    status: JobStatus,
    run_id: str
    _step: JobStep | None = None
    def __init__(
        self,
        job_id: str,
        job_config: Any,
        job_folder: Optional[Path] = None,
        start_step: Optional[str] = "start",
        run_id: Optional[str] = None,
    ) -> None:
        """
        Initialize a Job instance with the given step.

        :param step: The step name to start from (e.g. "raw", "transform", etc.). If None, the job will start from the beginning.
        :type step: str
        """
        self.id = job_id
        if start_step:
            self._step = self._get_step_by_name(start_step)
        self.job_config = job_config
        self.folder = job_folder or self.init_folder(job_config.output_path)
        self.metadata = {
            "job_id": self.id,
            "status": self.status,
            "run_id": str(uuid.uuid4())[:8],
            "dataset": self.job_config.dataset_name,
        }


    def init_folder(self, base_dir: str | Path) -> Path:
        base_dir = Path(base_dir)
        base_dir.mkdir(parents=True, exist_ok=True)

        folder_path = base_dir / self._step.__class__.__name__ / self.id / self.run_id
        folder_path.mkdir(parents=True, exist_ok=True)
        return folder_path

    def _get_step_by_name(self, name: str) -> Type[JobStep]:
        """Helper method to get a step class type by name."""
        return JobStep.get_step_class_by_name(name)

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
