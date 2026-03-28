import os
import socket
from datetime import datetime
from typing import TYPE_CHECKING

import msgspec
import structlog

from apps.ingestion.src.core.models.job.manifest import BasePayload

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Task


LOG = structlog.getLogger(__name__)


class StartStep(ExecutionStage):
    name = StageName.START.label
    manifest: BasePayload

    def execute(self, job: "Task") -> str:
        # persist job-start metadata using engine helper
        start_timestamp = datetime.now().astimezone().isoformat()
        try:
            # 3. Gather System Metadata
            commit_hash = self._get_commit_hash()  # Use the helper above
            worker_id = f"{socket.gethostname()}-{os.getpid()}"

            # 4. Create Payload
            payload = BasePayload(
                commit_hash=commit_hash,
                source_params={},
                worker_id=job.worker_id,
                start_timestamp_utc=start_timestamp,
            )
            ctx = msgspec.structs.asdict(payload)

            # 5. Finalize (using the generic helper we discussed)
            # Note: Pass the Struct directly if finalize() handles to_builtins
            self.finalize(job, results=ctx)
            LOG.info(
                "Task initialized",
                stage=self.name,
                job_id=job.job_id,
                run_id=job.run_id,
            )
            return str(self._transit(job))
        except Exception as e:
            # Ensure we capture the traceback in the manifest
            self.finalize(job, exception=e)
            raise

    def _get_commit_hash(self) -> str:
        import subprocess

        try:
            # Returns the short hash (e.g., a1b2c3d)
            return (
                subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
                .decode("ascii")
                .strip()
            )
        except Exception:
            return "unknown"
            return "unknown"
            return "unknown"
            return "unknown"
            return "unknown"
            return "unknown"
            return "unknown"
            return "unknown"
            return "unknown"
