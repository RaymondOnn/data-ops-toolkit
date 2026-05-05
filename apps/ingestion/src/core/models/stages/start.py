import os
import socket
from typing import TYPE_CHECKING

import msgspec
from apps.ingestion.src.core.models.task.manifest import BasePayload
from apps.ingestion.src.utils.constants import STRIP_TZ_FOR_DB
from libs.utils.dates import get_current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


LOG = logger


class StartStage(ExecutionStage):
    name = StageName.START.label
    manifest: BasePayload

    def execute(self, task: "Task") -> str:
        # persist job-start metadata using engine helper
        start_timestamp = get_current_timestamp(strip_tz=STRIP_TZ_FOR_DB)
        
        try:
            # 3. Gather System Metadata
            commit_hash = self._get_commit_hash()  # Use the helper above
            worker_id = f"{socket.gethostname()}-{os.getpid()}"

            # 4. Create Payload
            payload = BasePayload(
                commit_hash=commit_hash,
                source_params={},
                worker_id=task.worker_id,
                start_timestamp=start_timestamp,
            )
            ctx = msgspec.structs.asdict(payload)

            # 5. Finalize (using the generic helper we discussed)
            # Note: Pass the Struct directly if finalize() handles to_builtins
            self.finalize(task, results=ctx)
            LOG.info(
                "Task initialized",
                stage=self.name,
                job_id=task.job_id,
                run_id=task.run_id,
            )
            return str(self._transit(task))
        except Exception as e:
            # Ensure we capture the traceback in the manifest
            self.finalize(task, exception=e)
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
