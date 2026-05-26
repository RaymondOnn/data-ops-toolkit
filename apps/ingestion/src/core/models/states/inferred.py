import time
from typing import TYPE_CHECKING, Any, Optional

from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import MANIFEST_FILENAME, STRIP_TZ_FOR_DB
from libs.utils.dates import get_current_timestamp, standardize_timestamp
from loguru import logger

from .base import InferredState

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.enums import JobRecord

LOG = logger


class ZombieState(InferredState):
    """The expert on detecting silently deceased distributed processes."""

    folder_name = "active"
    target_status = ExecutionStatus.WAITING

    @classmethod
    def is_applicable(
        cls,
        record: Optional["JobRecord"] = None,
        **kwargs: Any,
    ) -> bool:
        """DIAGNOSIS: Evaluates physical and logical telemetry for silent death."""
        meta = kwargs.get("meta")
        active_tasks = kwargs.get("active_tasks", {})
        exec_ctx = kwargs.get("exec_ctx")
        cache_key = kwargs.get("cache_key")

        if not meta or not exec_ctx:
            return False

        # 1. RAY CORE MESH CHECK
        if cache_key and cache_key in active_tasks.values():
            return False

        # 2. LOGICAL HEARTBEAT DRIFT CHECK (5 Minute Boundary)
        if time.time() - meta.last_hb < 300:
            return False

        # 3. PHYSICAL DISK MANIFEST MUTATION CHECK
        active_path = find_path(exec_ctx.active_path, meta.run_id)
        if active_path:
            manifest_file = active_path / MANIFEST_FILENAME
            if manifest_file.exists() and time.time() - manifest_file.stat().st_mtime < 300:
                    return False

        return True


class ExpiredState(InferredState):
    """Declarative policy rule engine for outlived data lifecycles."""

    target_status = ExecutionStatus.EXPIRED

    @classmethod
    def is_applicable(
        cls,
        record: Optional["JobRecord"] = None,
        **kwargs: Any,
    ) -> bool:
        """INFERRED: Determines if a database track record has breached its TTL."""
        if not record or not record.IS_SNAPSHOT or not record.EXPIRATION_THRESHOLD:
            return False

        if ExecutionStatus(record.JOB_STATUS).is_terminal:
            return False

        threshold = standardize_timestamp(
            record.EXPIRATION_THRESHOLD, force_naive=STRIP_TZ_FOR_DB
        )
        current_time = kwargs.get("now") or get_current_timestamp(
            strip_tz=STRIP_TZ_FOR_DB
        )
        return current_time > threshold
