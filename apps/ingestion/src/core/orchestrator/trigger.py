from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

import msgspec
from apps.ingestion.src.core.contexts.task import TaskContext, load_task_context
from apps.ingestion.src.core.models.states import ExpiredState
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, STRIP_TZ_FOR_DB
from libs.utils.dates import get_current_timestamp, standardize_timestamp
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts import ExecutionContext

    from .enums import JobRecord

LOG = logger


def resolve_task_context(
    exec_ctx: "ExecutionContext", run: "JobRecord"
) -> TaskContext | None:
    """
    Attempts to locate and load a TaskContext from either the dispatched
    workspace or the pending config in the active root.
    """
    job_path = exec_ctx.get_run_path(
        run.JOB_ID, run.DATASET_ID, str(run.PARTITION_DATE), run.RUN_ID
    )
    identifier = exec_ctx.get_task_identifier(
        run.JOB_ID, run.DATASET_ID, str(run.PARTITION_DATE)
    )
    pending_config = (
        exec_ctx.active_path / f"{identifier}:{run.RUN_ID}_{CONFIG_FILENAME}"
    )

    try:
        if job_path.exists():
            return load_task_context(job_path)
        if pending_config.exists():
            with pending_config.open("rb") as f:
                return msgspec.json.decode(f.read(), type=TaskContext)
    except Exception:
        pass
    return None


class TriggerDecision(msgspec.Struct):
    """The outcome of a policy evaluation."""

    action: Literal["trigger", "purge", "wait"]
    rule_name: str
    record: "JobRecord"
    context: TaskContext | None = None


class TriggerRule(Protocol):
    """The Specification interface for triggering or purging jobs."""

    def apply(self, record: "JobRecord", **kwargs: Any) -> bool: ...


class MisfireRule(TriggerRule):
    def apply(self, record: "JobRecord", **kwargs: Any) -> bool:
        now = kwargs.get("now", get_current_timestamp(strip_tz=STRIP_TZ_FOR_DB))
        return record.is_misfired(now)


class ExpiryRule(TriggerRule):
    def apply(self, record: "JobRecord", **kwargs: Any) -> bool:
        # Delegate expiry check directly to the ExpiredState class

        return ExpiredState.is_applicable(task=None, record=record, **kwargs)


class CronScheduleRule(TriggerRule):
    def apply(self, record: "JobRecord", **kwargs: Any) -> bool:
        now = kwargs.get("now", get_current_timestamp(strip_tz=STRIP_TZ_FOR_DB))
        sched = standardize_timestamp(
            record.SCHEDULED_TIMESTAMP_LC, force_naive=STRIP_TZ_FOR_DB
        )
        return now >= sched


class FileArrivalRule(TriggerRule):
    def apply(self, record: "JobRecord", **kwargs: Any) -> bool:
        if not record.WATCH_FILE_PATH:
            return False

        # Sanitize: Prevent absolute paths or traversal
        watch_path = record.WATCH_FILE_PATH.lstrip("/")
        if ".." in watch_path:
            LOG.warning(
                f"Security: Blocked traversal attempt in WATCH_FILE_PATH: {watch_path}"
            )
            return False

        # Restrict globbing to the workspace data directory
        parent = Path(watch_path).parent
        return any(parent.glob(watch_path))


class CompositeRule(TriggerRule):
    """Composite specification for logical OR."""

    def __init__(self, *rules: TriggerRule):
        self.rules = rules

    def apply(self, record: "JobRecord", **kwargs: Any) -> bool:
        return any(rule.apply(record, **kwargs) for rule in self.rules)


class TriggerManager:
    """
    Handles the evaluation of job schedules and the provisioning of new runs.
    """

    def __init__(
        self,
        exec_ctx: "ExecutionContext",
    ):
        self.exec_ctx = exec_ctx
        # Cache to mitigate IO Complexity
        self._context_cache: dict[str, TaskContext | None] = {}

        # Composite Policies
        self.purge_policy = CompositeRule(MisfireRule(), ExpiryRule())
        self.trigger_rules = {
            "CRON": CronScheduleRule(),
            "FILE": FileArrivalRule(),
            "MANUAL": CronScheduleRule(),
        }

    def evaluate(self, job_records: list["JobRecord"]) -> list[TriggerDecision]:
        """Applies trigger policies to determine the fate of each record."""
        now = get_current_timestamp(
            timezone=self.exec_ctx.timezone, strip_tz=STRIP_TZ_FOR_DB
        )
        decisions: list[TriggerDecision] = []

        # Cache Housekeeping
        current_run_ids = {r.RUN_ID for r in job_records if r.RUN_ID}
        self._context_cache = {
            k: v for k, v in self._context_cache.items() if k in current_run_ids
        }

        for record in job_records:
            # Only evaluate triggers for jobs that are currently PENDING
            if ExecutionStatus.PENDING.value != record.JOB_STATUS:
                continue

            # --- 1. Decision: Resolve Context ---
            if record.RUN_ID not in self._context_cache:
                ctx = resolve_task_context(self.exec_ctx, record)

                # SELF-HEALING: Detect "Ghost Tasks"
                # If a Run ID exists in the DB but the config is missing on disk,
                # the workspace is corrupt.
                if ctx is None and record.RUN_ID:
                    job_path = self.exec_ctx.get_run_path(
                        record.JOB_ID,
                        record.DATASET_ID,
                        str(record.PARTITION_DATE or ""),
                        record.RUN_ID,
                    )
                    if job_path.exists():
                        LOG.warning(
                            f"Self-healing: Ghost task detected for {record.RUN_ID}. "
                            "Purging corrupt workspace."
                        )
                        decisions.append(
                            TriggerDecision(
                                action="purge",
                                rule_name="GHOST_RECOVERY",
                                record=record,
                            )
                        )
                        continue
                self._context_cache[record.RUN_ID] = ctx

            task_ctx = self._context_cache[record.RUN_ID]

            # --- 2. Decision: Purge? (Composite Policy) ---
            if self.purge_policy.apply(
                record, now=now, task_ctx=task_ctx, exec_ctx=self.exec_ctx
            ):
                decisions.append(
                    TriggerDecision(
                        action="purge",
                        rule_name="PURGE_POLICY",
                        record=record,
                        context=task_ctx,
                    )
                )
                continue

            # --- 3. Decision: Trigger? ---
            trigger_type = "FILE" if record.WATCH_FILE_PATH else record.TRIGGER_TYPE
            rule = self.trigger_rules.get(trigger_type, self.trigger_rules["CRON"])

            if rule.apply(record, now=now, task_ctx=task_ctx, exec_ctx=self.exec_ctx):
                decisions.append(
                    TriggerDecision(
                        action="trigger",
                        rule_name=f"READY:{trigger_type}",
                        record=record,
                        context=task_ctx,
                    )
                )

        return decisions


# TODO: How can I implement a 'dry_run' mode in Orchestrator that logs the TriggerManager's decisions without calling trigger_job or janitor.process_expired_run?
# TODO: How can I implement a 'dry_run' toggle in the Orchestrator to log these Specification decisions without executing them?
