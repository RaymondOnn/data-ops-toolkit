"""Publish stage for promoting staged data to production."""

from typing import TYPE_CHECKING

from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.stages.contracts.stage import ExecutionStage
from src.core.stages.models import WriteMode
from src.core.stages.types import Stage
from src.services.factory import ServiceFactory
from src.utils.exceptions import RollbackRequired

from .config import PublishConfig
from .enums import PUBLISH_POLICIES, PublishPayload, PublishPolicy

if TYPE_CHECKING:
    from src.core.models.task import TaskManifest, TaskWorkspace
    from src.core.stages.types import StageContext, StagePayload
    from src.services.health.system import SystemMonitor


LOG = logger


@ExecutionStage.register(key=Stage.PUBLISH.value)
class PublishStage(ExecutionStage[PublishConfig]):
    """Stage for promoting staged data to production.

    This stage handles the "Promotion" phase of the load strategy, moving data
    from a temporary staging area (created in the WRITE stage) to the final
    production destination.
    """

    requires_disk_space: bool = False
    config_class = PublishConfig
    payload: "StagePayload"

    def pre_flight(
        self,
        system: "SystemMonitor",
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> None:
        """Initialize sink and verify staging artifact exists.

        Args:
            task (Task): The current task instance being executed.

        Raises:
            RollbackRequired: If the WRITE stage manifest or the staging
                artifact identifier is missing.

        Notes:
            We explicitly check for the `staging_artifact` here to prevent
            the loader from attempting a promotion on a non-existent or
            corrupted staging state. If missing, we rewind to WRITE to
            re-attempt the staging process.
        """
        super().pre_flight(system, ctx, workspace, manifest)

        # Resolve Placeholders in config
        self.source = self.render_placeholders(
            target=self.config.staging_artifact_ref,
            manifest=manifest,
            step_id=self.step_id,
        )
        if self.source is None:
            raise RollbackRequired(Stage.WRITE.value, "Staging artifact not found")

        self.sink = ServiceFactory.get_sink(**self.config.connection)

    def _execute(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> str:
        """Promote staged data to production.

        Args:
            task (Task): The task instance containing the manifest and context.

        Returns:
            str: The name of the next stage (usually ARCHIVE) or 'FINISH'.

        Raises:
            RollbackRequired: If the write manifest is unexpectedly None.
            Exception: Propagates any underlying database or promotion errors.

        Notes:
            - We use the `loader.promote` method to ensure that the data swap
              is handled according to the specific sink's best practices
              (e.g., partition exchange, atomic renames, or transactional deletes).
        """
        start_ts = current_timestamp(naive=True).isoformat(sep=" ")

        try:
            if manifest.is_empty_result_set:
                LOG.info(
                    f"Task manifest is_empty_result_set=True. Bypassing promotion to protect {self.config.target}."
                )
                payload = PublishPayload(
                    step_id=self.step_id,
                    final_path=self.config.target,
                    rows_processed=0,
                    start_time=start_ts,
                )
                self.save_stage_outcome(
                    workspace=workspace, manifest=manifest, payload=payload
                )
                return self._next_step(ctx)

            # 2. Extract partition metrics from WRITE stage outcome
            partitions = self.render_placeholders(
                "${{ upstream['partitions'] }}", manifest=manifest, step_id=self.step_id
            )
            LOG.info(f"Promoting {self.source} to {self.config.target}")

            # Finalize sink operations
            policy = self.get_publish_policy(self.config)
            total_promoted_rows = 0

            if partitions:
                LOG.info(
                    f"Publishing {len(partitions)} partition(s) from '{self.source}' to '{self.config.target}'"
                )

                for part_metric in partitions:
                    p_val = part_metric["partition_date"]
                    p_expected_count = part_metric["rows_processed"]

                    LOG.info(
                        f"Promoting partition '{p_val}' ({p_expected_count:_} rows) to {self.config.target}..."
                    )

                    self.sink.promote(
                        source=self.source,
                        destination=self.config.target,
                        expected_count=p_expected_count,
                        update_key=self.config.update_key,
                        partition_value=p_val,
                        primary_keys=self.config.primary_keys,
                        merge_ops=policy.merge_ops,
                        soft_delete_missing=self.config.soft_delete_missing,
                        soft_delete_column=self.config.soft_delete_column,
                    )
                    total_promoted_rows += p_expected_count
                    LOG.success(f"Successfully promoted partition: '{p_val}'")

            payload = PublishPayload(
                step_id=self.step_id,
                final_path=self.config.target,
                rows_processed=total_promoted_rows,
                start_time=start_ts,
            )

            self.save_stage_outcome(
                workspace=workspace, manifest=manifest, payload=payload
            )
            LOG.success(
                f"Publish complete: {self.config.target} ({total_promoted_rows:_} total rows promoted)."
            )
            return self._next_step(ctx)

        except Exception as e:
            self.save_stage_outcome(workspace=workspace, manifest=manifest, error=e)
            raise

    @staticmethod
    def get_publish_policy(config: PublishConfig) -> PublishPolicy:
        mode = config.mode
        delta_merge_type = config.delta_merge_type

        # Guarantee delta_merge_type is only evaluated for DELTA mode
        merge_type = delta_merge_type if mode == WriteMode.DELTA else None

        policy = PUBLISH_POLICIES.get((mode, merge_type))
        if not policy:
            raise NotImplementedError(
                f"No policy defined for WriteMode '{mode}' with DeltaMergeType '{merge_type}'"
            )

        # 1. Enforce strict preconditions early
        if policy.requires_pks and not config.primary_keys:
            raise ValueError(
                f"WriteMode '{mode.value}' requires primary_keys to be defined."
            )

        if policy.requires_update_key and not config.update_key:
            raise ValueError(
                f"WriteMode '{mode.value}' requires update_key to be defined."
            )
        return policy
