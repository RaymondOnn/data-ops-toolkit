import os
import sys
from pathlib import Path

import msgspec

# Ensure we can import from apps.ingestion.src
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root / "apps" / "ingestion"))

from apps.ingestion.src.core.contexts import ExecutionMode, TaskContextBuilder
from apps.ingestion.src.core.contexts.job import (
    ArchiveConfig,
    ExtractConfig,
    LoadConfig,
    TaskContext,
    TransformConfig,
)
from apps.ingestion.src.core.models.job import Task
from apps.ingestion.src.core.models.stages.utils import get_stage_class_by_name
from apps.ingestion.src.services.factory import ServiceFactory


def main():
    job_id = "csv_ingestion"
    dataset_id = "orders"
    partition_date = "2023-10-27"
    run_id = "migration_test_001"

    # 1. Initialize Execution Context using the Builder
    # This resolves the workspace_dir and environment settings.
    builder = TaskContextBuilder()
    exec_ctx = builder.get_execution_context(mode=ExecutionMode.NORMAL)

    # 2. Setup Manual TaskContext (Bypassing the full YAML-to-Context for this test)
    ctx = TaskContext(
        job_id=job_id,
        dataset_id=dataset_id,
        partition_date=partition_date,
        output_path=str(
            exec_ctx.active_path / f"{job_id}:{dataset_id}_{partition_date}_{run_id}"
        ),
        extract=ExtractConfig(
            source_type="flat_file",
            source_identifier="samples/sample_orders.csv",
            source_config={
                "url": f"file://{os.getcwd()}/apps/ingestion",
            },
            num_workers=1,
            load_mode="snapshot",
        ),
        transform=TransformConfig(),
        load=LoadConfig(
            sink_type="clickhouse",
            sink_identifier="orders",
            sink_config={
                "host": "localhost",
                "port": 8123,
                "user": "default",
                "password": "password",
                "database": "default",
            },
        ),
        archive=ArchiveConfig(enabled=False),
    )

    # 3. Initialize Task
    # The Task class manages its own manifest and state transitions.
    composite_key = f"{job_id}:{dataset_id}"
    job = Task(
        run_id=run_id,
        composite_key=composite_key,
        partition_date=partition_date,
        worker_id="migration_worker",
        exec_ctx=exec_ctx,
        target_stage="start",
    )

    # 4. Persistence: Manually write the config file into the workspace so the Task can find it
    # Normally the Orchestrator does this.
    active_root = exec_ctx.active_path
    active_root.mkdir(parents=True, exist_ok=True)
    prefix = f"{job_id}:{dataset_id}_{partition_date}_{run_id}"
    config_path = exec_ctx.active_path / f"{prefix}_config.json"

    with config_path.open("wb") as f:
        f.write(msgspec.json.encode(ctx))

    # Trigger folder creation and manifest init
    _ = job.folder
    print(f"Task initialized in folder: {job.folder}")

    # 5. Run the 6-Step Pipeline
    # Steps: start -> extract -> transform -> write -> publish -> complete
    # (Audit is skipped as per user request)

    stages_to_run = ["start", "extract", "transform", "write", "publish", "complete"]

    for stage_name in stages_to_run:
        print(f"\n--- Phase: {stage_name.upper()} ---")
        stage_instance = get_stage_class_by_name(stage_name)
        job.set_stage(stage_instance)
        job.execute()
        print(f"{stage_name.capitalize()} completed.")

    # 6. Verification
    print("\n--- Verification ---")
    ch_service = ServiceFactory.get_service("clickhouse", **ctx.load.sink_config)
    result = ch_service.sql(f"SELECT count(*) FROM {ctx.load.sink_identifier}")
    print(f"Final row count in ClickHouse ({ctx.load.sink_identifier}): {result[0][0]}")

    # 7. Cleanup Check
    # Note: 'complete' stage purges extract/transform folders.
    run_parent = job.folder.parent
    if not (run_parent / "extract").exists():
        print("Success: Temporary extract files purged.")
    if not (run_parent / "transform").exists():
        print("Success: Temporary transform files purged.")


if __name__ == "__main__":
    main()
