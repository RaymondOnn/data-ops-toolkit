import os
import sys
from pathlib import Path

import msgspec
import polars as pl

# Ensure we can import from src
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root / "apps" / "ingestion"))

from src.core.models.job import Job
from src.core.models.steps import JobStep
from src.core.contexts import JobContextBuilder, ExecutionMode
from src.core.contexts.job import (
    JobContext,
    ExtractConfig,
    TransformConfig,
    LoadConfig,
    ArchiveConfig,
)
from src.services.factory import ServiceFactory


def main():
    job_id = "csv_ingestion"
    dataset_id = "orders"
    run_date = "2023-10-27"
    run_id = "migration_test_001"

    # 1. Initialize Execution Context using the Builder
    # This resolves the workspace_dir and environment settings.
    builder = JobContextBuilder()
    exec_ctx = builder.get_execution_context(mode=ExecutionMode.NORMAL)

    # 2. Setup Manual JobContext (Bypassing the full YAML-to-Context for this test)
    ctx = JobContext(
        job_id=job_id,
        dataset_id=dataset_id,
        run_date=run_date,
        output_path=str(
            exec_ctx.active_path / f"{job_id}:{dataset_id}_{run_date}_{run_id}"
        ),
        extract=ExtractConfig(
            source_type="flat_file",
            source_identifier="samples/sample_orders.csv",
            source_config={
                "account_id": "local_storage",
                "url": f"file://{os.getcwd()}/apps/ingestion",
            },
            num_partitions=1,
            load_mode="snapshot",
        ),
        transform=TransformConfig(),
        load=LoadConfig(
            sink_type="clickhouse",
            sink_identifier="orders",
            sink_config={
                "account_id": "local_clickhouse",
                "host": "localhost",
                "port": 8123,
                "user": "default",
                "password": "password",
                "database": "default",
            },
        ),
        archive=ArchiveConfig(enabled=False),
    )

    # 3. Initialize Job
    # The Job class manages its own manifest and state transitions.
    composite_key = f"{job_id}:{dataset_id}"
    job = Job(
        run_id=run_id,
        composite_key=composite_key,
        run_date=run_date,
        worker_id="migration_worker",
        exec_ctx=exec_ctx,
        target_step="start",
    )

    # 4. Persistence: Manually write the config file into the workspace so the Job can find it
    # Normally the Orchestrator does this.
    active_root = exec_ctx.active_path
    active_root.mkdir(parents=True, exist_ok=True)
    prefix = f"{job_id}:{dataset_id}_{run_date}_{run_id}"
    config_path = exec_ctx.active_path / f"{prefix}_config.json"

    with config_path.open("wb") as f:
        f.write(msgspec.json.encode(ctx))

    # Trigger folder creation and manifest init
    _ = job.folder
    print(f"Job initialized in folder: {job.folder}")

    # 5. Run the 6-Step Pipeline
    # Steps: start -> extract -> transform -> write -> publish -> complete
    # (Audit is skipped as per user request)

    steps_to_run = ["start", "extract", "transform", "write", "publish", "complete"]

    for step_name in steps_to_run:
        print(f"\n--- Phase: {step_name.upper()} ---")
        step_instance = JobStep.get_step_class_by_name(step_name)
        job.set_step(step_instance)
        job.execute()
        print(f"{step_name.capitalize()} completed.")

    # 6. Verification
    print("\n--- Verification ---")
    ch_service = ServiceFactory.get_service("clickhouse", **ctx.load.sink_config)
    result = ch_service.sql(f"SELECT count(*) FROM {ctx.load.sink_identifier}")
    print(f"Final row count in ClickHouse ({ctx.load.sink_identifier}): {result[0][0]}")

    # 7. Cleanup Check
    # Note: 'complete' step purges extract/transform folders.
    run_parent = job.folder.parent
    if not (run_parent / "extract").exists():
        print("Success: Temporary extract files purged.")
    if not (run_parent / "transform").exists():
        print("Success: Temporary transform files purged.")


if __name__ == "__main__":
    main()
