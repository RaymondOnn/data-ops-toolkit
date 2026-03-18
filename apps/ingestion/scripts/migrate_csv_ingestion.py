import os
import sys
import msgspec
import polars as pl

# Ensure we can import from src
sys.path.append(os.path.join(os.getcwd(), "apps/ingestion"))

from src.core.models.job import Job
from src.core.models.steps import JobStep
from src.core.contexts.job import (
    JobContext, 
    ExecutionMode, 
    ExtractConfig, 
    TransformConfig, 
    LoadConfig, 
    ArchiveConfig
)
from src.utils.constants import JOB_STEPS_BASE_DIR

def setup_workspace(job_id: str, dataset_id: str, run_date: str, run_id: str):
    """Sets up the initial workspace for a job run."""
    active_root = JOB_STEPS_BASE_DIR / "active"
    active_root.mkdir(parents=True, exist_ok=True)
    
    # Create the job context manually for this migration test
    ctx = JobContext(
        job_id=job_id,
        dataset_id=dataset_id,
        run_date=run_date,
        execution_mode=ExecutionMode.NORMAL,
        output_path=str(active_root),
        extract=ExtractConfig(
            source_type="flat_file", 
            source_identifier="samples/sample_orders.csv",
            source_config={
                "account_id": "local_storage",
                "url": f"file://{os.getcwd()}/apps/ingestion"
            },
            num_partitions=1,
            load_mode="snapshot"
        ),
        transform=TransformConfig(),
        load=LoadConfig(
            sink_type="clickhouse",
            sink_identifier="orders",
            sink_config={
                "account_id": "local_clickhouse",
                "host": "localhost",
                "port": 8123,
                "username": "default",
                "password": "password",
                "database": "default"
            }
        ),
        archive=ArchiveConfig(enabled=False)
    )

    prefix = f"{job_id}:{dataset_id}_{run_date}_{run_id}"
    config_path = active_root / f"{prefix}_config.json"
    
    with open(config_path, "wb") as f:
        f.write(msgspec.json.encode(ctx))
        
    print(f"Workspace setup complete. Config at: {config_path}")
    return ctx, config_path

def main():
    job_id = "csv_ingestion"
    dataset_id = "orders"
    run_date = "2023-10-27"
    run_id = "migration_test_001"
    
    # 0. Setup
    ctx, config_path = setup_workspace(job_id, dataset_id, run_date, run_id)
    
    # Rehydrate Job
    composite_key = f"{job_id}:{dataset_id}"
    job = Job(run_id, composite_key, run_date, "migration_worker", target_step="start")
    
    # Triggers folder creation and manifest init
    _ = job.folder
    
    print(f"Job initialized in folder: {job.folder}")
    
    # Phase 1: RawStep
    print("\n--- Phase 1: RawStep ---")
    raw_step = JobStep.get_step_class_by_name("raw")
    job.set_step(raw_step)
    raw_step.execute(job)
    print("RawStep completed.")
    
    # Phase 2: TransformStep (Passthrough)
    print("\n--- Phase 2: TransformStep ---")
    transform_step = JobStep.get_step_class_by_name("transform")
    job.set_step(transform_step)
    transform_step.execute(job)
    print("TransformStep completed.")
    
    # Phase 3: WriteStep
    print("\n--- Phase 3: WriteStep ---")
    write_step = JobStep.get_step_class_by_name("write")
    job.set_step(write_step)
    write_step.execute(job)
    print("WriteStep completed.")
    
    # Verify final data in ClickHouse
    from src.services.factory import ServiceFactory
    ch_service = ServiceFactory.get_service("clickhouse", **ctx.load.sink_config)
    result = ch_service.sql("SELECT count(*) FROM orders")
    print(f"\nFinal row count in ClickHouse: {result[0][0]}")

if __name__ == "__main__":
    main()
