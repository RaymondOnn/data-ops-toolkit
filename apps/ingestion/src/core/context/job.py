from typing import Any

import msgspec
from dynaconf import Dynaconf

class JobContext(msgspec.Struct):
    job_id: str
    run_id: str
    output_path: str
    tables: list[str]
    worker_id: str
    
    # Governance & Privacy
    enable_archival: bool = True
    retention_days: int = 2555  # Default 7 years
    archive_base_path: str = "/mnt/archive/ingestion"
    
    # Validation
    validation_cmd: str = "validation-app"
    
    # original_config is stored here for the workers to know HOW to do the work
    config: dict[str, Any]


def resolve_job_context(
    job_id: str, 
    runtime_overrides: dict[str, Any] | None = None
) -> list[JobContext]:
    """
    1. Loads YAML via Dynaconf.
    2. Applies environment & runtime overrides.
    3. Fans out multiple tables into a list of JobConfigs.
    """
    if not job_id:
        raise ValueError("Job ID is required")
    
    runtime_overrides = runtime_overrides or {}
    
    # Load settings (Dynaconf handles Default + Env merge)
    settings = Dynaconf(
        settings_files=[f"config/jobs/{job_id}.yaml"],
        environments=True,
    )

    # Apply manual overrides (e.g., from DB or CLI)
    if runtime_overrides:
        settings.update(runtime_overrides)

    # Handle the Multi-Table Edge Case
    # If the YAML says 'tables: [a, b]', we create two configs.
    table_list = settings.get("tables", [settings.dataset_name])
    
    return [
        JobContext(
            job_id=job_id,
            run_id=settings.run_id,
            output_path=settings.output_path,
            dataset_name=table,
            worker_id=settings.worker_id,
            enable_archival=settings.enable_archival,
            retention_days=settings.retention_days,
            archive_base_path=settings.archive_base_path,
            validation_cmd=settings.validation_cmd,
            config=settings.get("params", {})
        )
        for table in table_list
    ]