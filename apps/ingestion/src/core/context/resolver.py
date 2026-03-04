from dynaconf import Dynaconf
from src.core.entities.job import JobConfig

def resolve_job_config(job_id: str, runtime_overrides: dict = None) -> list[JobConfig]:
    """
    1. Loads YAML via Dynaconf.
    2. Applies environment & runtime overrides.
    3. Fans out multiple tables into a list of JobConfigs.
    """
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
        JobConfig(
            job_id=job_id,
            source_url=settings.source_url,
            dataset_name=table,
            output_path=settings.output_path,
            params=settings.get("params", {})
        )
        for table in table_list
    ]