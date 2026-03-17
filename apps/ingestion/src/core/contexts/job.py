from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from enum import StrEnum

import structlog # type: ignore
import msgspec # type: ignore
from dynaconf import Dynaconf # type: ignore


from src.core.models.steps import JobSteps
from src.utils.constants import APP_CURRENT_ENV


LOG = structlog.getLogger(__name__)
APP_DEFAULT_CONFIG = "apps/ingestion/config/app.yaml"

class ExecutionMode(StrEnum):
    NORMAL = "normal"
    DEBUG = "debug"
    TEST = "test"
    DRYRUN = "dry_run"


def parse_set_options(settings: Optional[list[str]]) -> dict[str, Any]:
    """
    Parses CLI '--set' options into a scoped dictionary for configuration overrides.

    The parser supports two levels of specificity:
    1. Global Overrides: Applied to all datasets (e.g., '--set batch_size=5000')
    2. Scoped Overrides: Applied ONLY to a specific dataset (e.g., '--set sales:batch_size=1000')

    Syntax:
        - Global: [key]=[value]
        - Scoped: [dataset_name]:[key]=[value]

    Args:
        settings: A list of strings provided via the '--set' or '-s' CLI flag.

    Returns:
        A dictionary with a mandatory '_global' key and optional dataset keys.
        Example:
        {
            "_global": {"timeout": 60},
            "sales": {"batch_size": 5000}
        }
    """
    result = {"_global": {}}
    if not settings:
        return result
    
    for item in settings:
        if "=" not in item:
            continue # Or raise an error for invalid format
            
        key_val = item.split("=", 1)
        raw_key, value = key_val[0].strip(), key_val[1].strip()
        
        # Simple Type Inference
        if value.isdigit():
            value = int(value)
        elif value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False

        # Check for scoping (dataset:key)
        if ":" in raw_key:
            dataset_scope, clean_key = raw_key.split(":", 1)
            dataset_scope = dataset_scope.strip()
            if dataset_scope not in result:
                result[dataset_scope] = {}
            result[dataset_scope][clean_key.strip()] = value
        else:
            result["_global"][raw_key] = value
            
    return result

class JobContext(msgspec.Struct):
    # Core identifiers
    job_id: str
    dataset_id: str              # human-friendly dataset name
    run_date: str

    # Runtime Details
    execution_mode: ExecutionMode
    from_step: str = JobSteps.first_step().label 
    to_step: str = JobSteps.last_step().label

    # Paths
    output_path: str               # root for all step folders & manifests

    # Extraction Details
    source_identifier: str
    num_partitions: int = 10       # parallelism for ingest
    load_mode: Literal["snapshot", "delta"]
    schema_file: str | None = None
    source_params: dict[str, Any] = {}  # e.g. "filter_sql", "bind_params"
    
    source_type: str               # e.g. "postgres", "s3", "local"
    source_config: dict[str, Any] = {} # connection / credentials / options
    
    transform_script: str | None = None
    transform_params: dict[str, Any] = {}
    
    # Audit column names (internal metadata)
    audit_cols: list[str] = ["_ingested_at", "_partition_key", "_job_id", "_row_hash"]
    
    # Load/sink config
    sink_type: str          # e.g. "postgres", "s3", "snowflake"
    sink_identifier: str        # final sink identifier (table, path, etc.)
    sink_config: dict[str, Any] = {}
    load_params: dict[str, Any] = {}
    
    # Governance & archival
    enable_archival: bool = True
    retention_days: int = 2555
    archive_base_path: str = "/mnt/archive/ingestion"
    archive_type: str = "s3"       # archival service type
    archive_config: dict[str, Any] = {}  # e.g. bucket, prefix, kms key
    
    # Lifecycle / validation
    expires_at: float | None = None       # epoch, used by orchestrator
    validation_cmd: str = "validation-app"
    
    # Free-form extra params for services / readers
    extras: dict[str, Any] = {}
        
    def is_debug(self) -> bool:
        return self.execution_mode == ExecutionMode.DEBUG

    def is_test(self) -> bool:
        return self.execution_mode == ExecutionMode.TEST

    def is_normal(self) -> bool:
        return self.execution_mode == ExecutionMode.NORMAL


class JobContextBuilder:
    def __init__(self, app_cfg_path: str | None=None):
        # 1. Initialize Dynaconf with multiple layers
        # Dynaconf handles the APP__ env var overrides automatically
        self.app_cfg_path = app_cfg_path or APP_DEFAULT_CONFIG

    def _resolve_relative_date(
        self, 
        spec: dict[str, Any],
        run_date_str: str | None = None, 
    ) -> str:
        """Resolves T-x logic into a formatted string."""
        if run_date_str:
            date_val = datetime.strptime(run_date_str, "%Y-%m-%d")
        else:
            offset = spec.get("offset_days", 0)
            date_val = datetime.now() + timedelta(days=offset)
        
        fmt = date_val.strftime(spec.get("format", "%Y-%m-%d"))
        return f"'{fmt}'" if spec.get("wrap_quotes") else fmt

    def build_job_contexts(
        self, 
        job_id: str, 
        run_date_str: str | None = None,
        overrides_json: Path | None = None,
        env: str = APP_CURRENT_ENV
    ) -> list[JobContext]:
        """Maps merged config into a list of msgspec JobContext objects."""
        
        job_cfg_path = Path("apps/ingestion/config") / job_id /"config.yaml"        
        settings_files = [self.app_cfg_path, job_cfg_path]
        if overrides_json:
            settings_files.append(overrides_json)
        
        settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=settings_files,
            environments=True,
            env=env,
            load_dotenv=True,
        )

        contexts = []
        datasets = settings.get("datasets", {})
        for ds_name, ds_cfg in datasets.items():
            # Resolve Filter SQL Tokens (e.g., {{start_date}})
            source_params = ds_cfg.get("source_params", {})
            bind_params = source_params.get("bind_params", {})
            resolved_sql = source_params.get("filter_sql", "")

            for key, val in bind_params.items():
                if isinstance(val, dict) and val.get("type") == "relative_date":
                    actual_val = self._resolve_relative_date(val, run_date_str)
                    resolved_sql = resolved_sql.replace(f"{{{{{key}}}}}", actual_val)

            # Resolve Account/Service Reference
            # Fetches credentials from accounts based on account_ref
            archive_conf = ds_cfg.get("archive", settings.get("archive", {}))
            service_ref = archive_conf.get("service_ref")
            service_details = settings.get(f"services.{service_ref}", {})
            
            # Instantiate JobContext via msgspec
            ctx = msgspec.json.decode(
                msgspec.json.encode({
                    "job_id": job_id,
                    "run_date": actual_val,
                    "dataset_id": ds_name,
                    "target_destination": ds_cfg.get("target_destination"),
                    # "output_path": f"/tmp/{job_id}/{run_id}/{ds_name}",
                    "source_type": settings.get("source.type"),
                    "source_path": ds_cfg.get("source_path"),
                    "load_mode": ds_cfg.get("load_mode", "append"),
                    "num_partitions": ds_cfg.get("num_partitions", settings.get("num_partitions")),
                    "enable_archival": archive_conf.get("enable_archival", True),
                    "archive_type": service_details.get("type", "standard_archive"),
                    "archive_config": service_details,
                    "db_config": settings.get("source.config", {}),
                    "filter_sql": resolved_sql
                }),
                type=JobContext
            )
            contexts.append(ctx)
        return contexts