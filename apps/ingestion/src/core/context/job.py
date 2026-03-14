from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import structlog
import msgspec
from dynaconf import Dynaconf




LOG = structlog.getLogger(__name__)
APP_DEFAULT_CONFIG = "apps/ingestion/config/app.yaml"
APP_CURRENT_ENV = "production"


class JobContext(msgspec.Struct):
    # Core identifiers
    job_id: str
    # Logical data identifiers
    dataset_name: str              # human-friendly dataset name
    # Paths
    output_path: str               # root for all step folders & manifests
    # Ingest/source config
    source_type: str               # e.g. "postgres", "s3", "local"
    source_path: str | None = None # file/bucket path for file-based sources
    source_params: dict[str, Any] = {}  # e.g. "filter_sql", "bind_params"
    num_partitions: int = 10       # parallelism for ingest
    db_config: dict[str, Any] = {} # connection / credentials / options

    transform_script: str | None = None
    schema_file: str | None = None
    
    # Audit column names (internal metadata)
    audit_cols: list[str] = ["_ingested_at", "_partition_key", "_job_id", "_row_hash"]
    # Load/sink config
    destination_type: str          # e.g. "postgres", "s3", "snowflake"
    target_destination: str        # final sink identifier (table, path, etc.)
    load_mode: Literal["overwrite", "append", "upsert"] = "append"
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
    params: dict[str, Any] = {}
    
    # original_config is stored here for the workers to know HOW to do the work
    config: dict[str, Any]



class JobContextBuilder:
    def __init__(self, app_cfg_path: str | None=None):
        # 1. Initialize Dynaconf with multiple layers
        # Dynaconf handles the APP__ env var overrides automatically
        self.app_cfg_path = app_cfg_path or APP_DEFAULT_CONFIG

    def _resolve_relative_date(self, spec: dict[str, Any]) -> str:
        """Resolves T-x logic into a formatted string."""
        offset = spec.get("offset_days", 0)
        date_val = datetime.now() + timedelta(days=offset)
        fmt = date_val.strftime(spec.get("format", "%Y-%m-%d"))
        return f"'{fmt}'" if spec.get("wrap_quotes") else fmt

    def build_job_contexts(
        self, 
        job_id: str, 
        env: str | None=None
    ) -> list[JobContext]:
        env = env or APP_CURRENT_ENV
        """Maps merged config into a list of msgspec JobContext objects."""
        
        job_cfg_path = Path("apps/ingestion/config") / job_id /"config.yaml"        
        settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=[self.app_cfg_path, job_cfg_path],
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
                    actual_val = self._resolve_relative_date(val)
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
                    "dataset_name": ds_name,
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