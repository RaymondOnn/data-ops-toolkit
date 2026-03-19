from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import structlog
import msgspec
from dynaconf import Dynaconf

from src.core.contexts.job import JobContext, ExecutionMode
from src.utils.constants import APP_CURRENT_ENV

LOG = structlog.get_logger()
APP_DEFAULT_CONFIG = "apps/ingestion/config/app.yaml"


def parse_set_options(settings: list[str] | None) -> dict[str, Any]:
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
            continue  # Or raise an error for invalid format

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


class JobContextBuilder:
    def __init__(self, app_cfg_path: str | None = None, env: str = APP_CURRENT_ENV):
        # 1. Initialize Dynaconf with the global app config and environment overrides
        self.app_cfg_path = app_cfg_path or APP_DEFAULT_CONFIG
        self.env = env

        self.app_settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=[self.app_cfg_path],
            environments=True,
            env=self.env,
            load_dotenv=True,
        )

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
    ) -> list[JobContext]:
        """Maps merged config into a list of msgspec JobContext objects."""

        job_cfg_path = Path("apps/ingestion/config") / job_id / "config.yaml"

        # We create a new Dynaconf instance for this specific job run,
        # using the pre-loaded app_settings as the base.
        settings_files = [self.app_cfg_path, job_cfg_path]
        if overrides_json:
            settings_files.append(overrides_json)

        settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=settings_files,
            environments=True,
            env=self.env,
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
            ctx_data = {
                "job_id": job_id,
                "dataset_id": ds_name,
                "run_date": actual_val,
                "execution_mode": ExecutionMode.NORMAL,
                "output_path": f"storage/active/{job_id}",
                "extraction": {
                    "source_type": settings.get("source.type"),
                    "source_identifier": ds_cfg.get("source_path"),
                    "num_partitions": ds_cfg.get(
                        "num_partitions", settings.get("num_partitions", 10)
                    ),
                    "load_mode": ds_cfg.get("load_mode", "snapshot"),
                    "source_config": settings.get("source.config", {}),
                    "source_params": {"filter_sql": resolved_sql},
                    "schema_items": ds_cfg.get("schema_items", []),
                },
                "transform": {
                    "script": ds_cfg.get("transform_script"),
                    "params": ds_cfg.get("transform_params", {}),
                },
                "load": {
                    "sink_type": ds_cfg.get("sink_type", settings.get("sink.type")),
                    "sink_identifier": ds_cfg.get("target_destination"),
                    "sink_config": ds_cfg.get("sink_config", settings.get("sink.config", {})),
                    "partition_col": ds_cfg.get("partition_col"),
                    "partition_value": ds_cfg.get("partition_value") or actual_val,
                },
                "archival": {
                    "enabled": archive_conf.get("enable_archival", True),
                    "type": service_details.get("type", "standard_archive"),
                    "config": service_details,
                    "base_path": archive_conf.get("base_path", "/mnt/archive/ingestion"),
                },
            }

            ctx = msgspec.json.decode(msgspec.json.encode(ctx_data), type=JobContext)
            contexts.append(ctx)
        return contexts
