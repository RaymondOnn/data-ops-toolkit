from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import msgspec
import structlog
from dynaconf import Dynaconf
from src.core.contexts.execution import ExecutionContext, ExecutionMode
from src.core.contexts.job import JobContext
from src.utils.constants import APP_CURRENT_ENV

LOG = structlog.get_logger()
APP_DEFAULT_CONFIG = "./config/example/app.yaml"


def parse_set_options(settings: list[str] | None) -> dict[str, Any]:
    """
    Parses CLI '--set' options into a scoped dictionary for configuration overrides.

    The parser supports two levels of specificity:
    1. Global Overrides: Applied to all datasets (e.g., '--set batch_size=5000')
    2. Scoped Overrides: Applied ONLY to a specific dataset
        (e.g., '--set sales:batch_size=1000')

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
            date_val = datetime.now(UTC) + timedelta(days=offset)

        fmt = date_val.strftime(spec.get("format", "%Y-%m-%d"))
        return f"'{fmt}'" if spec.get("wrap_quotes") else fmt

    def get_execution_context(
        self, mode: ExecutionMode = ExecutionMode.NORMAL
    ) -> ExecutionContext:
        """Resolves the global app settings into a typed context."""
        workspace = Path(
            self.app_settings.get("workspace_dir", "~/.ingestion/data/")
        ).expanduser()

        return ExecutionContext(workspace_dir=workspace, execution_mode=mode)

    def _resolve_service(self, settings: Dynaconf, ref_key: str) -> dict:
        """
        Resolves a service_ref string to its full config dict from services.*.
        Falls back to the inline config block if no ref is given.
        """
        ref = settings.get(f"{ref_key}.service_ref")
        if ref:
            return dict(settings.get(f"services.{ref}", {}))
        return dict(settings.get(f"{ref_key}.config", {}))

    def build_job_contexts(
        self,
        job_id: str,
        run_date_str: str | None = None,
        overrides_json: Path | None = None,
    ) -> list[JobContext]:
        """Maps merged config into a list of msgspec JobContext objects."""

        job_cfg_path = Path("apps/ingestion/config") / job_id / "config.yaml"

        # 1. Initialize Dynaconf with job-specific overrides
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

        # 2. Establish run_date
        # Priority: run_date_str > CLI --set run_date > today
        run_date = (
            run_date_str
            or settings.get("run_date")
            or datetime.now(UTC).strftime("%Y-%m-%d")
        )

        contexts = []
        datasets = settings.get("datasets", {})
        for ds_name, ds_cfg in datasets.items():
            # Resolve Filter SQL Tokens (e.g., {{run_date}})
            source_params = ds_cfg.get("extract", {}).get("source_params", {})
            bind_params = source_params.get("bind_params", {})
            resolved_sql = source_params.get("filter_sql", "")

            actual_date = run_date
            for key, val in bind_params.items():
                if isinstance(val, dict) and val.get("type") == "relative_date":
                    actual_date = self._resolve_relative_date(val, run_date)
                    resolved_sql = resolved_sql.replace(f"{{{{{key}}}}}", actual_date)

            # Resolve Archival Config
            archive_conf = ds_cfg.get("archive", settings.get("archive", {}))
            archive_service_details = self._resolve_service(settings, "archive")

            # Resolve Transform Type
            # In config.yaml it might be nested under transform.type
            transform_cfg = ds_cfg.get("transform", {})
            transform_type = transform_cfg.get("type", "default")
            transform_params = transform_cfg.get("options", {})

            # Instantiate JobContext via msgspec
            ctx_data = {
                "job_id": job_id,
                "dataset_id": ds_name,
                "run_date": run_date,
                "output_path": f"storage/active/{job_id}/{ds_name}",
                "extract": {
                    "source_type": (
                        ds_cfg.get("extract", {}).get("source_type")
                        or settings.get("source.type")
                    ),
                    "source_identifier": (
                        ds_cfg.get("extract", {}).get("source_identifier")
                        or ds_cfg.get("source_path")
                    ),
                    "num_partitions": ds_cfg.get(
                        "num_partitions", settings.get("num_partitions", 10)
                    ),
                    "load_mode": ds_cfg.get("extract", {}).get("load_mode", "snapshot"),
                    "source_config": self._resolve_service(settings, "source"),
                    "source_params": {**source_params, "filter_sql": resolved_sql},
                    "schema_items": ds_cfg.get("schema_items", []),
                },
                "transform": {
                    "transform_type": ds_cfg.get("transform_type"),
                    "transform_params": ds_cfg.get("transform_params", {}),
                },
                "load": {
                    "sink_type": (
                        ds_cfg.get("load", {}).get("sink_type")
                        or settings.get("sink.type")
                    ),
                    "sink_identifier": (
                        ds_cfg.get("load", {}).get("sink_identifier")
                        or ds_cfg.get("target_destination")
                    ),
                    "sink_config": self._resolve_service(settings, "sink"),
                    "partition_col": (
                        ds_cfg.get("partition_col")
                        or settings.get("partition_col", "run_date")
                    ),
                    "partition_value": ds_cfg.get("partition_value") or actual_date,
                    "load_params": ds_cfg.get("load", {}).get("load_params", {}),
                },
                "archive": {
                    "enabled": archive_conf.get("enable_archival", True),
                    "retention_days": archive_conf.get(
                        "retention_days", settings.get("retention_days", 2555)
                    ),
                    "base_path": archive_conf.get(
                        "base_path",
                        settings.get("archive_base_path", "/mnt/archive/ingestion"),
                    ),
                    "type": archive_service_details.get("type", "s3"),
                    "config": archive_service_details,
                },
            }

            ctx = msgspec.json.decode(msgspec.json.encode(ctx_data), type=JobContext)
            contexts.append(ctx)

        return contexts
