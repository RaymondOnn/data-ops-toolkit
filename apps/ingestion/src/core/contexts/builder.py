from collections import ChainMap
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import msgspec
import structlog
from apps.ingestion.src.core.contexts.execution import ExecutionContext, ExecutionMode
from apps.ingestion.src.core.contexts.job import JobContext
from apps.ingestion.src.utils.constants import APP_CONFIG_ROOT, APP_CURRENT_ENV
from dynaconf import Dynaconf

LOG = structlog.get_logger()
APP_DEFAULT_CONFIG = APP_CONFIG_ROOT / "app.yaml"


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
    result: dict[str, Any] = {"_global": {}}
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
        print(f"Using App Config: {self.app_cfg_path}")
        self.app_settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=[self.app_cfg_path],
            environments=True,
            env=self.env,
            load_dotenv=True,
        )

        # --- DEBUG INSTRUMENTATION ---
        LOG.debug(f"DEBUG: Config Path Absolute: {Path(self.app_cfg_path).resolve()}")
        LOG.debug(f"DEBUG: File Exists: {Path(self.app_cfg_path).exists()}")
        LOG.debug(f"DEBUG: Loaded Keys: {list(self.app_settings.keys())}")

        try:
            LOG.info("Resolved App Config", config=self.app_settings.to_dict())
        except Exception:
            LOG.warning("Could not serialize app config for logging")

    def _resolve_run_date(
        self,
        spec: dict[str, Any],
        run_date_str: str | None = None,
    ) -> str:
        """Resolves T-x logic into a formatted string."""
        if run_date_str:
            date_val = datetime.strptime(run_date_str, "%Y-%m-%d")
        else:
            offset = spec.get("offset_days", 0)
            date_val = datetime.now().astimezone() + timedelta(days=offset)

        fmt = date_val.strftime(spec.get("format", "%Y-%m-%d"))
        return f"'{fmt}'" if spec.get("wrap_quotes") else fmt

    def get_execution_context(
        self, mode: ExecutionMode = ExecutionMode.NORMAL
    ) -> ExecutionContext:
        """Resolves the global app settings into a typed context."""
        workspace = Path(
            self.app_settings.get("workspace_dir", "~/.ingestion/data/")
        ).expanduser()

        # Ensure the base workspace directory exists so lock files
        # and subdirectories can be created safely.
        workspace.mkdir(parents=True, exist_ok=True)

        return ExecutionContext(workspace_dir=workspace, execution_mode=mode)

    def _resolve_service(
        self, settings: Dynaconf, ref_key: str, dataset_id: str
    ) -> dict:
        # Search hierarchy for service_ref:
        # Dataset (Active -> Default) > Job (Active -> Default)
        ref = (
            settings.get(f"datasets.{dataset_id}.{ref_key}.service_ref")
            or settings.from_env("default").get(
                f"datasets.{dataset_id}.{ref_key}.service_ref"
            )
            or settings.get(f"job.{ref_key}.service_ref")
            or settings.from_env("default").get(f"job.{ref_key}.service_ref")
        )

        if ref:
            # 1. Try Global app.yaml (Active env, then fallback to default)
            global_def = self.app_settings.get(
                f"services.{ref}"
            ) or self.app_settings.from_env("default").get(f"services.{ref}")
            if global_def:
                return dict(global_def)

            # 2. Try Job-level config.yaml services block
            job_level_def = settings.get(f"services.{ref}") or settings.from_env(
                "default"
            ).get(f"services.{ref}")
            return dict(job_level_def or {})

        # Fallback to inline config block:
        # Dataset (Active -> Default) > Job (Active -> Default)
        return dict(
            settings.get(f"datasets.{dataset_id}.{ref_key}.config")
            or settings.from_env("default").get(
                f"datasets.{dataset_id}.{ref_key}.config"
            )
            or settings.get(f"job.{ref_key}.config")
            or settings.from_env("default").get(f"job.{ref_key}.config")
            or {}
        )

    def build(
        self,
        job_id: str,
        dataset_id: str | None = None,
        run_date_str: str | None = None,
        overrides_json: Path | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> list[JobContext]:
        """Maps merged config into a list of msgspec JobContext objects."""
        LOG.debug("Building job contexts", job_id=job_id, run_date=run_date_str)

        job_cfg_path = Path("apps/ingestion/config") / job_id / "config.yaml"

        # 1. Initialize Dynaconf with job-specific overrides
        settings_files = [job_cfg_path]
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
            or datetime.now().astimezone().strftime("%Y-%m-%d")
        )

        # 4. Get the Job-level defaults and the Dataset-level specifics
        job_defaults = settings.get("job", {})
        all_datasets = settings.get("datasets", {})

        # Filter if a specific dataset was requested via CLI
        target_dataset_ids = [dataset_id] if dataset_id else list(all_datasets.keys())

        contexts = []
        for ds_id in target_dataset_ids:
            if ds_id not in all_datasets:
                LOG.warning(f"Dataset '{ds_id}' not found in job '{job_id}'. Skipping.")
                continue

            ds_cfg = all_datasets[ds_id]

            # 5. Build the context using the resolution: Dataset Spec > Job Default
            ctx = self._create_job_context(
                job_id=job_id,
                dataset_id=ds_id,
                run_date=run_date,
                # Pass both layers for hierarchical lookup
                # job_defaults=job_defaults,
                # ds_cfg=ds_cfg,
                # Pass the full settings object to resolve global service refs
                settings=settings,
            )

            # 6. Final Layer: Apply Runtime CLI Overrides (--set)
            if overrides:
                # Create a prioritized view: Dataset overrides > Global overrides
                active_overrides = ChainMap(
                    overrides.get(ds_id, {}), overrides.get("_global", {})
                )

                for key, value in active_overrides.items():
                    if hasattr(ctx, key):
                        setattr(ctx, key, value)
                    else:
                        ctx.custom_overrides[key] = value

            contexts.append(ctx)

        LOG.debug("Resolved contexts", count=len(contexts), job_id=job_id)
        return contexts

    def _create_job_context(
        self,
        job_id: str,
        dataset_id: str,
        run_date: str,
        settings: Dynaconf,
    ) -> JobContext:
        """
        Helper that implements the 'Dataset > Job' fallback logic.
        """

        def get_val(path: str, default: Any = None) -> Any:
            """
            Hierarchical lookup helper.
            Search priority:
            Dataset (Env) -> Dataset (Default) -> Job (Env) -> Job (Default) -> Fallback
            """
            search_paths = [f"datasets.{dataset_id}.{path}", f"job.{path}"]
            for p in search_paths:
                # 1. Check active environment
                val = settings.get(p)
                if val is not None:
                    return val

                # 2. Check 'default' environment fallback
                val = settings.from_env("default").get(p)
                if val is not None:
                    return val
            return default

        # 1. Resolve full service dictionaries (respecting service_ref)
        source_svc = self._resolve_service(settings, "extract", dataset_id)
        sink_svc = self._resolve_service(settings, "load", dataset_id)
        archive_svc = self._resolve_service(settings, "archive", dataset_id)

        # 2. Extract specific 'type' and leave residual as 'config'
        source_type = source_svc.pop("type", get_val("source.type", "flat_file"))
        sink_type = sink_svc.pop("type", get_val("sink.type", "clickhouse"))
        archive_type = archive_svc.pop("type", get_val("archive.type", "s3"))

        ctx_data = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "run_date": run_date,
            "output_path": f"storage/active/{job_id}/{dataset_id}",
            "extract": {
                "source_type": source_type,
                "source_identifier": get_val("extract.source_identifier")
                or get_val("source_identifier"),
                "num_partitions": get_val("num_partitions", 1),
                "load_mode": get_val("extract.load_mode", "snapshot"),
                "source_config": source_svc,
                "source_params": get_val("extract.source_params", {}),
                "schema_items": get_val("schema_items", []),
            },
            "transform": {
                "transform_type": get_val("transform.type", "default"),
                "transform_params": get_val("transform.options", {}),
            },
            "load": {
                "sink_type": sink_type,
                "sink_identifier": get_val("load.sink_identifier")
                or get_val("target_destination"),
                "sink_config": sink_svc,
                "partition_col": get_val("partition_col", "run_date"),
                "partition_value": get_val("partition_value", run_date),
                "load_params": get_val("load.load_params", {}),
            },
            "archive": {
                "enabled": get_val("archive.enable_archival", True),
                "retention_days": get_val("archive.retention_days", 2555),
                "base_path": get_val("archive.base_path", "/mnt/archive"),
                "type": archive_type,
                "config": archive_svc,
            },
        }

        # Validate via msgspec
        return msgspec.json.decode(msgspec.json.encode(ctx_data), type=JobContext)
        return msgspec.json.decode(msgspec.json.encode(ctx_data), type=JobContext)
