from collections import ChainMap
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import msgspec
from apps.ingestion.src.core.contexts.execution import ExecutionContext, ExecutionMode
from apps.ingestion.src.core.contexts.job import TaskContext
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    APP_CURRENT_ENV,
    DEFAULT_PARTITION_COL,
)
from dateutil.relativedelta import relativedelta
from dynaconf import Dynaconf
from loguru import logger

LOG = logger
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
        settings: A set of strings provided via the '--set' or '-s' CLI flag.

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


class TaskContextBuilder:
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
        self._settings_cache: dict[str, Dynaconf] = {}

        # --- DEBUG INSTRUMENTATION ---
        LOG.debug(f"DEBUG: Config Path Absolute: {Path(self.app_cfg_path).resolve()}")
        LOG.debug(f"DEBUG: File Exists: {Path(self.app_cfg_path).exists()}")
        LOG.debug(
            f"DEBUG: Loaded Keys: {', '.join(filter(lambda x: 'DYNACONF' not in x, set(self.app_settings.keys())))}"
        )

        try:
            LOG.info("Resolved App Config", config=self.app_settings.to_dict())
        except Exception:
            LOG.warning("Could not serialize app config for logging")

    def _resolve_partition_date(
        self,
        spec: dict[str, Any],
        partition_date_str: str | None = None,
    ) -> str | None:
        """Resolves T-x logic into a formatted string."""
        if partition_date_str:
            date_val = datetime.strptime(partition_date_str, "%Y-%m-%d")
        elif spec:
            # Use app-level timezone or default to Asia/Singapore
            tz_name = self.app_settings.get("timezone", "Asia/Singapore")
            base_date = datetime.now(ZoneInfo(tz_name))

            # Support multi-unit offsets (years, months, days)
            offset = spec.get("offset", {})

            # We merge with the legacy 'offset_days' for backward compatibility
            date_val = base_date + relativedelta(
                years=offset.get("years", 0),
                months=offset.get("months", 0),
                days=offset.get("days", spec.get("offset_days", 0)),
            )
        else:
            return None

        fmt = date_val.strftime(spec.get("format", "%Y-%m-%d"))
        formatted_date = date_val.strftime(fmt)
        return (
            f"'{formatted_date}'" if spec.get("wrap_quotes", False) else formatted_date
        )

    def get_execution_context(
        self, mode: ExecutionMode = ExecutionMode.NORMAL
    ) -> ExecutionContext:
        """Resolves the global app settings into a typed context."""
        workspace = Path(self.app_settings.get("workspace_dir")).expanduser().resolve()

        # Ensure the base workspace directory exists so lock files
        # and subdirectories can be created safely.
        workspace.mkdir(parents=True, exist_ok=True)

        cache_cfg = self.app_settings.get("cache").to_dict()

        return ExecutionContext(
            workspace_dir=workspace,
            execution_mode=mode,
            env=self.env,
            cache_config=cache_cfg,
        )

    # TODO: Skip archive if enable_archival = False
    def _resolve_service(
        self, settings: Dynaconf, ref_key: str, dataset_id: str
    ) -> dict:
        # Search hierarchy for service_ref:
        # Dataset (Active -> Default) > Task (Active -> Default)
        ref = (
            settings.get(f"datasets.{dataset_id}.{ref_key}.service_ref")
            or settings.from_env("default").get(
                f"datasets.{dataset_id}.{ref_key}.service_ref"
            )
            or settings.get(f"job.{ref_key}.service_ref")
            or settings.from_env("default").get(f"job.{ref_key}.service_ref")
        )

        LOG.debug(
            "Resolving service reference",
            ref_key=ref_key,
            ref=ref,
            dataset_id=dataset_id,
        )

        if ref:
            # 1. Try Global app.yaml (Active env, then fallback to default)
            global_def = self.app_settings.get(
                f"services.{ref}"
            ) or self.app_settings.from_env("default").get(f"services.{ref}")

            if global_def:
                svc_dict = global_def.to_dict()
                LOG.debug(
                    "Found service definition in global app.yaml",
                    ref=ref,
                    env=self.app_settings.current_env,
                    config=svc_dict,
                )
                return svc_dict

            # 2. Try Task-level config.yaml services block
            job_level_def = settings.get(f"services.{ref}") or settings.from_env(
                "default"
            ).get(f"services.{ref}")

            if job_level_def:
                svc_dict = job_level_def.to_dict()
                LOG.debug(
                    "Found service definition in job config.yaml",
                    ref=ref,
                    config=svc_dict,
                )
                return svc_dict

            LOG.warning(
                f"Service reference '{ref}' found, "
                "but no definition exists in services block.",
                ref_key=ref_key,
            )

            # Fallback to inline config block:
            # Dataset (Active -> Default) > Task (Active -> Default)
            svc_dict = dict(
                settings.get(f"datasets.{dataset_id}.{ref_key}.config")
                or settings.from_env("default").get(
                    f"datasets.{dataset_id}.{ref_key}.config"
                )
                or settings.get(f"job.{ref_key}.config")
                or settings.from_env("default").get(f"job.{ref_key}.config")
                or {}
            )

            if svc_dict is not None and "type" not in svc_dict:
                LOG.warning(
                    f"Service dictionary for '{ref}' is missing the required 'type' key. "
                    "Factory initialization will likely fail.",
                    dataset_id=dataset_id,
                    config=svc_dict,
                )
            return svc_dict

        LOG.critical(
            f"No service reference found for '{ref_key}' in dataset '{dataset_id}'. "
            "Falling back to inline configuration if available."
        )
        return {}

    def _load_schema_file(self, job_id: str, schema_file: str) -> list[dict[str, Any]]:
        """
        Reads a CSV schema file from the job's config directory.
        Expected format: CSV with headers matching the required schema contract.
        """
        import csv

        schema_path = APP_CONFIG_ROOT / job_id / schema_file
        if not schema_path.exists():
            LOG.warning(
                "Schema file defined but not found on disk", path=str(schema_path)
            )
            return []

        LOG.debug("Loading schema from file", path=str(schema_path))
        with schema_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def build(
        self,
        job_id: str,
        dataset_id: str | None = None,
        partition_date_str: str | None = None,
        overrides_json: Path | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> list[TaskContext]:
        """Maps merged config into a set of msgspec TaskContext objects."""
        LOG.debug(
            "Building job contexts", job_id=job_id, partition_date=partition_date_str
        )

        task_cfg_path = APP_CONFIG_ROOT / job_id / "config.yaml"

        # 1. Initialize Dynaconf with job-specific overrides (cached)
        cache_key = f"{job_id}:{overrides_json}"
        if cache_key not in self._settings_cache:
            settings_files = [task_cfg_path]
            if overrides_json:
                settings_files.append(overrides_json)

            self._settings_cache[cache_key] = Dynaconf(
                envvar_prefix="APP",
                argv_prefix="--APP",
                settings_files=settings_files,
                environments=True,
                env=self.env,
                load_dotenv=True,
            )

        settings = self._settings_cache[cache_key]

        # 2. Establish partition_date
        # Priority: partition_date_str > CLI --set partition_date > today
        partition_date = (
            self._resolve_partition_date(
                settings.get("partition_date_spec", {}), partition_date_str
            )
            or settings.get("partition_date")
            or datetime.now().astimezone().strftime("%Y-%m-%d")
        )

        # 4. Get the Task-level defaults and the Dataset-level specifics
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

            # 5. Build the context using the resolution: Dataset Spec > Task Default
            ctx = self._create_task_context(
                job_id=job_id,
                dataset_id=ds_id,
                partition_date=partition_date,
                # Pass the full settings object to resolve global service refs
                settings=settings,
                overrides=overrides,
            )

            contexts.append(ctx)

        LOG.debug("Resolved contexts", count=len(contexts), job_id=job_id)
        return contexts

    def _create_task_context(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        settings: Dynaconf,
        overrides: dict[str, Any] | None = None,
    ) -> TaskContext:
        """
        Helper that implements the 'Dataset > Task' fallback logic.
        """

        def get_val(path: str, default: Any = None) -> Any:
            """
            Hierarchical lookup helper.
            Search priority:
            1. Job Config: Dataset (Active Env) -> Dataset (Default)
            2. Job Config: Job-wide (Active Env) -> Job-wide (Default)
            3. App Config: Global Environment-specific (e.g. 'local')
            4. App Config: Global Defaults
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
            # 2. Fallback to global app.yaml (respecting active environment)
            val = self.app_settings.get(path)
            if val is not None:
                return val

            return default

        # 0. Handle Schema File Loading
        schema_file = get_val("extract.schema_file")
        schema_items = get_val("schema_items", [])
        if schema_file:
            schema_items = self._load_schema_file(job_id, schema_file)

        # 1. Resolve full service dictionaries (respecting service_ref)
        # pop() extracts specific 'type' and leave residual as 'config'
        source_svc = self._resolve_service(settings, "extract", dataset_id)
        source_type = (
            source_svc.pop("type", get_val("extract.source_type"))
            if source_svc
            else get_val("extract.source_type")
        )

        sink_svc = self._resolve_service(settings, "load", dataset_id)
        sink_type = (
            sink_svc.pop("type", get_val("load.sink_type"))
            if sink_svc
            else get_val("load.sink_type")
        )

        if enable_archival := get_val("archive.enable_archival"):
            archive_svc = self._resolve_service(settings, "archive", dataset_id)
            archive_type = (
                archive_svc.pop("type", get_val("archive.archive_type"))
                if archive_svc
                else get_val("archive.archive_type")
            )

            retention_days = get_val("archive.retention_days")
            base_path = get_val("archive.base_path")
        else:
            archive_svc = {}
            archive_type = None
            retention_days = None
            base_path = None

        ctx_data = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "partition_date": partition_date,
            "output_path": f"storage/active/{job_id}/{dataset_id}",
            "extract": {
                "source_type": source_type,
                "source_identifier": get_val("extract.source_identifier")
                or get_val("source_identifier"),
                "num_workers": get_val("extract.num_workers"),
                "load_mode": get_val("extract.load_mode"),
                "source_config": source_svc,
                "source_params": get_val("extract.source_params", {}),
                "schema_items": schema_items,
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
                "partition_col": get_val("load.partition_col", DEFAULT_PARTITION_COL),
                "partition_value": get_val("load.partition_value", partition_date),
                "load_params": get_val("load.load_params", {}),
            },
            "archive": {
                "enabled": enable_archival,
                "retention_days": retention_days,
                "base_path": base_path,
                "type": archive_type,
                "config": archive_svc,
            },
        }

        # 3. Final Layer: Apply Runtime CLI Overrides (--set) BEFORE freezing
        if overrides:
            active_overrides = ChainMap(
                overrides.get(dataset_id, {}), overrides.get("_global", {})
            )
            for key, value in active_overrides.items():
                if key in ctx_data:
                    ctx_data[key] = value
                else:
                    ctx_data.setdefault("custom_overrides", {})[key] = value

        # Validate via msgspec
        return msgspec.json.decode(msgspec.json.encode(ctx_data), type=TaskContext)
        return msgspec.json.decode(msgspec.json.encode(ctx_data), type=TaskContext)
