"""Configuration builder for task contexts with hierarchical resolution."""

import os
import re
from collections import ChainMap
from pathlib import Path
from typing import Any

import msgspec
import pendulum
from apps.ingestion.src.core.contexts.execution import ExecutionContext, ExecutionMode
from apps.ingestion.src.core.contexts.task import (
    ArchiveConfig,
    ColumnMapping,
    ExtractConfig,
    LoadConfig,
    TaskContext,
    TransformConfig,
)
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    APP_CURRENT_ENV,
    DEFAULT_PARTITION_COL,
)
from dynaconf import Dynaconf
from libs.utils.dates import current_timestamp
from loguru import logger

LOG = logger
DEFAULT_CONFIG_PATH = APP_CONFIG_ROOT / "app.yaml"


def interpolate_env_vars(value: Any) -> Any:
    """Recursively resolve ${VAR:-DEFAULT}, ${VAR}, or $VAR syntax."""
    if isinstance(value, dict):
        return {k: interpolate_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_env_vars(v) for v in value]

    if not isinstance(value, str) or "$" not in value:
        return value

    pattern = re.compile(r"\$?\$\{([^:-]+)(?::-([^}]*))?\}|\$([a-zA-Z_][a-zA-Z0-9_]*)")

    def replacer(match):
        var_name = match.group(1) or match.group(3)
        default = match.group(2)
        return os.getenv(var_name, default if default is not None else "")

    return pattern.sub(replacer, value)


def parse_cli_overrides(settings: list[str] | None) -> dict[str, Any]:
    """
    Parse CLI '--set' options into scoped overrides.

    Format:
        - Global: key=value
        - Scoped: dataset:key=value

    Returns:
        {"_global": {...}, "dataset_name": {...}}
    """
    result: dict[str, Any] = {"_global": {}}
    if not settings:
        return result

    for item in settings:
        if "=" not in item:
            continue

        key, raw_value = item.split("=", 1)
        key = key.strip()
        value = raw_value.strip()

        # Type inference
        if value.lower() in {"true", "false", "none"}:
            value = {"true": True, "false": False, "none": None}[value.lower()]
        elif value.isdigit():
            value = int(value)

        if ":" in key:
            scope, actual_key = key.split(":", 1)
            scope = scope.strip()
            if scope not in result:
                result[scope] = {}
            result[scope][actual_key.strip()] = value
        else:
            result["_global"][key] = value

    return result


class TaskContextBuilder:
    """Builds task contexts from hierarchical configuration sources."""

    def __init__(self, config_path: str | None = None, env: str = APP_CURRENT_ENV):
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self.env = env

        LOG.info(f"Loading config from: {self.config_path}")

        self.app_settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=[self.config_path],
            environments=True,
            env=self.env,
            load_dotenv=True,
        )

        self._settings_cache: dict[str, Dynaconf] = {}
        self._log_config_summary()

    def _log_config_summary(self) -> None:
        """Log loaded configuration keys for debugging."""
        try:
            keys = [k for k in self.app_settings if "DYNACONF" not in k]
            LOG.debug(f"Loaded config keys: {', '.join(keys)}")
        except Exception:
            LOG.debug("Could not log config summary")

    def build_execution_context(
        self, mode: ExecutionMode = ExecutionMode.NORMAL
    ) -> ExecutionContext:
        """Build execution context from app settings."""
        workspace = Path(self.app_settings.get("workspace_dir")).expanduser().resolve()
        workspace.mkdir(parents=True, exist_ok=True)

        return ExecutionContext(
            workspace_dir=workspace,
            execution_mode=mode,
            env=self.env,
            cache_config=self.app_settings.get("cache", {}).to_dict(),
            drain_timeout_secs=self.app_settings.get("drain_timeout_secs", 600),
        )

    def _resolve_partition_date(
        self,
        spec: dict[str, Any],
        override: str | None = None,
    ) -> str | None:
        """Resolve partition date from spec or override."""
        if override:
            return pendulum.from_format(override, "YYYY-MM-DD").strftime("%Y-%m-%d")

        if not spec:
            return None

        tz = self.app_settings.get("timezone", "Asia/Singapore")
        base = current_timestamp(timezone=tz, naive=True)
        offset = spec.get("offset", {})

        date = pendulum.instance(base).add(
            years=offset.get("years", 0),
            months=offset.get("months", 0),
            days=offset.get("days", spec.get("offset_days", 0)),
        )

        fmt = spec.get("format", "%Y-%m-%d")
        result = date.strftime(fmt)

        if spec.get("wrap_quotes", False):
            return f"'{result}'"
        return result

    def _get_nested(
        self, settings: Dynaconf, dataset_id: str, path: str, default: Any = None
    ) -> Any:
        """Get configuration value with hierarchical fallback."""
        # Priority: dataset.<id>.<path> > job.<path> > app.<path>
        search_paths = [f"datasets.{dataset_id}.{path}", f"job.{path}"]

        for sp in search_paths:
            val = settings.get(sp)
            if val is not None:
                return interpolate_env_vars(val)

        val = self.app_settings.get(path)
        return interpolate_env_vars(val) if val is not None else default

    def _resolve_service_ref(
        self, settings: Dynaconf, role: str, dataset_id: str
    ) -> dict:
        """Resolve service configuration by reference or inline."""
        ref = self._get_nested(settings, dataset_id, f"{role}.service_ref")

        if ref:
            svc = self._get_nested(settings, "GLOBAL", f"services.{ref}")
            if svc:
                return svc if isinstance(svc, dict) else svc.to_dict()

            LOG.warning(f"Service reference '{ref}' not found", role=role)

        return self._get_nested(settings, dataset_id, f"{role}.config", default={})

    def _load_schema_file(self, job_id: str, schema_file: str) -> list[dict[str, Any]]:
        """Load schema from CSV file."""
        import csv

        schema_path = APP_CONFIG_ROOT / job_id / schema_file
        if not schema_path.exists():
            LOG.warning(f"Schema file not found: {schema_path}")
            return []

        LOG.debug(f"Loading schema from: {schema_path}")

        mappings = []
        with schema_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    mapping = ColumnMapping.from_csv_row(row)
                    mappings.append(mapping)
                except Exception as e:
                    LOG.error(f"Failed to parse row {row}: {e}")
                    raise

        LOG.debug(f"Loaded {len(mappings)} schema mappings")
        if mappings:
            LOG.debug(
                f"First mapping: source_col={mappings[0].source_col}, "
                f"target_col={mappings[0].target_col}"
            )

        return mappings

    def build(
        self,
        job_id: str,
        dataset_id: str | None = None,
        partition_date: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> list[TaskContext]:
        """Build task contexts for job/dataset."""
        LOG.debug(f"Building contexts for job={job_id}, dataset={dataset_id}")

        config_file = APP_CONFIG_ROOT / job_id / "config.yaml"

        # Load settings with cache
        cache_key = f"{job_id}"
        if cache_key not in self._settings_cache:
            self._settings_cache[cache_key] = Dynaconf(
                envvar_prefix="APP",
                settings_files=[config_file],
                environments=True,
                env=self.env,
                load_dotenv=True,
            )

        settings = self._settings_cache[cache_key]

        # Resolve partition date
        date_spec = settings.get("partition_date_spec", {})
        final_date = self._resolve_partition_date(date_spec, partition_date)
        if not final_date:
            tz = self.app_settings.get("timezone", "Asia/Singapore")
            final_date = current_timestamp(timezone=tz, naive=True).strftime("%Y-%m-%d")

        # Get datasets
        all_datasets = settings.get("datasets", {})
        target_ids = [dataset_id] if dataset_id else list(all_datasets.keys())

        contexts = []
        for ds_id in target_ids:
            if ds_id not in all_datasets:
                LOG.warning(f"Dataset '{ds_id}' not found in job '{job_id}'")
                continue

            ctx = self._build_dataset_context(
                job_id=job_id,
                dataset_id=ds_id,
                partition_date=final_date,
                settings=settings,
                overrides=overrides,
            )
            contexts.append(ctx)

        LOG.debug(f"Built {len(contexts)} contexts")
        return contexts

    def _build_dataset_context(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        settings: Dynaconf,
        overrides: dict[str, Any] | None = None,
    ) -> TaskContext:
        """Build a single task context."""

        def get(p: str, default: Any = None) -> Any:
            return self._get_nested(settings, dataset_id, p, default)

        # Load schema
        schema = get("schema", [])
        if schema_file := get("extract.schema_file"):
            schema = self._load_schema_file(job_id, schema_file)

        # Resolve services
        services = {
            role: self._resolve_service_ref(settings, role, dataset_id)
            for role in ("extract", "load", "archive")
        }

        ctx_data = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "partition_date": partition_date,
            "output_path": f"storage/active/{job_id}/{dataset_id}",
            "extract": ExtractConfig.from_params(
                source_params=get("extract.source_params", {}),
                service=services["extract"],
                schema=schema,
                num_workers=get("num_workers", 10),
                load_mode=get("extract.load_mode", "snapshot"),
            ),
            "transform": TransformConfig.from_params(
                transform_type=get("transform.type", "default"),
                transform_params=get("transform.options", {}),
                # source_dir=get("transform.source_dir", None),
            ),
            "load": LoadConfig.from_params(
                sink_params=get("load.sink_params", {}),
                service=services["load"],
                partition_by=get("load.partition_by", DEFAULT_PARTITION_COL),
                partition_value=get("load.partition_value", partition_date),
            ),
            "archive": ArchiveConfig.from_params(
                archive_enabled=get("archive.enable_archival", False),
                archive_params=get("archive.archive_params", {}),
                service=services["archive"],
                retention_days=get("archive.retention_days", None),
                base_path=get("archive.base_path", None),
                type=get("archive.archive_type", None),
            ),
            "flags": get("feature_flags", {}),
        }

        # Apply overrides
        if overrides:
            active = ChainMap(
                overrides.get(dataset_id, {}), overrides.get("_global", {})
            )
            custom = {}
            for key, value in active.items():
                if key in ctx_data:
                    ctx_data[key] = value
                else:
                    custom[key] = value
            if custom:
                ctx_data["custom_overrides"] = custom

        return msgspec.convert(ctx_data, type=TaskContext)
