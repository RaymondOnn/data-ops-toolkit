"""Diagnostic utilities for environment health and configuration."""

import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import msgspec
import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.services.database import DatabaseSink
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import APP_CONFIG_ROOT
from libs.network import NetworkDoctor
from libs.utils.system import get_disk_usage
from loguru import logger

LOG = logger


class Doctor:
    """Diagnostic utility for ingestion engine health checks."""

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        db_resolver: Callable[[], DatabaseSink],
        builder: Any = None,
    ):
        self.exec_ctx = exec_ctx
        self._db_resolver = db_resolver
        self.builder = builder

    @property
    def _db(self) -> DatabaseSink:
        """Lazy-loaded database connection."""
        return self._db_resolver()

    # =========================================================================
    # Filesystem Checks
    # =========================================================================

    def check_filesystem(self, debug: bool = False) -> bool:
        """Verify disk space and directory permissions."""
        LOG.info("🩺 Checking filesystem health...")
        all_ok = self._check_disk_space()

        for path in self._get_managed_paths():
            if not self._verify_path(path):
                all_ok = False

        return all_ok

    def _check_disk_space(self) -> bool:
        """Check available disk space."""
        try:
            usage = get_disk_usage(self.exec_ctx.workspace_dir)
            LOG.info(
                f"✅ Disk: {usage.free_gb:.2f}GB free ({usage.percent:.2f}% used) "
                f"on {self.exec_ctx.workspace_dir}"
            )
            return True
        except Exception:
            LOG.exception("❌ Failed to get disk usage")
            return False

    def _get_managed_paths(self) -> Iterable[Path]:
        """Yield all managed paths for validation."""
        yield from self.exec_ctx.get_managed_directories()
        yield self.exec_ctx.lock_file

        for pex in [self.exec_ctx.code_pex_path, self.exec_ctx.deps_pex_path]:
            if pex:
                yield pex

    def _verify_path(self, path: Path) -> bool:
        """Verify path existence and permissions."""
        if not path or not path.exists():
            if path.suffix == ".pex":
                LOG.warning(f"⚠️ Optional PEX missing: {path}")
                return True
            LOG.error(f"❌ Missing: {path}")
            return False

        try:
            if path.is_dir():
                test_file = path / f".doctor_{os.getpid()}"
                test_file.touch()
                test_file.unlink()
                LOG.info(f"✅ Dir writable: {path}")
            else:
                with path.open("rb") as f:
                    f.read(1)
                LOG.info(f"✅ File readable: {path}")
            return True
        except (OSError, PermissionError):
            LOG.exception(f"❌ Access denied: {path}")
            return False

    # =========================================================================
    # Ray Checks
    # =========================================================================

    def check_ray(self, debug: bool = False) -> bool:
        """Verify Ray connectivity and cluster resources."""
        LOG.info("🩺 Checking Ray connectivity...")

        if ray.is_initialized():
            LOG.info("✅ Ray is initialized")
            try:
                resources = ray.cluster_resources()
                LOG.info(f"✅ Cluster resources: {resources}")
                return True
            except Exception:
                LOG.exception("❌ Failed to get resources")
                return False

        LOG.info("⚠️ Ray not initialized - testing local mode...")
        try:
            ray.init(local_mode=True, ignore_reinit_error=True, log_to_driver=False)
            LOG.info("✅ Ray local mode works")
            ray.shutdown()
            return True
        except Exception:
            LOG.exception("❌ Ray initialization failed")
            return False

    # =========================================================================
    # PEX Checks
    # =========================================================================

    def check_pex_files(self, debug: bool = False) -> bool:
        """Verify PEX file existence."""
        LOG.info("🩺 Checking PEX files...")
        all_ok = True

        if self.exec_ctx.code_pex_path:
            if self.exec_ctx.code_pex_path.exists():
                LOG.info(f"✅ Code PEX: {self.exec_ctx.code_pex_path}")
            else:
                LOG.warning(f"⚠️ Code PEX missing: {self.exec_ctx.code_pex_path}")
                all_ok = False
        else:
            LOG.info("No code PEX configured")

        if self.exec_ctx.deps_pex_path:
            if self.exec_ctx.deps_pex_path.exists():
                LOG.info(f"✅ Deps PEX: {self.exec_ctx.deps_pex_path}")
            else:
                LOG.warning(f"⚠️ Deps PEX missing: {self.exec_ctx.deps_pex_path}")
                all_ok = False
        else:
            LOG.info("No deps PEX configured")

        return all_ok

    # =========================================================================
    # Configuration Checks
    # =========================================================================

    def check_config(self, job_id: str | None = None, all_jobs: bool = False) -> bool:
        """Validate app.yaml and job configs."""
        LOG.info("🩺 Validating configuration...")

        if not self.builder:
            LOG.error("❌ Builder not available")
            return False

        all_ok = self._validate_app_config()

        targets = self._resolve_job_targets(job_id, all_jobs)
        for tid in targets:
            if not self._validate_job(tid):
                all_ok = False

        return all_ok

    def _validate_app_config(self) -> bool:
        """Validate global app.yaml."""
        try:
            required = ["workspace_dir", "cache", "services"]
            missing = [k for k in required if k not in self.builder.app_settings]
            if missing:
                LOG.error(f"❌ app.yaml missing: {', '.join(missing)}")
                return False
            LOG.info(f"✅ app.yaml valid (env={self.exec_ctx.env})")
            return True
        except Exception:
            LOG.exception("❌ app.yaml error")
            return False

    def _resolve_job_targets(self, job_id: str | None, all_jobs: bool) -> list[str]:
        """Resolve job targets for validation."""
        if all_jobs and APP_CONFIG_ROOT.exists():
            return [
                d.name
                for d in APP_CONFIG_ROOT.iterdir()
                if d.is_dir() and (d / "config.yaml").exists()
            ]
        return [job_id] if job_id else []

    def _validate_job(self, job_id: str) -> bool:
        """Validate a single job configuration."""
        LOG.info(f"🧐 Validating job: {job_id}")
        job_root = APP_CONFIG_ROOT / job_id

        try:
            contexts = self.builder.build(job_id=job_id)
            LOG.info("   ✅ config.yaml valid")

            for ctx in contexts:
                if not ctx.extract.schema_file:
                    continue
                schema_path = job_root / ctx.extract.schema_file
                if not schema_path.exists():
                    LOG.error(f"   ❌ Missing schema: {ctx.extract.schema_file}")
                    return False
                LOG.debug(f"   ✅ Schema: {ctx.extract.schema_file}")
            return True
        except Exception:
            LOG.exception("   ❌ Validation failed")
            return False

    def inspect_merged_config(self, job_id: str, dataset_id: str | None = None) -> None:
        """Print merged configuration for a job/dataset."""
        LOG.info(f"🧐 Inspecting merged config for job: {job_id}")
        try:
            contexts = self.builder.build(job_id=job_id, dataset_id=dataset_id)
            for ctx in contexts:
                formatted = msgspec.json.format(msgspec.json.encode(ctx))
                print(formatted.decode())
        except Exception:
            LOG.exception("❌ Failed to resolve config")

    # =========================================================================
    # Service Checks
    # =========================================================================

    def check_service(self, service_type: str) -> bool:
        """Test connectivity for all services of a given type."""
        LOG.info(f"🩺 Testing service type: '{service_type}'")

        if not self.builder:
            LOG.error("❌ Builder not available")
            return False

        self._init_secret_provider()
        services = self.builder.app_settings.get("services", {})

        found = False
        all_ok = True

        for name, config in services.items():
            cfg = config.to_dict() if hasattr(config, "to_dict") else dict(config)
            if cfg.get("type") != service_type:
                continue

            found = True
            LOG.info(f"   🔎 Testing: {name}")
            if self._test_service_connection(service_type, cfg):
                LOG.success(f"   ✅ Reachable: {name}")
            else:
                LOG.error(f"   ❌ Failed: {name}")
                all_ok = False

        if not found:
            LOG.warning(f"⚠️ No services of type '{service_type}' found")
            return False

        return all_ok

    def _init_secret_provider(self) -> None:
        """Initialize secret provider if available."""
        try:
            provider_cfg = self.builder.app_settings.get(
                "secret_provider", {}
            ).to_dict()
            ServiceFactory.get_provider(provider_cfg)
        except Exception as e:
            LOG.debug(f"Secret provider init skipped: {e}")

    def _test_service_connection(self, service_type: str, config: dict) -> bool:
        """Test a single service connection."""
        try:
            svc = ServiceFactory.get(**config)
            if hasattr(svc, "fetch"):
                svc.fetch("SELECT 1")
            elif hasattr(svc, "client") and hasattr(svc.client, "sql"):
                svc.client.sql("SELECT 1")
            return True
        except Exception:
            return False

    # =========================================================================
    # Network Checks
    # =========================================================================

    def check_network_path(
        self, host: str, port: int, proxy_url: str | None = None
    ) -> bool:
        """Deep network diagnostic to target host."""
        proxy = proxy_url or "http://127.0.0.1:3128"
        LOG.info(f"🩺 Network diagnostic to {host}:{port} via {proxy}")
        doctor = NetworkDoctor(target_host=host, target_port=port, proxy_url=proxy)
        return doctor.run_diagnostics()

    def trace_route(self, host: str) -> bool:
        """Visualize network hops to target host."""
        doctor = NetworkDoctor(target_host=host, target_port=0)
        return doctor.trace_route()

    # =========================================================================
    # All Checks
    # =========================================================================

    def run_all(self, debug: bool = False) -> bool:
        """Run all diagnostic checks."""
        LOG.info("--- Starting Comprehensive Doctor Check ---")

        results = [
            self.check_config(debug=debug),
            self.check_filesystem(debug),
            self.check_service("clickhouse_db"),
            self.check_ray(debug),
            self.check_pex_files(debug),
        ]

        all_ok = all(results)
        LOG.info(f"--- Doctor Check: {'✅ PASS' if all_ok else '❌ FAIL'} ---")
        return all_ok
