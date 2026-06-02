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
from libs.utils.network import NetworkDoctor
from libs.utils.system import get_disk_usage
from loguru import logger

LOG = logger


class Doctor:
    """
    A diagnostic utility to check the health and configuration of the ingestion engine.
    """

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
    def db(self) -> DatabaseSink:
        """Lazy-loaded database connection."""
        return self._db_resolver()

    def _get_all_managed_paths(self) -> Iterable[Path]:
        """Yields all directories and files managed by the orchestrator."""
        yield from self.exec_ctx.get_managed_directories()
        yield self.exec_ctx.lock_file
        for p in [self.exec_ctx.code_pex_path, self.exec_ctx.deps_pex_path]:
            if p:
                yield p

    def check_filesystem(self, debug: bool = False) -> bool:
        """Checks existence, permissions, and disk space for critical directories."""
        LOG.info("🩺 Checking Filesystem Health...")
        all_ok = True

        # 1. Check Disk Space
        try:
            usage = get_disk_usage(self.exec_ctx.workspace_dir)
            LOG.info(
                f"✅ Disk space: {usage.free_gb:.2f}GB free "
                f"({usage.percent:.2f}% used) on {self.exec_ctx.workspace_dir}"
            )
        except Exception as e:
            LOG.error(f"❌ Failed to get disk usage: {e}")
            all_ok = False

        # 2. Check Managed Paths
        for path in self._get_all_managed_paths():
            if not self._validate_path_permissions(path):
                all_ok = False
        return all_ok

    def _validate_path_permissions(self, path: Path) -> bool:
        """Helper to verify existence and access levels for a path."""
        if not path:
            return True
        if not path.exists():
            if path.suffix == ".pex":
                LOG.warning(f"⚠️  Missing: {path} (Optional)")
                return True
            LOG.error(f"❌ Missing: {path}")
            return False

        try:
            if path.is_dir():
                # Test Writability
                test_file = path / f".doctor_{os.getpid()}"
                test_file.touch()
                test_file.unlink()
                LOG.info(f"✅ Dir:  {path} (Writable)")
            else:
                # Test Readability
                with path.open("rb") as f:
                    f.read(1)
                LOG.info(f"✅ File: {path} (Readable)")
            return True
        except (OSError, PermissionError) as e:
            LOG.error(f"❌ Access Denied: {path} ({e})")
            return False

    def check_ray_connectivity(self, debug: bool = False) -> bool:
        """Checks if Ray is initialized and reports cluster resources."""
        LOG.info("🩺 Checking Ray Connectivity...")
        if ray.is_initialized():
            LOG.info("✅ Ray is already initialized.")
            try:
                resources = ray.cluster_resources()
                LOG.info(f"✅ Ray Cluster Resources: {resources}")
                return True
            except Exception as e:
                LOG.error(f"❌ Failed to get Ray cluster resources: {e}")
                return False
        else:
            LOG.info("⚠️ Ray is not initialized. Attempting to start in local mode...")
            try:
                # Try to init Ray in local mode to verify basic setup
                ray.init(local_mode=True, ignore_reinit_error=True, log_to_driver=False)
                LOG.info("✅ Ray initialized successfully in local mode.")
                ray.shutdown()
                LOG.info("✅ Ray shut down cleanly.")
                return True
            except Exception as e:
                LOG.error(f"❌ Failed to initialize Ray: {e}")
                LOG.warning(
                    "Hint: Ensure Ray is installed and its dependencies are met. "
                    "If running in cluster mode, ensure 'ray start --head' is active."
                )
                return False

    def check_pex_files(self, debug: bool = False) -> bool:
        """Verifies the existence of application and dependency PEX files."""
        LOG.info("🩺 Checking PEX Files...")
        all_ok = True
        if self.exec_ctx.code_pex_path:
            if self.exec_ctx.code_pex_path.exists():
                LOG.info(f"✅ Code PEX found: {self.exec_ctx.code_pex_path}")
            else:
                LOG.warning(f"⚠️ Code PEX not found: {self.exec_ctx.code_pex_path}")
                all_ok = False
        else:
            LOG.info("Code PEX path not configured.")

        if self.exec_ctx.deps_pex_path:
            if self.exec_ctx.deps_pex_path.exists():
                LOG.info(f"✅ Deps PEX found: {self.exec_ctx.deps_pex_path}")
            else:
                LOG.warning(f"⚠️ Deps PEX not found: {self.exec_ctx.deps_pex_path}")
                all_ok = False
        else:
            LOG.info("Deps PEX path not configured.")

        return all_ok

    def inspect_config(self, job_id: str, dataset_id: str | None = None) -> None:
        """Resolves and prints the fully merged configuration for a job/dataset."""
        LOG.info(f"🧐 Inspecting merged configuration for job: {job_id}")
        try:
            # This performs the full hierarchical merge
            contexts = self.builder.build(job_id=job_id, dataset_id=dataset_id)

            for ctx in contexts:
                # Pretty-print the msgspec Struct as JSON
                formatted_json = msgspec.json.format(msgspec.json.encode(ctx))
                print(formatted_json.decode("utf-8"))
        except Exception as e:
            LOG.error(f"❌ Failed to resolve merged configuration: {e}")

    def run_network_diagnostics(
        self, host: str, port: int, proxy_url: str | None = None
    ) -> bool:
        """Leverages NetworkDoctor for a deep-dive diagnostic of the connection path."""
        proxy = proxy_url or "http://127.0.0.1:3128"
        LOG.info(f"🩺 Initiating deep network diagnostic for {host}:{port} via {proxy}")
        doctor = NetworkDoctor(target_host=host, target_port=port, proxy_url=proxy)
        return doctor.run_diagnostics()

    def run_network_trace(self, host: str) -> bool:
        """Visualizes the network hops to the target host."""
        # Port is not needed for standard traceroute binaries
        doctor = NetworkDoctor(target_host=host, target_port=0)
        return doctor.trace_route()

    def check_service_connectivity(self, service_type: str) -> bool:
        """
        Tests connectivity for all configured services matching the requested type.
        """
        LOG.info(f"🩺 Testing service type: '{service_type}'...")

        if not self.builder:
            LOG.error("❌ TaskContextBuilder missing.")
            return False

        services_config = self.builder.app_settings.get("services", {})
        found, all_ok = False, True

        # Ensure Secret Provider is initialized for the current environment
        try:
            provider_cfg = self.builder.app_settings.get(
                "secret_provider", {}
            ).to_dict()
            ServiceFactory.get_provider(self.exec_ctx.env, provider_cfg)
        except Exception as e:
            LOG.warning(f"SecretProvider initialization skipped: {e}")

        for name, config in services_config.items():
            cfg = config.to_dict() if hasattr(config, "to_dict") else dict(config)
            if cfg.get("type") != service_type:
                continue

            found = True
            LOG.info(f"   🔎 Testing instance: {name}")
            try:
                svc = ServiceFactory.get_service(service_type=service_type, **cfg)

                # Explicit Ping
                if hasattr(svc, "fetch"):
                    svc.fetch("SELECT 1")
                elif hasattr(svc, "client") and hasattr(svc.client, "sql"):
                    svc.client.sql("SELECT 1")

                LOG.success(f"   ✅ Reachable: {name}")
            except Exception as e:
                LOG.error(f"   ❌ Failed: {name} ({e})")
                all_ok = False

        if not found:
            LOG.warning(f"⚠️  No services of type '{service_type}' found.")
            return False
        return all_ok

    def check_config(
        self, job_id: str | None = None, all_jobs: bool = False, debug: bool = False
    ) -> bool:
        """Validates the global app.yaml and job-specific config.yaml files."""
        LOG.info("🩺 Validating Configuration Integrity...")
        if not self.builder:
            LOG.error("❌ TaskContextBuilder missing.")
            return False

        all_ok = True

        # 1. Validate Global App Config (app.yaml)
        try:
            critical_keys = ["workspace_dir", "cache", "services"]
            missing = [k for k in critical_keys if k not in self.builder.app_settings]
            if missing:
                LOG.error(f"❌ app.yaml missing keys: {', '.join(missing)}")
                all_ok = False
            else:
                LOG.info(f"✅ app.yaml is valid (Env: {self.exec_ctx.env})")
        except Exception as e:
            LOG.error(f"❌ app.yaml parse error: {e}")
            all_ok = False

        # 2. Resolve target Job IDs
        targets = []
        if all_jobs and APP_CONFIG_ROOT.exists():
            targets = [
                i.name
                for i in APP_CONFIG_ROOT.iterdir()
                if i.is_dir() and (i / "config.yaml").exists()
            ]
        elif job_id:
            targets.append(job_id)

        if not targets:
            LOG.info("No job-specific IDs provided; skipping job validation.")
            return all_ok

        # 3. Validate Target Jobs
        for tid in targets:
            if not self._validate_job_config(tid, debug):
                all_ok = False
        return all_ok

    def _validate_job_config(self, job_id: str, debug: bool) -> bool:
        """Validates a single job's config and associated schema files."""
        LOG.info(f"🧐 Validating Job: {job_id}")
        job_root = APP_CONFIG_ROOT / job_id
        try:
            contexts = self.builder.build(job_id=job_id)
            LOG.info("   ✅ config.yaml is valid.")
            for ctx in contexts:
                if not ctx.extract.schema_file:
                    continue
                spath = job_root / ctx.extract.schema_file
                if not spath.exists():
                    LOG.error(f"      ❌ Missing Schema: {ctx.extract.schema_file}")
                    return False
                LOG.debug(f"      ✅ Schema verified: {ctx.extract.schema_file}")
            return True
        except Exception as e:
            LOG.error(f"   ❌ Validation failed: {e}")
            return False

    def check_all(self, debug: bool = False) -> bool:
        """Runs all available diagnostic checks."""
        LOG.info("--- Starting Comprehensive Doctor Check ---")
        results = [
            self.check_config(debug=debug),
            self.check_filesystem(debug),
            self.check_service_connectivity("clickhouse_db"),
            self.check_ray_connectivity(debug),
            self.check_pex_files(debug),
        ]
        ok = all(results)
        LOG.info(f"--- Doctor Check Complete: {'✅ OK' if ok else '❌ FAIL'} ---")
        return ok
