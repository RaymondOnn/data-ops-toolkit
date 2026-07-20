"""Compute resource management for Ray-based task execution.

Manages resource allocation, workload costing, and worker spawning with
adaptive backpressure based on system health.
"""

import sys
from enum import StrEnum

import psutil
import ray
import ray.util.state
from apps.ingestion.src.core.contexts import RayMode
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.system import HealthStatus, SystemMonitor
from loguru import logger

LOG = logger

# Resource mapping between logical and Ray names
RAY_RESOURCE_NAMES = {"CPU": "CPU", "IO": "IO", "MEM": "memory"}


class WorkloadClass(StrEnum):
    """Classification of workload types for resource costing."""

    DEFAULT = "default"
    CPU_INTENSIVE = "cpu"
    IO_INTENSIVE = "io"


# Resource costs per workload class (CPU cores, IO slots, Memory GB)
WORKLOAD_COSTS = {
    WorkloadClass.DEFAULT: {"CPU": 0.2, "IO": 0.1, "MEM": 0.5},
    WorkloadClass.CPU_INTENSIVE: {"CPU": 1.0, "IO": 0.2, "MEM": 2.0},
    WorkloadClass.IO_INTENSIVE: {"CPU": 0.2, "IO": 1.0, "MEM": 1.0},
}

# Map pipeline stages to workload classes
STAGE_WORKLOAD_MAP = {
    Stage.START: WorkloadClass.DEFAULT,
    Stage.EXTRACT: WorkloadClass.IO_INTENSIVE,  # Network I/O heavy
    Stage.TRANSFORM: WorkloadClass.CPU_INTENSIVE,  # Computation heavy
    Stage.WRITE: WorkloadClass.IO_INTENSIVE,
    Stage.PUBLISH: WorkloadClass.IO_INTENSIVE,
    Stage.ARCHIVE: WorkloadClass.DEFAULT,
}


class Compute:
    """Manages Ray cluster resources and worker scheduling."""

    def __init__(self, exec_ctx: ExecutionContext):
        """Initializes the compute manager and provisions Ray resources.

        Args:
            exec_ctx: The global execution context.

        """
        self.exec_ctx = exec_ctx
        self._resource_limits = self._get_resource_limits()

        self._init_ray_cluster()
        self._purge_orphaned_tasks()

    @property
    def available_resources(self) -> dict[str, float]:
        return ray.cluster_resources() if ray.is_initialized() else {}

    @staticmethod
    def calculate_resource_limits(
        cpu_ratio: float = 0.8,
        memory_ratio: float = 0.7,
    ) -> tuple[int, int, int]:
        """Calculate logical resource limits based on physical hardware.

        Args:
            cpu_ratio: Percentage of physical cores to use (0.0 to 1.0)
            memory_ratio: Percentage of RAM to use (0.0 to 1.0)

        Returns:
            Tuple of (cpu_cores, io_slots, memory_gb)
        """
        physical_cores = psutil.cpu_count(logical=False) or 2
        logical_cores = max(1, int(physical_cores * cpu_ratio))

        # IO slots = cores * 5 (one IO slot can handle multiple concurrent operations)
        io_slots = logical_cores * 5

        total_memory_bytes = psutil.virtual_memory().total
        memory_gb = int((total_memory_bytes / (1024**3)) * memory_ratio)

        return logical_cores, io_slots, memory_gb

    def _get_resource_limits(self) -> dict[str, int]:
        """Calculates and returns the logical resource limits based on hardware.

        Returns:
            dict[str, int]: A dictionary mapping resource names (CPU, IO, MEM)
                to their calculated integer limits.

        Notes:
        - Dynamic resource limits based on physical hardware allows for flexible
        adaptation to different deployment environments without requiring
        manual configuration changes.
        """
        cores, io_slots, memory_gb = self.calculate_resource_limits()
        return {"CPU": cores, "IO": io_slots, "MEM": memory_gb}

    def _init_ray_cluster(self) -> None:
        """Initialize Ray cluster with custom resources."""
        LOG.trace(
            "[DISPATCH] ray init start",
            mode=self.exec_ctx.ray_mode,
            resources=self._resource_limits,
        )
        ray_mode = self.exec_ctx.ray_mode
        address = getattr(self.exec_ctx, "ray_address", None)

        # Local mode cannot use custom address
        if address:
            ray_mode = RayMode.CLUSTER

        LOG.info(f"Initializing Ray (mode={ray_mode})...")

        if not ray.is_initialized():
            # Calculate 50% of Ray's allocated memory (which is in GB) for the object store
            ray_mem_bytes = self._resource_limits["MEM"] * (1024**3)
            target_object_store_size = int(ray_mem_bytes * 0.50)

            if sys.platform == "darwin":
                mac_limit = 2 * (1024**3)  # 2.0 GiB
                target_object_store_size = min(target_object_store_size, mac_limit)

            ctx = ray.init(
                address=address,
                ignore_reinit_error=True,
                local_mode=(ray_mode == RayMode.LOCAL),
                include_dashboard=True,
                dashboard_host="0.0.0.0",
                dashboard_port=8265,
                namespace="ingestion",
                num_cpus=self._resource_limits["CPU"],
                runtime_env={"working_dir": ".", "excludes": ["**/.git", ".venv"]},
                _memory=ray_mem_bytes,
                object_store_memory=target_object_store_size,
                resources={"IO": self._resource_limits["IO"]},
            )
            dashboard_url = (
                ctx.dashboard_url if ray_mode != RayMode.LOCAL else "localhost:8265"
            )
        else:
            dashboard_url = ray.get_runtime_context().dashboard_url

        resources = self.available_resources
        LOG.info(
            f"Ray initialized | CPUs: {resources.get('CPU', 0)} | "
            f"Memory: {resources.get('memory', 0) / (1024**3):.1f}GB | "
            f"Dashboard: http://{dashboard_url}"
        )
        LOG.trace(
            "[DISPATCH] ray init success",
            dashboard_url=dashboard_url,
            resources=resources,
        )

    def _get_workload_cost(self, stage: Stage) -> dict[str, float]:
        """Retrieves the estimated resource cost for a given pipeline stage.

        Args:
            stage: The pipeline stage (e.g., EXTRACT, TRANSFORM).

        Returns:
            dict[str, float]: A dictionary mapping resource types (CPU, IO, MEM)
                to their estimated cost for the given stage.

        Decision: Workload-Driven Costing.
        Different stages have different resource profiles. By assigning
        workload classes, we can more accurately model the resource demands
        of each stage, leading to better scheduling decisions.
        """
        workload = STAGE_WORKLOAD_MAP.get(stage, WorkloadClass.DEFAULT)
        return WORKLOAD_COSTS[workload]

    def has_capacity(self, stage: Stage) -> bool:
        """Check if enough resources are available to spawn a worker."""
        # 1. Leverage system.py to detect host degradation, critical state, or blockages
        # Eliminates the duplicate psutil checks and local threshold variables!
        health_report = SystemMonitor.force_refresh()
        if health_report and (
            health_report.disk_status in (HealthStatus.CRITICAL, HealthStatus.BLOCKED)
            or health_report.memory_status
            in (HealthStatus.CRITICAL, HealthStatus.BLOCKED)
        ):
            LOG.warning(
                f"Backpressure engaged: System state is {health_report.status.value.upper()}"
            )
            return False

        # 2. Proceed with Ray scheduling checks if cluster is active
        if self.exec_ctx.ray_mode == RayMode.LOCAL:
            return True

        if not ray.is_initialized():
            return False

        available = self.available_resources
        cost = self._get_workload_cost(stage)

        for resource_type, cost_amt in cost.items():
            ray_resource = RAY_RESOURCE_NAMES.get(resource_type, resource_type)
            if not resource_type:
                continue

            if available.get(ray_resource, 0) < cost_amt:
                LOG.trace(
                    f"Insufficient cluster resource: {resource_type} (Required: {cost_amt})"
                )
                return False

        return True

    def _purge_orphaned_tasks(self) -> None:
        """Terminates any orphaned Ray tasks left over from previous runs.

        Notes:
        - During startup, the Compute manager actively scans for and cancels
        any Ray tasks that might have been left running by a crashed or
        improperly shut down orchestrator. This prevents 'ghost' tasks
        from consuming resources or interfering with new runs.
        - Only target tasks whose names start with a known pipeline stage
        label, avoiding the accidental termination of unrelated Ray actors.
        """
        try:
            if not ray.is_initialized():
                return

            all_tasks = ray.util.state.list_tasks(filters=[("state", "=", "RUNNING")])
            stage_labels = {s.value for s in Stage}
            purged = 0

            for task in all_tasks:
                name = task.get("name", "")
                if name and "_" in name and name.split("_")[0] in stage_labels:
                    ray.cancel(task["task_id"], force=True)
                    purged += 1

            if purged > 0:
                LOG.info(f"Purged {purged} orphaned Ray tasks")
        except Exception as e:
            LOG.debug(f"Orphan purge skipped: {e}")
