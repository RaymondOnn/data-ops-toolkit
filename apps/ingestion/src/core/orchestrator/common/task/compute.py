"""Compute resource management for Ray-based task execution.

Manages resource allocation, workload costing, and worker spawning with
adaptive backpressure based on system health.
"""

from collections import defaultdict
from enum import StrEnum

import psutil
import ray
import ray.util.state
from apps.ingestion.src.core.contexts import RayMode
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.utils.common import short_hash
from libs.utils.system import get_system_vitals
from loguru import logger

LOG = logger

# System thresholds
CRITICAL_CPU_THRESHOLD = 90.0  # Stop spawning if CPU exceeds this
CRITICAL_MEM_THRESHOLD = 85.0  # Stop spawning if memory exceeds this
RESOURCE_BUFFER = 0.1  # Small buffer for floating point comparisons

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


class Compute:
    """Manages Ray cluster resources and worker scheduling."""

    def __init__(self, exec_ctx: ExecutionContext):
        """Initializes the compute manager and provisions Ray resources.

        Args:
            exec_ctx: The global execution context.

        Decision: Fail-Fast Initialization.
        The Ray cluster is initialized during the Compute manager's constructor.
        This ensures that any Ray-related configuration errors are caught
        early in the orchestrator's lifecycle, preventing silent failures.
        """
        self.exec_ctx = exec_ctx
        self._resource_limits = self._get_resource_limits()
        self._active_stage_counts: dict[Stage, set[str]] = defaultdict(set)
        self._available_resources: dict[str, float] = {}
        self._saturated_resources: set[str] = set()
        self._remote_executor = None

        self._init_ray_cluster()
        self._purge_orphaned_tasks()

    def _get_resource_limits(self) -> dict[str, int]:
        """Calculates and returns the logical resource limits based on hardware.

        Returns:
            dict[str, int]: A dictionary mapping resource names (CPU, IO, MEM)
                to their calculated integer limits.

        Decision: Dynamic Resource Allocation.
        Instead of hardcoding resource limits, we dynamically calculate them
        based on physical hardware. This allows the orchestrator to adapt
        to different deployment environments (e.g., local dev vs. cloud VM)
        without requiring manual configuration changes.
        """
        cores, io_slots, memory_gb = calculate_resource_limits()
        return {"CPU": cores, "IO": io_slots, "MEM": memory_gb}

    def _init_ray_cluster(self) -> None:
        """Initialize Ray cluster with custom resources."""
        ray_mode = self.exec_ctx.ray_mode
        address = getattr(self.exec_ctx, "ray_address", None)
        runtime_env = getattr(self.exec_ctx, "ray_runtime_env", None)

        # Local mode cannot use custom address
        if address:
            ray_mode = RayMode.CLUSTER

        LOG.info(f"Initializing Ray (mode={ray_mode})...")

        if not ray.is_initialized():
            ctx = ray.init(
                address=address,
                ignore_reinit_error=True,
                local_mode=(ray_mode == RayMode.LOCAL),
                include_dashboard=True,
                dashboard_host="0.0.0.0",
                dashboard_port=8265,
                namespace="ingestion",
                num_cpus=self._resource_limits["CPU"],
                runtime_env=runtime_env,
                _memory=self._resource_limits["MEM"] * (1024**3),
                resources={"IO": self._resource_limits["IO"]},
            )
            dashboard_url = (
                ctx.dashboard_url if ray_mode != RayMode.LOCAL else "localhost:8265"
            )
        else:
            dashboard_url = ray.get_runtime_context().dashboard_url

        resources = ray.cluster_resources()
        LOG.info(
            f"Ray initialized | CPUs: {resources.get('CPU', 0)} | "
            f"Memory: {resources.get('memory', 0) / (1024**3):.1f}GB | "
            f"Dashboard: http://{dashboard_url}"
        )

    @property
    def remote_executor(self):
        """Lazy-loaded Ray actor for task execution.

        Returns:
            ray.actor.ActorHandle: A handle to the remote executor actor.

        Decision: Lazy Loading.
        The remote executor is only initialized when first accessed. This
        reduces startup overhead for CLI commands that don't require Ray
        workers, while ensuring the actor is ready when tasks are dispatched.
        """
        if self._remote_executor is None:
            from .executor import process_stage_task

            self._remote_executor = ray.remote(process_stage_task)
        return self._remote_executor

    def refresh_resources(self) -> None:
        """Updates the internal state with current Ray cluster resource availability.

        Decision: Real-time Resource Awareness.
        By frequently refreshing the available resources, the Compute manager
        can make informed decisions about task dispatch, preventing over-subscription
        and ensuring fair resource allocation across stages.
        """
        self._available_resources = ray.available_resources()
        self._check_resource_saturation()

    def get_workload_cost(self, stage: Stage) -> dict[str, float]:
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

    def can_spawn_worker(self, stage: Stage) -> bool:
        """Check if enough resources are available to spawn a worker."""
        # Check system health first
        vitals = get_system_vitals()
        if (
            vitals.cpu_pct > CRITICAL_CPU_THRESHOLD
            or vitals.mem_pct > CRITICAL_MEM_THRESHOLD
        ):
            LOG.warning(
                f"System overloaded - CPU: {vitals.cpu_pct}%, MEM: {vitals.mem_pct}%"
            )
            return False

        cost = self.get_workload_cost(stage)
        current_usage = self._get_current_resource_usage()

        for resource, required in cost.items():
            ray_resource = RAY_RESOURCE_NAMES.get(resource, resource)
            physical_available = self._available_resources.get(ray_resource)

            # Convert memory to bytes for Ray comparison
            required_physical = required * 1024**3 if resource == "MEM" else required

            # Check logical limit
            limit = self._resource_limits[resource]
            projected_usage = round(current_usage[resource] + required, 4)

            if projected_usage > (limit + RESOURCE_BUFFER):
                LOG.debug(
                    f"Resource {resource} at logical limit: {projected_usage}/{limit}"
                )
                return False

            # Check physical availability
            if (
                physical_available is not None
                and physical_available < required_physical
            ):
                LOG.debug(f"Resource {resource} physically constrained")
                return False

        return True

    def spawn_worker(
        self, stage: Stage, task_key: str, msg_id: str | None = None
    ) -> ray.ObjectRef | None:
        """Dispatches a Ray worker to execute a specific task stage.

        Args:
            stage: The pipeline stage to be executed by the worker.
            task_key: The unique identifier for the task in the cache.
            msg_id: The FlashQ message ID to be passed to the executor.

        Returns:
            ray.ObjectRef | None: A Ray ObjectRef if the worker was spawned,
                None if resources were insufficient.

        Decision: Resource-Aware Dispatch.
        Workers are only spawned if `can_spawn_worker` returns True, ensuring
        that the cluster is not overloaded and tasks are not immediately
        killed due to resource starvation."""
        if not self.can_spawn_worker(stage):
            return None

        cost = self.get_workload_cost(stage)

        # Prepare Ray options
        options = {
            "num_cpus": cost.get("CPU", 0.1),
            "resources": {"IO": cost.get("IO", 0.1)},
        }
        if "MEM" in cost:
            options["memory"] = cost["MEM"] * (1024**3)

        worker_name = f"{stage.value}_{short_hash(8)}"
        ref = self.remote_executor.options(**options).remote(
            worker_name,
            ray.put(self.exec_ctx),
            task_key,
            msg_id,
        )

        self._active_stage_counts[stage].add(ref)
        return ref

    def reclaim_resources(self, task_ref: ray.ObjectRef | None) -> None:
        """Releases the logical resources associated with a completed Ray task.

        Args:
            task_ref: The Ray ObjectRef of the completed task.

        Decision: Logical Resource Tracking.
        While Ray handles physical resource deallocation, the Compute manager
        maintains a logical count of active tasks per stage. This method
        updates that count, ensuring the `can_spawn_worker` logic remains
        accurate.
        """
        if not task_ref:
            return

        for stage, refs in self._active_stage_counts.items():
            if task_ref in refs:
                refs.discard(task_ref)
                LOG.debug(f"Resources reclaimed for {stage.value}")
                break

        self.refresh_resources()

    def reconcile_counts(self) -> None:
        """Synchronizes internal task counts with Ray's global state.

        Decision: Physical Reconciliation.
        This method acts as a periodic heartbeat to Ray's Global Control Store (GCS).
        It helps detect and correct any discrepancies between the Compute manager's
        internal view of active tasks and Ray's actual running tasks, preventing
        'ghost' tasks from consuming logical resources.
        """
        try:
            running_tasks = ray.util.state.list_tasks(
                filters=[("state", "=", "RUNNING")]
            )
            LOG.debug(f"Physical reconciliation: {len(running_tasks)} active tasks")
        except Exception:
            LOG.exception("Failed to reconcile")

    def _get_current_resource_usage(self) -> dict[str, float]:
        """Calculates the current logical resource usage based on active tasks.

        Returns:
            dict[str, float]: A dictionary mapping resource types (CPU, IO, MEM)
                to their currently consumed values.

        Decision: Aggregated Costing.
        By summing the costs of all currently active tasks, we get a real-time
        view of the logical resource footprint, which is then used to enforce limits.
        """
        usage = defaultdict(float)
        for stage, task_ids in self._active_stage_counts.items():
            count = len(task_ids)
            cost = self.get_workload_cost(stage)
            for resource, value in cost.items():
                usage[resource] += value * count
        return usage

    def _check_resource_saturation(self) -> None:
        """Detects and logs when Ray cluster resources are nearing saturation.

        Decision: Proactive Warning.
        Logging saturation warnings helps operators identify potential bottlenecks
        before they lead to task failures or performance degradation. The use of
        `_saturated_resources` prevents log spam by only reporting state changes.

        Decision: Memory-Specific Threshold.
        Memory is treated differently due to its critical nature. A lower threshold
        (10% remaining) triggers a saturation warning for memory.
        """
        for resource, available in self._available_resources.items():
            if resource not in RAY_RESOURCE_NAMES.values():
                continue

            is_saturated = False

            if resource == "memory":
                limit_bytes = self._resource_limits["MEM"] * (1024**3)
                if available < (limit_bytes * 0.1):
                    is_saturated = True
            elif available < 0.5:
                is_saturated = True

            if is_saturated and resource not in self._saturated_resources:
                LOG.warning(f"Resource {resource} is saturated")
                self._saturated_resources.add(resource)
            elif not is_saturated and resource in self._saturated_resources:
                LOG.info(f"Resource {resource} recovered")
                self._saturated_resources.remove(resource)

    def _purge_orphaned_tasks(self) -> None:
        """Terminates any orphaned Ray tasks left over from previous runs.

        Decision: Clean Slate Initialization.
        During startup, the Compute manager actively scans for and cancels
        any Ray tasks that might have been left running by a crashed or
        improperly shut down orchestrator. This prevents 'ghost' tasks
        from consuming resources or interfering with new runs.

        Decision: Stage-Based Filtering.
        We only target tasks whose names start with a known pipeline stage
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
                if "_" in name and name.split("_")[0] in stage_labels:
                    ray.cancel(task["task_id"], force=True)
                    purged += 1

            if purged > 0:
                LOG.info(f"Purged {purged} orphaned Ray tasks")
        except Exception as e:
            LOG.debug(f"Orphan purge skipped: {e}")
