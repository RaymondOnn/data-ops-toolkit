from collections import defaultdict
from enum import StrEnum
from typing import Any

import psutil
import ray
import ray.util.state
from apps.ingestion.src.core.contexts import RayMode
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.utils.common import make_short_hash
from loguru import logger

LOG = logger
CRITICAL_CPU_THRESHOLD = 90.0  # Stop spawning if system CPU % exceeds this
RAY_RESOURCE_MAP = {"CPU": "CPU", "IO": "IO", "MEM": "memory"}


class WorkloadType(StrEnum):
    DEFAULT = "default"
    CPU = "cpu"
    IO = "io"


# 1. Map Workload Types to logical resource costs
WORKLOAD_COSTS: dict[WorkloadType, dict[str, float]] = {
    WorkloadType.DEFAULT: {
        "CPU": 0.2,
        "IO": 0.1,
        "MEM": 0.5,
    },  # Units: Cores, Slots, GB
    WorkloadType.CPU: {"CPU": 1.0, "IO": 0.2, "MEM": 2.0},
    WorkloadType.IO: {"CPU": 0.2, "IO": 1.0, "MEM": 1.0},
}

# 2. Assign every stage to a Workload Type
STAGE_WORKLOAD_MAP: dict[StageName, WorkloadType] = {
    StageName.START: WorkloadType.DEFAULT,
    StageName.EXTRACT: WorkloadType.IO,  # Network heavy, moderate RAM
    StageName.TRANSFORM: WorkloadType.CPU,  # Computation heavy, high RAM
    StageName.WRITE: WorkloadType.IO,
    StageName.PUBLISH: WorkloadType.IO,
    StageName.ARCHIVE: WorkloadType.DEFAULT,
}


def get_limits(cpu_limit_pct: float = 0.8, mem_limit_pct: float = 0.7):
    """
    Detects hardware and defines the logical resource pool.
    :param cpu_limit_pct: Percentage of physical cores to reserve (0.0 to 1.0)
    :param mem_limit_pct: Percentage of total RAM to reserve (0.0 to 1.0)
    """
    cores = psutil.cpu_count(logical=False) or 2  # Physical cores
    # Ensure logical cores is an integer to satisfy Ray requirements for static node capacity
    logical_cores = int(max(1, float(cores) * cpu_limit_pct))
    # IO budget represents discrete slots/concurrency limits and must be an integer
    total_io_budget = int(logical_cores * 5)

    # Detect Memory
    total_mem_bytes = psutil.virtual_memory().total
    logical_mem_gb = (total_mem_bytes / (1024**3)) * mem_limit_pct

    return logical_cores, total_io_budget, int(logical_mem_gb)


class Compute:
    def __init__(
        self,
        exec_ctx: ExecutionContext,
    ):
        self.exec_ctx = exec_ctx

        # Use 80% of cores and 70% of RAM by default as a safe cap
        num_cores, num_io_slots, num_mem_gb = get_limits(
            cpu_limit_pct=0.8, mem_limit_pct=0.7
        )

        # Resource Capping: Prevent app from consuming whole server
        self._resource_limits = {
            "CPU": num_cores,
            "IO": num_io_slots,
            "MEM": num_mem_gb,
        }
        self._saturated_resources: set[str] = set()
        # Track how many workers of each stage are currently active
        self._stage_counts: dict[StageName, set[str]] = defaultdict(set)
        self._available_resources: dict[str, float] = {}
        self._remote_executor: Any | None = None
        self._initialize_cluster(
            ray_mode=self.exec_ctx.ray_mode,
            limits=self._resource_limits,
            address=getattr(self.exec_ctx, "ray_address", None),
            runtime_env=getattr(self.exec_ctx, "ray_runtime_env", None),
        )

        # Self-Healing: Purge orphaned workers from previous runs to reset the budget
        self.purge_leaked_tasks()

    @property
    def remote_executor(self) -> Any:
        """Lazy-loaded Ray actor class to minimize GCS overhead."""
        if self._remote_executor is None:
            from .worker import process_stage_task

            self._remote_executor = ray.remote(process_stage_task)
        return self._remote_executor

    def _initialize_cluster(
        self,
        ray_mode: str = RayMode.LOCAL,
        limits: dict[str, float] | None = None,
        address: str | None = None,
        runtime_env: dict[str, Any] | None = None,
    ) -> str:
        """
        Initializes the Ray cluster with custom resource definitions.

        Example `runtime_env` config for production (passed via ExecutionContext):
        ```python
        # Assuming PEX files (e.g., 'ingestion.pex', 'deps.pex') are in the project root
        # or a subdirectory that 'working_dir' covers.
        runtime_env = {
            # Zips and uploads the current directory to workers.
            # This is how your application code and PEX files get to workers.
            "working_dir": ".",
            # Ensures core dependencies are present.
            # Can be omitted if deps.pex covers all.
            "pip": ["psutil", "loguru", "polars", "msgspec"],
            "env_vars": {
                "PYTHONPATH": ".", # Ensures Python can find modules in the working_dir
                # # If you want to explicitly use deps.pex for the worker's own env
                # "PEX_PATH": "./deps.pex"
            }
        }
        ```
        """
        local_mode = ray_mode == RayMode.LOCAL
        dashboard_url = "N/A"
        limits = limits or {}

        # local_mode is incompatible with 'address'. If connecting to a cluster,
        # we must disable local_mode.
        if address:
            local_mode = False
            LOG.info(f"Connecting to existing Ray cluster at: {address}")

        LOG.info(f"Initializing Ray (mode={ray_mode}, local={local_mode})...")

        if not ray.is_initialized():
            ctx = ray.init(
                address=address,
                ignore_reinit_error=True,
                local_mode=local_mode,
                include_dashboard=True,
                dashboard_host="0.0.0.0",
                dashboard_port=8265,
                namespace="ingestion",
                num_cpus=int(limits.get("CPU", 1)),
                runtime_env=runtime_env,
                # _memory is the total heap memory available to Ray (in bytes).
                # Note: Only used when Ray starts the cluster (not when connecting via address)
                _memory=int(limits.get("MEM", 8) * (1024**3)),
                resources={"IO": limits.get("IO")},
            )

            dashboard_url = ctx.dashboard_url if not local_mode else "localhost:8265"
        elif not local_mode:  # If already initialized, get URL from runtime context
            dashboard_url = ray.get_runtime_context().dashboard_url

        resources = ray.cluster_resources()
        cpu_count = resources.get("CPU", 0)
        memory_gb = resources.get("memory", 0) / (1024**3)

        LOG.info(
            f"🚀 Ray Initialized | Cores: {cpu_count} | Mem: {memory_gb:.2f}GB | "
            f"Dashboard: http://{dashboard_url}"
        )
        return "already_initialized"

    def refresh_resources(self) -> None:
        """
        Snapshots the current cluster resources.
        Should be called at the start of a processing tick.
        """
        self._available_resources = ray.available_resources()
        self.check_system_saturation(self._available_resources)

    def get_cost(self, stage: StageName) -> dict[str, float]:
        """Returns the resource cost for a given stage."""
        workload = STAGE_WORKLOAD_MAP.get(stage, WorkloadType.DEFAULT)
        return WORKLOAD_COSTS[workload]

    def _can_fit(self, stage: StageName) -> bool:
        """
        Checks both physical Ray availability and logical resource capping.
        """
        cost = self.get_cost(stage)
        current_usage = self._get_logical_usage()

        # 0. Global System Safety Check
        system_cpu = psutil.cpu_percent(interval=None)
        if system_cpu > CRITICAL_CPU_THRESHOLD:
            LOG.warning(
                "Critical system CPU load detected. Throttling dispatch.",
                system_cpu=f"{system_cpu}%",
            )
            return False

        for resource, required_val in cost.items():
            resource_key = RAY_RESOURCE_MAP.get(resource, resource)
            # 1. Physical Check (What does the hardware say?)
            # In local mode, available_resources is often empty;
            # we fall back to logical limits
            physical_val = self._available_resources.get(resource_key)

            # Conversion: Logical MEM is in GB, Ray 'memory' is in Bytes
            if resource == "MEM":
                required_phys_val = required_val * (1024**3)
            else:
                required_phys_val = required_val

            # 2. Logical Check (Are we over our own internal cap?)
            limit_val = self._resource_limits[resource]

            # Round to avoid floating point precision issues (e.g. 4.000000001 > 4)
            # that trigger false-positive backpressure.
            projected_usage = round(current_usage[resource] + required_val, 4)

            # Only apply physical constraint if Ray is actually reporting it
            # (prevents local mode lock)
            is_physically_constrained = (
                physical_val is not None and physical_val < required_phys_val
            )
            is_logically_constrained = projected_usage > (limit_val + 0.1) # Add a tiny 0.1 buffer

            if is_physically_constrained or is_logically_constrained:
                # Convert physical free bytes to GB for readable logs
                phys_free_str = "N/A"
                if physical_val is not None:
                    phys_free_gb = (
                        physical_val / (1024**3) if resource == "MEM" else physical_val
                    )
                    phys_free_str = (
                        f"{phys_free_gb:.2f}GB"
                        if resource == "MEM"
                        else str(physical_val)
                    )

                LOG.debug(
                    f"Backpressure: {stage.label} waiting for {resource}",
                    stage=stage.label,
                    resource=resource,
                    usage=f"{projected_usage}/{limit_val}",
                    physical_free=phys_free_str,
                )
                return False

        return True

    def check_system_saturation(self, available: dict[str, float]) -> None:
        """Detects if specific hardware resources are near exhaustion."""
        for res, val in available.items():
            if res not in ["CPU", "IO", "memory"]:
                continue

            # Special handling for Ray's internal 'memory' key (usually in bytes)
            threshold = 0.5
            if res == "memory":
                # If available memory is less than 10% of our logical limit, warn
                limit_bytes = self._resource_limits.get("MEM", 0) * (1024**3)
                if val < (limit_bytes * 0.1):
                    val = 0.0  # Force saturation

            if val < 0.5:  # Threshold for resource saturation
                if res not in self._saturated_resources:
                    LOG.warning(f"Resource {res} is now SATURATED (Available: {val})")
                    self._saturated_resources.add(res)
            elif res in self._saturated_resources:
                LOG.info(f"Resource {res} saturation cleared (Available: {val})")
                self._saturated_resources.remove(res)

    def _get_logical_usage(self) -> dict[str, float]:
        """
        Calculates current resource usage based on active stage counts.
        """
        usage = defaultdict(float)
        for stage, actor_ids in self._stage_counts.items():
            count = len(actor_ids)
            cost = self.get_cost(stage)
            for res, val in cost.items():
                usage[res] += val * count
        return usage

    def spawn_worker(
        self,
        stage: StageName,
        key: str
    ) -> ray.ObjectRef | None:
        """
        Creates a worker and increments internal resource tracking.
        """
        if not self._can_fit(stage):
            return None

        # Construct options directly from cost
        cost = self.get_cost(stage)
        
        # 2. Tell Ray exactly what this SPECIFIC task costs
        options = {
            "num_cpus": cost.get("CPU", 0.1),
            "resources": {"IO": cost.get("IO", 0.1)},
            # Ray also handles memory reservations if specified
            "memory": cost.get("MEM", 0.5) * (1024**3) if "MEM" in cost else None
        }
        options = {k: v for k, v in options.items() if v is not None}

        # Trigger a stateless TASK instead of an ACTOR
        ref = self.remote_executor.options(**options).remote(
            f"{stage.label}_{make_short_hash(8)}", # worker_name, 
            ray.put(self.exec_ctx), 
            key
        )
        
        # Track the task ID for logical resource counting
        self._stage_counts[stage].add(ref)
        return ref

    def reclaim_resources(self, ref: ray.ObjectRef | None) -> None:
        """
        Terminates the worker and removes its identity from the logical resource budget.
        """
        if not ref:
            return

        found_stage = None

        # Search for the actor ID across all tracked stages to perform the discard.
        # This allows the Manager to be agnostic of the stage when reclaiming.
        for stage, refs in self._stage_counts.items():
            if ref in refs:
                refs.discard(ref)
                found_stage = stage.label
                break

        if found_stage:
            LOG.info(
                "Logical resources reclaimed",
                stage=found_stage,
                task_id=str(ref)
            )
        self.refresh_resources()

    def reconcile_counts(self) -> None:
        """
        Synchronizes internal stage counts with actual running Ray Tasks.
        """
        try:
            # 1. List all physically RUNNING tasks in this namespace
            all_tasks = ray.util.state.list_tasks(filters=[("state", "=", "RUNNING")])
            
            # 2. We keep our current refs, but we can use this to double-check 
            # if our logical budget is way off from reality.
            # However, because Tasks are short-lived, it's often safer to just 
            # trust the cleanup_finished_tasks loop.
            
            LOG.debug(f"Physical task reconciliation: {len(all_tasks)} tasks active.")
        except Exception as e:
            LOG.error("Failed to reconcile task counts", error=str(e))

    def purge_leaked_tasks(self) -> None:
        """
        Terminates any RUNNING tasks from previous sessions matching our naming
        convention to ensure a clean resource budget on startup.
        """
        try:
            if not ray.is_initialized():
                return

            # 1. List all physically RUNNING tasks
            all_tasks = ray.util.state.list_tasks(filters=[("state", "=", "RUNNING")])
            purged = 0
            valid_labels = {s.label for s in StageName}

            for task in all_tasks:
                name = task.get("name")
                # Our convention: {stage_label}_{hash}
                if name and "_" in name:
                    label = name.split("_")[0]
                    if label in valid_labels:
                        # 2. Cancel the task to free up resources immediately
                        ray.cancel(task["task_id"], force=True)
                        purged += 1

            if purged > 0:
                LOG.info("Purged leaked Ray tasks on startup", count=purged)
        except Exception as e:
            LOG.debug("Leaked task purge skipped or failed", error=str(e))
