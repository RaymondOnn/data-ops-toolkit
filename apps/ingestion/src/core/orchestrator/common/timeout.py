# timeout.py - Optimized timeout management with batched pre-fetching

import threading
import time
from typing import TYPE_CHECKING, Any

import msgspec
from apps.ingestion.src.core.models.stages.enums import ALL_STAGES
from apps.ingestion.src.services.factory import ServiceFactory
from libs.resilience.timeout import TimeoutManager
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import TaskRef
    from apps.ingestion.src.core.orchestrator.enums import TaskMetadata

LOG = logger

# Stage defaults (fallback when no history)
STAGE_DEFAULTS: dict[str, float] = {
    "extract": 900,
    "extract_snapshot": 1800,
    "validate": 300,
    "transform": 1200,
    "load": 1800,
    "load_snapshot": 2700,
    "cleanup": 300,
    "index": 600,
}

# Safety caps (absolute maximums)
STAGE_CAPS: dict[str, float] = {
    "extract": 7200,  # 2 hours
    "transform": 14400,  # 4 hours
    "load": 14400,  # 4 hours
}
EXECUTION_HISTORY_TBL = "META.EXECUTION_HISTORY"


class TimeoutState(msgspec.Struct):
    """Minimal timeout state stored in TaskMetadata."""

    stage_timeout: float = 600.0  # timeout for current stage (seconds)
    budget_total: float | None = None
    budget_used: float = 0.0
    stage_start_time: float = 0.0


class TimeoutMonitor:
    """
    App-specific timeout monitor with batched pre-fetching.

    FLOW OVERVIEW:
    ==============

    Phase 1: FIRST TASK FOR A JOB/DATASET
    --------------------------------------
    1. Task is created → create_timeout_state(job_id, dataset_id, stage)
    2. resolve_timeout(job_id, dataset_id, stage) called
       └─> ensure_dataset_timeouts(job_id, dataset_id)
           ├─> Cache check: job_id/dataset_id NOT in _timeout_cache
           └─> _pre_fetch_stage_timeouts(job_id, dataset_id)
               ├─> EXECUTES 1 BATCHED QUERY for ALL stages
               │   SELECT stage, p95_duration FROM history
               │   WHERE job_id = ? AND dataset_id = ? AND stage IN (ALL_STAGES)
               ├─> Calculates timeout for each stage (p95 * 1.5)
               ├─> Falls back to STAGE_DEFAULTS for stages without history
               └─> Stores ALL timeouts in _timeout_cache[job_id][dataset_id]
    3. resolve_budget(job_id, dataset_id) called
       └─> ensure_dataset_timeouts(job_id, dataset_id)
           └─> Cache check: job_id/dataset_id IN _timeout_cache ✓
               └─> 0 additional queries - sums cached values
    4. TimeoutState created with cached values

    RESULT: 1 database query (not 2N queries)

    Phase 2: SUBSEQUENT TASKS FOR SAME JOB/DATASET
    ------------------------------------------------
    1. Task is created → create_timeout_state(job_id, dataset_id, stage)
    2. resolve_timeout(job_id, dataset_id, stage) called
       └─> ensure_dataset_timeouts(job_id, dataset_id)
           └─> Cache check: job_id/dataset_id IN _timeout_cache ✓
               └─> 0 queries - returns cached value
    3. resolve_budget(job_id, dataset_id) called
       └─> ensure_dataset_timeouts(job_id, dataset_id)
           └─> Cache check: job_id/dataset_id IN _timeout_cache ✓
               └─> 0 queries - sums cached values

    RESULT: 0 database queries

    Phase 3: DIFFERENT JOB, SAME DATASET
    -------------------------------------
    1. Task with different job_id (e.g., snapshot) uses same dataset_id
    2. ensure_dataset_timeouts(new_job_id, dataset_id) called
       └─> Cache check: new_job_id/dataset_id NOT in _timeout_cache
       └─> _pre_fetch_stage_timeouts(new_job_id, dataset_id)
           └─> EXECUTES 1 BATCHED QUERY for the new job

    RESULT: Different jobs have independent timeout profiles

    Phase 4: STALE CACHE (TTL EXPIRED - 24h safety net)
    ----------------------------------------------------
    1. Task is created → create_timeout_state(job_id, dataset_id, stage)
    2. resolve_timeout() called
       └─> ensure_dataset_timeouts(job_id, dataset_id)
           └─> Cache check: job_id/dataset_id IN _timeout_cache ✓
               └─> Checks timestamps: all entries > 24h old?
                   └─> YES - cache is stale
                       └─> _pre_fetch_stage_timeouts(job_id, dataset_id)
                           └─> EXECUTES 1 BATCHED QUERY to refresh ALL

    RESULT: 1 database query (only when stale)

    Phase 5: TASK COMPLETION (Cache Cleanup)
    -----------------------------------------
    1. Task reaches terminal state (SUCCESS, FAILED, BLOCKED, etc.)
    2. Terminal handler calls release_dataset_cache(job_id, dataset_id)
       ├─> Decrements active task count for "job_id:dataset_id"
       └─> If count == 0:
           ├─> Removes dataset from _timeout_cache[job_id]
           └─> If no more datasets for job, removes job from cache

    RESULT: Clean cache, no memory leaks

    Phase 6: ORPHANED CACHE CLEANUP (Maintenance Sweep)
    ----------------------------------------------------
    1. Maintenance sweep calls cleanup_stale_cache()
       └─> Scans _timeout_cache for entries with zero active tasks
           └─> Checks if ALL entries have exceeded TTL
               └─> If yes, removes dataset from cache

    RESULT: Safety net for crashed tasks that never called release

    DATABASE QUERY SUMMARY:
    =======================
    Current Implementation:  2N queries per task (N = number of stages)
    Optimized Implementation: 1 query per (job_id, dataset_id) combination

    For a job with 6 stages and 10 tasks across 3 datasets:
        Optimized: 3 datasets * 1 query = 3 queries
    """

    def __init__(self, state_store=None, db_config: dict[str, Any] | None = None):
        self.db_config = db_config or {}
        self._db = None
        self._manager = TimeoutManager()
        self._cache_lock = threading.RLock()

        """
        Combined cache structure:
        _timeout_cache = {
            "job_extract_snapshot": {                 # job_id level
                "dataset_456": {                      # dataset_id level
                    "extract": (900.0, 1234567890.0),      # (timeout, timestamp)
                    "transform": (1200.0, 1234567890.0),
                    "load": (1800.0, 1234567890.0),
                },
                "dataset_789": {
                    "extract": (600.0, 1234567890.0),
                    "transform": (800.0, 1234567890.0),
                }
            },
            "job_snapshot": {                         # Different job_id
                "dataset_456": {                      # Same dataset, different job
                    "extract": (1800.0, 1234567890.0),     # Different timeout!
                    "transform": (2400.0, 1234567890.0),
                }
            }
        }
        """
        self._timeout_cache: dict[str, dict[str, dict[str, tuple[float, float]]]] = {}

        """
        Track active task counts: "job_id:dataset_id" -> active_task_count
        This prevents premature cache release when multiple tasks run concurrently
        for the same job/dataset combination.
        Example: {"job_extract_snapshot:dataset_456": 3}
        """
        self._active_task_counts: dict[str, int] = {}

        # Safety TTL: 24 hours - protects against memory leaks from crashed tasks
        self._cache_ttl = 86400  # 24 hours

    @property
    def db(self):
        """Lazy-loaded database client."""
        if self._db is None:
            config = self.db_config.copy()
            service_type = config.pop("type", "postgres")
            LOG.debug("Initializing database client", service_type=service_type)
            self._db = ServiceFactory.get(service_type=service_type, **config)
        return self._db

    # ========================================================================
    # CACHE OPERATIONS
    # ========================================================================

    def _get_cache_key(self, job_id: str, dataset_id: str) -> str:
        """Generate cache key for active task counting."""
        return f"{job_id}:{dataset_id}"

    def _get_cached_timeout(
        self, job_id: str, dataset_id: str, stage: str
    ) -> float | None:
        """
        Get timeout from cache if valid and not stale.

        Returns:
            float | None: Timeout value if found and valid, else None
        """
        with self._cache_lock:
            if (
                job_id in self._timeout_cache
                and dataset_id in self._timeout_cache[job_id]
            ):
                dataset = self._timeout_cache[job_id][dataset_id]
                if stage in dataset:
                    timeout, timestamp = dataset[stage]
                    # Check TTL as safety net
                    if time.time() - timestamp < self._cache_ttl:
                        return timeout
                    # TTL expired - remove this entry
                    del dataset[stage]
                    if not dataset:
                        del self._timeout_cache[job_id][dataset_id]
                    if not self._timeout_cache[job_id]:
                        del self._timeout_cache[job_id]
            return None

    def _cache_timeouts_for_job_dataset(
        self, job_id: str, dataset_id: str, timeouts: dict[str, float]
    ) -> None:
        """Cache all timeouts for a job/dataset combination at once."""
        with self._cache_lock:
            if job_id not in self._timeout_cache:
                self._timeout_cache[job_id] = {}
            if dataset_id not in self._timeout_cache[job_id]:
                self._timeout_cache[job_id][dataset_id] = {}
            now = time.time()
            for stage, timeout in timeouts.items():
                self._timeout_cache[job_id][dataset_id][stage] = (timeout, now)

    def _remove_dataset_from_cache(self, job_id: str, dataset_id: str) -> None:
        """Remove a dataset from cache."""
        with self._cache_lock:
            if (
                job_id in self._timeout_cache
                and dataset_id in self._timeout_cache[job_id]
            ):
                del self._timeout_cache[job_id][dataset_id]
                if not self._timeout_cache[job_id]:
                    del self._timeout_cache[job_id]

    def _is_dataset_cached(self, job_id: str, dataset_id: str) -> bool:
        """Check if dataset is cached and not stale."""
        with self._cache_lock:
            if job_id not in self._timeout_cache:
                return False
            if dataset_id not in self._timeout_cache[job_id]:
                return False
            dataset = self._timeout_cache[job_id][dataset_id]
            now = time.time()
            return all(now - ts < self._cache_ttl for _, ts in dataset.values())

    # ========================================================================
    # BATCHED PRE-FETCHING - THE KEY OPTIMIZATION
    # ========================================================================

    def ensure_dataset_timeouts(self, job_id: str, dataset_id: str) -> None:
        """
        Ensure all stage timeouts are cached for a job/dataset combination.

        FLOW:
        1. Check if job_id/dataset_id is already cached and all entries are valid
        2. If cache miss OR stale → trigger single batched query
        3. Cache all timeouts for the job/dataset
        4. Subsequent calls use cache → 0 queries

        This is called from resolve_timeout() and resolve_budget().
        The first call triggers the batched query, subsequent calls use cache.
        """
        # Check if already cached and not stale
        if self._is_dataset_cached(job_id, dataset_id):
            LOG.trace(f"Timeouts already cached for {job_id}:{dataset_id}")
            return

        LOG.debug(
            f"Cache miss or stale for {job_id}:{dataset_id}, " f"pre-fetching timeouts"
        )

        # Cache miss or stale - fetch all at once (outside lock to avoid blocking)
        self._pre_fetch_stage_timeouts(job_id, dataset_id)

    def _pre_fetch_stage_timeouts(self, job_id: str, dataset_id: str) -> None:
        """
        Fetch all stage timeouts in one batched query.

        FLOW:
        1. Build query for ALL stages with a single WHERE IN clause
        2. Execute 1 query (not N queries)
        3. Map results to timeout values
        4. Use defaults for stages without history
        5. Store ALL in cache

        This is called only on cache miss or refresh.

        NOTE: The job_id parameter is the pipeline name (e.g., "extract_snapshot")
        and is used to filter historical data for that specific job type.
        """
        stages = list(ALL_STAGES)
        timeouts = {}

        try:
            # Single batched query for ALL stages for this job/dataset
            placeholders = ",".join(["%s"] * len(stages))
            query = f"""
                SELECT
                    JOB_ID as stage,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration) as p95_duration,
                    COUNT(*) as sample_count
                FROM (
                    SELECT
                        JOB_ID,
                        dateDiff('second', START_TIMESTAMP_LC, END_TIMESTAMP_LC) as duration
                    FROM {EXECUTION_HISTORY_TBL}
                    WHERE JOB_STATUS = 'SUCCESS'
                        AND JOB_ID = %s
                        AND DATASET_ID = %s
                        AND JOB_ID IN ({placeholders})
                        AND START_TIMESTAMP_LC > now() - INTERVAL 30 DAY
                ) t
                GROUP BY JOB_ID
            """
            # Note: job_id here is the pipeline name (e.g., "extract_snapshot")
            # The first %s is the job_id from the task, second is dataset_id
            params = [job_id, dataset_id, *stages]
            results = self.db.fetch(query, params)

            # Build timeout dict from results
            history_count = 0
            for row in results:
                stage = row["stage"]
                p95 = row["p95_duration"]
                sample_count = row.get("sample_count", 0)

                # Only use historical data if we have enough samples
                if sample_count > 10:
                    timeout = min(p95 * 1.5, STAGE_CAPS.get(stage, 14400))
                    timeouts[stage] = timeout
                    history_count += 1
                else:
                    # Not enough samples - use default
                    timeouts[stage] = STAGE_DEFAULTS.get(stage, 1800)

            # Use defaults for stages without history
            for stage in stages:
                if stage not in timeouts:
                    timeouts[stage] = STAGE_DEFAULTS.get(stage, 1800)

            LOG.debug(
                f"Pre-fetched {len(timeouts)} timeouts for {job_id}:{dataset_id} "
                f"({history_count} from history, {len(stages) - history_count} defaults)"
            )

        except Exception as e:
            LOG.warning(f"Failed to pre-fetch timeouts for {job_id}:{dataset_id}: {e}")
            # Fallback to defaults
            for stage in stages:
                timeouts[stage] = STAGE_DEFAULTS.get(stage, 1800)

        # Store in cache
        self._cache_timeouts_for_job_dataset(job_id, dataset_id, timeouts)

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def resolve_timeout(self, job_id: str, dataset_id: str, stage: str) -> float:
        """
        Calculate timeout from historical performance for a specific stage.

        FLOW:
        1. Check cache first (fast path)
        2. If cache miss → ensure_dataset_timeouts() triggers batched query
        3. Return cached value

        This method is idempotent and can be called many times.
        """
        # Check cache first
        cached = self._get_cached_timeout(job_id, dataset_id, stage)
        if cached is not None:
            return cached

        # Cache miss - ensure dataset timeouts (triggers batched query if needed)
        self.ensure_dataset_timeouts(job_id, dataset_id)

        # Now retrieve from cache (should exist after ensure_dataset_timeouts)
        with self._cache_lock:
            if (
                job_id in self._timeout_cache
                and dataset_id in self._timeout_cache[job_id]
                and stage in self._timeout_cache[job_id][dataset_id]
            ):
                timeout, _ = self._timeout_cache[job_id][dataset_id][stage]
                return timeout

        # Fallback (should rarely happen)
        LOG.warning(
            f"Timeout not found for {job_id}:{dataset_id}:{stage}, using default"
        )
        return STAGE_DEFAULTS.get(stage, 1800)

    def resolve_budget(
        self, job_id: str, dataset_id: str, stages: list[str] | None = None
    ) -> float:
        """
        Calculate total budget = sum of stage timeouts + overhead.

        FLOW:
        1. Ensure dataset timeouts are cached (triggers batched query if needed)
        2. Sum all stage timeouts from cache
        3. Return total * 1.2 (20% overhead)

        This uses cached values, so 0 additional queries after first pre-fetch.
        """
        stages = stages or list(ALL_STAGES)

        # Ensure timeouts are cached (triggers batched query if needed)
        self.ensure_dataset_timeouts(job_id, dataset_id)

        # Sum all stage timeouts from cache
        total = 0
        with self._cache_lock:
            if job_id in self._timeout_cache:
                if dataset_id in self._timeout_cache[job_id]:
                    dataset = self._timeout_cache[job_id][dataset_id]
                    for stage in stages:
                        if stage in dataset:
                            timeout, _ = dataset[stage]
                            total += timeout
                        else:
                            # Fallback if stage not found
                            total += STAGE_DEFAULTS.get(stage, 1800)
                else:
                    # Fallback if dataset not found
                    for stage in stages:
                        total += STAGE_DEFAULTS.get(stage, 1800)
            else:
                # Fallback if job not found
                for stage in stages:
                    total += STAGE_DEFAULTS.get(stage, 1800)

        # 20% overhead for safety
        return total * 1.2

    def create_timeout_state(self, task_ref: "TaskRef") -> TimeoutState:
        """
        Create initial timeout state for a task.

        FLOW:
        1. Track dataset as active (increment task count)
        2. Get stage timeout (triggers batched pre-fetch if cache miss)
        3. Get total budget (uses cached values, no additional queries)
        4. Return TimeoutState

        This is the main entry point for task creation.
        """
        job_id = task_ref.identity.job_id
        dataset_id = task_ref.identity.dataset_id
        stage = task_ref.stage
        cache_key = self._get_cache_key(job_id, dataset_id)

        # Track active task count (prevents premature cache cleanup)
        with self._cache_lock:
            self._active_task_counts[cache_key] = (
                self._active_task_counts.get(cache_key, 0) + 1
            )

        # Create state (will trigger pre-fetch if needed)
        state = TimeoutState()
        state.stage_timeout = self.resolve_timeout(job_id, dataset_id, stage)
        state.budget_total = self.resolve_budget(job_id, dataset_id)
        state.budget_used = 0.0
        state.stage_start_time = 0.0

        LOG.debug(
            f"Created timeout state for {job_id}:{dataset_id}:{stage}, "
            f"timeout={state.stage_timeout:.1f}s, budget={state.budget_total:.1f}s, "
            f"active_tasks={self._active_task_counts.get(cache_key, 0)}"
        )
        return state

    def release_dataset_cache(self, job_id: str, dataset_id: str) -> None:
        """
        Release all cache entries for a dataset when task completes.

        FLOW:
        1. Decrement active task count for "job_id:dataset_id"
        2. If count == 0 (no more active tasks):
           a. Remove dataset from _timeout_cache[job_id]
           b. If no more datasets for job, remove job from cache
           c. Remove from _active_task_counts
        3. If count > 0: keep cache for other running tasks

        This is called from terminal handlers (SUCCESS, FAILED, BLOCKED, etc.)
        """
        cache_key = self._get_cache_key(job_id, dataset_id)

        with self._cache_lock:
            # Decrement active task count
            count = self._active_task_counts.get(cache_key, 0) - 1

            if count <= 0:
                # No more active tasks - safe to release
                self._active_task_counts.pop(cache_key, None)

                # Release cache for this dataset
                if job_id in self._timeout_cache:
                    if dataset_id in self._timeout_cache[job_id]:
                        stage_count = len(self._timeout_cache[job_id][dataset_id])
                        del self._timeout_cache[job_id][dataset_id]
                        LOG.debug(
                            f"Released cache for {cache_key} "
                            f"({stage_count} stages, no active tasks)"
                        )
                    # If no more datasets for this job, remove job
                    if not self._timeout_cache[job_id]:
                        del self._timeout_cache[job_id]
            else:
                # Still has active tasks - keep cache
                self._active_task_counts[cache_key] = count
                LOG.trace(
                    f"Dataset {cache_key} still has {count} active tasks, "
                    f"keeping cache"
                )

    def release_job_cache(self, job_id: str) -> None:
        """
        Release all cache entries for a job.

        This is useful when a job is completely done and we want to clean up
        all its dataset caches at once.
        """
        with self._cache_lock:
            # Remove all active task counts for this job
            keys_to_remove = [
                key for key in self._active_task_counts if key.startswith(f"{job_id}:")
            ]
            for key in keys_to_remove:
                del self._active_task_counts[key]

            # Remove all datasets for this job from cache
            if job_id in self._timeout_cache:
                dataset_count = len(self._timeout_cache[job_id])
                del self._timeout_cache[job_id]
                LOG.debug(
                    f"Released all cache for job {job_id} "
                    f"({dataset_count} datasets)"
                )

    def cleanup_stale_cache(self) -> None:
        """
        Periodically clean up cache entries that have expired their TTL.

        FLOW:
        1. Scan all jobs/datasets in cache
        2. Skip entries that still have active tasks
        3. Check if ALL entries have exceeded TTL
        4. If yes, remove dataset from cache

        This is a safety net for tasks that crash and never call release.
        Called by maintenance sweep.
        """
        with self._cache_lock:
            now = time.time()
            to_remove = []

            # Find expired datasets
            for job_id, job_data in self._timeout_cache.items():
                for dataset_id, dataset in job_data.items():
                    cache_key = self._get_cache_key(job_id, dataset_id)

                    # Skip if still has active tasks
                    if self._active_task_counts.get(cache_key, 0) > 0:
                        continue

                    # Check if all stages in this dataset have expired
                    all_expired = all(
                        now - ts >= self._cache_ttl for _, ts in dataset.values()
                    )

                    if all_expired:
                        to_remove.append((job_id, dataset_id))

            # Remove expired datasets
            for job_id, dataset_id in to_remove:
                self._remove_dataset_from_cache(job_id, dataset_id)
                LOG.debug(
                    f"Cleaned up expired cache for {job_id}:{dataset_id} "
                    f"(TTL expired)"
                )

            # Remove empty jobs
            for job_id in list(self._timeout_cache.keys()):
                if not self._timeout_cache[job_id]:
                    del self._timeout_cache[job_id]

    def get_cache_stats(self) -> dict:
        """Get cache statistics for monitoring."""
        with self._cache_lock:
            total_stages = 0
            for job_data in self._timeout_cache.values():
                for dataset in job_data.values():
                    total_stages += len(dataset)

            return {
                "cached_jobs": len(self._timeout_cache),
                "cached_datasets": sum(
                    len(job_data) for job_data in self._timeout_cache.values()
                ),
                "total_stages_cached": total_stages,
                "active_task_counts": self._active_task_counts.copy(),
                "cache_ttl": self._cache_ttl,
            }

    # ========================================================================
    # STAGE TIMING (delegated to TimeoutManager)
    # ========================================================================

    def get_deadline(self, timeout_state: TimeoutState) -> float:
        """Get effective deadline = min(stage timeout, remaining budget)."""
        now = time.time()

        # Stage timeout
        stage_deadline = now + timeout_state.stage_timeout

        # Budget (if set)
        if timeout_state.budget_total is not None:
            budget_remaining = timeout_state.budget_total - timeout_state.budget_used
            budget_deadline = now + budget_remaining
            return min(stage_deadline, budget_deadline)

        return stage_deadline

    def start_stage(self, state: TimeoutState) -> None:
        """Record stage start."""
        state.stage_start_time = time.time()

    def complete_stage(self, state: TimeoutState, duration: float) -> None:
        """Record stage completion."""
        state.budget_used += duration
        state.stage_start_time = 0.0

    def is_budget_exceeded(self, state: TimeoutState) -> bool:
        """Check if budget exceeded."""
        if state.budget_total is None:
            return False
        return state.budget_used > state.budget_total

    def get_schedule_warning_threshold(self, stage: str) -> float:
        """Get threshold for SCHEDULE_TO_START warning."""
        return STAGE_DEFAULTS.get(stage, 600) * 3  # 3x normal timeout

    # Delegate generic timeout operations to base manager
    def get_timeout(self, timeout_type, **kwargs):
        """Delegate to base TimeoutManager."""
        return self._manager.get_timeout(timeout_type, **kwargs)

    def set_timeout(self, timeout_type, value, **kwargs):
        """Delegate to base TimeoutManager."""
        return self._manager.set_timeout(timeout_type, value, **kwargs)


class TimeoutContext:
    """
    Context manager for timeout checking.

    Handles both cases:
    1. With timeout: tracks elapsed time, checks deadline, updates budget
    2. Without timeout: no-op context manager

    Usage:
        with TimeoutContext(metadata, timeout_monitor) as ctx:
            if ctx.has_timeout:
                ctx.check()  # Check periodically
            # Do work
    """

    def __init__(
        self,
        metadata: "TaskMetadata",
        timeout_monitor: TimeoutMonitor,
        timeout_state: TimeoutState | None,
    ):
        self.metadata = metadata
        self.monitor = timeout_monitor
        self.state = timeout_state

        if self.state:
            # Check budget before starting
            if self.monitor.is_budget_exceeded(self.state):
                raise TimeoutError(
                    f"Job budget exceeded: {self.state.budget_used}s > "
                    f"{self.state.budget_total}s"
                )

            # Calculate deadline
            self.deadline = self.monitor.get_deadline(self.state)
            self.start_time = time.time()
            self._check_counter = 0
            self._check_interval = 50  # Check every 50 calls
        else:
            # No timeout - set empty values
            self.deadline = float("inf")
            self.start_time = time.time()
            self._check_counter = 0

    def __enter__(self):
        """Start the stage timing (if timeout is enabled)."""
        if self.state:
            self.monitor.start_stage(self.state)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Record completion and save state (if timeout is enabled)."""
        if self.state:
            duration = time.time() - self.start_time
            self.monitor.complete_stage(self.state, duration)
            # Save state back to metadata
            self.metadata.timeout_state = self.state

        # Don't suppress exceptions
        return False

    def check(self):
        """
        Check if timeout has been exceeded.

        Call this periodically during execution (e.g., every loop iteration).
        If no timeout is configured, this is a no-op.
        """
        if not self.state:
            return

        self._check_counter += 1

        # Only check every N iterations to minimize overhead
        if self._check_counter % self._check_interval != 0:
            return

        if time.time() > self.deadline:
            raise TimeoutError(
                f"Stage {self.metadata.current_stage} exceeded timeout deadline "
                f"(timeout: {self.state.stage_timeout}s, "
                f"elapsed: {time.time() - self.start_time:.1f}s)"
            )

    def get_remaining(self) -> float | None:
        """Get remaining time before timeout. Returns None if no timeout."""
        if not self.state:
            return None
        return max(0, self.deadline - time.time())

    def get_elapsed(self) -> float:
        """Get elapsed time for current stage."""
        return time.time() - self.start_time

    def get_state(self) -> TimeoutState | None:
        """Get the timeout state if available."""
        return self.state
