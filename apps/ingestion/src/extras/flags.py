from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import msgspec
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts.task import TaskContext

LOG = logger


class FeatureFlags(msgspec.Struct, kw_only=True):
    """Centralized management for experimental logic and environment simulations."""

    # Enable service swapping for cost/performance comparison
    benchmark_mode: bool = False
    # Enable shadow writes or environment-specific mocks
    pre_prod_sim: bool = False
    # Toggle stage order: Archive -> Load vs Load -> Archive
    archive_first: bool = False
    # Used with benchmark_mode to redirect sinks
    experimental_sink_type: str | None = None

    # --- Migration Flags ---
    # If True, WriteStage should hit both primary and experimental sinks
    dual_write: bool = False
    # If True, bypass delta logic for full-refresh bulk migration
    migration_mode: bool = False
    # Toggle for Audits: compare against Target instead of Legacy Source
    read_from_target: bool = False

    # # --- Simulation & Environment ---
    # shadow_write_enabled: bool = False # If True, WriteStage emits data to both primary and experimental sinks

    # # --- Infrastructure Benchmarking (Cost Comparison) ---
    # experimental_sink_type: str | None = None  # The implementation to test (e.g. 'snowflake_db')
    # experimental_config: dict[str, Any] = msgspec.field(default_factory=dict) # Config for the test tool

    # # --- Logic & Flow Toggles ---
    # archive_first: bool = False     # Toggle order: Archive -> Load vs default Load -> Archive
    # dry_run: bool = False           # Process everything but skip terminal DB/S3 commits
    # bypass_audit: bool = False      # Skip the Audit stage for high-priority emergency runs


@contextmanager
def feature_flag(flag_attr: str, context: "TaskContext") -> Generator[bool, None, None]:
    """
    Context manager to scope experimental logic based on TaskContext flags.
    Provides automated logging and performance tracking for experiments.
    """
    import time

    # 1. Resolve flag state from the current run context
    is_active = getattr(context.feature_flags, flag_attr, False)
    start_time = time.perf_counter()

    if is_active:
        LOG.info(f"🚀 Feature Flag '{flag_attr}' active for this block")

    try:
        yield is_active
    finally:
        if is_active:
            duration = time.perf_counter() - start_time
            # Scoped telemetry: perfect for benchmarking the 'Cost' of a new logic path
            LOG.debug(
                f"🏁 Feature block '{flag_attr}' finished",
                duration_ms=round(duration * 1000, 2),
            )
