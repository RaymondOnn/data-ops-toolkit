from unittest.mock import patch

from src.core.orchestrator.enums import TaskMetadata


def test_stage_priority_ordering(runtime):
    """
    GIVEN multiple tasks in the cache with different stages
    WHEN the compute resources only allow one task to be dispatched
    THEN the TaskManager should pick the task with the higher priority stage
    (e.g. ARCHIVE > EXTRACT).
    """
    # 1. Setup: Seed the cache with two tasks
    # Task A: In EXTRACT (Priority 20)
    # Task B: In ARCHIVE (Priority 100)

    # Triggering naturally to get them into the cache as WAITING
    ref_extract_base = runtime.exec_ctx.parse_identifier("job1:ds1:2024-01-01:run_ext")
    ref_extract = ref_extract_base.with_updates(stage="extract", status="WAITING")

    ref_archive_base = runtime.exec_ctx.parse_identifier("job2:ds2:2024-01-01:run_arc")
    ref_archive = ref_archive_base.with_updates(stage="archive", status="WAITING")

    # Manually inject into cache to simulate queued state
    runtime.orchestrator.tasks.cache[ref_extract.build()] = TaskMetadata.from_ref(
        ref_extract, "dummy_path"
    )
    runtime.orchestrator.tasks.cache[ref_archive.build()] = TaskMetadata.from_ref(
        ref_archive, "dummy_path"
    )

    # 2. Action: Force Compute to only allow 1 worker slot
    # We mock _can_fit to simulate a full cluster that can take only 1 task
    with patch.object(runtime.orchestrator.tasks.compute, "_can_fit") as mock_fit:
        # First call (Archive) returns True, Second call (Extract) returns False
        mock_fit.side_effect = [True, False]

        # Trigger dispatch loop
        runtime.orchestrator.tasks.dispatch()

    # 3. Verification
    active_tasks = runtime.orchestrator.tasks.active_tasks.values()

    # The ARCHIVE task should be the one that was DISPATCHED
    assert any("run_arc" in key and ":DISPATCHED:" in key for key in active_tasks)

    # The EXTRACT task should still be WAITING
    all_keys = runtime.orchestrator.tasks._get_all_keys()
    assert any("run_ext" in k and ":WAITING:" in k for k in all_keys)
