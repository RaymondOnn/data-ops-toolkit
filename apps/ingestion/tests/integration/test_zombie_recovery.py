import time

import ray
from apps.ingestion.src.core.models.task import ExecutionStatus, Task


def test_zombie_resurrection_on_worker_death(runtime, tmp_path):
    """
    GIVEN a task that is currently RUNNING on a Ray worker
    WHEN the Ray task is forcibly cancelled (simulating a worker crash)
    THEN the maintenance loop should detect the zombie and re-queue it as WAITING.
    """
    # 1. Setup: Trigger a standard job
    # We point to a dummy source so the extraction starts
    overrides = {
        "extract": {
            "source_type": "flat_file",
            "source_identifier": str(tmp_path / "landing"),
        }
    }
    (tmp_path / "landing").mkdir()
    (tmp_path / "landing" / "data.csv").write_text("id,val\n1,test")

    run_ids = runtime.orchestrator._trigger_job(
        job_id="chaos_job", dataset_id="chaos_ds", overrides=overrides
    )
    run_id = next(iter(run_ids))

    # 2. Drive the engine until the task is DISPATCHED to Ray
    # We need to loop a few times to move from PROVISIONED -> WAITING -> DISPATCHED
    found_ref = None
    for _ in range(10):
        runtime.orchestrator._drive_engine()
        if runtime.orchestrator.tasks._active_tasks:
            # Capture the Ray ObjectRef
            found_ref = next(iter(runtime.orchestrator.tasks._active_tasks.keys()))
            break
        time.sleep(0.1)

    assert found_ref is not None, "Task was never dispatched to Ray"

    # 3. THE CHAOS: Forcibly cancel the Ray task
    # This mimics a SIGKILL on the worker process or a Node failure
    ray.cancel(found_ref, force=True)

    # Remove from the local registry to simulate Ray losing track of the ref
    # but the Hot Cache still thinks it's running.
    runtime.orchestrator.tasks._active_tasks.pop(found_ref)

    # 4. Maintenance: Trigger the zombie detection logic
    # This is what the background daemon runs every 5 minutes
    runtime.orchestrator.tasks.recover_zombie_tasks()

    # 5. Verification
    # The task should no longer be in the 'RUNNING' or 'DISPATCHED' status keys
    # It should have been moved back to 'WAITING' for resurrection
    all_keys = runtime.orchestrator.tasks._get_all_keys()

    # Verify a WAITING key exists for this run_id
    resurrected_key = next(
        (k for k in all_keys if run_id in k and ":WAITING:" in k), None
    )
    assert (
        resurrected_key is not None
    ), f"Task {run_id} was not resurrected into WAITING state"

    # Verify manifest reflects the state change
    task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
    task = Task.from_folder(task_path, runtime.exec_ctx)
    assert task.manifest.status == ExecutionStatus.PENDING

    # 6. Final Drive: Ensure it can be picked up again
    runtime.orchestrator._drive_engine()
    new_active_tasks = runtime.orchestrator.tasks._active_tasks
    assert len(new_active_tasks) == 1, "Task failed to re-dispatch after resurrection"

    new_ref = next(iter(new_active_tasks.keys()))
    assert new_ref != found_ref, "Resurrected task reused the dead Ray Ref"
