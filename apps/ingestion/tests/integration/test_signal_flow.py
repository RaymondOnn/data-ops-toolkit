from apps.ingestion.src.core.models.task import TaskSignal

def test_signal_detection_and_coalescing(runtime, tmp_path):
    """
    GIVEN multiple Ray workers finishing and dropping .done signals
    WHEN the SignalProcessor collects events
    THEN it should batch all unique Run IDs into a single event list and purge the files.
    """
    sig_path = runtime.exec_ctx.signal_path
    
    # 1. Setup: Simulate 3 workers finishing simultaneously
    runs = ["run_A", "run_B", "run_C"]
    for rid in runs:
        # Pattern: job:ds:date:run.done
        sig_file = sig_path / f"job:ds:2024-01-01:{rid}{TaskSignal.DONE.value}"
        sig_file.touch()

    # 2. Action: Collect signals
    events = runtime.orchestrator.signals.collect_events()

    # 3. Assertions
    assert len(events) == 3
    captured_ids = {e.task_ref.run_id for e in events}
    assert captured_ids == set(runs)
    
    # Handshake Check: The files MUST be gone so we don't process them twice
    remaining = list(sig_path.glob("*.done"))
    assert len(remaining) == 0