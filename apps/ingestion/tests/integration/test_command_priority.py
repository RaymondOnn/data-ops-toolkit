import time

import msgspec


def test_command_priority_sorting(runtime, tmp_path):
    """
    GIVEN a signal directory with a low-priority ADHOC_RUN and a high-priority STOP
    WHEN process_commands is invoked
    THEN the high-priority STOP should be at the front of the queue regardless of file time.
    """
    sig_path = runtime.exec_ctx.signal_path
    sig_path.mkdir(parents=True, exist_ok=True)

    # 1. Setup: Create a low-priority ADHOC_RUN (Priority 3)
    adhoc_file = sig_path / "ADHOC_RUN_test.cmd"
    adhoc_file.write_bytes(
        msgspec.json.encode({"job_id": "test", "partition_date": "2024-01-01"})
    )

    # Small sleep to ensure file mtime is different if needed
    time.sleep(0.01)

    # 2. Setup: Create a high-priority STOP (Priority 0)
    stop_file = sig_path / "STOP_halt.cmd"
    stop_file.write_text("drain")

    # 3. Action: Process via CommandProcessor
    queue = runtime.commands.process_commands()

    # 4. Assertions: STOP must come first
    assert len(queue) == 2
    first_handler, first_payload = queue[0]
    assert "stop" in str(first_handler.__name__).lower()
    assert first_payload == "drain"

    # Verify files were purged from disk after being queued
    assert not adhoc_file.exists()
    assert not stop_file.exists()
