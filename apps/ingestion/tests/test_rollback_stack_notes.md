# Rollback Stack Verification & Test Scenarios

This document lists test scenarios to verify for `RollbackRequired` stack-based return progression.

## Scenarios to Verify

### 1. Single-Level Rollback & Direct Return (`E -> D -> E`)
- **Setup**: Start execution at Step E.
- **Action**: Raise `RollbackRequired(target_step_id="D")`.
- **Expected Outcome**:
  - `rollback_stack` becomes `["E"]`.
  - `current_step_id` switches to `"D"`.
  - When Step D executes and completes successfully, `ProgressOutcome` pops `"E"` from `rollback_stack`.
  - `rollback_stack` becomes `[]`.
  - Task resumes execution at Step E.

### 2. Multi-Level Nested Rollback (`E -> C -> B -> C -> E`)
- **Setup**: Start execution at Step E.
- **Actions & Progression**:
  1. At Step E, raise `RollbackRequired(target_step_id="C")`.
     - `rollback_stack`: `["E"]`
     - Current step: `"C"`
  2. At Step C, raise `RollbackRequired(target_step_id="B")`.
     - `rollback_stack`: `["E", "C"]`
     - Current step: `"B"`
  3. Step B executes and completes successfully.
     - `ProgressOutcome` checks stack top (`"C"`), which matches standard next step after `"B"`.
     - Pops `"C"`.
     - `rollback_stack`: `["E"]`
     - Current step: `"C"`
  4. Step C executes and completes successfully.
     - `ProgressOutcome` checks stack top (`"E"`).
     - Pops `"E"`.
     - `rollback_stack`: `[]`
     - Current step jumps directly to `"E"`.

### 3. State Persistence & Crash Recovery
- **Verification**:
  - Ensure `rollback_stack` is correctly serialized to disk in `manifest.json` (`TaskManifest`).
  - Verify that when `TaskManifest.from_path()` or `TaskMetadata` re-initializes after orchestrator restart, `rollback_stack` is intact.

### 4. Safety Bounds (Max Depth & Infinite Loop Safeguards)
- **Verification**:
  - Attempting excessive recursive rollbacks exceeding `MAX_ROLLBACK_DEPTH` triggers `FailedOutcome` instead of crashing or looping indefinitely.
