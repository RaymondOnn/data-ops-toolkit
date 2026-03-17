
from pathlib import Path

from src.utils.constants import JOB_STEPS_BASE_DIR


def find_active_path(run_id: str) -> Path:
    active_root = JOB_STEPS_BASE_DIR / "active"
    for path in active_root.rglob(run_id):
        if path.is_dir:
            return path
    raise FileNotFoundError(f"Active path not found for run_id: {run_id}")