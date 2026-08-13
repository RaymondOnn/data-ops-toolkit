from msgspec import field

from src.core.stages.contracts.payload import BasePayload


def get_commit_hash() -> str:
    """
    Retrieves the short git commit hash for the current HEAD.

    Returns:
        str: The 7-character commit hash, or 'unknown' if git is unavailable.
    """
    import subprocess

    try:
        # Returns the short hash (e.g., a1b2c3d)
        return (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
            .decode("ascii")
            .strip()
        )
    except Exception:
        return "unknown"


class StartPayload(BasePayload, kw_only=True, tag="start"):
    """Start stage payload - tracks task initialization."""

    commit_hash: str = field(default_factory=get_commit_hash)
    # stage: Stage = Stage.START
    # worker_id: str = ""
