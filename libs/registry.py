import logging
from abc import ABC, abstractmethod
from typing import Any

import diskcache
import ray

LOG = logging.getLogger(__name__)

class BaseRegistry(ABC):
    @abstractmethod
    def update(self, service: str, status: str) -> None: ...

    @abstractmethod
    def get_status(self, service: str) -> str: ...


# --- Implementation A: Diskcache (Laptop/Single EC2) ---
class LocalDiskRegistry(BaseRegistry):
    def __init__(self, cache_dir: str = ".cache/registry") -> None:
        self.cache = diskcache.Cache(cache_dir)

    def update(self, service, status) -> None:
        self.cache.set(service, status, expire=300)  # Auto-reset after 5 mins

    def get_status(self, service) -> str:
        return self.cache.get(service, "UP")


# --- Implementation B: Named Actor (Kubernetes/Ray) ---
@ray.remote(num_cpus=0)
class RayRegistryActor:
    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    def update(self, s, st) -> None:
        self._data[s] = st

    def get(self, s):
        return self._data.get(s, "UP")


class RemoteRayRegistry(BaseRegistry):
    def __init__(self):
        # The 'Safe Getter' logic: Get or Create
        try:
            self.actor = ray.get_actor("HealthRegistry")
        except ValueError:
            self.actor = RayRegistryActor.options(
                name="HealthRegistry", lifetime="detached"
            ).remote()

    def update(self, service, status):
        self.actor.update.remote(service, status)

    def get_status(self, service):
        return ray.get(self.actor.get.remote(service))


def get_registry() -> BaseRegistry:
    # Check if we are currently connected to a Ray cluster (local or remote)
    if ray.is_initialized():
        try:
            return RemoteRayRegistry()
        except Exception as e:
            LOG.warning(
                f"Failed to connect to Ray Actor, falling back to disk: {e}"
            )
            return LocalDiskRegistry()
    else:
        # This handles cases where you're running a unit test
        # or a simple CLI tool without Ray.
        return LocalDiskRegistry()
