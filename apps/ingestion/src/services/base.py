# src/core/services/base.py
from abc import ABC, abstractmethod
from typing import Any


class Service(ABC):
    """
    Base class for all resilient services.
    Ensures the decorator can find the 'name' for the Registry.
    """

    def __init__(self, name: str, **config: Any):
        self.name = name
        self.config = config

    @abstractmethod
    def get_work_units(self, target: str, num_partitions: int) -> list[str]:
        """How this service splits 50M rows into chunks."""
        pass
