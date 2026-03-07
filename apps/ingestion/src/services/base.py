# src/core/services/base.py
from abc import ABC, abstractmethod
from typing import Any

class Service(ABC):
    """
    Base class for all resilient services.
    Ensures the decorator can find the 'name' for the Registry.
    """
    def __init__(self, name: str, account_id: str, **config: Any):
        self.name = name
        self.account_id = account_id
        self.config = config

    @abstractmethod
    def get_work_units(self, target: str, parallelism: int) -> list[Any]:
        """How this service splits 50M rows into chunks."""
        pass