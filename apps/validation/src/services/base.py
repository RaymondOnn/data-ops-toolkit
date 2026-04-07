# src/core/services/base.py
from abc import abstractmethod
from typing import Any

import polars as pl


class Service:
    """
    Base class for all resilient services.
    Ensures the decorator can find the 'name' for the Registry.
    """

    def __init__(self, name: str, **config: Any):
        self.name = name
        self.config = config
        
    @abstractmethod
    def scan(self, **kwargs) -> pl.LazyFrame:
        """Must return a Polars LazyFrame without materializing data."""
        pass
