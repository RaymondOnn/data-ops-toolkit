import io
from abc import ABC, abstractmethod
from typing import Any

import polars as pl

class FormatHandler(ABC):
    def __init__(self, fs, storage_options: dict) -> None:
        self.fs = fs
        self.opts = storage_options

    @abstractmethod
    def to_df(self, target: str, **kwargs: Any) -> pl.LazyFrame: 
        """High-level: Streaming/Repair -> LazyFrame"""
        pass

    @abstractmethod
    def from_df(self, lf: pl.LazyFrame, target: str) -> None: 
        """High-level: LazyFrame -> File (Streaming)"""
        pass

    @abstractmethod
    def read_mem(self, target: str, **kwargs: Any) -> io.BytesIO: 
        """Low-level: Read + Repair -> Memory Buffer"""
        pass

    @abstractmethod
    def write_file(self, data: bytes, target: str) -> None: 
        """Low-level: Raw Bytes -> Storage"""
        pass

class HandlerFactory:
    @staticmethod
    def get_handler(ext: str, fs, opts: dict) -> FormatHandler:
        from libs.file.formats import JSONHandler, JSONLHandler, CSVHandler, ParquetHandler, XMLHandler
        mapping = {
            "json": JSONHandler,   # Defensive / Eager
            "jsonl": JSONLHandler, # Performance / Lazy
            "ndjson": JSONLHandler,# Alias for JSONL
            "csv": CSVHandler,
            "parquet": ParquetHandler,
            "xml": XMLHandler
        }
        return mapping[ext](fs, opts)