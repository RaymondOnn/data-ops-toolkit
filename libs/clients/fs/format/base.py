from abc import ABC, abstractmethod
import polars as pl
import io

class FormatHandler(ABC):
    def __init__(self, fs, storage_options: dict):
        self.fs = fs
        self.opts = storage_options

    @abstractmethod
    def to_df(self, target: str, **kwargs) -> pl.LazyFrame: 
        """High-level: Streaming/Repair -> LazyFrame"""
        pass

    @abstractmethod
    def from_df(self, lf: pl.LazyFrame, target: str): 
        """High-level: LazyFrame -> File (Streaming)"""
        pass

    @abstractmethod
    def read_mem(self, target: str, **kwargs) -> io.BytesIO: 
        """Low-level: Read + Repair -> Memory Buffer"""
        pass

    @abstractmethod
    def write_file(self, data: bytes, target: str): 
        """Low-level: Raw Bytes -> Storage"""
        pass

class HandlerFactory:
    @staticmethod
    def get_handler(ext: str, fs, opts: dict) -> FormatHandler:
        mapping = {
            "json": JSONHandler,   # Defensive / Eager
            "jsonl": JSONLHandler, # Performance / Lazy
            "ndjson": JSONLHandler,# Alias for JSONL
            "csv": CSVHandler,
            "parquet": ParquetHandler,
            "xml": XMLHandler
        }
        # ...