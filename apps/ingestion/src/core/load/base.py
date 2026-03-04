



class LoadFactory:
    """
    Resolves the correct behavioral strategy for the data load.
    Example: (Sink: S3 + Mode: Overwrite) -> S3OverwriteStrategy
    """
    
    # Map of (Destination Type, Load Mode) -> Strategy Class
    _REGISTRY = {
        # Database Sinks
        ("postgres", "append"): PostgresAppendStrategy,
        ("postgres", "overwrite"): PostgresOverwriteStrategy,
        ("postgres", "upsert"): PostgresUpsertStrategy,
        
        # Object Storage Sinks
        ("s3", "append"): S3AppendStrategy,
        ("s3", "overwrite"): S3OverwriteStrategy,
        
        # Snowflake Sinks
        ("snowflake", "upsert"): SnowflakeUpsertStrategy,
    }

    @classmethod
    def get_strategy(cls, sink_type: str, mode: str) -> LoadStrategy:
        # Normalize inputs
        lookup = (sink_type.lower(), mode.lower())
        
        strategy_class = cls._REGISTRY.get(lookup)
        
        if not strategy_class:
            # Provide a helpful error if the combination isn't supported
            supported = ", ".join([f"{s}/{m}" for s, m in cls._REGISTRY.keys()])
            raise NotImplementedError(
                f"No strategy found for {sink_type} with mode '{mode}'. "
                f"Supported combinations: {supported}"
            )
            
        return strategy_class()



class LoadStrategy(ABC):
    """
    Defines the behavioral contract for moving data into production.
    Each implementation (Append, Upsert, Overwrite) handles the 
    logic for both Staging and Promotion.
    """
    
    @abstractmethod
    def load(self, client: "BaseClient", lf: pl.LazyFrame, target: str) -> dict:
        """
        Phase 1: Moves data from Silver (Parquet) to a temporary 'Staging' area.
        Returns metadata about the staged data (rows, temp_path/temp_table).
        """
        pass

    @abstractmethod
    def promote(self, client: "BaseClient", staging_info: dict, target: str) -> None:
        """
        Phase 2: Moves data from 'Staging' to the 'Production' destination.
        This is where 'Atomic Swaps' or 'Merges' happen.
        """
        pass

class UpsertLoadStrategy(LoadStrategy):
    def promote(self, client: "DatabaseClient", staging_table: str, target_table: str):
        # Implementation uses a 'MERGE' or 'INSERT ... ON CONFLICT' SQL command
        sql = f"""
            MERGE INTO {target_table} AS target
            USING {staging_table} AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET ...
            WHEN NOT MATCHED THEN INSERT ...
        """
        client.execute_sql(sql)
        client.execute_sql(f"DROP TABLE {staging_table}")
        
