class IngestionFactory:
    @staticmethod
    def get_strategy(source_type: str):
        strategies = {
            "s3": S3IngestionStrategy(),
            "local": LocalFileStrategy(),
            "api": ApiIngestionStrategy(),
        }
        if source_type not in strategies:
            raise ValueError(f"Unsupported source type: {source_type}")
        return strategies[source_type]

class IngestionStream:
    """
    A fluent wrapper that consumes a Polars DataFrame generator 
    and writes the chunks to the filesystem.
    """
    def __init__(self, generator: Generator[pl.DataFrame, None, None]):
        self._generator = generator

    def to_parquet(self, destination: Path) -> List[Dict[str, Any]]:
        """
        Consumes the stream and saves each chunk as a unique parquet file.
        Returns metadata required for the FileInfo structs.
        """
        destination.mkdir(parents=True, exist_ok=True)
        metadata_list = []

        for i, df in enumerate(self._generator):
            if df.is_empty():
                continue
                
            file_path = destination / f"part_{i:04d}.parquet"
            
            # Write with snappy compression for a good balance of speed/size
            df.write_parquet(file_path, compression="snappy")
            
            # Capture metadata for the RawStep to process
            metadata_list.append({
                "path": file_path,
                "rows": len(df),
                "schema": df.schema
            })
            
        return metadata_list
    
class IngestionStrategy(ABC):
    """
    Base Strategy class. 
    Subclasses implement _get_data_generator to handle source-specific logic.
    """
    
    @abstractmethod
    def _get_data_generator(self, source_params: Dict[str, Any]) -> Generator[pl.DataFrame, None, None]:
        """
        Strategy-specific logic to yield DataFrames.
        e.g. For API, this would yield one DataFrame per paginated response.
        """
        pass

    def fetch(self, source_params: Dict[str, Any]) -> IngestionStream:
        """
        Entry point for the RawStep. Returns a chainable stream object.
        """
        generator = self._get_data_generator(source_params)
        return IngestionStream(generator)
    
class ApiIngestionStrategy(IngestionStrategy):
    def _get_data_generator(self, source_params: Dict[str, Any]) -> Generator[pl.DataFrame, None, None]:
        url = source_params["endpoint"]
        headers = source_params.get("headers", {})
        params = source_params.get("query_params", {})
        
        has_more = True
        page = 1
        
        while has_more:
            params["page"] = page
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            records = data.get("results", [])
            
            if not records:
                has_more = False
                continue
            
            # Yield a chunk of data as a Polars DataFrame
            yield pl.DataFrame(records)
            
            # Simple pagination logic
            page += 1
            if page > data.get("total_pages", 0):
                has_more = False