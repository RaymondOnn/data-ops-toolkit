from msgspec import Struct, field, replace

class JobContext(Struct):
    job_id: str
    run_id: str
    output_path: str
    dataset_name: str
    worker_id: str
    
    # original_config is stored here for the workers to know HOW to do the work
    config: dict
    
    def copy_with(self, **changes):
        """Creates a new instance with updated fields."""
        return replace(self, **changes)