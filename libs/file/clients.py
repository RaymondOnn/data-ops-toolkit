from typing import Any, Optional
import fsspec
from libs.file.base import FileSystemClient


class S3Client(FileSystemClient):
    """
    S3 Driver for AWS, MinIO, or LocalStack.
    
    Storage Options:
        - key (str): AWS Access Key ID
        - secret (str): AWS Secret Access Key
        - token (str): Temporary session token
        - client_kwargs (dict): e.g., {'region_name': 'us-east-1', 'endpoint_url': '...'}
        - config_kwargs (dict): e.g., {'retries': {'max_attempts': 10}}
        - default_fill_cache (bool): Set to False to prevent 2GB RAM spikes.
    """
    def __init__(self, url: str, storage_options: Optional[dict[str, Any]] = None) -> None:
        super().__init__(url, storage_options)            
        self.fs = self.connect()
        
    def connect(self) -> fsspec.S3FileSystem:
        """Establishes the S3 connection using mapped credentials."""
        if not self.fs:
            # Map generic 'password' to S3-specific 'secret'
            if 'password' in self.opts:
                self.opts['secret'] = self.opts.pop('password')
                
            self.fs: fsspec.S3FileSystem = fsspec.filesystem("s3", **self.opts)
        return self.fs

    def reconnect(self, max_retries: int = 3) -> None:
        """Exponential backoff for long 50M row streams."""
        import time
        for i in range(max_retries):
            try:
                self.connection = None # Force clear
                self.connect()
                return
            except Exception as e:
                if i == max_retries - 1: raise
                time.sleep(2 ** i)
                
    def delete_dir(self, path: str):
        """
        Uses the S3 Batch Delete API (up to 1000 objects per request).
        """
        bucket = self.fs.Bucket(self.bucket_name)
        # Find all objects in the partition/folder
        objects_to_delete = [{'Key': obj.key} for obj in bucket.objects.filter(Prefix=path)]
        
        # S3 limits delete batches to 1000 items
        for i in range(0, len(objects_to_delete), 1000):
            bucket.delete_objects(Delete={'Objects': objects_to_delete[i:i+1000]})

    def move_dir(self, source_path: str, target_path: str):
        """
        Server-side bulk copy + Batch delete.
        """
        bucket = self.s3.Bucket(self.bucket_name)
        
        # 1. Parallel Server-Side Copy
        # This keeps data on the AWS backbone (0% your RAM used)
        for obj in bucket.objects.filter(Prefix=source_dir):
            new_key = obj.key.replace(source_dir, target_path, 1)
            # copy_from is a metadata-only trigger for S3
            bucket.Object(new_key).copy_from(CopySource={'Bucket': self.bucket_name, 'Key': obj.key})
            
        # 2. Clean up staging
        self.delete_dir(source_path)
        
        
# class GCSClient(FileSystemClient):
#     """
#     Google Cloud Storage Driver.
    
#     Storage Options:
#         - token (str/dict): Path to JSON key or 'google_default'
#         - project (str): GCP Project ID
#         - consistency (str): 'strong' or None
#         - cache_timeout (int): Set to 0 for large 50M row metadata scans.
#     """
#     def __init__(self, url: str, storage_options: Optional[dict[str, Any]] = None) -> None:
#         super().__init__(url, storage_options)
#         self.fs = fsspec.filesystem("gcs", **self.opts)
    
#     def connect(self) -> None:
#         if self.connection:
#             return
#         # GCS usually expects 'token' for the credential path/dict
#         self.connection = fsspec.filesystem("gcs", **self.opts)

class AzureClient(FileSystemClient):
    """
    Azure Blob Storage (ABFS) Driver.
    
    Storage Options:
        - account_name (str): Azure Storage Account Name
        - account_key (str): Azure Storage Access Key
        - connection_string (str): Full Azure connection string
    """
    def __init__(self, url: str, storage_options: Optional[dict[str, Any]] = None) -> None:
        super().__init__(url, storage_options)
        self.fs = self.connect()

    def connect(self) -> fsspec.AzureBlobFileSystem:
        if not self.fs:

            # Map generic 'password' to Azure-specific 'account_key'
            if 'password' in self.opts:
                self.opts['account_key'] = self.opts.pop('password')
                
            self.fs = fsspec.filesystem("abfs", **self.opts)

        return self.fs        

class LocalClient(FileSystemClient):
    """
    Local FileSystem Driver.
    Storage Options:
        - auto_mkdir: True (Automatically create parent directories)
    """
    def __init__(self, url: str, storage_options: Optional[dict[str, Any]] = None):
        super().__init__(url, storage_options)
        self.fs = self.connect()
        
    def connect(self) -> fsspec.LocalFileSystem:
        if not self.fs:
            self.fs = fsspec.filesystem("file", **self.opts)
        return self.fs
        