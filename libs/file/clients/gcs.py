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
