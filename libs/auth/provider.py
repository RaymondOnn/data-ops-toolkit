import os
from abc import ABC, abstractmethod

from cryptography.fernet import Fernet

class SecretProvider(ABC):
    @abstractmethod
    def get_secret(self, secret_id: str) -> str:
        pass

class LocalSecretProvider(SecretProvider):
    """For Dev/Test: Reads from environment variables or a local file."""
    def get_secret(self, secret_id: str) -> str:
        return os.getenv(secret_id, "dev_fallback_value")

class AWSSecretProvider(SecretProvider):
    """For Production: Fetches from AWS Secrets Manager."""
    def __init__(self, region: str="us-east-1") -> None:
        import boto3
        self.client = boto3.client("secretsmanager", region_name=region)

    def get_secret(self, secret_id: str) -> str:
        # Implementation of boto3 get_secret_value
        response = self.client.get_secret_value(SecretId=secret_id)
        return str(response['SecretString'])
    
def encrypt_local_secret(plaintext, key) -> str:
    f = Fernet(key)
    return str(f.encrypt(plaintext.encode()).decode())

class LocalEncryptedProvider(SecretProvider):
    def __init__(self, encrypted_file_path: str, master_key: str) -> None:
        self.path = encrypted_file_path
        self.f = Fernet(master_key)

    def get_secret(self, secret_id: str) -> str:
        import json
        
        with open(self.path, "r") as file:
            encrypted_data = json.load(file)
            
        encrypted_val = encrypted_data.get(secret_id)
        if not encrypted_val:
            raise ValueError(f"Secret {secret_id} not found locally.")
            
        # Decrypt in memory only
        return str(self.f.decrypt(encrypted_val.encode()).decode())
        
        # Decrypt to a file
        # with open("output.txt", "wb") as f:
        #     f.write(self.f.decrypt(encrypted_val.encode()))
