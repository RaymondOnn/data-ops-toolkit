import json
from pathlib import Path

from libs.auth.provider import encrypt_local_secret

# 1. Generate a Key (Save this in your MASTER_KEY env var)
# from cryptography.fernet import Fernet
# print(Fernet.generate_key().decode())

MASTER_KEY = "your-generated-fernet-key-here"
SECRETS = {
    "CLICKHOUSE_PASSWORD": "password",
    "S3_ACCESS_KEY": "some_access_key",
}

encrypted_map = {k: encrypt_local_secret(v, MASTER_KEY) for k, v in SECRETS.items()}

with Path("./.secrets.json").open("w") as f:
    json.dump(encrypted_map, f, indent=2)

print("✅ .secrets.json generated successfully.")
