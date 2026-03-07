import json


import structlog
from cryptography.fernet import Fernet

LOG = structlog.getLogger(__name__)





# 1. Generate a key once and save it in your machine's environment variables
# key = Fernet.generate_key() 




# --- Advanced: The Logging Filter ---
