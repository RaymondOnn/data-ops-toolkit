#!/bin/bash
# init-localstack.sh - Automate LocalStack S3 Setup

CONTAINER_NAME="localstack"
BUCKET_NAME="landing-zone"
SECRETS_FILE="$(dirname "$0")/../.secrets.json"

echo "⏳ Waiting for LocalStack to be ready..."
# LocalStack health check endpoint
until [ "$(docker inspect -f {{.State.Health.Status}} $CONTAINER_NAME)" == "healthy" ]; do
    echo "   ...still waiting for healthy status"
    sleep 2
done
echo "✅ LocalStack container is healthy."

# 1. Create S3 Bucket
echo "🏗️  Provisioning S3 bucket: $BUCKET_NAME..."
docker exec $CONTAINER_NAME awslocal s3 mb s3://$BUCKET_NAME

# 2. Optional: Set CORS (useful if your UI/App needs to access S3 directly)
docker exec $CONTAINER_NAME awslocal s3api put-bucket-cors --bucket "$BUCKET_NAME" --cors-configuration '{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "PUT", "POST", "DELETE"],
      "AllowedHeaders": ["*"]
    }
  ]
}'

# 3. LocalStack default credentials
# In localstack, these are usually 'test' / 'test'
ACCESS_KEY="test"
SECRET_KEY="test"
REGION="ap-southeast-1"
ENDPOINT="http://localhost:4566"

# 4. Automate .secrets.json update
echo "📝 Updating $SECRETS_FILE..."
python3 - <<EOF
import json
from pathlib import Path

path = Path("$SECRETS_FILE")
data = {}
if path.exists():
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        pass

data["S3_ACCESS_KEY"] = "$ACCESS_KEY"
data["S3_SECRET_KEY"] = "$SECRET_KEY"
data["S3_REGION"] = "$REGION"
data["S3_ENDPOINT"] = "$ENDPOINT"
data["S3_BUCKET"] = "$BUCKET_NAME"

with open(path, "w") as f:
    json.dump(data, f, indent=2)
EOF

echo "--------------------------------------"
echo "🚀 LocalStack S3 is ready!"
echo "Endpoint: $ENDPOINT"
echo "Bucket:   $BUCKET_NAME"
echo "--------------------------------------"