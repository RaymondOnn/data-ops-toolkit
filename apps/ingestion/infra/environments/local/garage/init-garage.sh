#!/bin/bash
# init-garage.sh - Automate Garage S3 Setup for Data Ops Toolkit

CONTAINER_NAME="garage"
BUCKET_NAME="landing-zone"
KEY_NAME="toolkit-key"
SECRETS_FILE="$(dirname "$0")/../.secrets.json"

# until [ "$(docker inspect --format='{{.State.Health.Status}}' $CONTAINER_NAME)" == "healthy" ]; do
#     echo "   ...still waiting for healthy status"
#     sleep 2
# done
# echo "✅ Garage container is healthy."

# 1. Get the Node ID
NODE_ID=$(docker exec $CONTAINER_NAME /garage status | grep "Node ID" | head -n 1 | awk '{print $3}' | tr -d '\r')
echo "✅ Found Node ID: $NODE_ID"

# 2. Assign role and apply layout (Required for Garage to function)
echo "🏗️  Configuring cluster layout..."
# Note: -c 1G is capacity, -z dc1 is zone. Required for the ring to initialize.
docker exec $CONTAINER_NAME /garage layout assign "$NODE_ID" -z local -c 10G
# docker exec $CONTAINER_NAME /garage layout apply --version 1

echo "📝 Committing cluster layout..."
docker exec $CONTAINER_NAME /garage layout apply --version 1 --ignore-already-applied || \
docker exec $CONTAINER_NAME /garage layout apply --version $(date +%s)

# echo "📡 Verifying S3 API availability..."
# until docker exec $CONTAINER_NAME /garage status | grep -q "Storage nodes"; do
#   echo "   ...waiting for storage engine to settle"
#   sleep 2
# done

# 3. Create API Key and Bucket
echo "🔑 Provisioning S3 resources..."
if ! docker exec $CONTAINER_NAME /garage bucket list | grep -q "$BUCKET_NAME"; then
    docker exec $CONTAINER_NAME /garage bucket create "$BUCKET_NAME"
fi

# Create key and capture credentials
# We use 'key create' first. If it exists, we fall back to 'key info'.
# tr -d '\r' is critical to remove hidden carriage returns from Docker output.
# 3.1. Idempotent Key Management
# We check if the key already exists to prevent generating new IDs on every script run.
if docker exec $CONTAINER_NAME /garage key list | grep -q "$KEY_NAME"; then
    echo "🔑 Key '$KEY_NAME' already exists. Retrieving info..."
    KEY_INFO=$(docker exec $CONTAINER_NAME /garage key info "$KEY_NAME")
else
    echo "🔑 Creating new key: $KEY_NAME"
    KEY_INFO=$(docker exec $CONTAINER_NAME /garage key create "$KEY_NAME")
fi
ACCESS_KEY=$(echo "$KEY_INFO" | grep "Key ID" | awk '{print $3}' | tr -d '\r')
SECRET_KEY=$(echo "$KEY_INFO" | grep "Secret key" | awk '{print $3}' | tr -d '\r')

# 4. Link Key to Bucket
docker exec $CONTAINER_NAME /garage bucket allow "$BUCKET_NAME" --key "$KEY_NAME" --read --write

# 4.1 Enable Website Access (Local Testing)
echo "🌐 Enabling website access for verification..."
docker exec $CONTAINER_NAME /garage bucket website "$BUCKET_NAME" --index index.html

# 5. Automate secret.json update
echo "📝 Updating $SECRETS_FILE..."
python3 - <<EOF
import json
import os
from pathlib import Path

path = Path("$SECRETS_FILE")
data = {}
if path.exists():
    with open(path, "r") as f:
        data = json.load(f)

data["GARAGE_S3_ACCESS_KEY"] = "$ACCESS_KEY"
data["GARAGE_S3_SECRET_KEY"] = "$SECRET_KEY"

with open(path, "w") as f:
    json.dump(data, f, indent=2)
EOF

echo "--------------------------------------"
echo "🚀 Garage is ready!"
echo "Endpoint: http://localhost:3900"
echo "Bucket:   $BUCKET_NAME"
echo ""
echo "Credential Info (Update your .secrets.json with these):"
echo "$KEY_INFO"
echo "--------------------------------------"
