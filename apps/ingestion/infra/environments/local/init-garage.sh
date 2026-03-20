#!/bin/bash
# init-garage.sh - Automate Garage S3 Setup

CONTAINER_NAME="garage"
BUCKET_NAME="my-bucket"
KEY_NAME="my-key"

echo "⏳ Waiting for Garage to start..."
until docker exec $CONTAINER_NAME /garage status > /dev/null 2>&1; do
  sleep 2
done

# 1. Get the Node ID
NODE_ID=$(docker exec $CONTAINER_NAME /garage status | grep "Node ID" | awk '{print $3}')
echo "✅ Found Node ID: $NODE_ID"

# 2. Assign role and apply layout (Required for Garage to function)
echo "🏗️  Configuring cluster layout..."
docker exec $CONTAINER_NAME /garage layout assign "$NODE_ID" -z dc1 -c 1G
docker exec $CONTAINER_NAME /garage layout apply --version 1

# 3. Create API Key and Bucket
echo "🔑 Creating S3 credentials and bucket..."
docker exec $CONTAINER_NAME /garage key create "$KEY_NAME"
docker exec $CONTAINER_NAME /garage bucket create "$BUCKET_NAME"

# 4. Link Key to Bucket
docker exec $CONTAINER_NAME /garage bucket allow "$BUCKET_NAME" --key "$KEY_NAME" --read --write

echo "--------------------------------------"
echo "🚀 Garage is ready!"
echo "Endpoint: http://localhost:3900"
echo "Bucket:   $BUCKET_NAME"
echo "--------------------------------------"
# Display the keys so you can copy them into your app
docker exec $CONTAINER_NAME /garage key list
