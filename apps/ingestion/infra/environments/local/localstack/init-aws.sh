#!/bin/bash

set -e # Exit immediately if a command exits with a non-zero status
echo "Initializing LocalStack resources..."

# 0. Setup AWS CLI Profile for internal container tools
# This ensures the [localstack] profile exists for any tool or worker
# running inside the LocalStack environment.
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test
aws configure set region "${AWS_DEFAULT_REGION:-ap-southeast-1}"
aws configure set output json

# Function to validate S3 bucket naming conventions
validate_bucket_name() {
    local bucket_name=$1
    
    # 1. Length check (3-63 characters)
    if [[ ${#bucket_name} -lt 3 || ${#bucket_name} -gt 63 ]]; then
        echo "❌ Error: Bucket name '$bucket_name' must be between 3 and 63 characters."
        exit 1
    fi

    # 2. Character check (lowercase letters, numbers, dots, hyphens)
    # 3. Start/End check (must start and end with a letter or number)
    if [[ ! $bucket_name =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
        echo "❌ Error: Bucket name '$bucket_name' is invalid."
        echo "   Rules: Lowercase, numbers, dots, or hyphens only. Must start/end with alphanumeric."
        exit 1
    fi

    # 4. Adjacent dots check
    if [[ $bucket_name =~ \.\. ]]; then
        echo "❌ Error: Bucket name '$bucket_name' cannot contain adjacent dots."
        exit 1
    fi
}

# 0. Resolve Bucket Names
LANDING_BUCKET=${LANDING_ZONE_BUCKET:-landing-zone}
ARCHIVE_BUCKET=${ARCHIVE_VAULT_BUCKET:-archive-vault}
CAS_BUCKET=${CAS_VAULT_BUCKET:-cas-vault}

# Validate resolved names before provisioning
validate_bucket_name "$LANDING_BUCKET"
validate_bucket_name "$ARCHIVE_BUCKET"
validate_bucket_name "$CAS_BUCKET"

# 1. Create S3 Buckets using awslocal (automatically routes to localhost:4566)
# No need to specify --endpoint-url when using the awslocal wrapper.
awslocal s3 mb s3://"$LANDING_BUCKET"
awslocal s3 mb s3://"$ARCHIVE_BUCKET"
awslocal s3 mb s3://"$CAS_BUCKET"

# 1b. Set CORS for Landing Zone (Useful for local debugging tools)
awslocal s3api put-bucket-cors --bucket "$LANDING_BUCKET" --cors-configuration '{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "PUT", "POST", "DELETE"],
      "AllowedHeaders": ["*"]
    }
  ]
}'

# 2. Create a Secret for the Ingestion Engine
awslocal secretsmanager create-secret \
    --name "ingestion/database/clickhouse" \
    --description "Database credentials for local development" \
    --secret-string '{"user":"default","password":"password"}'

# 2b. Create S3 credentials for local_s3 service (Mimicking static keys)
awslocal secretsmanager create-secret \
    --name "local/s3/credentials" \
    --secret-string '{"key":"test","secret_key":"test"}'

# 3. Create the IAM Role for STS AssumeRole
# This role is used by AWSSessionManager. 
# Note: LocalStack Community doesn't strictly enforce policy JSON, 
# but the role must exist for assume_role to succeed.
awslocal iam create-role \
    --role-name ProdIngestionRole \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": { "Service": "lambda.amazonaws.com" }
        }]
    }'

# 4. Create CloudWatch Log Group for Telemetry
awslocal logs create-log-group --log-group-name "/ingestion/orchestrator"
awslocal logs create-log-stream \
    --log-group-name "/ingestion/orchestrator" \
    --log-stream-name "local-dev-stream"

echo "LocalStack initialization complete."
echo "Endpoints available at http://localhost:4566"