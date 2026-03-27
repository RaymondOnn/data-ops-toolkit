# Moto S3 Integration Guide

This document explains how to use [Moto](https://github.com/getmoto/moto) to mock AWS S3 services for local development and testing. This allows the ingestion pipeline to run storage-heavy workflows without requiring real AWS credentials or incurring costs.

## Prerequisites

Ensure the required testing dependencies are installed in your environment:

```bash
pip install "moto[server]" boto3
```

---

## 1. Mode A: Ray Local Mode (In-Memory Mocking)

**Best for:** Rapid debugging, unit tests, and single-machine development.

### How it Works

When you run Ray in `local` mode, the Orchestrator and all Workers share the same Python process. The `mock_aws` context manager intercepts all S3 calls at the `botocore` level and redirects them to a virtual S3 environment stored in the process RAM.

### Configuration (`app.yaml`)

Use a service definition that includes the `use_mock: true` flag. This tells the internal `S3Client` to bootstrap the bucket automatically since Moto starts with an empty state.

```yaml
local:
  services:
    mock_s3:
      type: data_lake
      url: "s3://landing-zone"
      storage_options:
        endpoint_url: "http://localhost:5000"
        use_ssl: false
        use_mock: true  # Triggers S3Client._fs.mkdir()
        key: "testing"
        secret: "testing"
        client_kwargs:
          region_name: "us-east-1"
```

### Execution

Run the CLI with the `--debug` and `--ray-mode local` flags:

```bash
python apps/ingestion/__main__.py run 2023-10-27 \
  -j test_job -d orders \
  --debug --ray-mode local \
  --set extract.service_ref=mock_s3
```

### Verification

You can use any recent version of moto[server] (e.g., moto[server]>=4.0.0) to inspect the state of your mocked S3. The key is to interact with it using boto3 (the AWS SDK for Python), pointing boto3 to the same endpoint that your s3fs client is using.

**Here's how you can inspect the mocked S3**

Since mock_aws() wraps your entire application, you can add a small boto3 snippet within the mock_aws() context in your **main**.py (or a test file) to inspect the bucket

``` py
# Example snippet to add within the mock_aws() context in __main__.py or a test
import boto3

# This client will automatically be mocked by moto
s3_client = boto3.client("s3", region_name="us-east-1") 

# List buckets
print("Mocked S3 Buckets:", [b['Name'] for b in s3_client.list_buckets()['Buckets']])

# List objects in your landing zone
bucket_name = "landing-zone" # From your app.yaml
try:
    objects = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in objects:
        print(f"Objects in {bucket_name}:")
        for obj in objects['Contents']:
            print(f"  - {obj['Key']} (Size: {obj['Size']} bytes)")
            # To get content:
            # response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            # content = response['Body'].read().decode('utf-8')
            # print(f"    Content: {content[:100]}...") # Print first 100 chars
    else:
        print(f"No objects found in {bucket_name}.")
except s3_client.exceptions.NoSuchBucket:
    print(f"Bucket {bucket_name} does not exist in mock.")

```

---

## 2. Mode B: Ray Cluster Mode (Standalone Moto Server)

**Best for:** Testing distributed behavior, multi-node clusters, or performance benchmarking.

### How it Works

In `cluster` mode, Ray Workers run in separate processes (or separate containers). Since they do not share memory, an in-memory mock will not work. Instead, we run a standalone Moto server that acts as a local S3 web server.

### Step 1: Start the Moto Server

In a separate terminal window:

```bash
moto_server s3 -p 5000
```

### Step 2: Configuration

Define a service in your `app.yaml` that explicitly points to the `moto_server`'s endpoint.

```yaml
# apps/ingestion/config/app.yaml
# ...
local:
  # ... other local settings
  services:
    # ... other services
    moto_server_s3:
      type: data_lake # Or flat_file, standard_archive, cas_archive
      url: "s3://landing-zone" # The bucket name you want to mock
      storage_options:
        endpoint_url: "http://localhost:5000" # IMPORTANT: This is where moto_server runs
        key: "testing"
        secret: "testing"
        use_ssl: false
        use_mock: false # Set to true if you want the client to auto-create the bucket on the server
        client_kwargs:
          region_name: "us-east-1"
```

### Step 3: Execution

Run the CLI with the default cluster mode:

```bash
python apps/ingestion/__main__.py run 2023-10-27 \
  -j test_job -d orders \
  --ray-mode cluster \
  --set extract.service_ref=mock_s3
```

### Verification

You would run the boto3 inspection code in a separate script or terminal and explicitly point it to the Moto server's endpoint.

``` py
# Separate script or terminal
import boto3

s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:5000", # Match your moto_server port
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
    region_name="us-east-1",
)

# ... (rest of the inspection code as above)

```
---

## Summary of Logic

| Component | Logic |
| :--- | :--- |
| **CLI (`__main__.py`)** | Wraps the run in `mock_aws()` context ONLY if `ray_mode == LOCAL`. |
| **Engine (`engine.py`)** | Initializes Ray with `local_mode=True` if requested, ensuring memory sharing. |
| **S3 Client (`s3.py`)** | Checks `use_mock: true`. If enabled, it automatically calls `mkdir()` on the target bucket if it doesn't exist. |
| **Workers** | Rehydrate the `S3Client` using the provided config; if in local mode, they automatically "see" the mocked S3. |
