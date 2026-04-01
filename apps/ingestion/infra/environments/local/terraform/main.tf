terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# 1. Configure the AWS Provider to point to LocalStack
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3             = "http://localhost:4566"
    secretsmanager = "http://localhost:4566"
    iam            = "http://localhost:4566"
    sts            = "http://localhost:4566"
  }
}

# 2. Provision the S3 Landing Zone
resource "aws_s3_bucket" "landing_zone" {
  bucket = "landing-zone"
}

resource "aws_s3_bucket_public_access_block" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 3. Provision the Database Password in Secrets Manager
resource "aws_secretsmanager_secret" "clickhouse_password" {
  name = "CLICKHOUSE_PASSWORD"
}

resource "aws_secretsmanager_secret_version" "clickhouse_password" {
  secret_id     = aws_secretsmanager_secret.clickhouse_password.id
  secret_string = "password"
}

# 4. Provision IAM Role for the Ingestion App
resource "aws_iam_role" "ingestion_worker_role" {
  name = "ingestion-worker-role"

  # Trust Policy: Allows the EC2 service (simulated) or the account itself to assume this role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })
}

# 5. Provision IAM Policy for S3 and Secrets Access
resource "aws_iam_policy" "ingestion_worker_policy" {
  name        = "ingestion-worker-policy"
  description = "Permissions for ingestion worker to access S3 and Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.landing_zone.arn,
          "${aws_s3_bucket.landing_zone.arn}/*"
        ]
      },
      {
        Action   = ["secretsmanager:GetSecretValue"]
        Effect   = "Allow"
        Resource = [aws_secretsmanager_secret.clickhouse_password.arn]
      }
    ]
  })
}

# 6. Attach Policy to Role
resource "aws_iam_role_policy_attachment" "ingestion_attach" {
  role       = aws_iam_role.ingestion_worker_role.name
  policy_arn = aws_iam_policy.ingestion_worker_policy.arn
}

# 7. Output for developer convenience
output "s3_endpoint" {
  value = "http://localhost:4566"
}

output "bucket_name" {
  value = aws_s3_bucket.landing_zone.bucket
}

output "ingestion_role_arn" {
  value = aws_iam_role.ingestion_worker_role.arn
}