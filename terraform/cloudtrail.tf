# ============================================================
# CloudTrail Configuration for FinGuard
# ============================================================
# Provides comprehensive AWS-level audit logging:
# - Management events (IAM, EKS API, etc.)
# - Data events for S3 buckets (optional)
# - Data events for DynamoDB tables (optional)
# ============================================================

# ============================================================
# CloudTrail Trail
# ============================================================
resource "aws_cloudtrail" "finguard" {
  name                          = "finguard-cloudtrail"
  s3_bucket_name               = aws_s3_bucket.cloudtrail_logs.id
  s3_key_prefix                = "cloudtrail"
  include_global_service_events = true
  is_multi_region_trail        = true
  enable_log_file_validation   = true
  kms_key_id                   = aws_kms_key.cloudtrail.arn

  # CloudWatch Logs integration (for real-time alerting)
  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.cloudtrail.arn}:*"
  cloud_watch_logs_role_arn  = aws_iam_role.cloudtrail_cloudwatch.arn

  # ============================================================
  # Data Events: S3 (optional - for high-value buckets)
  # ============================================================
  event_selector {
    read_write_type           = "All"
    include_management_events = true

    # Log data events for specific S3 buckets
    data_resource {
      type   = "AWS::S3::Object"
      values = [
        "${var.raw_bucket_arn}/*",
        "${var.processed_bucket_arn}/*",
      ]
    }
  }

  # ============================================================
  # Data Events: DynamoDB (optional - for sensitive tables)
  # ============================================================
  # Uncomment if you need DynamoDB data events
  # event_selector {
  #   read_write_type           = "WriteOnly"  # Only writes to reduce volume
  #   include_management_events = false
  #
  #   data_resource {
  #     type   = "AWS::DynamoDB::Table"
  #     values = [
  #       var.social_locks_table_arn,
  #       var.social_state_table_arn,
  #     ]
  #   }
  # }

  tags = {
    Environment = var.environment
    Application = "finguard"
    ManagedBy   = "terraform"
  }
}

# ============================================================
# S3 Bucket for CloudTrail Logs
# ============================================================
resource "aws_s3_bucket" "cloudtrail_logs" {
  bucket = "finguard-cloudtrail-logs-${data.aws_caller_identity.current.account_id}"

  tags = {
    Application = "finguard"
    Purpose     = "cloudtrail-audit-logs"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.cloudtrail.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id

  rule {
    id     = "archive-old-logs"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

resource "aws_s3_bucket_policy" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AWSCloudTrailAclCheck"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:GetBucketAcl"
        Resource  = aws_s3_bucket.cloudtrail_logs.arn
      },
      {
        Sid       = "AWSCloudTrailWrite"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.cloudtrail_logs.arn}/cloudtrail/*"
        Condition = {
          StringEquals = {
            "s3:x-amz-acl" = "bucket-owner-full-control"
          }
        }
      }
    ]
  })
}

# ============================================================
# KMS Key for CloudTrail Encryption
# ============================================================
resource "aws_kms_key" "cloudtrail" {
  description             = "KMS key for CloudTrail log encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "Enable IAM User Permissions"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "Allow CloudTrail to encrypt logs"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = [
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Application = "finguard"
    Purpose     = "cloudtrail-encryption"
    ManagedBy   = "terraform"
  }
}

resource "aws_kms_alias" "cloudtrail" {
  name          = "alias/finguard-cloudtrail"
  target_key_id = aws_kms_key.cloudtrail.key_id
}

# ============================================================
# CloudWatch Log Group for CloudTrail
# ============================================================
resource "aws_cloudwatch_log_group" "cloudtrail" {
  name              = "/aws/cloudtrail/finguard"
  retention_in_days = 90

  tags = {
    Application = "finguard"
    ManagedBy   = "terraform"
  }
}

# ============================================================
# IAM Role for CloudTrail → CloudWatch Logs
# ============================================================
resource "aws_iam_role" "cloudtrail_cloudwatch" {
  name = "finguard-cloudtrail-cloudwatch"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Application = "finguard"
    ManagedBy   = "terraform"
  }
}

resource "aws_iam_role_policy" "cloudtrail_cloudwatch" {
  name = "cloudtrail-cloudwatch-logs"
  role = aws_iam_role.cloudtrail_cloudwatch.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.cloudtrail.arn}:*"
      }
    ]
  })
}

# ============================================================
# Data Sources
# ============================================================
data "aws_caller_identity" "current" {}

# ============================================================
# Variables
# ============================================================
variable "raw_bucket_arn" {
  description = "ARN of the raw data S3 bucket"
  type        = string
  default     = ""
}

variable "processed_bucket_arn" {
  description = "ARN of the processed data S3 bucket"
  type        = string
  default     = ""
}

# ============================================================
# Outputs
# ============================================================
output "cloudtrail_arn" {
  description = "ARN of the CloudTrail trail"
  value       = aws_cloudtrail.finguard.arn
}

output "cloudtrail_logs_bucket" {
  description = "S3 bucket for CloudTrail logs"
  value       = aws_s3_bucket.cloudtrail_logs.id
}
