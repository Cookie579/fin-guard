#############################################
# S3 bucket for cleaned filings
#############################################
resource "aws_s3_bucket" "processed" {
  bucket        = var.processed_bucket_name  # must be globally unique
  acl           = "private"

  # ❗ Dev-only: destroy bucket even if objects exist.
  #   Remove or set to false for production.
  force_destroy = true
}

#############################################
# Enable object versioning
#############################################
resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id

  versioning_configuration {
    status = "Enabled"
  }
}

#############################################
# Lifecycle rule – auto-expire dev objects
#############################################
resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    id     = "expire-old-dev-objects"
    status = "Enabled"

    expiration {
      days = 14   # delete objects after 14 days (dev convenience)
    }
  }
}
