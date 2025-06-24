# terraform/ingest_sec_go_iam.tf
resource "aws_iam_policy" "ingest_sec_go_kinesis_s3" {
  name   = "ingest-sec-go-kinesis-s3"
  path   = "/"
  description = "Allow ingest-sec-go to PutRecord to fin-guard-stream and PutObject to fin-processed-filings S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "KinesisPut"
        Effect = "Allow"
        Action = ["kinesis:PutRecord"]
        Resource = "arn:aws:kinesis:us-east-1:084071638445:stream/fin-guard-stream"
      },
      {
        Sid    = "S3Put"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = "arn:aws:s3:::fin-processed-filings/*"
      }
    ]
  })
}
