output "kinesis_stream_name" {
  value = aws_kinesis_stream.ingest.name
}

output "s3_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}
