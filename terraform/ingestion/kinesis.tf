resource "aws_kinesis_stream" "ingest" {
  name             = var.kinesis_stream_name
  shard_count      = 1
  retention_period = 24   # hours, good for dev
}
