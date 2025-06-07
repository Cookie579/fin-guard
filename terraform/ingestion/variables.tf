variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "kinesis_stream_name" {
  type    = string
  default = "fin-guard-stream"
}

variable "processed_bucket_name" {
  type    = string
  default = "fin-processed-filings"   # must be globally unique; add suffix if needed
}

# For dev convenience: name of IAM user to attach the policy to.
# Set this to your user (e.g., "agasthya") or leave blank when you switch to IRSA.
variable "attach_to_user" {
  type    = string
  default = "agasthya-admin"   
}
