variable "aws_region" {
  default = "us-east-1"
}

variable "cluster_name" {
  default = "fin-guard"
}

variable "namespace" {
  default = "default"
}

variable "service_account_name" {
  default = "preprocess-python-sa"
}

variable "irsa_role_name" {
  default = "preprocess-irsa-role"
}

variable "policy_arn" {
  description = "ARN of the IAM policy to attach (e.g., fin-preprocess-kinesis-s3)"
}
