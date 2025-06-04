variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  default     = "fin-guard"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnets" {
  description = "List of public subnet CIDR blocks (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "private_subnets" {
  description = "List of private subnet CIDR blocks (one per AZ)"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "node_group_instance_type" {
  description = "EC2 instance type for worker nodes"
  type        = string
  default     = "t3.medium"
}

variable "node_group_desired_capacity" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 3
}

variable "node_group_min_capacity" {
  description = "Minimum worker node count"
  type        = number
  default     = 2
}

variable "node_group_max_capacity" {
  description = "Maximum worker node count"
  type        = number
  default     = 4
}
