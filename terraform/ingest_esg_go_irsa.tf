#####################
# IAM POLICY (only what ingest-esg-go needs)
#####################
resource "aws_iam_policy" "ingest_esg_go_policy" {
  name   = "ingest-esg-go-s3-kinesis"
  path   = "/"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # ➊ if it really needs to send to Kinesis
      {
        Sid      = "PutToFinGuardStream"
        Effect   = "Allow"
        Action   = ["kinesis:PutRecord"]
        Resource = "arn:aws:kinesis:us-east-1:084071638445:stream/fin-guard-stream"
      },
      # ➋ read/write in the ESG bucket
      {
        Sid      = "S3Access"
        Effect   = "Allow"
        Action   = ["s3:GetObject","s3:PutObject"]
        Resource = "arn:aws:s3:::fin-processed-esg-reports/*"
      }
    ]
  })
}

#####################
# IRSA ROLE via your eks-irsa module
#####################
module "ingest_esg_go_irsa" {
  source               = "./eks-irsa"
  cluster_name         = "fin-guard"
  namespace            = "default"
  service_account_name = "fin-guard-sa"
  irsa_role_name       = "ingest-esg-go-irsa-role"
  policy_arn           = aws_iam_policy.ingest_esg_go_policy.arn

  depends_on = [
    module.eks        
  ]
}
