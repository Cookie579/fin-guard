#####################
# IAM POLICY (Kinesis + S3 write access)
#####################
resource "aws_iam_policy" "ingest_sec_go_policy" {
  name   = "ingest-sec-go-kinesis-s3"
  path   = "/"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid      = "KinesisPut"
        Effect   = "Allow"
        Action   = ["kinesis:PutRecord"]
        Resource = "arn:aws:kinesis:us-east-1:084071638445:stream/fin-guard-stream"
      },
      {
        Sid      = "S3Put"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:aws:s3:::fin-processed-filings/*"
      }
    ]
  })
}

#####################
# IRSA ROLE via your eks-irsa module
#####################
module "ingest_sec_go_irsa" {
  source               = "./eks-irsa"
  cluster_name         = "fin-guard"
  namespace            = "default"
  service_account_name = "ingest-sec-go-sa"
  irsa_role_name       = "ingest-sec-go-irsa-role"
  policy_arn           = aws_iam_policy.ingest_sec_go_policy.arn

  depends_on = [
    module.eks
  ]
}
