############################################################
# Build a policy  =  Kinesis READ
#                  + S3 WRITE   (fin-processed-filings)
#                  + S3 R/W/L   (fin-processed-esg-reports)
############################################################
data "aws_iam_policy_document" "preprocess_access" {

  ########################################
  # 1. Kinesis read-only
  ########################################
  statement {
    sid     = "KinesisRead"
    effect  = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:Get*",
      "kinesis:ListShards"
    ]
    resources = [aws_kinesis_stream.ingest.arn]
  }

  ########################################
  # 2. Write cleaned filings (existing)
  ########################################
  statement {
    sid       = "S3WriteFilings"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.processed.arn}/*"]
  }

  ########################################
  # 3. Domain JSON bucket  (NEW)
  ########################################
  # 3-a  List the bucket
  statement {
    sid       = "S3DomainList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.esg.arn]
  }

  # 3-b  Read / write objects
  statement {
    sid       = "S3DomainReadWrite"
    effect    = "Allow"
    actions   = [
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = ["${aws_s3_bucket.esg.arn}/*"]
  }
}

resource "aws_iam_policy" "preprocess_policy" {
  name   = "fin-preprocess-kinesis-s3"
  policy = data.aws_iam_policy_document.preprocess_access.json
}

#################################################################
# Attach policy to ONE principal for now (local dev = IAM user)
#################################################################
resource "aws_iam_user_policy_attachment" "attach_to_user" {
  count      = var.attach_to_user == "" ? 0 : 1
  user       = var.attach_to_user
  policy_arn = aws_iam_policy.preprocess_policy.arn
}
