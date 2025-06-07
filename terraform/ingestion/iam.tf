############################################################
# Build a policy = Kinesis READ  +  S3 WRITE
############################################################
data "aws_iam_policy_document" "preprocess_access" {
  statement {
    sid    = "KinesisRead"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:Get*",
      "kinesis:ListShards"
    ]
    resources = [aws_kinesis_stream.ingest.arn]
  }

  statement {
    sid    = "S3Write"
    effect = "Allow"
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.processed.arn}/*"
    ]
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
