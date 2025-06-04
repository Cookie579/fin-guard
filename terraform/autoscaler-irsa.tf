############################################################
#  IRSA role for Cluster Autoscaler
############################################################
locals {
  ca_service_account = "cluster-autoscaler"
  ca_namespace       = "kube-system"
}

module "ca_irsa_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = ">= 5.4"

  create_role = true
  role_name   = "${var.cluster_name}-autoscaler"

  provider_url = module.eks.cluster_oidc_issuer_url
  role_policy_arns = [aws_iam_policy.ca_policy.arn]

  oidc_fully_qualified_subjects = [
    "system:serviceaccount:${local.ca_namespace}:${local.ca_service_account}"
  ]

  # Wait until the EKS cluster (and its OIDC provider) exist
  depends_on = [module.eks]
}

resource "aws_iam_policy" "ca_policy" {
  name        = "${var.cluster_name}-autoscaler"
  description = "Permissions for Kubernetes Cluster Autoscaler"
  policy      = data.aws_iam_policy_document.ca.json
}

data "aws_iam_policy_document" "ca" {
  statement {
    effect  = "Allow"
    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeLaunchConfigurations",
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
      "autoscaling:DescribeTags",
      "ec2:DescribeLaunchTemplateVersions",
    ]
    resources = ["*"]
  }
}

output "autoscaler_role_arn" {
  value = module.ca_irsa_role.iam_role_arn
}
