output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_id
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_certificate_authority" {
  description = "Base64-encoded CA for Kubernetes"
  value       = module.eks.cluster_certificate_authority_data
}

output "oidc_issuer" {
  description = "OIDC issuer URL"
  value       = module.eks.cluster_oidc_issuer_url
}

output "managed_node_group_role_arn" {
  description = "IAM role ARN for the default managed node group"
  value       = module.eks.eks_managed_node_groups["default"].iam_role_arn
}


# Argo CD and Grafana service info
output "argocd_namespace" {
  description = "Namespace where Argo CD is deployed"
  value       = "argocd"
}

output "grafana_namespace" {
  description = "Namespace where Grafana is deployed"
  value       = "monitoring"
}
