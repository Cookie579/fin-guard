############################################################
# 0️⃣  Data – Availability Zones
############################################################
data "aws_availability_zones" "available" {}

############################################################
# 1️⃣  VPC
############################################################
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = ">= 5.0.0, < 6.0.0"
 
  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  public_subnets  = var.public_subnets
  private_subnets = var.private_subnets

  enable_nat_gateway = true
  single_nat_gateway = true

  tags = {
    Name    = "${var.cluster_name}-vpc"
    Project = var.cluster_name
  }
}

############################################################
# 2️⃣  Amazon EKS (v20.36.0, Kubernetes 1.33, t3.small)
############################################################
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "20.36.0"

  cluster_name    = var.cluster_name
  cluster_version = "1.33"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # Public & private API endpoints
  cluster_endpoint_public_access       = true
  cluster_endpoint_private_access      = true
  cluster_endpoint_public_access_cidrs = ["0.0.0.0/0"] # tighten later

  enable_cluster_creator_admin_permissions = true

  eks_managed_node_groups = {
    default = {
      desired_size = 3 # var.node_group_desired_capacity
      min_size     = 2 # var.node_group_min_capacity
      max_size     = 4 # var.node_group_max_capacity
      instance_types   = ["t3.small"]
    }
  }

  tags = {
    Project = var.cluster_name
  }
}

############################################################
# 3️⃣  Helm Releases
############################################################
resource "helm_release" "argo_cd" {
  name             = "argo-cd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  create_namespace = true
  namespace        = "argocd"

  wait    = false          # don’t wait for Pods/Jobs
  timeout = 600            # (optional) give it 10 min if you ever flip it back

  values     = [file("${path.module}/values/argo-cd-values.yaml")]
  depends_on = [module.eks]
}



############################################################
# 4️⃣  kube-prometheus-stack (Prometheus + Grafana + Dashboards)
############################################################
resource "helm_release" "kube_prometheus_stack" {
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "59.0.0"
  create_namespace = true
  namespace        = "monitoring"

  wait    = false          # don’t wait for Pods/Jobs
  timeout = 600            # (optional) give it 10 min if you ever flip it back

  values = [
    file("${path.module}/values/kube-prometheus-values.yaml")
  ]

  depends_on = [module.eks]
}

