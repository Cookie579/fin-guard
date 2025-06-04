terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.99.1"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.37.1"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "2.17.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_eks_cluster" "fin_guard" {
  name       = var.cluster_name # "fin-guard"
  depends_on = [module.eks]
}

data "aws_eks_cluster_auth" "fin_guard" {
  name       = var.cluster_name
  depends_on = [module.eks]
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.fin_guard.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.fin_guard.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.fin_guard.token
  load_config_file       = false
}

provider "helm" {
  kubernetes {
    host                   = data.aws_eks_cluster.fin_guard.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.fin_guard.certificate_authority[0].data)
    token                  = data.aws_eks_cluster_auth.fin_guard.token
  }
}
