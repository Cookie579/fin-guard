# ============================================================
# Kubernetes Cost Controls - Resource Quotas and Scaling Limits
# ============================================================
# These resources implement cost ceiling controls for the FinGuard EKS cluster.
# They prevent runaway resource consumption at both namespace and cluster level.

# ============================================================
# B) Namespace-level cap: ResourceQuota
# ============================================================
# Prevents runaway resource usage across many deployments within the finguard namespace.
# This is the simplest "budget fence" inside Kubernetes.

resource "kubernetes_resource_quota" "finguard_quota" {
  metadata {
    name      = "finguard-quota"
    namespace = "finguard"
  }

  spec {
    hard = {
      pods               = "50"
      "requests.cpu"     = "20"
      "requests.memory"  = "40Gi"
      "limits.cpu"       = "40"
      "limits.memory"    = "80Gi"
    }
  }

  depends_on = [kubernetes_namespace.finguard]
}

# ============================================================
# LimitRange: Default + Min/Max for containers
# ============================================================
# Why: ResourceQuota on requests.*/limits.* can be quietly undermined if
# pods don't set requests/limits (they may count as 0 for quota purposes,
# and HPA CPU utilization math also depends on requests).
#
# This ensures every container gets defaults + sane min/max.
# Even if you set requests/limits everywhere, this enforces consistency
# and stops future "oops we forgot" deploys.

resource "kubernetes_limit_range" "finguard_limits" {
  metadata {
    name      = "finguard-limits"
    namespace = "finguard"
  }

  spec {
    limit {
      type = "Container"

      # Default limits applied if not specified
      default = {
        cpu    = "500m"
        memory = "512Mi"
      }

      # Default requests applied if not specified
      default_request = {
        cpu    = "250m"
        memory = "256Mi"
      }

      # Maximum allowed per container
      max = {
        cpu    = "2"
        memory = "2Gi"
      }

      # Minimum required per container
      min = {
        cpu    = "50m"
        memory = "64Mi"
      }
    }
  }

  depends_on = [kubernetes_namespace.finguard]
}

# ============================================================
# HPA Best Practices (ensure ceiling is enforced at workload level)
# ============================================================
# Even with cluster/node caps, HPA can spam scale-ups → you end up with
# tons of Pending pods (wasted churn + noisy incidents).
#
# Minimum standard per HPA:
#   - maxReplicas set (conservative)
#   - behavior.scaleUp rate-limited
#   - behavior.scaleDown stabilized
#
# Example HPA spec (apply in your Helm values or K8s manifests):
#
#   spec:
#     maxReplicas: 10  # <- Hard ceiling on replicas
#     behavior:
#       scaleUp:
#         stabilizationWindowSeconds: 60
#         policies:
#           - type: Pods
#             value: 2
#             periodSeconds: 60  # Max 2 pods per minute
#       scaleDown:
#         stabilizationWindowSeconds: 300
#         policies:
#           - type: Percent
#             value: 10
#             periodSeconds: 60  # Max 10% reduction per minute

# Ensure the finguard namespace exists
resource "kubernetes_namespace" "finguard" {
  metadata {
    name = "finguard"
    labels = {
      name        = "finguard"
      environment = var.environment
      managed-by  = "terraform"
    }
  }
}

# ============================================================
# C) Cluster-level cap (the real cost stop)
# ============================================================
# Choose ONE of the following approaches based on your autoscaler:

# ------------------------------------------------------------
# Option 1: Karpenter NodePool limits (recommended for Karpenter users)
# ------------------------------------------------------------
# Karpenter supports NodePool limits on total CPU/memory.
# Once limits are hit, Karpenter won't provision more nodes.
#
# Deploy this via kubectl or Helm if using Karpenter:
#
# apiVersion: karpenter.sh/v1
# kind: NodePool
# metadata:
#   name: default
# spec:
#   limits:
#     cpu: "64"        # <- total vCPU cap for this pool
#     memory: 256Gi    # <- total mem cap

# Karpenter NodePool as Kubernetes manifest (if using kubectl_manifest provider)
# Uncomment if you have the kubectl provider configured and use Karpenter
#
# resource "kubectl_manifest" "karpenter_nodepool" {
#   yaml_body = <<-YAML
#     apiVersion: karpenter.sh/v1
#     kind: NodePool
#     metadata:
#       name: finguard-pool
#     spec:
#       limits:
#         cpu: "64"
#         memory: "256Gi"
#       template:
#         spec:
#           requirements:
#             - key: kubernetes.io/arch
#               operator: In
#               values: ["amd64"]
#             - key: karpenter.sh/capacity-type
#               operator: In
#               values: ["spot", "on-demand"]
#           nodeClassRef:
#             name: default
#   YAML
# }

# ------------------------------------------------------------
# Option 2: Managed Node Groups + Cluster Autoscaler
# ------------------------------------------------------------
# Set a hard max size on the node group(s). Cluster Autoscaler respects ASG bounds.
# Even if pods want more, nodes cannot exceed your max.
# This is configured in the EKS module node group settings.

# The node group max_size is set in main.tf eks_managed_node_groups block.
# Example constraint (update in main.tf if needed):
#   min_size     = 2
#   max_size     = 10   # <- This is the cost ceiling
#   desired_size = 3

# ============================================================
# Budget-to-Limits Translation
# ============================================================
# Practical way to choose caps:
#
#   Let B = monthly budget for EKS compute
#   Let C = avg $/hour per node (blended)
#   Max average nodes ≈ B / (730 × C)
#
# Example:
#   B = $500/month, C = $0.10/hour (t3.medium spot)
#   Max nodes ≈ 500 / (730 × 0.10) ≈ 6.8 → round to 7
#
# Then translate into:
#   - NodeGroup max_size = 7, or
#   - Karpenter limits.cpu = 7 * 2 vCPU = 14
#
# You don't need perfect math—you just need a ceiling that's defensible.

# ============================================================
# Variables for cost control tuning
# ============================================================

variable "resource_quota_pods" {
  description = "Maximum number of pods in finguard namespace"
  type        = number
  default     = 50
}

variable "resource_quota_cpu_requests" {
  description = "Total CPU requests allowed in finguard namespace"
  type        = string
  default     = "20"
}

variable "resource_quota_memory_requests" {
  description = "Total memory requests allowed in finguard namespace"
  type        = string
  default     = "40Gi"
}

variable "resource_quota_cpu_limits" {
  description = "Total CPU limits allowed in finguard namespace"
  type        = string
  default     = "40"
}

variable "resource_quota_memory_limits" {
  description = "Total memory limits allowed in finguard namespace"
  type        = string
  default     = "80Gi"
}
