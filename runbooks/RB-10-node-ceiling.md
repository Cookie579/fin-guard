# RB-10: Node Ceiling Reached

| Field | Value |
|-------|-------|
| **ID** | RB-10 |
| **Incident** | Node Ceiling Reached |
| **Severity** | 🟡 Warning (at ceiling but stable) / 🔴 Critical (pods Pending, autoscaler blocked) |
| **Primary Signals** | `ClusterAutoscalerUnableToScaleUp`, `NodeCountAtStaticCeiling`, `PodsStuckPending`, `cluster_autoscaler_unschedulable_pods_count` |
| **Owner** | finguard-infra |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

The cluster has reached its maximum node count (ASG `max_size` or Karpenter limits). The Cluster Autoscaler cannot provision more nodes. This causes:

- **Pods stuck Pending** — no capacity to schedule
- **Job failures** — ingestion jobs timeout waiting for resources
- **System-wide impact** — affects all scaling workloads

**This is an intentional cost ceiling.** The question: is the demand legitimate and should we raise the ceiling?

**Current limits:**
- Ingestion node group: `max_size = 10`
- Processing node group: `max_size = 5`
- **Total cluster ceiling: 15 nodes**

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Check current node count
kubectl get nodes --no-headers | wc -l

# 2. Find Pending pods
kubectl get pods -n finguard -o wide | grep Pending

# 3. Check why pods are Pending
kubectl describe pod <pending-pod> -n finguard | grep -A10 Events
```

---

## Triage (Confirm Root Cause)

### Check Cluster Autoscaler Status

```bash
# CA status
kubectl logs -n kube-system -l app=cluster-autoscaler --since=10m | grep -i "scale\|unable\|unschedulable"

# Unschedulable pod count
kubectl get pods --all-namespaces --field-selector=status.phase=Pending --no-headers | wc -l
```

### Check Node Groups

```bash
# AWS ASG current vs max
aws autoscaling describe-auto-scaling-groups \
  --query 'AutoScalingGroups[?contains(Tags[?Key==`kubernetes.io/cluster/finguard`].Value, `owned`)].{Name:AutoScalingGroupName,Desired:DesiredCapacity,Max:MaxSize}' \
  --output table
```

Expected output:
```
| Name                     | Desired | Max |
|--------------------------|---------|-----|
| finguard-ingestion-ng    | 10      | 10  | <-- At max!
| finguard-processing-ng   | 5       | 5   | <-- At max!
```

### Check Why Pods Can't Schedule

```bash
# Get scheduling failure reason
kubectl describe pod <pending-pod> -n finguard | grep -A5 "Events"

# Common reasons:
# - Insufficient cpu/memory
# - Node affinity/selector mismatch
# - Taints without tolerations
# - PVC pending (storage)
```

### Check Node Resource Usage

```bash
# Node resource availability
kubectl describe nodes | grep -A5 "Allocatable" | head -30

# Current allocation
kubectl describe nodes | grep -A10 "Allocated resources"
```

---

## Decision Tree

```
Cluster at node ceiling
         │
         ▼
Are there Pending pods?
         │
         ├── No → At ceiling but stable
         │        → Monitor, no action needed
         │
         └── Yes → Capacity needed
                      │
                      ├── Is demand legitimate?
                      │   │
                      │   ├── Yes → Temporarily increase max_size
                      │   │        OR reduce workload
                      │   │
                      │   └── No  → Investigate (runaway jobs? attack?)
                      │
                      └── Can we free up existing capacity?
                          - Clean up completed jobs
                          - Scale down non-critical workloads
                          - Optimize resource requests
```

---

## Mitigations (Safest → Riskiest)

### Option 1: Free Up Existing Capacity

```bash
# Clean up completed jobs
kubectl delete jobs -n finguard --field-selector=status.successful=1

# Delete failed pods
kubectl delete pods -n finguard --field-selector=status.phase=Failed

# Scale down non-critical
kubectl scale deployment/llm-explainer -n finguard --replicas=1
```

### Option 2: Reduce Incoming Load

```bash
# Pause job orchestration
kubectl scale deployment/finguard-orchestrator -n finguard --replicas=0

# Let existing jobs complete naturally
kubectl get jobs -n finguard --field-selector=status.successful=0 | wc -l
```

### Option 3: Optimize Resource Requests

If pods are over-requesting:

```bash
# Check actual usage vs requests
kubectl top pods -n finguard

# If pods using <50% of requests, consider reducing
# This is a longer-term fix
```

### Option 4: Temporarily Increase Node Ceiling

```bash
# Increase ingestion node group max_size
aws autoscaling update-auto-scaling-group \
  --auto-scaling-group-name finguard-ingestion-ng \
  --max-size 15

# Or via Terraform (preferred)
# Edit terraform/eks.tf: max_size = 15
# terraform apply
```

⚠️ **Warning:** This increases cost. Get approval before increasing permanently.

```bash
# Remember to revert after incident
aws autoscaling update-auto-scaling-group \
  --auto-scaling-group-name finguard-ingestion-ng \
  --max-size 10
```

### Option 5: Add Spot Instances (Cost-Effective Scale)

If more capacity needed but cost is a concern:

```bash
# Modify node group to use mixed instances (spot + on-demand)
# This requires Terraform/CDK changes
```

---

## Verification / Exit Criteria

```bash
# No more Pending pods
kubectl get pods --all-namespaces --field-selector=status.phase=Pending --no-headers | wc -l
# Should be 0

# Cluster Autoscaler healthy
kubectl logs -n kube-system -l app=cluster-autoscaler --since=5m | grep -i error
# Should be empty

# Node count stable
kubectl get nodes --no-headers | wc -l
```

**Exit when:**
- No Pending pods
- All workloads healthy
- Autoscaler not blocked

---

## Escalation

| Condition | Action |
|-----------|--------|
| Need permanent max_size increase | Create change ticket for cost review |
| AWS quota limit hit | Contact AWS support |
| Cluster instability | Page infrastructure team |

---

## Aftercare

1. **Incident ticket**: Note peak demand, max_size changes, cleanup actions
2. **Cost review**: If max_size was increased, review actual spend after 24h
3. **Communication**: If this was intentional ceiling, inform stakeholders that system throttled correctly
4. **Follow-ups**:
   - [ ] Review if node ceiling is appropriate for expected load
   - [ ] Consider spot instances for burst capacity
   - [ ] Add alerting at 80% of node ceiling (early warning)
   - [ ] Review job concurrency limits
