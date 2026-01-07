# RB-09: ResourceQuota Near/At Limit

| Field | Value |
|-------|-------|
| **ID** | RB-09 |
| **Incident** | ResourceQuota Near/At Limit |
| **Severity** | 🟡 Warning (>80%) / 🔴 Critical (100%, pods rejected) |
| **Primary Signals** | `PodQuotaNearLimit`, `ResourceQuotaAtLimit`, `CPURequestQuotaNearLimit`, `MemoryRequestQuotaNearLimit` |
| **Owner** | finguard-dataplane |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

The `finguard` namespace has hit or is approaching its ResourceQuota limits. This causes:

- **Pod creation failures** — new pods rejected by API server
- **Job failures** — ingestion jobs can't start
- **Scaling blocked** — HPA can't create new replicas

**ResourceQuota is a cost safety mechanism.** Hitting it means either legitimate growth or a problem (stuck pods, leak).

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Check current quota usage
kubectl describe resourcequota -n finguard

# 2. Find what's consuming quota
kubectl get pods -n finguard --no-headers | wc -l

# 3. Check for stuck/failed pods
kubectl get pods -n finguard --field-selector=status.phase!=Running,status.phase!=Succeeded
```

---

## Triage (Confirm Root Cause)

### Check Quota Details

```bash
kubectl describe resourcequota finguard-quota -n finguard
```

Output shows:
```
Name:            finguard-quota
Namespace:       finguard
Resource         Used   Hard
--------         ----   ----
pods             48     50      <-- Near limit!
requests.cpu     18     20
requests.memory  35Gi   40Gi
limits.cpu       35     40
limits.memory    70Gi   80Gi
```

### Identify Top Consumers

```bash
# Pods by CPU request
kubectl get pods -n finguard -o json | jq -r '
  .items[] |
  {name: .metadata.name, cpu: .spec.containers[0].resources.requests.cpu} |
  "\(.cpu)\t\(.name)"' | sort -rn | head -10

# Pods by memory request
kubectl get pods -n finguard -o json | jq -r '
  .items[] |
  {name: .metadata.name, mem: .spec.containers[0].resources.requests.memory} |
  "\(.mem)\t\(.name)"' | sort -rn | head -10
```

### Check for Stuck Pods

```bash
# Failed pods still consuming quota
kubectl get pods -n finguard --field-selector=status.phase=Failed

# Pods in weird states
kubectl get pods -n finguard | grep -E "Error|CrashLoop|Pending|Unknown"

# Old completed jobs not cleaned up
kubectl get jobs -n finguard --field-selector=status.successful=1 | wc -l
```

---

## Mitigations (Safest → Riskiest)

### Option 1: Clean Up Completed/Failed Pods

```bash
# Delete completed jobs (and their pods)
kubectl delete jobs -n finguard --field-selector=status.successful=1

# Delete failed pods
kubectl delete pods -n finguard --field-selector=status.phase=Failed

# Delete pods in Error state
kubectl get pods -n finguard | grep Error | awk '{print $1}' | xargs kubectl delete pod -n finguard
```

### Option 2: Enable TTL for Jobs

If jobs aren't auto-cleaning, enable TTL:

```bash
# Jobs should have ttlSecondsAfterFinished
# Check current jobs
kubectl get jobs -n finguard -o jsonpath='{.items[0].spec.ttlSecondsAfterFinished}'

# If empty, consider patching job templates
```

### Option 3: Reduce Job Concurrency

```bash
# Limit concurrent jobs
kubectl set env deployment/finguard-orchestrator -n finguard MAX_CONCURRENT_JOBS=5
```

### Option 4: Temporarily Increase Quota

```bash
# Increase pod limit
kubectl patch resourcequota finguard-quota -n finguard --type=merge \
  -p '{"spec":{"hard":{"pods":"75"}}}'

# Or CPU
kubectl patch resourcequota finguard-quota -n finguard --type=merge \
  -p '{"spec":{"hard":{"requests.cpu":"30"}}}'
```

⚠️ **Warning:** This increases cost ceiling. Ensure node capacity exists.

### Option 5: Scale Down Non-Critical Workloads

```bash
# Temporarily reduce always-on services
kubectl scale deployment/nlp-pipeline -n finguard --replicas=1
kubectl scale deployment/llm-explainer -n finguard --replicas=1
```

---

## Verification / Exit Criteria

```bash
# Quota usage should decrease
kubectl describe resourcequota finguard-quota -n finguard | grep -E "pods|cpu|memory"

# No rejected pods
kubectl get events -n finguard --field-selector=reason=FailedCreate | grep -c quota
# Should be 0 recent

# New pods creating successfully
kubectl run test-pod --image=busybox -n finguard --rm -it --restart=Never -- echo "Quota OK"
kubectl delete pod test-pod -n finguard --ignore-not-found
```

**Exit when:**
- Quota usage < 80% of hard limit
- No pod creation failures
- Normal job flow resumed

---

## Escalation

| Condition | Action |
|-----------|--------|
| Can't free quota (all pods needed) | Request quota increase via change ticket |
| Quota increase rejected | Work with platform team on alternatives |
| Leak suspected (quota growing unexpectedly) | Investigate pod/job lifecycle |

---

## Aftercare

1. **Incident ticket**: Note which resource hit limit, cleanup actions
2. **Review**: Was quota sized appropriately?
3. **Follow-ups**:
   - [ ] Add job TTL to ensure cleanup
   - [ ] Review LimitRange defaults
   - [ ] Set up 80% quota alerts (early warning)
   - [ ] Consider namespace isolation for batch vs streaming
