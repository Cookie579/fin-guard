# RB-08: HPA at Max / Scaling Blocked

| Field | Value |
|-------|-------|
| **ID** | RB-08 |
| **Incident** | HPA at Max / Scaling Blocked |
| **Severity** | 🟡 Warning (at max but healthy) / 🔴 Critical (pods pending, latency high) |
| **Primary Signals** | `HPAAtMaxReplicas`, `HPAScaleUpBlocked`, `kube_horizontalpodautoscaler_status_current_replicas` |
| **Owner** | finguard-dataplane |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

A HorizontalPodAutoscaler has reached its `maxReplicas` limit. The system wants more pods but can't create them. This causes:

- **Increased latency** — existing pods handling more load
- **Potential request queuing** — if load exceeds capacity
- **Degraded throughput** — system can't scale to meet demand

**This is often intentional** — maxReplicas is a cost ceiling. The question is: is demand legitimate?

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Identify which HPA is at max
kubectl get hpa -n finguard

# 2. Check current vs desired replicas
kubectl describe hpa -n finguard | grep -A5 "Current\|Desired\|Max"

# 3. Check if pods are healthy
kubectl get pods -n finguard -l app=<affected-app> --no-headers | grep -v Running
```

---

## Triage (Confirm Root Cause)

### Check HPA Details

```bash
kubectl describe hpa <hpa-name> -n finguard
```

Look for:
- `AbleToScale: True/False` — can it scale?
- `ScalingActive: True/False` — is it trying?
- `Conditions` — any errors?

### Check Current Load

```bash
# CPU utilization
kubectl top pods -n finguard -l app=<affected-app>

# Request rate (if exposed via metrics)
kubectl exec -it deploy/<affected-app> -n finguard -- curl -s localhost:8080/metrics | grep request
```

### Check if Demand is Legitimate

```bash
# Recent job activity
kubectl logs deploy/finguard-orchestrator -n finguard --since=30m | \
  grep -c "job_started"

# Is there a batch backfill running?
kubectl get jobs -n finguard | wc -l
```

**Good state:** At max but pods healthy, latency acceptable
**Bad state:** At max, pods overloaded, latency spiking, Pending pods

---

## Decision Tree

```
HPA at maxReplicas
       │
       ▼
Are pods healthy + latency OK?
       │
       ├── Yes → This is intentional cost ceiling
       │         → Monitor, no action needed
       │         → Consider: is maxReplicas set correctly?
       │
       └── No → Demand exceeds capacity
                    │
                    ├── Is demand legitimate?
                    │   ├── Yes → Temporarily increase maxReplicas
                    │   └── No  → Investigate source (attack? bug?)
                    │
                    └── Are there Pending pods?
                        ├── Yes → Check node capacity (→ RB-10)
                        └── No  → HPA is working, just at limit
```

---

## Mitigations (Safest → Riskiest)

### Option 1: Monitor Only (If Healthy)

If pods are running fine and latency is acceptable, this is working as designed.

```bash
# Set up alert snooze for 1h
# Document that this was reviewed and accepted
```

### Option 2: Temporarily Increase maxReplicas

```bash
# Increase max from 10 to 15
kubectl patch hpa <hpa-name> -n finguard --type=merge \
  -p '{"spec":{"maxReplicas":15}}'

# Remember to revert after incident
kubectl patch hpa <hpa-name> -n finguard --type=merge \
  -p '{"spec":{"maxReplicas":10}}'
```

### Option 3: Reduce Incoming Load

```bash
# Pause new job creation
kubectl scale deployment/finguard-orchestrator -n finguard --replicas=0

# Let existing jobs complete
kubectl get jobs -n finguard --field-selector=status.successful=0
```

### Option 4: Optimize Pod Resources

If pods are CPU-bound, vertical scaling may help:

```bash
# Check current limits
kubectl get deployment <affected-app> -n finguard -o jsonpath='{.spec.template.spec.containers[0].resources}'

# Increase limits (requires pod restart)
kubectl set resources deployment/<affected-app> -n finguard \
  --limits=cpu=2,memory=2Gi --requests=cpu=1,memory=1Gi
```

---

## Verification / Exit Criteria

```bash
# HPA no longer at max
kubectl get hpa -n finguard | grep -v "10/10"

# OR if at max but healthy:
# - Latency within SLO
kubectl logs deploy/<affected-app> -n finguard --since=5m | grep "duration_ms" | \
  awk -F'=' '{sum+=$NF; count++} END {print "Avg:", sum/count, "ms"}'

# - No errors
kubectl logs deploy/<affected-app> -n finguard --since=5m | grep -ic error
# Should be 0 or very low
```

**Exit when:**
- HPA no longer at max, OR
- At max but pods healthy and latency acceptable

---

## Escalation

| Condition | Action |
|-----------|--------|
| All HPAs at max across multiple services | Page infrastructure — may need node scaling |
| Demand is suspicious (potential attack) | Page security team |
| Need to change maxReplicas permanently | Create change ticket for review |

---

## Aftercare

1. **Incident ticket**: Note which HPA, peak demand, action taken
2. **Review**: Was maxReplicas appropriate for the load?
3. **Follow-ups**:
   - [ ] Review HPA settings vs typical and peak load
   - [ ] Consider adjusting scaleUp/scaleDown policies
   - [ ] Add alerting at 80% of maxReplicas (early warning)
