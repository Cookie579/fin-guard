# RB-02: DynamoDB Throttling / Hot Partition

| Field | Value |
|-------|-------|
| **ID** | RB-02 |
| **Incident** | DynamoDB Throttling / Hot Partition |
| **Severity** | 🟡 Warning (transient) / 🔴 Critical (sustained >10 min) |
| **Primary Signals** | `DDBThrottledRequests`, `DDBLatencyP99High`, `ReadThrottledEvents`, `WriteThrottledEvents` |
| **Owner** | finguard-dataplane |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

DynamoDB is rejecting read/write requests due to exceeded provisioned capacity or hot partition issues. This causes:

- **Ingestion jobs failing** — state/seen checks fail
- **Lock acquisition failures** — distributed locks timeout
- **Data inconsistency risk** — seen tables may not update

**Likely causes:**
- Hot partition (single ticker dominating traffic)
- Burst traffic exceeding provisioned capacity
- Large backfill operation
- Missing TTL causing table bloat

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Identify throttled table
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ThrottledRequests \
  --dimensions Name=TableName,Value=social_state \
  --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum

# 2. Check current capacity mode
aws dynamodb describe-table --table-name social_state \
  --query 'Table.BillingModeSummary.BillingMode'

# 3. Reduce ingestion concurrency immediately
kubectl scale deployment finguard-orchestrator -n finguard --replicas=1
```

---

## Triage (Confirm Root Cause)

### Identify Hot Partition

```bash
# Check CloudWatch Contributor Insights (if enabled)
aws cloudwatch get-insight-rule-report \
  --rule-name DynamoDB-Hot-Keys \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300

# Check item counts per partition key prefix
aws dynamodb scan --table-name social_state \
  --select COUNT \
  --filter-expression "begins_with(pk, :prefix)" \
  --expression-attribute-values '{":prefix": {"S": "stocktwits#AAPL"}}'
```

### Check Table Metrics

```bash
# Consumed vs provisioned capacity
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ConsumedWriteCapacityUnits \
  --dimensions Name=TableName,Value=social_state \
  --start-time $(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 --statistics Sum
```

**Good state:** ConsumedWCU < 80% of ProvisionedWCU
**Bad state:** ConsumedWCU ~= ProvisionedWCU with throttles

### Check Ingestion Patterns

```bash
# Find which jobs are writing most
kubectl logs -l app=ingest-social-go -n finguard --since=10m | \
  grep -o 'ticker=[A-Z]*' | sort | uniq -c | sort -rn | head -10
```

---

## Mitigations (Safest → Riskiest)

### Option 1: Wait for Auto-Scaling (Safest)

If table is in on-demand mode or has auto-scaling:

```bash
# Verify auto-scaling is enabled
aws application-autoscaling describe-scalable-targets \
  --service-namespace dynamodb \
  --resource-ids "table/social_state"
```

Wait 1-2 minutes for capacity to scale up.

### Option 2: Stagger Hot Tickers

Temporarily exclude hot tickers from batch processing:

```bash
# Identify hot ticker
HOT_TICKER=$(kubectl logs -l app=ingest-social-go -n finguard --since=5m | \
  grep -o 'ticker=[A-Z]*' | sort | uniq -c | sort -rn | head -1 | awk '{print $2}')

# Skip in next batch
kubectl set env deployment/finguard-orchestrator -n finguard SKIP_TICKERS=$HOT_TICKER
```

### Option 3: Increase Provisioned Capacity

```bash
# Increase WCU (provisioned mode only)
aws dynamodb update-table \
  --table-name social_state \
  --provisioned-throughput ReadCapacityUnits=50,WriteCapacityUnits=100

# Or switch to on-demand
aws dynamodb update-table \
  --table-name social_state \
  --billing-mode PAY_PER_REQUEST
```

⚠️ **Warning:** Switching to on-demand can increase costs significantly.

### Option 4: Write Sharding (Longer-term)

If hot partition is structural, implement write sharding:

```python
# Instead of pk = "stocktwits#AAPL"
# Use pk = "stocktwits#AAPL#" + hash(job_id) % 4
```

This requires code changes and is a follow-up action.

---

## Verification / Exit Criteria

```bash
# Throttle events should stop
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ThrottledRequests \
  --dimensions Name=TableName,Value=social_state \
  --start-time $(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum
# Should show 0

# Jobs should complete normally
kubectl get pods -n finguard -l component=ingestion --no-headers | grep -v Completed
```

**Exit when:**
- ThrottledRequests = 0 for 5 minutes
- Job completion rate returns to normal
- No DDB-related errors in job logs

---

## Escalation

| Condition | Action |
|-----------|--------|
| Throttling persists >30 min | Page infrastructure team |
| Table approaching 10TB (partition limit) | Escalate to DBA |
| On-demand costs spike unexpectedly | Alert cost owner |

---

## Aftercare

1. **Incident ticket**: Note table, duration, root cause
2. **Follow-ups**:
   - [ ] Review partition key design for hot keys
   - [ ] Enable Contributor Insights for proactive monitoring
   - [ ] Consider write sharding for high-volume tickers
   - [ ] Review TTL settings to reduce table size
