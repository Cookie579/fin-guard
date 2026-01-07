# RB-03: Kinesis Backlog / Shard Saturation

| Field | Value |
|-------|-------|
| **ID** | RB-03 |
| **Incident** | Kinesis Backlog / Shard Saturation |
| **Severity** | 🟡 Warning (lag <1 hour) / 🔴 Critical (lag >1 hour or data loss risk) |
| **Primary Signals** | `KinesisIteratorAge`, `KinesisGetRecordsIteratorAgeMilliseconds`, `KinesisPutThrottled` |
| **Owner** | finguard-dataplane |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

The Kinesis stream `fin-guard-stream` has more incoming records than consumers can process, OR shards are saturated. This causes:

- **Delayed processing** — NLP/LLM see stale data
- **Data loss risk** — if records exceed retention period (24h default)
- **Backpressure** — producers may start failing PutRecords

**Likely causes:**
- Consumer down or slow
- Burst ingestion (large ticker batch)
- Shard count too low for throughput
- Lambda consumer errors (if using Lambda)

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Check iterator age (lag) per shard
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name GetRecords.IteratorAgeMilliseconds \
  --dimensions Name=StreamName,Value=fin-guard-stream \
  --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Maximum

# 2. Check if producers are being throttled
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name PutRecords.ThrottledRecords \
  --dimensions Name=StreamName,Value=fin-guard-stream \
  --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum

# 3. Pause new ingestion if critical
kubectl scale deployment finguard-orchestrator -n finguard --replicas=0
```

---

## Triage (Confirm Root Cause)

### Check Consumer Health

```bash
# If using ECS/K8s consumer
kubectl get pods -n finguard -l app=kinesis-consumer

# Check consumer logs for errors
kubectl logs -l app=kinesis-consumer -n finguard --since=10m | grep -i "error\|exception"

# If using Lambda
aws lambda list-event-source-mappings --event-source-arn arn:aws:kinesis:us-east-1:ACCOUNT:stream/fin-guard-stream
```

### Check Shard Metrics

```bash
# Incoming records vs capacity
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name IncomingRecords \
  --dimensions Name=StreamName,Value=fin-guard-stream \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 --statistics Sum

# Per-shard: each shard handles 1MB/sec or 1000 records/sec
aws kinesis describe-stream-summary --stream-name fin-guard-stream
```

**Good state:** IteratorAge < 60000ms (1 min)
**Bad state:** IteratorAge > 3600000ms (1 hour)

### Check Firehose Delivery (if applicable)

```bash
aws firehose describe-delivery-stream --delivery-stream-name fin-guard-firehose
```

---

## Mitigations (Safest → Riskiest)

### Option 1: Scale Consumers (Safest)

```bash
# Scale up consumer pods
kubectl scale deployment kinesis-consumer -n finguard --replicas=4

# Or increase Lambda concurrency
aws lambda put-function-concurrency \
  --function-name fin-guard-kinesis-processor \
  --reserved-concurrent-executions 50
```

### Option 2: Reduce Producer Rate

```bash
# Reduce ingestion batch size
kubectl set env deployment/finguard-orchestrator -n finguard KINESIS_BATCH_SIZE=100

# Or pause ingestion temporarily
kubectl scale deployment finguard-orchestrator -n finguard --replicas=0
```

### Option 3: Add Shards

```bash
# Double shard count (split each shard)
aws kinesis update-shard-count \
  --stream-name fin-guard-stream \
  --target-shard-count 4 \
  --scaling-type UNIFORM_SCALING

# Note: Takes ~30 seconds per shard
```

⚠️ **Warning:** Shard splits cost money and take time. Plan for 2-5 min.

### Option 4: Extend Retention (Buy Time)

```bash
# Extend retention from 24h to 168h (7 days)
aws kinesis increase-stream-retention-period \
  --stream-name fin-guard-stream \
  --retention-period-hours 168
```

This buys time to catch up without data loss.

---

## Verification / Exit Criteria

```bash
# Iterator age should decrease
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name GetRecords.IteratorAgeMilliseconds \
  --dimensions Name=StreamName,Value=fin-guard-stream \
  --start-time $(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Maximum

# Should show decreasing trend, target < 60000ms

# No throttled puts
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name PutRecords.ThrottledRecords \
  --dimensions Name=StreamName,Value=fin-guard-stream \
  --start-time $(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum
# Should show 0
```

**Exit when:**
- IteratorAge < 60s (1 minute lag)
- ThrottledRecords = 0
- Consumers processing normally

---

## Escalation

| Condition | Action |
|-----------|--------|
| IteratorAge approaching retention limit | Page immediately — data loss imminent |
| Consumer errors persisting | Page application team |
| Shard split fails | Page infrastructure team |

---

## Aftercare

1. **Incident ticket**: Note cause, peak lag, mitigation used
2. **Verify no data loss**: Compare record counts in/out
3. **Follow-ups**:
   - [ ] Review shard count vs typical throughput
   - [ ] Consider enhanced fan-out for consumers
   - [ ] Set up IteratorAge alarms at lower thresholds
   - [ ] Review producer batch sizes and compression
