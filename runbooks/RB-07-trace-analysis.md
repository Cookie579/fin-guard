# RB-07: Trace Analysis / Latency Investigation

| Field | Value |
|-------|-------|
| **ID** | RB-07 |
| **Incident** | Trace Analysis / Latency Investigation |
| **Severity** | 🟡 Warning (p95 elevated) / 🔴 Critical (user-impacting) |
| **Primary Signals** | X-Ray trace map, `AnalyzeTickerLatencyP95High`, segment timeline, downstream edges |
| **Owner** | finguard-processing |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

End-to-end latency for `Analyze(ticker)` requests is elevated. This is a **diagnostic runbook** for finding where time is being spent.

**Common bottlenecks:**
- External API slowness (EDGAR, Serper, Stocktwits)
- DynamoDB latency spikes
- Kinesis backpressure
- LLM response times
- Pod resource contention

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Check if this is isolated or widespread
kubectl logs deploy/finguard-orchestrator -n finguard --since=10m | \
  grep "duration_ms" | awk -F'=' '{print $NF}' | sort -n | tail -10

# 2. Identify slow ticker(s)
kubectl logs deploy/finguard-orchestrator -n finguard --since=10m | \
  grep -E "ticker=.*duration_ms" | sort -t= -k4 -rn | head -5

# 3. Check for obvious resource issues
kubectl top pods -n finguard --sort-by=cpu | head -10
```

---

## Triage (Find the Bottleneck)

### Step 1: Get Trace ID

```bash
# From orchestrator logs
kubectl logs deploy/finguard-orchestrator -n finguard --since=30m | \
  grep "trace_id" | head -5

# Or from a specific request
kubectl logs deploy/finguard-orchestrator -n finguard --since=30m | \
  grep "ticker=AAPL" | grep "trace_id"
```

### Step 2: View Trace in X-Ray

1. Go to AWS X-Ray Console
2. Paste trace ID in search
3. Click on the trace to see segment timeline

**What to look for:**
- Which segment is longest?
- Are there retries (multiple calls to same service)?
- Are external calls slow?

### Step 3: Check Each Segment

| Segment | Check | Normal | Slow |
|---------|-------|--------|------|
| Orchestrator | `kubectl logs deploy/finguard-orchestrator` | <500ms | >2s |
| SEC Job | `kubectl logs -l app=ingest-sec-go --since=30m` | <30s | >60s |
| ESG Job | `kubectl logs -l app=ingest-esg-go --since=30m` | <45s | >90s |
| Social Job | `kubectl logs -l app=ingest-social-go --since=30m` | <30s | >60s |
| NLP Pipeline | `kubectl logs deploy/nlp-pipeline --since=30m` | <60s | >120s |
| LLM Explainer | `kubectl logs deploy/llm-explainer --since=30m` | <120s | >300s |

### Step 4: Check External API Latency

```bash
# Broker metrics show external API performance
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/metrics | grep "acquire_duration"

# Check for high wait times (indicates rate limiting)
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/metrics | grep "wait_ms"
```

### Step 5: Check Storage Latency

```bash
# DynamoDB latency
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name SuccessfulRequestLatency \
  --dimensions Name=TableName,Value=social_state Name=Operation,Value=GetItem \
  --start-time $(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 --statistics Average

# S3 latency (check if large files)
aws s3api head-object --bucket finguard-data --key "raw/sec/AAPL/latest.json" 2>&1 | grep ContentLength
```

---

## Common Patterns & Fixes

### Pattern: External API Slow

**Symptom:** SEC/ESG/Social job segment is long, trace shows external call taking most time.

```bash
# Mitigation: Skip slow source temporarily
kubectl set env deployment/finguard-orchestrator -n finguard SKIP_SOURCES=esg
```

### Pattern: DynamoDB Throttling

**Symptom:** Job segments show retries, DDB calls are >100ms.

→ Go to [RB-02: DynamoDB Throttling](RB-02-ddb-throttling.md)

### Pattern: Broker Wait Time High

**Symptom:** Jobs waiting for tokens, broker `wait_ms` is high.

→ Go to [RB-01: External API 429 Storm](RB-01-external-429-storm.md)

### Pattern: LLM Slow

**Symptom:** LLM segment is >2 minutes.

```bash
# Check model being used
kubectl get deployment llm-explainer -n finguard -o jsonpath='{.spec.template.spec.containers[0].env}' | grep MODEL

# Consider switching to faster model
kubectl set env deployment/llm-explainer -n finguard OPENAI_MODEL=gpt-3.5-turbo
```

### Pattern: Pod Resource Contention

**Symptom:** CPU/memory high, multiple segments slow.

```bash
kubectl top pods -n finguard
# If any pod is at limits, consider scaling or increasing resources
```

---

## Verification / Exit Criteria

```bash
# Latency back to baseline
kubectl logs deploy/finguard-orchestrator -n finguard --since=5m | \
  grep "duration_ms" | awk -F'=' '{sum+=$NF; count++} END {print "Avg:", sum/count, "ms"}'
# Target: <120000ms (2 min) for full analyze request

# No queued requests
kubectl get pods -n finguard -l component=ingestion --field-selector=status.phase=Pending
# Should be empty
```

**Exit when:**
- P95 latency returns to baseline (<2 min for analyze)
- Bottleneck identified and mitigated
- No pending jobs in queue

---

## Escalation

| Condition | Action |
|-----------|--------|
| External API confirmed slow | Document and wait, or enable degraded mode |
| Cluster-wide resource issue | Page infrastructure team |
| Unknown bottleneck after 30 min | Page senior engineer for deep dive |

---

## Aftercare

1. **Incident ticket**: Note bottleneck found, mitigation applied
2. **Trace link**: Include X-Ray trace URL in ticket
3. **Follow-ups**:
   - [ ] Add specific latency alerts for bottleneck service
   - [ ] Consider caching for slow external calls
   - [ ] Review resource limits for affected pods
