# RB-01: External API 429 Storm

| Field | Value |
|-------|-------|
| **ID** | RB-01 |
| **Incident** | External API 429 Storm |
| **Severity** | 🔴 Critical (if sustained >5 min) / 🟡 Warning (transient) |
| **Primary Signals** | `Broker429RateHigh`, `BrokerWaitMsP95High`, `broker_tokens_denied_total` |
| **Owner** | finguard-dataplane |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

One or more external APIs (Serper, Stocktwits, YouTube) are rate-limiting us with HTTP 429 responses. The Rate Broker is detecting these and backing off, which causes:

- **Ingestion jobs waiting** — jobs block on `/v1/acquire` calls
- **Latency spikes** — job completion times increase
- **Potential cascading** — if broker wait times exceed job timeouts, jobs fail

**Likely causes:**
- Burst traffic exceeding API quota
- Key exhaustion (all API keys in cooldown)
- External API outage or changed rate limits

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Check current 429 rate
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/metrics | grep broker_429

# 2. Check which API is affected
kubectl logs deploy/rate-broker -n finguard --since=5m | grep -i "429\|retry"

# 3. Reduce incoming load (if safe)
kubectl scale deployment finguard-orchestrator -n finguard --replicas=1
```

**DO NOT** immediately add more API keys — this can worsen the situation if the external API is having issues.

---

## Triage (Confirm Root Cause)

### Check Broker Metrics

```bash
# Current wait times per API
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/metrics | grep broker_wait_ms

# Token bucket state
kubectl exec -it deploy/rate-broker -n finguard -- \
  redis-cli -h redis HGETALL "bucket:serper"
```

**Good state:** `tokens > 0`, `cooldown_until = 0`
**Bad state:** `tokens = 0`, `cooldown_until > now()`

### Check External API Status

| API | Status Page |
|-----|-------------|
| Serper | https://status.serper.dev |
| Stocktwits | https://status.stocktwits.com |
| YouTube Data API | https://status.cloud.google.com |

### Identify Affected Jobs

```bash
# Find jobs waiting on broker
kubectl get pods -n finguard -l app=ingest-esg-go --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | head -5

# Check job logs for acquire delays
kubectl logs <pod-name> -n finguard | grep -i "acquire\|wait_ms"
```

---

## Mitigations (Safest → Riskiest)

### Option 1: Wait for Cooldown (Safest)

If the external API imposed a short cooldown (< 15 min):

```bash
# Check cooldown remaining
kubectl exec -it deploy/rate-broker -n finguard -- \
  redis-cli -h redis GET "cooldown:serper"
```

Wait for cooldown to expire. Jobs will auto-resume.

### Option 2: Reduce Concurrency

Reduce number of concurrent ingestion jobs to lower request rate:

```bash
# Scale down orchestrator
kubectl scale deployment finguard-orchestrator -n finguard --replicas=1

# Or set max concurrent in config
kubectl set env deployment/finguard-orchestrator -n finguard MAX_CONCURRENT_JOBS=5
```

### Option 3: Rotate to Backup Keys

If the primary key is exhausted but we have backup keys:

```bash
# Check config.json for available keys
kubectl get secret rate-broker-config -n finguard -o jsonpath='{.data.config\.json}' | base64 -d | jq '.serper.keys'

# Force key rotation (if broker supports it)
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -X POST localhost:8080/admin/rotate-key?api=serper
```

### Option 4: Enable Degraded Mode (Riskiest)

Skip the affected source entirely and proceed with partial data:

```bash
# Set source to degraded mode
kubectl set env deployment/finguard-orchestrator -n finguard SKIP_SOURCES=esg

# This will:
# - Skip ESG ingestion for new requests
# - Mark reports as "partial" with data gap notice
```

⚠️ **Warning:** This produces incomplete reports. Use only if 429s persist >30 min.

---

## Verification / Exit Criteria

```bash
# 429 rate should drop to near-zero
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/metrics | grep broker_429_total

# Wait times should normalize (< 500ms p95)
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/metrics | grep broker_wait_ms

# Jobs should be completing normally
kubectl get pods -n finguard -l component=ingestion --no-headers | grep -c Running
```

**Exit when:**
- 429 rate sustained < 1/min for 5 minutes
- p95 wait_ms < 500ms
- No Pending pods in ingestion namespace

---

## Escalation

| Condition | Action |
|-----------|--------|
| 429s persist > 30 min | Contact external API support (Serper, RapidAPI) |
| All keys exhausted | Page infrastructure team for emergency key provisioning |
| External API confirmed down | Declare external dependency incident |

---

## Aftercare

1. **Incident ticket**: Note which API, duration, and mitigation used
2. **Postmortem trigger**: If >30 min or required degraded mode
3. **Follow-ups**:
   - [ ] Review key pool sizing
   - [ ] Consider adding more API keys
   - [ ] Check if burst was legitimate or attack
   - [ ] Update rate limits if API vendor changed terms
