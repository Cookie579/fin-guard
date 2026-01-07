# RB-04: Broker Outage / Redis Failure

| Field | Value |
|-------|-------|
| **ID** | RB-04 |
| **Incident** | Broker Outage / Redis Failure |
| **Severity** | 🔴 Critical |
| **Primary Signals** | `BrokerHealthCheckFailed`, `RedisConnectionFailed`, `broker_acquire_errors_total` |
| **Owner** | finguard-dataplane |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

The Rate Broker service or its Redis backend is down. This causes:

- **All API-gated ingestion blocked** — ESG and Social jobs cannot acquire tokens
- **Jobs waiting indefinitely or failing** — no rate limit coordination
- **Potential external API abuse** — if jobs bypass broker (fail-open mode)

**Likely causes:**
- Broker pod crashed or OOMKilled
- Redis pod/service unavailable
- Network partition between broker and Redis
- Redis memory exhaustion

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Check broker pod status
kubectl get pods -n finguard -l app=rate-broker

# 2. Check Redis status
kubectl get pods -n finguard -l app=redis

# 3. Check broker health endpoint
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/health

# 4. If broker down, restart immediately
kubectl rollout restart deployment/rate-broker -n finguard
```

---

## Triage (Confirm Root Cause)

### Check Broker Logs

```bash
# Recent logs
kubectl logs deploy/rate-broker -n finguard --since=10m

# Check for OOMKill
kubectl describe pod -l app=rate-broker -n finguard | grep -A5 "Last State"

# Check for crash loops
kubectl get pods -n finguard -l app=rate-broker -o jsonpath='{.items[*].status.containerStatuses[*].restartCount}'
```

### Check Redis Connectivity

```bash
# From broker pod
kubectl exec -it deploy/rate-broker -n finguard -- \
  redis-cli -h redis PING

# Should return: PONG
# If timeout: Redis unreachable

# Check Redis pod directly
kubectl exec -it deploy/redis -n finguard -- redis-cli PING
```

### Check Redis Memory

```bash
kubectl exec -it deploy/redis -n finguard -- redis-cli INFO memory

# Look for:
# used_memory_human: Should be < 80% of maxmemory
# evicted_keys: Should be 0 (if eviction is happening, data is being lost)
```

**Good state:** Broker PING returns, Redis connected
**Bad state:** Connection refused, OOMKilled, crash loop

---

## Mitigations (Safest → Riskiest)

### Option 1: Restart Broker (Safest)

```bash
# Simple restart of broker
kubectl rollout restart deployment/rate-broker -n finguard

# Wait for rollout
kubectl rollout status deployment/rate-broker -n finguard

# Verify health
kubectl exec -it deploy/rate-broker -n finguard -- curl -s localhost:8080/health
```

### Option 2: Restart Redis

```bash
# WARNING: This clears all token bucket state
# Jobs will need to re-acquire tokens

kubectl rollout restart statefulset/redis -n finguard

# Wait for Redis to be ready
kubectl wait --for=condition=ready pod -l app=redis -n finguard --timeout=120s

# Then restart broker to reconnect
kubectl rollout restart deployment/rate-broker -n finguard
```

### Option 3: Increase Redis Memory

```bash
# If OOM was the cause
kubectl set resources statefulset/redis -n finguard \
  --limits=memory=2Gi --requests=memory=1Gi

# Or edit maxmemory
kubectl exec -it deploy/redis -n finguard -- \
  redis-cli CONFIG SET maxmemory 1gb
```

### Option 4: Enable Fail-Closed Mode (Safest Degraded)

If broker cannot be restored quickly, pause API-dependent ingestion:

```bash
# Skip sources that require broker
kubectl set env deployment/finguard-orchestrator -n finguard SKIP_SOURCES=esg,social

# Only SEC (which doesn't use broker) will run
```

### Option 5: Enable Fail-Open Mode (Riskiest)

Allow jobs to bypass broker entirely with local rate limiting:

```bash
# DANGER: This risks hitting external API limits directly
kubectl set env deployment/ingest-esg-go -n finguard BROKER_FAIL_OPEN=true
kubectl set env deployment/ingest-social-go -n finguard BROKER_FAIL_OPEN=true
```

⚠️ **Warning:** Only use if broker will be down >1 hour AND external APIs are critical.

---

## Verification / Exit Criteria

```bash
# Broker health check passes
kubectl exec -it deploy/rate-broker -n finguard -- curl -s localhost:8080/health
# Should return: {"status": "ok"}

# Redis connected
kubectl exec -it deploy/rate-broker -n finguard -- redis-cli -h redis PING
# Should return: PONG

# Acquire calls succeeding
kubectl exec -it deploy/rate-broker -n finguard -- \
  curl -s localhost:8080/metrics | grep broker_acquire_success
# Should be incrementing

# Jobs no longer failing on acquire
kubectl logs -l component=ingestion -n finguard --since=5m | grep -c "acquire failed"
# Should be 0
```

**Exit when:**
- Broker health check passes
- Redis PING succeeds
- Jobs completing normally
- No acquire errors in logs

---

## Escalation

| Condition | Action |
|-----------|--------|
| Redis data corrupted | Page infrastructure — may need backup restore |
| Broker code bug (crash loop) | Page application team |
| Network issue between pods | Page infrastructure team |
| Persistent OOM despite memory increase | Page infrastructure — may need larger instance |

---

## Aftercare

1. **Incident ticket**: Note root cause, duration, data loss (if any)
2. **If Redis restarted**: Token bucket state is reset — this is fine, tokens refill
3. **Follow-ups**:
   - [ ] Review broker memory limits
   - [ ] Review Redis maxmemory and eviction policy
   - [ ] Consider Redis persistence (RDB/AOF) for state recovery
   - [ ] Add Redis sentinel or cluster for HA
