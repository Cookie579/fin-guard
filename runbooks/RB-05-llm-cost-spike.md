# RB-05: LLM Cost Spike / Degraded Mode

| Field | Value |
|-------|-------|
| **ID** | RB-05 |
| **Incident** | LLM Cost Spike / Degraded Mode |
| **Severity** | 🟡 Warning (budget alert) / 🔴 Critical (>150% budget) |
| **Primary Signals** | `LLMTokenRateHigh`, `CostBudgetExceeded`, `LLMLatencyP95High`, AWS Cost Anomaly alerts |
| **Owner** | finguard-processing |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

LLM API costs (OpenAI) are spiking above normal levels, OR the LLM service is degraded. This causes:

- **Budget burn** — exceeding daily/monthly cost targets
- **Report generation delays** — if throttling or errors occur
- **Potential service disruption** — if OpenAI rate limits trigger

**Likely causes:**
- Large batch of ticker analyses running
- Prompt engineering change increasing token count
- Retry loops on failed completions
- OpenAI API rate limiting or outage

---

## Immediate Stabilization (First 5 Minutes)

```bash
# 1. Check current LLM request rate
kubectl logs deploy/llm-explainer -n finguard --since=10m | \
  grep -c "openai\|completion"

# 2. Check for errors/retries
kubectl logs deploy/llm-explainer -n finguard --since=10m | \
  grep -i "error\|retry\|rate.limit"

# 3. Pause new report generation if critical
kubectl scale deployment/llm-explainer -n finguard --replicas=0
```

---

## Triage (Confirm Root Cause)

### Check Token Usage

```bash
# If you have token logging enabled
kubectl logs deploy/llm-explainer -n finguard --since=1h | \
  grep "tokens" | awk '{sum+=$NF} END {print "Total tokens:", sum}'

# Check METADATA events for cost estimates
aws s3 ls s3://finguard-data/metadata/ --recursive | tail -20
```

### Check OpenAI Status

- https://status.openai.com/

### Check AWS Cost Explorer (if budget alert fired)

```bash
aws ce get-cost-and-usage \
  --time-period Start=$(date -u -d '7 days ago' +%Y-%m-%d),End=$(date -u +%Y-%m-%d) \
  --granularity DAILY \
  --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Bedrock","Third Party"]}}' \
  --metrics BlendedCost
```

### Identify High-Cost Runs

```bash
# Find runs with high token counts
kubectl logs deploy/llm-explainer -n finguard --since=6h | \
  grep "job_id\|tokens" | head -50
```

**Good state:** Token rate stable, costs within budget
**Bad state:** Token rate 2-3x normal, budget alerts firing

---

## Mitigations (Safest → Riskiest)

### Option 1: Reduce Concurrency (Safest)

```bash
# Limit concurrent LLM jobs
kubectl set env deployment/llm-explainer -n finguard MAX_CONCURRENT=1

# Or scale down
kubectl scale deployment/llm-explainer -n finguard --replicas=1
```

### Option 2: Enable Token Budgets

```bash
# Set per-run token limit
kubectl set env deployment/llm-explainer -n finguard MAX_TOKENS_PER_RUN=50000
```

### Option 3: Switch to Cheaper Model

```bash
# Use GPT-3.5 instead of GPT-4 temporarily
kubectl set env deployment/llm-explainer -n finguard OPENAI_MODEL=gpt-3.5-turbo
```

⚠️ **Warning:** May affect report quality.

### Option 4: Enable Degraded Mode

Skip LLM-generated narrative, output data-only reports:

```bash
kubectl set env deployment/llm-explainer -n finguard LLM_DEGRADED_MODE=true
```

Reports will include:
- Charts and data
- "Narrative generation unavailable" notice
- No AI-generated analysis

### Option 5: Full Pause

```bash
# Stop all LLM processing
kubectl scale deployment/llm-explainer -n finguard --replicas=0

# New requests will queue; existing data is preserved
```

---

## Verification / Exit Criteria

```bash
# Token rate should normalize
kubectl logs deploy/llm-explainer -n finguard --since=5m | grep -c "completion"
# Should be reasonable (< 10/min for normal operations)

# No errors
kubectl logs deploy/llm-explainer -n finguard --since=5m | grep -i error
# Should be empty

# Cost alerts should clear (may take time to reflect)
aws cloudwatch describe-alarms --alarm-names "LLMCostBudgetExceeded" \
  --query 'MetricAlarms[0].StateValue'
```

**Exit when:**
- Token rate back to baseline
- No active budget alerts
- Reports generating normally

---

## Escalation

| Condition | Action |
|-----------|--------|
| OpenAI confirmed outage | Declare external dependency incident |
| Budget exceeded by >200% | Page finance/cost owner |
| Prompt change suspected | Page application team for rollback |

---

## Aftercare

1. **Incident ticket**: Note cause, cost impact, mitigation used
2. **Cost review**: Check actual spend in AWS Cost Explorer after 24h
3. **Follow-ups**:
   - [ ] Review token budgets per run type
   - [ ] Consider caching for repeated analyses
   - [ ] Evaluate cheaper model tiers for non-critical reports
   - [ ] Add per-ticker cost tracking to METADATA events
