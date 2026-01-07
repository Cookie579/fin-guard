# RB-06: PagerDuty Routing / Alarm Triage

| Field | Value |
|-------|-------|
| **ID** | RB-06 |
| **Incident** | PagerDuty Routing / Alarm Triage |
| **Severity** | N/A (Meta-runbook) |
| **Primary Signals** | Incoming PagerDuty page |
| **Owner** | on-call engineer |
| **Last Tested** | 2026-01-06 |

---

## What This Means (30 seconds)

This is a **meta-runbook** for handling any incoming PagerDuty page. Use this to:

1. Identify which runbook applies
2. Verify the alert is real (not noise)
3. Route to the correct responder if needed

**Remember:** Every page should link to a specific runbook. If it doesn't, fix the alert after the incident.

---

## Immediate Triage (First 2 Minutes)

### Step 1: Check Page Details

Look at the PagerDuty incident for:

| Field | Where to Find It |
|-------|-----------------|
| **Alert Name** | Incident title |
| **Runbook Link** | Custom field or in description |
| **Severity** | Custom field (P1/P2/P3) or key values |
| **dedup_key** | Incident key — tells you what's firing |
| **Affected Service** | Service field |

### Step 2: Find the Right Runbook

| Alert Pattern | Runbook |
|---------------|---------|
| `*429*`, `BrokerWaitMs*` | [RB-01: External API 429 Storm](RB-01-external-429-storm.md) |
| `DDB*Throttle*`, `DynamoDB*` | [RB-02: DynamoDB Throttling](RB-02-ddb-throttling.md) |
| `Kinesis*`, `IteratorAge*` | [RB-03: Kinesis Backlog](RB-03-kinesis-backlog.md) |
| `Broker*`, `Redis*` | [RB-04: Broker Outage](RB-04-broker-outage.md) |
| `LLM*`, `Token*Cost*` | RB-05: LLM Cost Spike |
| `HPA*Max*`, `Scale*Blocked` | RB-08: HPA at Max |
| `*Quota*` | RB-09: ResourceQuota |
| `*Autoscaler*`, `Pending*` | RB-10: Node Ceiling |

### Step 3: Verify It's Real

```bash
# Quick health check
kubectl get pods -n finguard | grep -v Running | grep -v Completed

# Recent errors
kubectl logs -l component=ingestion -n finguard --since=5m | grep -i error | tail -10

# CloudWatch alarm state
aws cloudwatch describe-alarms --alarm-names "<ALARM_NAME>" \
  --query 'MetricAlarms[0].StateValue'
```

**If state is `OK`**: Alert may have auto-resolved. Verify and close.
**If state is `ALARM`**: Proceed with appropriate runbook.

---

## Alert Routing Decision Tree

```
Incoming Page
     │
     ▼
Is it actionable?
     │
     ├── No → Snooze + create ticket to fix alert
     │
     └── Yes → Do I own this?
                    │
                    ├── No → Reassign in PagerDuty
                    │        Note: finguard-dataplane vs finguard-processing
                    │
                    └── Yes → Follow the linked runbook
```

---

## Common PagerDuty Operations

### Acknowledge (I'm looking at it)

```
In PagerDuty: Click "Acknowledge"
OR via CLI: pd incident:ack <incident_id>
```

### Reassign (Wrong team)

1. Click incident → "Reassign"
2. Select correct escalation policy:
   - `finguard-dataplane`: ingestion, broker, storage
   - `finguard-processing`: NLP, LLM, reports

### Snooze (Transient, will auto-resolve)

- Use sparingly
- Max snooze: 1 hour
- Add note explaining why

### Resolve (Issue fixed)

Only resolve when exit criteria from runbook are met.

---

## Understanding AlarmDescription JSON

All FinGuard CloudWatch alarms include structured JSON in the description:

```json
{
  "team": "finguard",
  "component": "broker",
  "severity": "warning",
  "runbook": "https://github.com/org/fin-guard/runbooks/RB-01-external-429-storm.md"
}
```

The Lambda relay extracts this to:
- Route to correct PagerDuty service
- Set severity/priority
- Include runbook link in incident

### If Runbook Link is Missing

```bash
# Check alarm description
aws cloudwatch describe-alarms --alarm-names "<ALARM_NAME>" \
  --query 'MetricAlarms[0].AlarmDescription'

# If empty or not JSON: this alarm needs fixing
# Create follow-up ticket: "Add runbook to <ALARM_NAME>"
```

---

## Escalation

| Condition | Action |
|-----------|--------|
| Can't identify which runbook | Page secondary on-call |
| Multiple related alerts firing | Declare incident, start bridge call |
| Alert is noise (fires frequently without issue) | Snooze + create ticket to tune threshold |
| No runbook exists | Handle ad-hoc, create runbook as follow-up |

---

## After Every Page

1. **Was the alert actionable?**
   - No → Create ticket to fix/tune/delete alert
   - Yes → Proceed

2. **Was the runbook helpful?**
   - No → Update runbook with what you learned
   - Yes → Note "last tested" date

3. **Was routing correct?**
   - No → Fix AlarmDescription JSON or PagerDuty routing rules

---

## Quick Reference: FinGuard Services

| PagerDuty Service | Components | Primary On-Call |
|-------------------|------------|-----------------|
| finguard-dataplane | ingest-*, broker, DynamoDB, Kinesis, S3 | Platform team |
| finguard-processing | NLP, LLM, report-gen | Application team |
| finguard-infra | EKS, nodes, networking | Infrastructure team |

---

## Metrics: Alert Health

To reduce page fatigue, track weekly:

```sql
-- Pages that auto-resolved < 5 min (noise candidates)
SELECT alarm_name, COUNT(*) as noise_pages
FROM alerts
WHERE resolved_at - triggered_at < INTERVAL '5 minutes'
GROUP BY alarm_name
ORDER BY noise_pages DESC
```

Target: <20% of pages are noise.
