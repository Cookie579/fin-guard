# FinGuard Runbook Index

This directory contains operational runbooks for FinGuard incident response. Each runbook follows a standardized template for consistency during on-call scenarios.

## All Runbooks ✅ Complete

| ID | Incident | Severity | Primary Alerts |
|----|----------|----------|----------------|
| [RB-01](RB-01-external-429-storm.md) | External API 429 Storm | Critical | `Broker429RateHigh`, `BrokerWaitMsP95High` |
| [RB-02](RB-02-ddb-throttling.md) | DynamoDB Throttling / Hot Partition | Warning→Critical | `DDBThrottledRequests`, `DDBLatencyP99High` |
| [RB-03](RB-03-kinesis-backlog.md) | Kinesis Backlog / Shard Saturation | Warning | `KinesisIteratorAge`, `KinesisPutThrottled` |
| [RB-04](RB-04-broker-outage.md) | Broker Outage / Redis Failure | Critical | `BrokerHealthCheckFailed`, `RedisConnectionFailed` |
| [RB-05](RB-05-llm-cost-spike.md) | LLM Cost Spike / Degraded Mode | Warning→Critical | `LLMTokenRateHigh`, `CostBudgetExceeded` |
| [RB-06](RB-06-pagerduty-routing.md) | PagerDuty Routing / Alarm Triage | N/A | (meta-runbook for alarm handling) |
| [RB-07](RB-07-trace-analysis.md) | Trace Analysis / Latency Investigation | Warning | X-Ray trace map, `AnalyzeTickerLatencyP95High` |
| [RB-08](RB-08-hpa-at-max.md) | HPA at Max / Scaling Blocked | Warning | `HPAAtMaxReplicas`, `HPAScaleUpBlocked` |
| [RB-09](RB-09-resourcequota-limit.md) | ResourceQuota Near/At Limit | Warning→Critical | `PodQuotaNearLimit`, `ResourceQuotaAtLimit` |
| [RB-10](RB-10-node-ceiling.md) | Node Ceiling Reached | Warning→Critical | `ClusterAutoscalerUnableToScaleUp`, `NodeCountAtStaticCeiling` |
| [RB-11](RB-11-waf-rate-limit.md) | WAF Rate Limit Triggered | Warning | `finguard-orchestrator-rate-limit` |
| [RB-12](RB-12-preflight-denies-job.md) | Preflight Denies Job Creation | Warning | `PREFLIGHT_CHECK_FAILED` |
| [RB-13](RB-13-kyverno-admission-deny.md) | Kyverno Admission Deny | Warning | Kyverno PolicyReport |
| [RB-14](RB-14-freshness-guard-deny.md) | Freshness Guard Deny | Warning | `422 STALE_DATA` |
| [RB-15](RB-15-budget-exceeded.md) | Budget Exceeded | Warning | `422 BUDGET_EXCEEDED` |
| [RB-16](RB-16-cli-issues.md) | CLI Orchestrator Issues | Info | N/A |

## Runbook Template Structure

Every runbook follows this structure for 2am skimmability:

1. **Header** — ID, Incident, Severity, Primary Signals, Owner, Last Tested
2. **What This Means** — 30-second impact summary
3. **Immediate Stabilization** — First 5 minutes, safe actions
4. **Triage** — Copy/paste diagnostic commands with "what good looks like"
5. **Mitigations** — Ordered safest → riskiest
6. **Verification / Exit Criteria** — How to know you're done
7. **Escalation** — When to page someone else
8. **Aftercare** — Incident writeup and follow-ups

## Quick Reference: Common Pivot Points

| Need to find... | Command |
|-----------------|---------|
| Job ID | `kubectl logs deploy/finguard-orchestrator -n finguard \| grep job_id` |
| Trace ID | `kubectl logs deploy/finguard-orchestrator -n finguard \| grep trace_id` |
| Ticker | `kubectl logs -l component=ingestion -n finguard \| grep ticker=` |
| Request ID | Check orchestrator logs or X-Ray |

## Integration

- **CloudWatch Alarms**: Runbook URLs in `AlarmDescription.runbook` field
- **Prometheus Alerts**: Runbook URLs in `annotations.runbook` field
- **PagerDuty**: Links automatically included in incident details via relay Lambda

## PagerDuty Services

| Service | Components | On-Call |
|---------|------------|---------|
| finguard-dataplane | ingest-*, broker, DynamoDB, Kinesis, S3 | Platform team |
| finguard-processing | NLP, LLM, report-gen | Application team |
| finguard-infra | EKS, nodes, networking | Infrastructure team |
