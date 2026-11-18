# RB-11: WAF Rate Limit Triggered

| ID | Incident | Severity | Primary Alerts | Owner |
|----|----------|----------|----------------|-------|
| RB-11 | WAF Rate Limit Triggered | Warning | `finguard-orchestrator-rate-limit` | Platform SRE |

## Triage & Mitigations

1. **Check CloudWatch metrics**: `finguard-orchestrator-rate-limit` to see the volume of blocked requests.
2. **Review WAF sampled requests**: Identify the offending IP address in the AWS WAF console.
3. **Analyze Traffic**:
   *   **Legitimate Traffic Spike**: If this is a known load test or valid high-traffic event, consider temporarily increasing the rate limit threshold.
   *   **Abuse/Attack**: If the traffic is malicious, add the IP address to the WAF blocklist permanently.
