# RB-12: Preflight Denies Job Creation

| ID | Incident | Severity | Primary Alerts | Owner |
|----|----------|----------|----------------|-------|
| RB-12 | Preflight Denies Job Creation | Warning | `PREFLIGHT_CHECK_FAILED` | Platform SRE |

## Triage & Mitigations

1. **Check Orchestrator Logs**: Look for `PREFLIGHT_CHECK_FAILED` events. The log message will contain the specific reason for denial.
2. **Verify Resources**:
   *   Check if the required Secret or ConfigMap exists:
       ```bash
       kubectl get secret -n finguard <name>
       kubectl get configmap -n finguard <name>
       ```
3. **Fix Missing Resources**:
   *   If missing, redeploy secrets via External Secrets Operator or manually:
       ```bash
       kubectl create secret generic <name> ...
       ```
   *   Ensure the name matches exactly what the job expects.
4. **Retry**: Re-trigger the job request once the dependency is in place.
