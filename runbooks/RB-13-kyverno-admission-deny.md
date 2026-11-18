# RB-13: Kyverno Admission Deny

| ID | Incident | Severity | Primary Alerts | Owner |
|----|----------|----------|----------------|-------|
| RB-13 | Kyverno Admission Deny | Warning | Kyverno PolicyReport | Platform SRE |

## Triage & Mitigations

1. **Get PolicyReport**:
   ```bash
   kubectl get policyreport -n finguard
   ```
2. **Identify Failed Rule**: Check the report to see which rule failed. Common causes:
   *   Missing labels (e.g., `finguard.io/jobType` missing).
   *   Secret not found (referencing a non-existent secret).
3. **Fix Manifest**:
   *   **Missing Label**: Add `finguard.io/jobType: <type>` to the Job manifest.
   *   **Missing Secret**: Deploy the required secret first (see RB-12).
4. **Retry**: Apply the fixed manifest.
