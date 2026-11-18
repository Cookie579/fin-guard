# RB-16: CLI Orchestrator Issues

| ID | Incident | Severity | Primary Alerts | Owner |
|----|----------|----------|----------------|-------|
| RB-16 | CLI Orchestrator Issues | Info/Warning | N/A | Application Team |

## Triage & Mitigations

1. **Auth Failures**:
   *   Check your local `kubeconfig` or the in-cluster ServiceAccount if running inside K8s.
2. **Polling Timeout**:
   *   If the CLI times out waiting for a job, the job may still be running in the background.
   *   Check status manually:
       ```bash
       finguard status --job-id <JOB_ID>
       ```
3. **Output Parsing**:
   *   If using the CLI in scripts, always use the `--json` flag for structured output.
