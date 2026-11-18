# RB-14: Freshness Guard Deny (STALE_DATA)

| ID | Incident | Severity | Primary Alerts | Owner |
|----|----------|----------|----------------|-------|
| RB-14 | Freshness Guard Deny | Warning | `422 STALE_DATA` | Application Team |

## Triage & Mitigations

1. **Check Error Details**: Look for the `422 STALE_DATA` response. It will list which sources are considered stale.
2. **Verify Timestamps**: Check the actual S3 objects to confirm age:
   ```bash
   aws s3 ls s3://finguard-processed/processed/{ticker}/{source}/ --recursive | tail
   ```
3. **Resolution Strategy**:
   *   **Data Genuinely Stale**: Re-run the ingestion jobs for that ticker/source first.
   *   **Staleness Acceptable**: If you need to generate a report despite old data, use the `--force` flag to override the guard.
       ```bash
       finguard analyze --ticker <TICKER> --force
       ```
