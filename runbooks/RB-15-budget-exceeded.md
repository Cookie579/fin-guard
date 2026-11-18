# RB-15: Budget Exceeded (BUDGET_EXCEEDED)

| ID | Incident | Severity | Primary Alerts | Owner |
|----|----------|----------|----------------|-------|
| RB-15 | Budget Exceeded | Warning | `422 BUDGET_EXCEEDED` | Application Team |

## Triage & Mitigations

1. **Check Error Details**: Look for `422 BUDGET_EXCEEDED`. It will show the estimated cost vs. the configured budget.
2. **Reduce Scope**:
   *   **Fewer Tickers**: Split the request into smaller batches.
   *   **Fewer Sources**: Run only specific sources (e.g., `--sources sec`).
   *   **Disable LLM**: Run without report generation if only data is needed.
3. **Increase Budget**: If the cost is approved, override the budget limit:
   ```bash
   finguard analyze --ticker <TICKER> --budget 100
   ```
