"""
FinGuard Utils - Common utilities for orchestrators and services

Modules:
- validation: Input validation for tickers, sources, dates
- admission_preflight: Pre-flight validation before K8s Job creation
- freshness: Freshness guard for stale data detection
- backfill_budget: Cost caps for backfill runs

For audit logging, use the separate `audit` package:
    from audit import AuditLogger, AuditAction
"""

from .validation import (
    validate_ticker,
    validate_source,
    validate_date_range,
    validate_page_limit,
    validate_payload_size,
    validate_analyze_request,
    VALID_SOURCES,
)

from .admission_preflight import (
    AdmissionPreflight,
    PreflightError,
    get_preflight,
)

from .freshness import (
    FreshnessGuard,
    StaleDataError,
    get_freshness_guard,
)

from .backfill_budget import (
    BackfillBudget,
    BudgetExceededError,
    CostEstimate,
    get_backfill_budget,
)

__all__ = [
    # Validation
    "validate_ticker",
    "validate_source",
    "validate_date_range",
    "validate_page_limit",
    "validate_payload_size",
    "validate_analyze_request",
    "VALID_SOURCES",
    # Preflight
    "AdmissionPreflight",
    "PreflightError",
    "get_preflight",
    # Freshness
    "FreshnessGuard",
    "StaleDataError",
    "get_freshness_guard",
    # Backfill Budget
    "BackfillBudget",
    "BudgetExceededError",
    "CostEstimate",
    "get_backfill_budget",
]
