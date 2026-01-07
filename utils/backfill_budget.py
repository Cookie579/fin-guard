"""
backfill_budget.py - Backfill budget controls for FinGuard

Caps cost per run to prevent runaway backfills.
Tracks estimated cost based on:
- Number of tickers
- Sources to process
- API call estimates
- Compute time estimates

Usage:
    from utils.backfill_budget import BackfillBudget, BudgetExceededError

    budget = BackfillBudget(max_cost_usd=50.0)
    
    # Check before starting backfill
    estimate = budget.estimate_cost(tickers=["AAPL", "GOOG"], sources=["sec", "esg", "social"])
    if estimate.exceeds_budget:
        raise BudgetExceededError(estimate)
    
    # Track actual costs during run
    budget.record_cost("api_calls", 5.0)
    budget.check_remaining()  # Raises if budget exhausted
"""

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from enum import Enum


class CostCategory(str, Enum):
    """Cost categories for tracking."""
    API_CALLS = "api_calls"
    COMPUTE = "compute"
    LLM_TOKENS = "llm_tokens"
    STORAGE = "storage"


@dataclass
class CostEstimate:
    """Estimated cost breakdown for a backfill run."""
    tickers: list[str]
    sources: list[str]
    
    # Itemized estimates
    api_calls_usd: float = 0.0
    compute_usd: float = 0.0
    llm_tokens_usd: float = 0.0
    storage_usd: float = 0.0
    
    # Totals
    total_usd: float = 0.0
    budget_usd: float = 0.0
    exceeds_budget: bool = False
    
    # Metadata
    estimated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> dict:
        return {
            "tickers": self.tickers,
            "sources": self.sources,
            "breakdown": {
                "api_calls": self.api_calls_usd,
                "compute": self.compute_usd,
                "llm_tokens": self.llm_tokens_usd,
                "storage": self.storage_usd,
            },
            "total_usd": self.total_usd,
            "budget_usd": self.budget_usd,
            "exceeds_budget": self.exceeds_budget,
        }


@dataclass
class BudgetExceededError(Exception):
    """Raised when estimated cost exceeds budget."""
    estimate: CostEstimate
    
    def __str__(self):
        return (
            f"BUDGET_EXCEEDED: Estimated cost ${self.estimate.total_usd:.2f} "
            f"exceeds budget ${self.estimate.budget_usd:.2f}. "
            f"Reduce tickers/sources or increase budget with --budget."
        )


class BackfillBudget:
    """
    Budget controller for backfill runs.
    
    Estimates and tracks costs to prevent runaway spending.
    """
    
    # Cost estimates per unit (USD)
    COST_PER_TICKER_SEC = 0.50      # SEC API calls + compute
    COST_PER_TICKER_ESG = 0.30      # Serper API + compute
    COST_PER_TICKER_SOCIAL = 0.20  # Social API + compute
    COST_PER_LLM_REPORT = 0.80     # GPT-4 tokens for report
    COST_PER_GB_STORAGE = 0.023    # S3 standard storage
    
    # Compute costs (per ticker)
    COST_PER_JOB_MINUTE = 0.01     # ~$0.60/hr for spot instance
    ESTIMATED_JOB_MINUTES = {
        "sec": 5,
        "esg": 3,
        "social": 2,
    }
    
    def __init__(
        self,
        max_cost_usd: float = 50.0,
        warn_threshold_pct: float = 80.0
    ):
        """
        Initialize budget controller.
        
        Args:
            max_cost_usd: Maximum allowed cost per run
            warn_threshold_pct: Warn when this % of budget is used
        """
        self.max_cost_usd = max_cost_usd
        self.warn_threshold_pct = warn_threshold_pct
        
        # Track actual costs during run
        self._actual_costs: dict[str, float] = {}
        self._started_at: Optional[datetime] = None
    
    def estimate_cost(
        self,
        tickers: list[str],
        sources: Optional[list[str]] = None,
        include_reports: bool = True
    ) -> CostEstimate:
        """
        Estimate cost for a backfill run.
        
        Args:
            tickers: List of tickers to process
            sources: List of sources (defaults to all)
            include_reports: Include LLM report generation cost
        
        Returns:
            CostEstimate with breakdown
        """
        sources = sources or ["sec", "esg", "social"]
        n_tickers = len(tickers)
        
        # API call costs
        api_costs = 0.0
        if "sec" in sources:
            api_costs += n_tickers * self.COST_PER_TICKER_SEC
        if "esg" in sources:
            api_costs += n_tickers * self.COST_PER_TICKER_ESG
        if "social" in sources:
            api_costs += n_tickers * self.COST_PER_TICKER_SOCIAL
        
        # Compute costs
        compute_minutes = sum(
            self.ESTIMATED_JOB_MINUTES.get(s, 3) for s in sources
        ) * n_tickers
        compute_costs = compute_minutes * self.COST_PER_JOB_MINUTE
        
        # LLM costs
        llm_costs = 0.0
        if include_reports:
            llm_costs = n_tickers * self.COST_PER_LLM_REPORT
        
        # Storage costs (negligible for most runs)
        storage_costs = n_tickers * len(sources) * 0.001  # ~1MB per result
        
        total = api_costs + compute_costs + llm_costs + storage_costs
        
        return CostEstimate(
            tickers=tickers,
            sources=sources,
            api_calls_usd=round(api_costs, 2),
            compute_usd=round(compute_costs, 2),
            llm_tokens_usd=round(llm_costs, 2),
            storage_usd=round(storage_costs, 4),
            total_usd=round(total, 2),
            budget_usd=self.max_cost_usd,
            exceeds_budget=total > self.max_cost_usd
        )
    
    def check_budget(
        self,
        tickers: list[str],
        sources: Optional[list[str]] = None,
        include_reports: bool = True
    ) -> CostEstimate:
        """
        Check if a run fits within budget.
        
        Args:
            tickers: List of tickers
            sources: List of sources
            include_reports: Include LLM costs
        
        Returns:
            CostEstimate
        
        Raises:
            BudgetExceededError: If estimate exceeds budget
        """
        estimate = self.estimate_cost(tickers, sources, include_reports)
        
        if estimate.exceeds_budget:
            raise BudgetExceededError(estimate)
        
        return estimate
    
    def start_run(self):
        """Mark the start of a budget-tracked run."""
        self._started_at = datetime.now(timezone.utc)
        self._actual_costs = {}
    
    def record_cost(self, category: str, amount_usd: float):
        """
        Record actual cost during a run.
        
        Args:
            category: Cost category (api_calls, compute, llm_tokens, storage)
            amount_usd: Cost amount
        """
        self._actual_costs[category] = self._actual_costs.get(category, 0.0) + amount_usd
    
    def get_actual_total(self) -> float:
        """Get total actual costs recorded so far."""
        return sum(self._actual_costs.values())
    
    def check_remaining(self) -> float:
        """
        Check remaining budget.
        
        Returns:
            Remaining budget in USD
        
        Raises:
            BudgetExceededError: If budget exhausted
        """
        actual = self.get_actual_total()
        remaining = self.max_cost_usd - actual
        
        if remaining <= 0:
            # Create a fake estimate for the error
            estimate = CostEstimate(
                tickers=[],
                sources=[],
                total_usd=actual,
                budget_usd=self.max_cost_usd,
                exceeds_budget=True
            )
            raise BudgetExceededError(estimate)
        
        # Warn if approaching threshold
        pct_used = (actual / self.max_cost_usd) * 100
        if pct_used >= self.warn_threshold_pct:
            import logging
            logging.warning(
                f"Budget warning: {pct_used:.1f}% used (${actual:.2f} of ${self.max_cost_usd:.2f})"
            )
        
        return remaining
    
    def get_summary(self) -> dict:
        """Get summary of budget usage."""
        actual = self.get_actual_total()
        return {
            "budget_usd": self.max_cost_usd,
            "actual_usd": round(actual, 2),
            "remaining_usd": round(self.max_cost_usd - actual, 2),
            "pct_used": round((actual / self.max_cost_usd) * 100, 1),
            "breakdown": self._actual_costs.copy(),
            "started_at": self._started_at.isoformat() if self._started_at else None,
        }


# ============================================================
# Convenience singleton
# ============================================================

_default_budget: Optional[BackfillBudget] = None


def get_backfill_budget(max_cost_usd: float = 50.0) -> BackfillBudget:
    """Get the default backfill budget instance."""
    global _default_budget
    if _default_budget is None:
        _default_budget = BackfillBudget(max_cost_usd=max_cost_usd)
    return _default_budget
