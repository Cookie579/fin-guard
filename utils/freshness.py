"""
freshness.py - Freshness guard for FinGuard report generation

Refuses report generation if processed artifacts are older than a configurable
threshold unless user explicitly overrides.

Usage:
    from utils.freshness import FreshnessGuard, StaleDataError

    guard = FreshnessGuard(max_age_hours=24)
    
    # Check before generating report
    try:
        guard.check_ticker_freshness("AAPL")
    except StaleDataError as e:
        if not user_override:
            return {"error": str(e), "stale_sources": e.stale_sources}, 422
        # User override: proceed anyway

    # Or use decorator
    @guard.require_fresh(max_age_hours=12)
    def generate_report(ticker: str):
        ...
"""

import os
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
import boto3
from botocore.exceptions import ClientError


@dataclass
class SourceFreshness:
    """Freshness info for a single data source."""
    source: str
    last_updated: Optional[datetime]
    age_hours: Optional[float]
    is_stale: bool
    threshold_hours: float


@dataclass
class StaleDataError(Exception):
    """Raised when data is too stale for report generation."""
    ticker: str
    stale_sources: list[SourceFreshness]
    threshold_hours: float
    
    def __str__(self):
        sources = ", ".join(s.source for s in self.stale_sources)
        return (
            f"STALE_DATA: Ticker {self.ticker} has stale data sources: {sources}. "
            f"Threshold: {self.threshold_hours}h. Use --force to override."
        )


class FreshnessGuard:
    """
    Guards against generating reports from stale data.
    
    Checks the last-modified timestamps of processed artifacts in S3
    and refuses generation if any source is older than the threshold.
    """
    
    # Default thresholds per source type (hours)
    DEFAULT_THRESHOLDS = {
        "sec": 168,      # 7 days - SEC filings don't change often
        "esg": 72,       # 3 days - ESG data updates weekly
        "social": 24,    # 1 day - Social sentiment is time-sensitive
    }
    
    def __init__(
        self,
        max_age_hours: float = 48,
        bucket: Optional[str] = None,
        source_thresholds: Optional[dict[str, float]] = None
    ):
        """
        Initialize freshness guard.
        
        Args:
            max_age_hours: Default max age for all sources
            bucket: S3 bucket name (defaults to PROCESSED_BUCKET env var)
            source_thresholds: Per-source thresholds (overrides max_age_hours)
        """
        self.max_age_hours = max_age_hours
        self.bucket = bucket or os.getenv("PROCESSED_BUCKET", "finguard-processed")
        self.source_thresholds = source_thresholds or self.DEFAULT_THRESHOLDS
        
        self._s3 = boto3.client("s3")
    
    def get_artifact_age(self, ticker: str, source: str) -> Optional[datetime]:
        """
        Get the last-modified time of processed artifacts for a ticker/source.
        
        Args:
            ticker: Stock ticker
            source: Data source (sec, esg, social)
        
        Returns:
            Last modified datetime, or None if not found
        """
        prefix = f"processed/{ticker}/{source}/"
        
        try:
            response = self._s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=10
            )
            
            if "Contents" not in response:
                return None
            
            # Find most recent object
            latest = max(response["Contents"], key=lambda x: x["LastModified"])
            return latest["LastModified"]
            
        except ClientError:
            return None
    
    def check_source_freshness(
        self,
        ticker: str,
        source: str,
        threshold_hours: Optional[float] = None
    ) -> SourceFreshness:
        """
        Check freshness of a single source.
        
        Args:
            ticker: Stock ticker
            source: Data source
            threshold_hours: Override threshold for this check
        
        Returns:
            SourceFreshness with staleness info
        """
        threshold = threshold_hours or self.source_thresholds.get(source, self.max_age_hours)
        last_updated = self.get_artifact_age(ticker, source)
        
        if last_updated is None:
            return SourceFreshness(
                source=source,
                last_updated=None,
                age_hours=None,
                is_stale=True,  # No data = stale
                threshold_hours=threshold
            )
        
        now = datetime.now(timezone.utc)
        age = now - last_updated
        age_hours = age.total_seconds() / 3600
        
        return SourceFreshness(
            source=source,
            last_updated=last_updated,
            age_hours=round(age_hours, 1),
            is_stale=age_hours > threshold,
            threshold_hours=threshold
        )
    
    def check_ticker_freshness(
        self,
        ticker: str,
        sources: Optional[list[str]] = None,
        threshold_hours: Optional[float] = None,
        allow_missing: bool = False
    ) -> dict[str, SourceFreshness]:
        """
        Check freshness of all sources for a ticker.
        
        Args:
            ticker: Stock ticker
            sources: List of sources to check (defaults to all)
            threshold_hours: Override threshold for all sources
            allow_missing: If True, missing sources don't count as stale
        
        Returns:
            Dict of source -> SourceFreshness
        
        Raises:
            StaleDataError: If any source is stale
        """
        sources = sources or ["sec", "esg", "social"]
        results = {}
        stale_sources = []
        
        for source in sources:
            freshness = self.check_source_freshness(ticker, source, threshold_hours)
            results[source] = freshness
            
            if freshness.is_stale:
                # Skip missing sources if allowed
                if allow_missing and freshness.last_updated is None:
                    continue
                stale_sources.append(freshness)
        
        if stale_sources:
            raise StaleDataError(
                ticker=ticker,
                stale_sources=stale_sources,
                threshold_hours=threshold_hours or self.max_age_hours
            )
        
        return results
    
    def require_fresh(
        self,
        max_age_hours: Optional[float] = None,
        sources: Optional[list[str]] = None
    ):
        """
        Decorator that requires fresh data before executing.
        
        Usage:
            @guard.require_fresh(max_age_hours=24)
            def generate_report(ticker: str):
                ...
        """
        def decorator(func):
            def wrapper(ticker: str, *args, force: bool = False, **kwargs):
                if not force:
                    self.check_ticker_freshness(
                        ticker=ticker,
                        sources=sources,
                        threshold_hours=max_age_hours
                    )
                return func(ticker, *args, **kwargs)
            return wrapper
        return decorator


# ============================================================
# Convenience singleton
# ============================================================

_default_guard: Optional[FreshnessGuard] = None


def get_freshness_guard() -> FreshnessGuard:
    """Get the default freshness guard instance."""
    global _default_guard
    if _default_guard is None:
        _default_guard = FreshnessGuard()
    return _default_guard
