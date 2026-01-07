"""
validation.py - Input validation for FinGuard orchestrators

This module provides:
- Ticker format validation and normalization
- Source validation
- Request payload validation
- Rate limiting helpers

Usage:
    from utils.validation import validate_ticker, validate_source, validate_request

    ticker = validate_ticker(request.ticker)  # raises ValueError on invalid
    source = validate_source(request.source)  # raises ValueError on invalid
"""

import re
from typing import Optional
from datetime import date, timedelta

# ============================================================
# Ticker Validation
# ============================================================

# US-only common: 1-5 uppercase letters, optional suffix (.A, .B, -A, etc.)
# Supports: AAPL, GOOG, BRK.B, RDS-A
TICKER_RE = re.compile(r"^[A-Z]{1,5}([.-][A-Z]{1,2})?$")

# Extended: includes exchange suffixes like 7203.T (Toyota on TSE)
TICKER_EXTENDED_RE = re.compile(r"^[A-Z0-9]{1,6}([.-][A-Z]{1,4})?$")

# Known valid tickers for quick validation (optional allowlist)
KNOWN_TICKERS = {
    "AAPL", "GOOG", "GOOGL", "MSFT", "AMZN", "META", "TSLA", "NVDA",
    "JPM", "V", "MA", "BRK.A", "BRK.B", "JNJ", "UNH", "HD", "PG",
}


def validate_ticker(ticker: str, *, strict: bool = True, use_allowlist: bool = False) -> str:
    """
    Validate and normalize a ticker symbol.
    
    Args:
        ticker: Raw ticker input
        strict: If True, use strict US-only pattern; if False, allow extended (exchange suffixes)
        use_allowlist: If True, also check against known good tickers
    
    Returns:
        Normalized ticker (uppercase, trimmed)
    
    Raises:
        ValueError: If ticker format is invalid
    """
    if not ticker or not isinstance(ticker, str):
        raise ValueError("INVALID_TICKER: ticker must be a non-empty string")
    
    # Normalize: trim whitespace and uppercase
    normalized = ticker.strip().upper()
    
    if len(normalized) == 0:
        raise ValueError("INVALID_TICKER: ticker cannot be empty")
    
    if len(normalized) > 10:
        raise ValueError("INVALID_TICKER: ticker too long (max 10 chars)")
    
    # Choose regex pattern based on strictness
    pattern = TICKER_RE if strict else TICKER_EXTENDED_RE
    
    if not pattern.fullmatch(normalized):
        raise ValueError(f"INVALID_TICKER_FORMAT: '{normalized}' does not match expected pattern")
    
    # Optional allowlist check
    if use_allowlist and normalized not in KNOWN_TICKERS:
        raise ValueError(f"INVALID_TICKER: '{normalized}' not in allowed ticker list")
    
    return normalized


# ============================================================
# Source Validation
# ============================================================

VALID_SOURCES = {"sec", "esg", "social"}


def validate_source(source: str) -> str:
    """
    Validate ingestion source.
    
    Args:
        source: Source type (sec, esg, social)
    
    Returns:
        Normalized source (lowercase)
    
    Raises:
        ValueError: If source is not valid
    """
    if not source or not isinstance(source, str):
        raise ValueError("INVALID_SOURCE: source must be a non-empty string")
    
    normalized = source.strip().lower()
    
    if normalized not in VALID_SOURCES:
        raise ValueError(f"INVALID_SOURCE: '{normalized}' not in {VALID_SOURCES}")
    
    return normalized


# ============================================================
# Request Payload Validation
# ============================================================

MAX_PAYLOAD_SIZE_BYTES = 1024 * 100  # 100KB max request body
MAX_DATE_RANGE_DAYS = 365  # Max 1 year lookback
MAX_PAGE_LIMIT = 100


def validate_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_range_days: int = MAX_DATE_RANGE_DAYS
) -> tuple[Optional[date], Optional[date]]:
    """
    Validate and parse date range parameters.
    
    Args:
        start_date: ISO format date string (YYYY-MM-DD)
        end_date: ISO format date string (YYYY-MM-DD)
        max_range_days: Maximum allowed range in days
    
    Returns:
        Tuple of (start_date, end_date) as date objects
    
    Raises:
        ValueError: If dates are invalid or range exceeds max
    """
    parsed_start = None
    parsed_end = None
    
    if start_date:
        try:
            parsed_start = date.fromisoformat(start_date)
        except ValueError:
            raise ValueError(f"INVALID_DATE: start_date '{start_date}' is not valid ISO format (YYYY-MM-DD)")
    
    if end_date:
        try:
            parsed_end = date.fromisoformat(end_date)
        except ValueError:
            raise ValueError(f"INVALID_DATE: end_date '{end_date}' is not valid ISO format (YYYY-MM-DD)")
    
    # Validate range if both provided
    if parsed_start and parsed_end:
        if parsed_start > parsed_end:
            raise ValueError("INVALID_DATE_RANGE: start_date cannot be after end_date")
        
        range_days = (parsed_end - parsed_start).days
        if range_days > max_range_days:
            raise ValueError(f"INVALID_DATE_RANGE: range of {range_days} days exceeds max of {max_range_days}")
    
    # Don't allow future dates
    today = date.today()
    if parsed_start and parsed_start > today:
        raise ValueError("INVALID_DATE: start_date cannot be in the future")
    if parsed_end and parsed_end > today:
        raise ValueError("INVALID_DATE: end_date cannot be in the future")
    
    return parsed_start, parsed_end


def validate_page_limit(limit: Optional[int] = None, max_limit: int = MAX_PAGE_LIMIT) -> int:
    """
    Validate pagination limit.
    
    Args:
        limit: Requested page limit
        max_limit: Maximum allowed limit
    
    Returns:
        Validated limit (default 20 if not provided)
    
    Raises:
        ValueError: If limit is invalid
    """
    if limit is None:
        return 20  # Default
    
    if not isinstance(limit, int) or limit < 1:
        raise ValueError("INVALID_LIMIT: limit must be a positive integer")
    
    if limit > max_limit:
        raise ValueError(f"INVALID_LIMIT: limit {limit} exceeds max of {max_limit}")
    
    return limit


def validate_payload_size(content_length: int) -> None:
    """
    Validate request payload size.
    
    Args:
        content_length: Size of request body in bytes
    
    Raises:
        ValueError: If payload exceeds max size
    """
    if content_length > MAX_PAYLOAD_SIZE_BYTES:
        raise ValueError(
            f"PAYLOAD_TOO_LARGE: {content_length} bytes exceeds max of {MAX_PAYLOAD_SIZE_BYTES}"
        )


# ============================================================
# Combined Request Validation
# ============================================================

def validate_analyze_request(
    ticker: str,
    source: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None
) -> dict:
    """
    Validate a complete analyze ticker request.
    
    Args:
        ticker: Stock ticker symbol
        source: Data source (sec, esg, social)
        start_date: Optional start date
        end_date: Optional end date
        limit: Optional page limit
    
    Returns:
        Dict with validated and normalized values
    
    Raises:
        ValueError: If any validation fails
    """
    validated_ticker = validate_ticker(ticker)
    validated_source = validate_source(source)
    parsed_start, parsed_end = validate_date_range(start_date, end_date)
    validated_limit = validate_page_limit(limit)
    
    return {
        "ticker": validated_ticker,
        "source": validated_source,
        "start_date": parsed_start,
        "end_date": parsed_end,
        "limit": validated_limit,
    }
