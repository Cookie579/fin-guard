"""
actions.py - Audit action type definitions

Standard audit action types for FinGuard services.
"""

from enum import Enum


class AuditAction(str, Enum):
    """Standard audit action types for FinGuard."""
    
    # Request lifecycle
    REQUEST_RECEIVED = "REQUEST_RECEIVED"
    REQUEST_VALIDATED = "REQUEST_VALIDATED"
    REQUEST_REJECTED = "REQUEST_REJECTED"
    
    # Run lifecycle (orchestrated batch of jobs)
    RUN_STARTED = "RUN_STARTED"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"
    
    # Job lifecycle
    JOB_CREATE_REQUEST = "JOB_CREATE_REQUEST"
    JOB_CREATE_DENIED = "JOB_CREATE_DENIED"
    JOB_CREATED = "JOB_CREATED"
    JOB_STARTED = "JOB_STARTED"
    JOB_COMPLETED = "JOB_COMPLETED"
    JOB_FAILED = "JOB_FAILED"
    
    # Preflight checks
    PREFLIGHT_CHECK_START = "PREFLIGHT_CHECK_START"
    PREFLIGHT_SECRET_MISSING = "PREFLIGHT_SECRET_MISSING"
    PREFLIGHT_CONFIGMAP_MISSING = "PREFLIGHT_CONFIGMAP_MISSING"
    PREFLIGHT_CHECK_PASSED = "PREFLIGHT_CHECK_PASSED"
    PREFLIGHT_CHECK_FAILED = "PREFLIGHT_CHECK_FAILED"
    
    # Rate limiting
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    
    # Cost / Budget
    COST_ESTIMATE = "COST_ESTIMATE"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    
    # Authentication
    AUTH_SUCCESS = "AUTH_SUCCESS"
    AUTH_FAILURE = "AUTH_FAILURE"
