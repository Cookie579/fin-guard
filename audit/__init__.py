"""
FinGuard Audit - Structured audit logging and AWS audit trail configuration

This package provides:
- Structured audit event logging for app-level actions
- CloudTrail integration for AWS-level audit
- Audit action types for job lifecycle tracking

Modules:
- logger: Structured JSON audit logging
- actions: Audit action type definitions
"""

from .logger import (
    AuditLogger,
    get_audit_logger,
)

from .actions import AuditAction

__all__ = [
    "AuditLogger",
    "AuditAction",
    "get_audit_logger",
]
