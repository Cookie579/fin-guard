"""
logger.py - Structured audit logging for FinGuard

This module provides:
- Structured audit event logging
- Request lifecycle tracking
- JSON output for CloudWatch/OTel pipeline

Usage:
    from audit import AuditLogger, AuditAction

    audit = AuditLogger(service_name="orchestrator-sec")
    audit.log(
        action=AuditAction.JOB_CREATE_REQUEST,
        ticker="AAPL",
        source="sec",
        actor="api-key-xxxx",
        decision="ALLOW"
    )
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from .actions import AuditAction


class AuditLogger:
    """
    Structured audit logging for FinGuard services.
    
    Emits JSON-structured audit events to stdout for collection
    by CloudWatch Logs, FluentBit, or OTEL pipeline.
    """
    
    def __init__(
        self,
        service_name: str,
        environment: Optional[str] = None,
        namespace: str = "finguard"
    ):
        """
        Initialize audit logger.
        
        Args:
            service_name: Name of the service (e.g., "orchestrator-sec")
            environment: Environment name (defaults to ENVIRONMENT env var)
            namespace: Kubernetes namespace
        """
        self.service_name = service_name
        self.environment = environment or os.getenv("ENVIRONMENT", "prod")
        self.namespace = namespace
        
        # Configure JSON logger
        self._logger = logging.getLogger(f"audit.{service_name}")
        self._logger.setLevel(logging.INFO)
        
        # Ensure we have a handler
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)
    
    def log(
        self,
        action: AuditAction,
        *,
        request_id: Optional[str] = None,
        actor: Optional[str] = None,
        ticker: Optional[str] = None,
        source: Optional[str] = None,
        job_name: Optional[str] = None,
        k8s_uid: Optional[str] = None,
        decision: Optional[str] = None,
        reason: Optional[str] = None,
        ip: Optional[str] = None,
        user_agent: Optional[str] = None,
        duration_ms: Optional[int] = None,
        extra: Optional[dict[str, Any]] = None
    ) -> dict:
        """
        Log a structured audit event.
        
        Args:
            action: The audit action type
            request_id: Unique request identifier (generated if not provided)
            actor: User ID, service principal, or API key ID
            ticker: Stock ticker if applicable
            source: Data source (sec, esg, social)
            job_name: Kubernetes job name if applicable
            k8s_uid: Kubernetes resource UID
            decision: ALLOW or DENY
            reason: Reason for decision (especially for DENY)
            ip: Client IP address
            user_agent: Client user agent
            duration_ms: Operation duration in milliseconds
            extra: Additional context
        
        Returns:
            The audit event dict that was logged
        """
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service_name,
            "environment": self.environment,
            "namespace": self.namespace,
            "action": action.value,
            "request_id": request_id or str(uuid.uuid4()),
        }
        
        # Add optional fields only if present
        if actor:
            event["actor"] = actor
        if ticker:
            event["ticker"] = ticker
        if source:
            event["source"] = source
        if job_name:
            event["job_name"] = job_name
        if k8s_uid:
            event["k8s_uid"] = k8s_uid
        if decision:
            event["decision"] = decision
        if reason:
            event["reason"] = reason
        if ip:
            event["ip"] = ip
        if user_agent:
            event["user_agent"] = user_agent
        if duration_ms is not None:
            event["duration_ms"] = duration_ms
        if extra:
            event["extra"] = extra
        
        # Emit as JSON
        self._logger.info(json.dumps(event))
        
        return event
    
    def request_received(
        self,
        request_id: str,
        ticker: str,
        source: str,
        actor: Optional[str] = None,
        ip: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> dict:
        """Log REQUEST_RECEIVED event."""
        return self.log(
            AuditAction.REQUEST_RECEIVED,
            request_id=request_id,
            ticker=ticker,
            source=source,
            actor=actor,
            ip=ip,
            user_agent=user_agent
        )
    
    def request_rejected(
        self,
        request_id: str,
        reason: str,
        ticker: Optional[str] = None,
        source: Optional[str] = None,
        actor: Optional[str] = None
    ) -> dict:
        """Log REQUEST_REJECTED event."""
        return self.log(
            AuditAction.REQUEST_REJECTED,
            request_id=request_id,
            ticker=ticker,
            source=source,
            actor=actor,
            decision="DENY",
            reason=reason
        )
    
    def job_created(
        self,
        request_id: str,
        ticker: str,
        source: str,
        job_name: str,
        k8s_uid: str,
        actor: Optional[str] = None
    ) -> dict:
        """Log JOB_CREATED event."""
        return self.log(
            AuditAction.JOB_CREATED,
            request_id=request_id,
            ticker=ticker,
            source=source,
            job_name=job_name,
            k8s_uid=k8s_uid,
            actor=actor,
            decision="ALLOW"
        )
    
    def job_create_denied(
        self,
        request_id: str,
        ticker: str,
        source: str,
        reason: str,
        actor: Optional[str] = None
    ) -> dict:
        """Log JOB_CREATE_DENIED event."""
        return self.log(
            AuditAction.JOB_CREATE_DENIED,
            request_id=request_id,
            ticker=ticker,
            source=source,
            actor=actor,
            decision="DENY",
            reason=reason
        )
    
    def preflight_failed(
        self,
        request_id: str,
        ticker: str,
        source: str,
        reason: str
    ) -> dict:
        """Log PREFLIGHT_CHECK_FAILED event."""
        return self.log(
            AuditAction.PREFLIGHT_CHECK_FAILED,
            request_id=request_id,
            ticker=ticker,
            source=source,
            decision="DENY",
            reason=reason
        )
    
    def run_started(
        self,
        request_id: str,
        tickers: list[str],
        sources: list[str],
        cost_estimate = None
    ) -> dict:
        """
        Log RUN_STARTED event with cost estimate (METADATA).
        
        This emits the cost estimate into the audit log stream,
        which can be parsed by downstream systems for cost tracking.
        """
        extra = {
            "tickers": tickers,
            "sources": sources,
            "stage": "METADATA",
        }
        
        if cost_estimate:
            extra["cost_estimate"] = {
                "total_usd": cost_estimate.total_usd,
                "budget_usd": cost_estimate.budget_usd,
                "breakdown": {
                    "api_calls_usd": cost_estimate.api_calls_usd,
                    "compute_usd": cost_estimate.compute_usd,
                    "llm_tokens_usd": cost_estimate.llm_tokens_usd,
                    "storage_usd": cost_estimate.storage_usd,
                },
                "ticker_count": len(cost_estimate.tickers),
                "source_count": len(cost_estimate.sources),
            }
        
        return self.log(
            AuditAction.RUN_STARTED,
            request_id=request_id,
            extra=extra
        )
    
    def run_completed(
        self,
        request_id: str,
        duration_ms: int,
        jobs_succeeded: int,
        jobs_failed: int,
        cost_estimate = None
    ) -> dict:
        """
        Log RUN_COMPLETED event with cost estimate (METADATA).
        """
        extra = {
            "stage": "METADATA",
            "jobs_succeeded": jobs_succeeded,
            "jobs_failed": jobs_failed,
            "jobs_total": jobs_succeeded + jobs_failed,
        }
        
        if cost_estimate:
            extra["cost_estimate"] = {
                "total_usd": cost_estimate.total_usd,
                "budget_usd": cost_estimate.budget_usd,
            }
        
        return self.log(
            AuditAction.RUN_COMPLETED,
            request_id=request_id,
            duration_ms=duration_ms,
            extra=extra
        )


# ============================================================
# Singleton for easy access
# ============================================================

_default_logger: Optional[AuditLogger] = None


def get_audit_logger(service_name: Optional[str] = None) -> AuditLogger:
    """
    Get the default audit logger instance.
    
    Args:
        service_name: Service name (required on first call)
    
    Returns:
        AuditLogger instance
    """
    global _default_logger
    
    if _default_logger is None:
        if service_name is None:
            service_name = os.getenv("SERVICE_NAME", "finguard-orchestrator")
        _default_logger = AuditLogger(service_name)
    
    return _default_logger

