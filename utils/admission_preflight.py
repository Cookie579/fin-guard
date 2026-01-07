"""
admission_preflight.py - Pre-flight validation before K8s Job creation

This module implements "admission-like" validation at the orchestrator level:
- Verify required Secrets exist
- Verify required ConfigMaps exist
- Verify ServiceAccount exists (optional)

If any required resource is missing, the Job creation is denied BEFORE
calling create_namespaced_job, preventing CrashLoopBackoff pods.

Usage:
    from utils.admission_preflight import AdmissionPreflight, PreflightError

    preflight = AdmissionPreflight(namespace="finguard")
    
    try:
        preflight.check_job_requirements(
            job_type="sec",
            secrets=["sec-edgar-api-key", "aws-credentials"],
            configmaps=["sec-config"]
        )
    except PreflightError as e:
        # Log audit event, return 422, do NOT create Job
        audit.preflight_failed(request_id, ticker, source, str(e))
        return {"error": str(e)}, 422
"""

import logging
from typing import Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class PreflightError(Exception):
    """Raised when preflight check fails."""
    
    def __init__(self, resource_type: str, resource_name: str, message: str):
        self.resource_type = resource_type
        self.resource_name = resource_name
        super().__init__(message)


class AdmissionPreflight:
    """
    Pre-flight validation for Kubernetes Job creation.
    
    Checks that required Secrets, ConfigMaps, and ServiceAccounts exist
    before creating a Job. Prevents CrashLoopBackoff from missing deps.
    """
    
    # Default required resources by job type
    DEFAULT_REQUIREMENTS = {
        "sec": {
            "secrets": ["sec-edgar-api-key"],
            "configmaps": ["sec-config"],
            "serviceaccount": "finguard-ingest",
        },
        "esg": {
            "secrets": ["serper-api-key"],
            "configmaps": ["esg-config"],
            "serviceaccount": "finguard-ingest",
        },
        "social": {
            "secrets": ["stocktwits-credentials"],
            "configmaps": ["social-config"],
            "serviceaccount": "finguard-ingest",
        },
        "common": {
            "secrets": ["aws-credentials", "pg-database-url"],
            "configmaps": ["finguard-common"],
        }
    }
    
    def __init__(
        self,
        namespace: str = "finguard",
        in_cluster: bool = True
    ):
        """
        Initialize preflight checker.
        
        Args:
            namespace: Kubernetes namespace to check resources in
            in_cluster: If True, use in-cluster config; else use kubeconfig
        """
        self.namespace = namespace
        
        # Load Kubernetes config
        try:
            if in_cluster:
                config.load_incluster_config()
            else:
                config.load_kube_config()
        except Exception as e:
            logger.warning(f"Failed to load K8s config: {e}. Preflight checks will be skipped.")
            self._k8s_available = False
            return
        
        self._k8s_available = True
        self._v1 = client.CoreV1Api()
    
    def check_secret_exists(self, name: str) -> bool:
        """
        Check if a Secret exists in the namespace.
        
        Args:
            name: Secret name
        
        Returns:
            True if exists
        
        Raises:
            PreflightError: If secret does not exist
        """
        if not self._k8s_available:
            logger.warning(f"K8s not available, skipping secret check for {name}")
            return True
        
        try:
            self._v1.read_namespaced_secret(name=name, namespace=self.namespace)
            logger.debug(f"Secret {name} exists in {self.namespace}")
            return True
        except ApiException as e:
            if e.status == 404:
                raise PreflightError(
                    resource_type="Secret",
                    resource_name=name,
                    message=f"MISSING_SECRET:{name} - Required secret does not exist in namespace {self.namespace}"
                )
            raise PreflightError(
                resource_type="Secret",
                resource_name=name,
                message=f"SECRET_CHECK_FAILED:{name} - {e.reason}"
            )
    
    def check_configmap_exists(self, name: str) -> bool:
        """
        Check if a ConfigMap exists in the namespace.
        
        Args:
            name: ConfigMap name
        
        Returns:
            True if exists
        
        Raises:
            PreflightError: If configmap does not exist
        """
        if not self._k8s_available:
            logger.warning(f"K8s not available, skipping configmap check for {name}")
            return True
        
        try:
            self._v1.read_namespaced_config_map(name=name, namespace=self.namespace)
            logger.debug(f"ConfigMap {name} exists in {self.namespace}")
            return True
        except ApiException as e:
            if e.status == 404:
                raise PreflightError(
                    resource_type="ConfigMap",
                    resource_name=name,
                    message=f"MISSING_CONFIGMAP:{name} - Required configmap does not exist in namespace {self.namespace}"
                )
            raise PreflightError(
                resource_type="ConfigMap",
                resource_name=name,
                message=f"CONFIGMAP_CHECK_FAILED:{name} - {e.reason}"
            )
    
    def check_serviceaccount_exists(self, name: str) -> bool:
        """
        Check if a ServiceAccount exists in the namespace.
        
        Args:
            name: ServiceAccount name
        
        Returns:
            True if exists
        
        Raises:
            PreflightError: If serviceaccount does not exist
        """
        if not self._k8s_available:
            logger.warning(f"K8s not available, skipping serviceaccount check for {name}")
            return True
        
        try:
            self._v1.read_namespaced_service_account(name=name, namespace=self.namespace)
            logger.debug(f"ServiceAccount {name} exists in {self.namespace}")
            return True
        except ApiException as e:
            if e.status == 404:
                raise PreflightError(
                    resource_type="ServiceAccount",
                    resource_name=name,
                    message=f"MISSING_SERVICEACCOUNT:{name} - Required serviceaccount does not exist in namespace {self.namespace}"
                )
            raise PreflightError(
                resource_type="ServiceAccount",
                resource_name=name,
                message=f"SERVICEACCOUNT_CHECK_FAILED:{name} - {e.reason}"
            )
    
    def check_job_requirements(
        self,
        job_type: str,
        secrets: Optional[list[str]] = None,
        configmaps: Optional[list[str]] = None,
        serviceaccount: Optional[str] = None,
        include_common: bool = True
    ) -> dict:
        """
        Check all requirements for a job type.
        
        Args:
            job_type: Type of job (sec, esg, social)
            secrets: List of required secret names (uses defaults if None)
            configmaps: List of required configmap names (uses defaults if None)
            serviceaccount: Required serviceaccount name (uses default if None)
            include_common: If True, also check common requirements
        
        Returns:
            Dict with check results
        
        Raises:
            PreflightError: If any required resource is missing
        """
        results = {
            "job_type": job_type,
            "secrets_checked": [],
            "configmaps_checked": [],
            "serviceaccount_checked": None,
            "passed": False
        }
        
        # Get defaults for job type
        defaults = self.DEFAULT_REQUIREMENTS.get(job_type, {})
        common = self.DEFAULT_REQUIREMENTS.get("common", {}) if include_common else {}
        
        # Determine what to check
        secrets_to_check = secrets if secrets is not None else defaults.get("secrets", [])
        configmaps_to_check = configmaps if configmaps is not None else defaults.get("configmaps", [])
        sa_to_check = serviceaccount if serviceaccount is not None else defaults.get("serviceaccount")
        
        # Add common requirements
        if include_common:
            secrets_to_check = list(set(secrets_to_check + common.get("secrets", [])))
            configmaps_to_check = list(set(configmaps_to_check + common.get("configmaps", [])))
        
        # Check secrets
        for secret_name in secrets_to_check:
            self.check_secret_exists(secret_name)
            results["secrets_checked"].append(secret_name)
        
        # Check configmaps
        for cm_name in configmaps_to_check:
            self.check_configmap_exists(cm_name)
            results["configmaps_checked"].append(cm_name)
        
        # Check serviceaccount
        if sa_to_check:
            self.check_serviceaccount_exists(sa_to_check)
            results["serviceaccount_checked"] = sa_to_check
        
        results["passed"] = True
        return results


# ============================================================
# Convenience function
# ============================================================

_default_preflight: Optional[AdmissionPreflight] = None


def get_preflight(namespace: str = "finguard") -> AdmissionPreflight:
    """Get the default preflight checker instance."""
    global _default_preflight
    
    if _default_preflight is None:
        _default_preflight = AdmissionPreflight(namespace=namespace)
    
    return _default_preflight
