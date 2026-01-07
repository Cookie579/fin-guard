#!/usr/bin/env python3
"""
finguard CLI - Single command orchestrator for FinGuard analysis

Usage:
    finguard analyze --ticker AAPL
    finguard analyze --ticker AAPL,GOOG,MSFT --sources sec,esg
    finguard analyze --ticker AAPL --force  # Override freshness check
    finguard analyze --ticker AAPL --budget 100  # Set max cost
    finguard status --job-id <job-id>
    finguard list-jobs --ticker AAPL

Features:
- Triggers ingestion jobs for all sources
- Polls for completion with progress display
- Freshness guard (refuses stale data unless --force)
- Backfill budget controls
- Structured output (JSON or human-readable)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional
import uuid

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.validation import validate_ticker, validate_source
from utils.freshness import FreshnessGuard, StaleDataError
from utils.backfill_budget import BackfillBudget, BudgetExceededError
from audit import AuditLogger, AuditAction

try:
    from kubernetes import client, config
    K8S_AVAILABLE = True
except ImportError:
    K8S_AVAILABLE = False


class FinGuardCLI:
    """
    Single-command orchestrator CLI for FinGuard.
    """
    
    NAMESPACE = "finguard"
    POLL_INTERVAL_SECONDS = 5
    MAX_POLL_ATTEMPTS = 120  # 10 minutes max
    
    def __init__(
        self,
        namespace: str = "finguard",
        in_cluster: bool = False,
        audit: Optional[AuditLogger] = None
    ):
        self.namespace = namespace
        self.audit = audit or AuditLogger("finguard-cli")
        self.freshness = FreshnessGuard()
        self.budget = BackfillBudget()
        
        if K8S_AVAILABLE:
            try:
                if in_cluster:
                    config.load_incluster_config()
                else:
                    config.load_kube_config()
                self._batch_v1 = client.BatchV1Api()
                self._core_v1 = client.CoreV1Api()
                self._k8s_available = True
            except Exception as e:
                print(f"Warning: K8s not available ({e}). Running in dry-run mode.")
                self._k8s_available = False
        else:
            self._k8s_available = False
    
    def analyze(
        self,
        tickers: list[str],
        sources: Optional[list[str]] = None,
        force: bool = False,
        budget_usd: float = 50.0,
        wait: bool = True,
        output_format: str = "human"
    ) -> dict:
        """
        Run analysis for one or more tickers.
        
        Args:
            tickers: List of stock tickers
            sources: List of sources (defaults to all)
            force: Override freshness check
            budget_usd: Maximum cost for this run
            wait: Block until jobs complete
            output_format: "human" or "json"
        
        Returns:
            Dict with job statuses
        """
        sources = sources or ["sec", "esg", "social"]
        request_id = str(uuid.uuid4())[:8]
        
        # Validate inputs
        validated_tickers = []
        for ticker in tickers:
            try:
                validated_tickers.append(validate_ticker(ticker))
            except ValueError as e:
                return {"error": str(e), "ticker": ticker}
        
        validated_sources = []
        for source in sources:
            try:
                validated_sources.append(validate_source(source))
            except ValueError as e:
                return {"error": str(e), "source": source}
        
        # Audit: request received
        self.audit.log(
            AuditAction.REQUEST_RECEIVED,
            request_id=request_id,
            extra={"tickers": validated_tickers, "sources": validated_sources}
        )
        
        # Budget check
        self.budget.max_cost_usd = budget_usd
        try:
            estimate = self.budget.check_budget(validated_tickers, validated_sources)
            if output_format == "human":
                print(f"📊 Estimated cost: ${estimate.total_usd:.2f} (budget: ${budget_usd:.2f})")
        except BudgetExceededError as e:
            self.audit.log(
                AuditAction.JOB_CREATE_DENIED,
                request_id=request_id,
                reason="BUDGET_EXCEEDED"
            )
            return {"error": str(e), "estimate": e.estimate.to_dict()}
        
        # Freshness check (unless force)
        if not force:
            for ticker in validated_tickers:
                try:
                    self.freshness.check_ticker_freshness(
                        ticker,
                        sources=validated_sources,
                        allow_missing=True  # Missing is OK, stale is not
                    )
                except StaleDataError as e:
                    if output_format == "human":
                        print(f"⚠️  {e}")
                        print("Use --force to override freshness check")
                    self.audit.log(
                        AuditAction.JOB_CREATE_DENIED,
                        request_id=request_id,
                        ticker=ticker,
                        reason="STALE_DATA"
                    )
                    return {"error": str(e), "stale_sources": [s.source for s in e.stale_sources]}
        
        # Emit METADATA: RUN_STARTED with cost estimate
        run_start_time = time.time()
        self.audit.run_started(
            request_id=request_id,
            tickers=validated_tickers,
            sources=validated_sources,
            cost_estimate=estimate
        )
        
        # Create jobs
        jobs_created = []
        for ticker in validated_tickers:
            for source in validated_sources:
                job_name = f"finguard-{source}-{ticker.lower()}-{request_id}"
                
                if self._k8s_available:
                    job = self._create_job(job_name, ticker, source)
                    if job:
                        jobs_created.append({
                            "job_name": job_name,
                            "ticker": ticker,
                            "source": source,
                            "status": "created"
                        })
                        self.audit.job_created(
                            request_id=request_id,
                            ticker=ticker,
                            source=source,
                            job_name=job_name,
                            k8s_uid=job.metadata.uid
                        )
                else:
                    # Dry run mode
                    jobs_created.append({
                        "job_name": job_name,
                        "ticker": ticker,
                        "source": source,
                        "status": "dry-run"
                    })
        
        if output_format == "human":
            print(f"🚀 Created {len(jobs_created)} jobs")
            for job in jobs_created:
                print(f"   - {job['job_name']}")
        
        # Wait for completion if requested
        if wait and self._k8s_available:
            results = self._poll_jobs([j["job_name"] for j in jobs_created], output_format)
            
            # Emit METADATA: RUN_COMPLETED with cost estimate
            duration_ms = int((time.time() - run_start_time) * 1000)
            succeeded = sum(1 for s in results.values() if s == "succeeded")
            failed = sum(1 for s in results.values() if s == "failed")
            self.audit.run_completed(
                request_id=request_id,
                duration_ms=duration_ms,
                jobs_succeeded=succeeded,
                jobs_failed=failed,
                cost_estimate=estimate
            )
            
            return {
                "request_id": request_id,
                "jobs": jobs_created,
                "results": results,
                "estimate": estimate.to_dict(),
                "duration_ms": duration_ms
            }
        
        return {
            "request_id": request_id,
            "jobs": jobs_created,
            "estimate": estimate.to_dict()
        }
    
    def _create_job(self, job_name: str, ticker: str, source: str):
        """Create a Kubernetes Job."""
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=self.namespace,
                labels={
                    "finguard.io/jobType": source,
                    "finguard.io/ticker": ticker.lower(),
                    "app": "finguard-ingest"
                }
            ),
            spec=client.V1JobSpec(
                backoff_limit=2,
                ttl_seconds_after_finished=3600,
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"app": "finguard-ingest", "finguard.io/jobType": source}
                    ),
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        service_account_name="finguard-ingest",
                        containers=[
                            client.V1Container(
                                name="ingest",
                                image=f"finguard/ingest-{source}:latest",
                                env=[
                                    client.V1EnvVar(name="TICKER", value=ticker),
                                    client.V1EnvVar(name="SOURCE", value=source),
                                ],
                                resources=client.V1ResourceRequirements(
                                    requests={"cpu": "250m", "memory": "256Mi"},
                                    limits={"cpu": "1", "memory": "1Gi"}
                                )
                            )
                        ]
                    )
                )
            )
        )
        
        try:
            return self._batch_v1.create_namespaced_job(
                namespace=self.namespace,
                body=job
            )
        except client.ApiException as e:
            print(f"Error creating job {job_name}: {e.reason}")
            return None
    
    def _poll_jobs(self, job_names: list[str], output_format: str) -> dict:
        """Poll jobs until completion."""
        results = {name: "pending" for name in job_names}
        
        for attempt in range(self.MAX_POLL_ATTEMPTS):
            all_done = True
            
            for job_name in job_names:
                if results[job_name] in ("succeeded", "failed"):
                    continue
                
                try:
                    job = self._batch_v1.read_namespaced_job(
                        name=job_name,
                        namespace=self.namespace
                    )
                    
                    if job.status.succeeded:
                        results[job_name] = "succeeded"
                    elif job.status.failed:
                        results[job_name] = "failed"
                    else:
                        all_done = False
                        results[job_name] = "running"
                        
                except client.ApiException:
                    results[job_name] = "not_found"
            
            if all_done:
                break
            
            if output_format == "human":
                completed = sum(1 for s in results.values() if s in ("succeeded", "failed"))
                print(f"⏳ Progress: {completed}/{len(job_names)} jobs complete", end="\r")
            
            time.sleep(self.POLL_INTERVAL_SECONDS)
        
        if output_format == "human":
            print()  # Newline after progress
            succeeded = sum(1 for s in results.values() if s == "succeeded")
            failed = sum(1 for s in results.values() if s == "failed")
            print(f"✅ Complete: {succeeded} succeeded, {failed} failed")
        
        return results
    
    def status(self, job_id: str) -> dict:
        """Get status of a specific job."""
        if not self._k8s_available:
            return {"error": "K8s not available"}
        
        try:
            job = self._batch_v1.read_namespaced_job(
                name=job_id,
                namespace=self.namespace
            )
            
            status = "pending"
            if job.status.succeeded:
                status = "succeeded"
            elif job.status.failed:
                status = "failed"
            elif job.status.active:
                status = "running"
            
            return {
                "job_name": job_id,
                "status": status,
                "start_time": job.status.start_time.isoformat() if job.status.start_time else None,
                "completion_time": job.status.completion_time.isoformat() if job.status.completion_time else None,
            }
        except client.ApiException as e:
            return {"error": f"Job not found: {e.reason}"}
    
    def list_jobs(self, ticker: Optional[str] = None) -> list[dict]:
        """List jobs, optionally filtered by ticker."""
        if not self._k8s_available:
            return [{"error": "K8s not available"}]
        
        label_selector = "app=finguard-ingest"
        if ticker:
            label_selector += f",finguard.io/ticker={ticker.lower()}"
        
        try:
            jobs = self._batch_v1.list_namespaced_job(
                namespace=self.namespace,
                label_selector=label_selector,
                limit=50
            )
            
            return [
                {
                    "job_name": job.metadata.name,
                    "ticker": job.metadata.labels.get("finguard.io/ticker"),
                    "source": job.metadata.labels.get("finguard.io/jobType"),
                    "status": "succeeded" if job.status.succeeded else "failed" if job.status.failed else "running",
                    "created": job.metadata.creation_timestamp.isoformat() if job.metadata.creation_timestamp else None,
                }
                for job in jobs.items
            ]
        except client.ApiException as e:
            return [{"error": f"Failed to list jobs: {e.reason}"}]


def main():
    parser = argparse.ArgumentParser(
        prog="finguard",
        description="FinGuard CLI - Single command orchestrator"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Run analysis for tickers")
    analyze_parser.add_argument(
        "--ticker", "-t",
        required=True,
        help="Ticker(s) to analyze (comma-separated)"
    )
    analyze_parser.add_argument(
        "--sources", "-s",
        default="sec,esg,social",
        help="Sources to process (comma-separated, default: sec,esg,social)"
    )
    analyze_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Override freshness check"
    )
    analyze_parser.add_argument(
        "--budget",
        type=float,
        default=50.0,
        help="Maximum cost in USD (default: 50)"
    )
    analyze_parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for job completion"
    )
    analyze_parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON"
    )
    
    # status command
    status_parser = subparsers.add_parser("status", help="Get job status")
    status_parser.add_argument("--job-id", required=True, help="Job ID to check")
    status_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    # list-jobs command
    list_parser = subparsers.add_parser("list-jobs", help="List jobs")
    list_parser.add_argument("--ticker", help="Filter by ticker")
    list_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    cli = FinGuardCLI()
    
    if args.command == "analyze":
        tickers = [t.strip() for t in args.ticker.split(",")]
        sources = [s.strip() for s in args.sources.split(",")]
        output_format = "json" if args.json else "human"
        
        result = cli.analyze(
            tickers=tickers,
            sources=sources,
            force=args.force,
            budget_usd=args.budget,
            wait=not args.no_wait,
            output_format=output_format
        )
        
        if args.json:
            print(json.dumps(result, indent=2, default=str))
        elif "error" in result:
            print(f"❌ Error: {result['error']}")
            sys.exit(1)
    
    elif args.command == "status":
        result = cli.status(args.job_id)
        if args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print(f"Job: {result.get('job_name')}")
            print(f"Status: {result.get('status')}")
    
    elif args.command == "list-jobs":
        result = cli.list_jobs(args.ticker)
        if args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            for job in result:
                if "error" in job:
                    print(f"❌ {job['error']}")
                else:
                    print(f"  {job['job_name']} | {job['ticker']} | {job['source']} | {job['status']}")


if __name__ == "__main__":
    main()
