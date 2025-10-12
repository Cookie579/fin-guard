"""
FastAPI orchestrator for ESG ingest workers.

This module exposes a small control plane for launching ESG ingest jobs
as Kubernetes Jobs or CronJobs. Clients can trigger one‑off runs via
POST /jobs/run or schedule recurring runs via POST /cron/schedule.
Each request specifies a list of tickers and optional CLI arguments
which are passed through to the worker binary. The orchestrator offloads
Job creation to background tasks so HTTP responses return quickly.

The endpoints mirror those of ingest‑social‑go but are adapted for
ESG ingest. See k8s_launcher.py for the Job/CronJob definitions.
"""

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Optional

from k8s_launcher import launch_job, launch_cronjob

app = FastAPI(title="ESG Ingest Orchestrator")


class RunReq(BaseModel):
    """Parameters required to launch one or more ESG ingest Jobs."""
    source: str = Field(..., description="Source name: esg")
    tickers: List[str] = Field(..., description="List of ticker symbols")
    args: Optional[Dict[str, str]] = Field(default_factory=dict, description="Additional CLI flags for the worker")


class CronReq(RunReq):
    """Parameters required to schedule recurring ESG CronJobs."""
    schedule: str = Field(..., description="Cron schedule, e.g., */30 * * * *")


@app.get("/")
def health() -> dict[str, bool]:
    """Simple health check to verify the orchestrator is running."""
    return {"ok": True}


@app.post("/jobs/run")
def run_job(req: RunReq, bg: BackgroundTasks) -> dict[str, str]:
    """Launch one or more ESG ingest Jobs via background tasks."""
    for t in req.tickers:
        # Convert args dict into CLI flags. For example {"mode":"esg+domain"}
        # becomes ["--mode=esg+domain"]. Keys are lower‑cased to match
        # flag names.
        extra_args: List[str] = []
        if req.args:
            for k, v in req.args.items():
                # skip empty values
                if v is None or v == "":
                    continue
                extra_args.append(f"--{k}={v}")
        # Each ticker is launched as its own Job. The background task
        # ensures the HTTP handler returns immediately.
        bg.add_task(launch_job, req.source, t, extra_args)
    return {"message": "job(s) submitted"}


@app.post("/cron/schedule")
def schedule_cron(req: CronReq) -> dict[str, str]:
    """Create or update CronJobs for the given tickers."""
    for t in req.tickers:
        extra_args: List[str] = []
        if req.args:
            for k, v in req.args.items():
                if v is None or v == "":
                    continue
                extra_args.append(f"--{k}={v}")
        launch_cronjob(req.source, t, req.schedule, extra_args)
    return {"message": "cronjob(s) created"}