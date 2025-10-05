"""
FastAPI orchestrator for SEC ingest workers.

This module implements a simple control plane for launching ingest
jobs as Kubernetes Jobs or CronJobs. It exposes REST endpoints to run
one‑off SEC ingest jobs for a single ticker or to schedule recurring
CronJobs. Jobs are launched via the Kubernetes Python client using
environment variables to locate the cluster, runner image and service
account. See k8s_launcher.py for the Job/CronJob definitions.

Usage:

    POST /jobs/run       → run a one‑off SEC ingest job
    POST /cron/schedule  → create a CronJob on a schedule
    GET  /               → health check

Requests should include JSON bodies matching the RunReq or CronReq
schemas defined below. The orchestrator offloads Job creation to a
background task so the HTTP response can return immediately. CronJobs
are created synchronously.
"""

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, Field

from k8s_launcher import launch_job, launch_cronjob

app = FastAPI(title="SEC Ingest Orchestrator")


class RunReq(BaseModel):
    """Parameters required to launch a single SEC ingest Job."""

    ticker: str = Field(..., description="Ticker symbol, e.g., AAPL")
    extra_args: list[str] = Field(
        default_factory=list, description="Additional CLI flags for the worker",
    )


class CronReq(RunReq):
    """Parameters required to schedule a recurring CronJob."""

    schedule: str = Field(..., description="Cron schedule, e.g., */60 * * * *")


@app.get("/")
def health() -> dict[str, bool]:
    """Simple health check to verify the orchestrator is running."""
    return {"ok": True}


@app.post("/jobs/run")
def run_job(req: RunReq, bg: BackgroundTasks) -> dict[str, str]:
    """Launch a one‑off SEC ingest Job via a background task."""
    # Offload job creation so the HTTP request can return immediately. If
    # the background task raises, it will be logged by FastAPI but not
    # returned to the client. In practice this is fine as the client
    # expects a best‑effort launch.
    bg.add_task(launch_job, req.ticker, req.extra_args)
    return {"message": "job submitted"}


@app.post("/cron/schedule")
def schedule_cron(req: CronReq) -> dict[str, str]:
    """Create or update a CronJob on the given schedule."""
    launch_cronjob(req.ticker, req.schedule, req.extra_args)
    return {"message": "cronjob created"}