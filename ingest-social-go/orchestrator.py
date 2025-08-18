from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from k8s_launcher import launch_job, launch_cronjob

app = FastAPI(title="Social Ingest Orchestrator")

class RunReq(BaseModel):
    source: str           # "stocktwits" | "youtube" | "reddit"
    ticker: str           # e.g., "AAPL"
    extra_args: list[str] = []  # optional flags

class CronReq(RunReq):
    schedule: str         # cron string, e.g., "*/15 * * * *"

@app.get("/")
def health():
    return {"ok": True}

@app.post("/jobs/run")
def run_job(req: RunReq, bg: BackgroundTasks):
    bg.add_task(launch_job, req.source, req.ticker, req.extra_args)
    return {"message": "job submitted"}

@app.post("/cron/schedule")
def schedule_cron(req: CronReq):
    launch_cronjob(req.source, req.ticker, req.schedule, req.extra_args)
    return {"message": "cronjob created"}
