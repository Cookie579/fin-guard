# main.py  –  preprocessing control API
# ------------------------------------
#
# Requires:
#   fastapi
#   uvicorn
#   kinesis_reader.py
#   go_scraper_runner.py   (contains run_esg_scraper / run_domain_scraper)

from fastapi import FastAPI, BackgroundTasks
from kinesis_reader import process_kinesis_stream
from go_scraper_runner import run_esg_scraper, run_domain_scraper

app = FastAPI(title="Fin-Guard Pre-processing Service")


# ── health / root ────────────────────────────────────────────────
@app.get("/", tags=["meta"])
def root():
    """Simple heartbeat."""
    return {"status": "Pre-processing service is up"}


# ── 1️⃣  start continuous Kinesis consumer ───────────────────────
@app.post("/start-processing", tags=["kinesis"])
def start_processing(background_tasks: BackgroundTasks):
    """Begin the long-running Kinesis stream processor."""
    background_tasks.add_task(process_kinesis_stream)
    return {"message": "Kinesis stream processing started"}


# ── 2️⃣  ESG link scraper (Go, mode=esg) ─────────────────────────
@app.post("/scrape-esg", tags=["scraper"])
def scrape_esg(background_tasks: BackgroundTasks):
    """Trigger the Go scraper to look up ESG links only."""
    background_tasks.add_task(run_esg_scraper)
    return {"message": "ESG link scraping started"}


# ── 3️⃣  Domain discovery scraper (Go, mode=domain) ─────────────
@app.post("/scrape-domain", tags=["scraper"])
def scrape_domain(background_tasks: BackgroundTasks):
    """Trigger the Go scraper to discover company domains only."""
    background_tasks.add_task(run_domain_scraper)
    return {"message": "Domain discovery started"}
