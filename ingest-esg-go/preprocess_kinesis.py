# main.py  –  preprocessing control API
# ------------------------------------
#
# Requires:
#   fastapi
#   uvicorn
#   go_scraper_runner.py   (contains run_* helpers)

from fastapi import FastAPI, BackgroundTasks
from go_scraper_runner import (
    run_esg_scraper,
    run_domain_scraper,
    run_download_scraper,   # ⬅️ new
)

app = FastAPI(title="Fin-Guard Pre-processing Service")


# ── health / root ────────────────────────────────────────────────
@app.get("/", tags=["meta"])
def root():
    """Simple heartbeat."""
    return {"status": "Pre-processing service is up"}


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


# ── 4️⃣  ESG downloader (Go, mode=download) ─────────────────────
@app.post("/scrape-download", tags=["scraper"])
def scrape_download(background_tasks: BackgroundTasks):
    """Trigger the Go scraper to download ESG docs (mode=download)."""
    background_tasks.add_task(run_download_scraper)
    return {"message": "ESG download started"}
