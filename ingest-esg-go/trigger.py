from fastapi import FastAPI, BackgroundTasks
import subprocess
import os

app = FastAPI(title="ESG Ingest Trigger")

@app.post("/trigger")
def trigger_downloads(bg: BackgroundTasks):
    bg.add_task(run_go_ingestor)
    return {"status": "Ingestion started"}

def run_go_ingestor():
    try:
        result = subprocess.run(["./ingest-esg-go"], check=True, capture_output=True, text=True)
        print("✅ Go ingestion completed:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("❌ Go ingestion failed:\n", e.stderr)