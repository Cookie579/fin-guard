from fastapi import FastAPI, BackgroundTasks
from social_runner import run_stocktwits, run_reddit, run_youtube  # add more as needed

app = FastAPI(title="ingest-social-go Orchestrator")

@app.get("/")
def root():
    return {"status": "ingest-social-go is up"}

@app.post("/run/stocktwits")
def run_st(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_stocktwits)
    return {"message": "StockTwits collector started"}

@app.post("/run/reddit")
def run_rd(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_reddit)
    return {"message": "Reddit collector started"}

@app.post("/run/youtube")
def run_yt(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_youtube)
    return {"message": "YouTube collector started"}
