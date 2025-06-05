from fastapi import FastAPI, BackgroundTasks
from kinesis_reader import process_kinesis_stream

app = FastAPI()

@app.get("/")
def root():
    return {"status": "Preprocessing service is up"}

@app.post("/start-processing")
def start_processing(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_kinesis_stream)
    return {"message": "Kinesis stream processing started"}
