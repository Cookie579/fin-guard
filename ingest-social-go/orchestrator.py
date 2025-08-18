# orchestrator.py
from fastapi import FastAPI, HTTPException, Query
from kubernetes import client, config
from datetime import datetime
import uuid, base64

# --- config ---
NAMESPACE = "default"
IMAGE = "ghcr.io/cookie579/ingest-social-go:latest"
SA = "preprocess-python-sa"
PULL_SECRET = "ghcr-creds"
CONFIG_SECRET = "preprocess-config"  # contains config.json
# ---------------

app = FastAPI(title="Ingest Social Job Orchestrator")

# load kube config (in-cluster or local)
try:
    config.load_incluster_config()
except Exception:
    config.load_kube_config()

batch = client.BatchV1Api()
core = client.CoreV1Api()

def _job_manifest(name: str, args: list[str]):
    return client.V1Job(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=NAMESPACE,
            labels={"app":"ingest-social-go","source":args[0],"ticker":(args[2] if len(args)>2 else "")}
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=3600,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    service_account_name=SA,
                    restart_policy="Never",
                    image_pull_secrets=[client.V1LocalObjectReference(name=PULL_SECRET)],
                    volumes=[client.V1Volume(
                        name="config",
                        secret=client.V1SecretVolumeSource(secret_name=CONFIG_SECRET)
                    )],
                    containers=[client.V1Container(
                        name="worker",
                        image=IMAGE,
                        args=args,
                        volume_mounts=[client.V1VolumeMount(
                            name="config", mount_path="/app/config.json", sub_path="config.json"
                        )]
                    )]
                )
            )
        )
    )

@app.post("/jobs/stocktwits")
def start_stocktwits(ticker: str = Query(...), max_pages: int = 2, since_id: int = 0, until_id: int = 0):
    jname = f"st-{ticker.lower()}-{uuid.uuid4().hex[:6]}"
    args = ["stocktwits","--ticker",ticker.upper(),"--max-pages",str(max_pages)]
    if since_id: args += ["--since-id", str(since_id)]
    if until_id: args += ["--until-id", str(until_id)]
    job = _job_manifest(jname, args)
    batch.create_namespaced_job(NAMESPACE, job)
    return {"job_name": jname, "args": args}

@app.post("/jobs/youtube")
def start_youtube(ticker: str = Query(...), max_videos: int = 50, extra_pages: int = -1, delay_ms: int = 100):
    jname = f"yt-{ticker.lower()}-{uuid.uuid4().hex[:6]}"
    args = ["youtube","--ticker",ticker.upper(),
            "--max-videos",str(max_videos),
            "--extra-pages",str(extra_pages),
            "--delay-ms",str(delay_ms)]
    job = _job_manifest(jname, args)
    batch.create_namespaced_job(NAMESPACE, job)
    return {"job_name": jname, "args": args}

@app.get("/jobs/{name}/logs")
def job_logs(name: str):
    # find the pod for this job
    pods = core.list_namespaced_pod(NAMESPACE, label_selector=f"job-name={name}").items
    if not pods:
        raise HTTPException(404, "pod not found yet")
    pod = pods[0].metadata.name
    logs = core.read_namespaced_pod_log(pod, NAMESPACE, container="worker", tail_lines=2000, _preload_content=False).read()
    # return text (avoid base64 for readability)
    return {"pod": pod, "logs": logs.decode("utf-8", errors="ignore")}

@app.delete("/jobs/{name}")
def stop_job(name: str):
    # delete job and its pods
    propagation = client.V1DeleteOptions(propagation_policy="Foreground")
    try:
        batch.delete_namespaced_job(name, NAMESPACE, body=propagation)
    except client.exceptions.ApiException as e:
        raise HTTPException(e.status, e.reason)
    return {"deleted": name}
