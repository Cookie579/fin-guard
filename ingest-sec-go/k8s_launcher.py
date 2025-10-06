import os
import uuid
from kubernetes import client, config
from datetime import datetime

# Read settings from environment (set by the Deployment)
NAMESPACE = os.getenv("POD_NAMESPACE", os.getenv("NAMESPACE", "default"))
RUNNER_IMAGE = os.getenv("RUNNER_IMAGE", "ghcr.io/cookie579/ingest-sec-go:latest")
RUNNER_SERVICEACCOUNT = os.getenv("RUNNER_SERVICEACCOUNT", "ingest-sec-go-sa")
IMAGE_PULL_SECRET = os.getenv("IMAGE_PULL_SECRET", "")  # e.g. ghcr-creds
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Optional: keep job around long enough so you can tail logs
TTL_SECONDS_AFTER_FINISHED = int(os.getenv("JOB_TTL_SECONDS", "1200"))  # 20 minutes

def _ensure_kubeconfig():
    """
    Load in-cluster config if available; fall back to default kubeconfig
    for local testing.
    """
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

def launch_job(ticker: str, extra_args=None) -> str:
    """
    Launch a Kubernetes Job that runs the SEC runner for the given ticker.
    Returns the Job name.
    """
    if extra_args is None:
        extra_args = []

    _ensure_kubeconfig()
    batch = client.BatchV1Api()

    # Generate a unique, readable name: sec-<ticker>-<5char>
    suffix = uuid.uuid4().hex[:5]
    name = f"sec-{ticker.lower()}-{suffix}"

    # Container args: pass --ticker and any extra args
    args = ["--ticker", ticker]
    if extra_args:
        args.extend(extra_args)

    # Environment for the runner
    env = [
        client.V1EnvVar(name="AWS_REGION", value=AWS_REGION),
    ]

    container = client.V1Container(
        name="runner",
        image=RUNNER_IMAGE,
        args=args,
        env=env,
    )

    # Explicitly set SA and imagePullSecrets to avoid admission/injection issues
    image_pull_secrets = []
    if IMAGE_PULL_SECRET:
        image_pull_secrets = [client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)]

    pod_spec = client.V1PodSpec(
        service_account_name=RUNNER_SERVICEACCOUNT,
        restart_policy="Never",
        containers=[container],
        image_pull_secrets=image_pull_secrets
    )

    # Labels for traceability
    labels = {
        "app.kubernetes.io/name": "ingest-sec-go",
        "app.kubernetes.io/component": "runner",
        "sec.ticker": ticker.upper(),
        "job.launchTs": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels=labels),
        spec=pod_spec,
    )

    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=0,  # fail fast so we see real errors
        ttl_seconds_after_finished=TTL_SECONDS_AFTER_FINISHED
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels=labels),
        spec=job_spec
    )

    batch.create_namespaced_job(namespace=NAMESPACE, body=job)
    return name
