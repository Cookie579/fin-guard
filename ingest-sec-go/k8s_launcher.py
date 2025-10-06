import os
import re
import uuid
from datetime import datetime
from kubernetes import client, config

# Read settings from environment (set by the Deployment)
NAMESPACE = os.getenv("POD_NAMESPACE", os.getenv("NAMESPACE", "default"))
RUNNER_IMAGE = os.getenv("RUNNER_IMAGE", "ghcr.io/cookie579/ingest-sec-go:latest")
RUNNER_SERVICEACCOUNT = os.getenv("RUNNER_SERVICEACCOUNT", "ingest-sec-go-sa")
IMAGE_PULL_SECRET = os.getenv("IMAGE_PULL_SECRET", "")  # e.g., ghcr-creds
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Keep finished Jobs a while so you can tail logs
TTL_SECONDS_AFTER_FINISHED = int(os.getenv("JOB_TTL_SECONDS", "1200"))  # 20 minutes

# ----- helpers: label-safe strings -----
_LABEL_MAX = 63
_LABEL_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")

def _labelize(value: str) -> str:
    """
    Make a value safe for k8s label value constraints:
      - only [A-Za-z0-9._-]
      - must start/end with alphanumeric
      - <= 63 chars
    """
    if value is None:
        value = ""
    s = _LABEL_SAFE_RE.sub("-", str(value))
    # trim non-alnum at start/end
    s = re.sub(r"^[^A-Za-z0-9]+", "", s)
    s = re.sub(r"[^A-Za-z0-9]+$", "", s)
    if not s:
        s = "x"
    return s[:_LABEL_MAX]

def _launch_ts_label() -> str:
    # e.g., 20251006T153730Z (label-safe, sortable-ish)
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _ensure_kubeconfig():
    """
    Load in-cluster config if available; fall back to default kubeconfig for local testing.
    """
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

def _make_container(ticker: str, extra_args=None) -> client.V1Container:
    if extra_args is None:
        extra_args = []
    args = ["--ticker", ticker]
    if extra_args:
        args.extend(extra_args)

    env = [
        client.V1EnvVar(name="AWS_REGION", value=AWS_REGION),
        client.V1EnvVar(
            name="SEC_HTTP_UA",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name="sec-http-ua", key="SEC_HTTP_UA", optional=True
                )
            ),
        ),
    ]

    return client.V1Container(
        name="runner",
        image=RUNNER_IMAGE,
        args=args,
        env=env,
    )

def _make_pod_spec(container: client.V1Container) -> client.V1PodSpec:
    image_pull_secrets = []
    if IMAGE_PULL_SECRET:
        image_pull_secrets = [client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)]

    return client.V1PodSpec(
        service_account_name=RUNNER_SERVICEACCOUNT,
        restart_policy="Never",
        containers=[container],
        image_pull_secrets=image_pull_secrets,
    )

def _default_labels(ticker: str) -> dict:
    return {
        "app.kubernetes.io/name": "ingest-sec-go",
        "app.kubernetes.io/component": "runner",
        "sec.ticker": _labelize(ticker.upper()),
        "job.launchTs": _launch_ts_label(),
    }

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
    name = f"sec-{_labelize(ticker).lower()}-{suffix}"

    container = _make_container(ticker, extra_args)
    pod_spec = _make_pod_spec(container)
    labels = _default_labels(ticker)

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels=labels),
        spec=pod_spec,
    )

    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=0,  # fail fast so we see real errors
        ttl_seconds_after_finished=TTL_SECONDS_AFTER_FINISHED,
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels=labels),
        spec=job_spec,
    )

    batch.create_namespaced_job(namespace=NAMESPACE, body=job)
    return name

def launch_cronjob(ticker: str, schedule: str, extra_args=None, suspend: bool = False) -> str:
    """
    Launch (create or replace) a Kubernetes CronJob that runs the SEC runner on a schedule.
    - schedule: standard cron string, e.g. "0 */2 * * *"
    - suspend: if True, the CronJob is created but won't schedule runs until resumed
    Returns the CronJob name.
    """
    if not schedule or not isinstance(schedule, str):
        raise ValueError("schedule (cron string) is required, e.g. '*/15 * * * *'")

    if extra_args is None:
        extra_args = []

    _ensure_kubeconfig()
    batch = client.BatchV1Api()

    # Stable, readable name for the cron workload
    name = f"sec-{_labelize(ticker).lower()}-cron"

    container = _make_container(ticker, extra_args)
    pod_spec = _make_pod_spec(container)
    labels = _default_labels(ticker)

    pod_template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels=labels),
        spec=pod_spec,
    )

    # Each scheduled run creates a Job from this template
    job_spec = client.V1JobSpec(
        template=pod_template,
        backoff_limit=0,
        ttl_seconds_after_finished=TTL_SECONDS_AFTER_FINISHED,
    )

    job_template = client.V1JobTemplateSpec(
        metadata=client.V1ObjectMeta(labels=labels),
        spec=job_spec,
    )

    cron_spec = client.V1CronJobSpec(
        schedule=schedule,
        suspend=suspend,
        concurrency_policy="Forbid",
        successful_jobs_history_limit=1,
        failed_jobs_history_limit=3,
        job_template=job_template,
    )

    cron = client.V1CronJob(
        api_version="batch/v1",
        kind="CronJob",
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels=labels),
        spec=cron_spec,
    )

    # Create or replace
    try:
        batch.read_namespaced_cron_job(name=name, namespace=NAMESPACE)
        batch.replace_namespaced_cron_job(name=name, namespace=NAMESPACE, body=cron)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            batch.create_namespaced_cron_job(namespace=NAMESPACE, body=cron)
        else:
            raise

    return name
