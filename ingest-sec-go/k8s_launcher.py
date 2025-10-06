import os
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


def _ensure_kubeconfig():
    """
    Load in-cluster config if available; fall back to default kubeconfig for local testing.
    """
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()


def _make_container(ticker: str, extra_args=None) -> client.V1Container:
    """
    Construct the container spec for a SEC ingest Job.

    The ingest‑sec‑go image contains both the compiled Go binary and a
    bootstrap script that starts the FastAPI orchestrator. When
    launching a one‑off Job we must bypass the bootstrap and invoke
    the Go runner directly; otherwise the default entrypoint will
    ignore our flags and start uvicorn, resulting in an exit status
    of 127. The compiled binary is placed at ``/app/bin/ingest-sec-go``
    during the image build (see bootstrap.sh). Here we set the
    container ``command`` to that binary and supply flags via
    ``args``. The ticker is upper‑cased to match downstream AWS and
    DynamoDB keys. Additional CLI flags are appended verbatim.

    Args:
        ticker: Stock ticker symbol.
        extra_args: Optional additional flags for the Go runner.

    Returns:
        A configured V1Container object for inclusion in the Pod spec.
    """
    if extra_args is None:
        extra_args = []
    # Construct argument list: always prefix with the --ticker flag.  The
    # Go runner supports flags in the form --ticker=SYMBOL, but it also
    # accepts "--ticker SYMBOL".  Here we use the latter to avoid
    # parsing issues when passing through Kubernetes YAML.  Uppercase
    # the ticker to match the case used in DynamoDB keys.
    args = ["--ticker", ticker.upper()]
    if extra_args:
        # Extend with any extra CLI flags provided by the client.  Do not
        # uppercase these as they may include values (e.g. --since-days 90).
        args.extend(extra_args)

    # Propagate the AWS region into the pod environment.  Without this
    # environment variable the AWS SDK defaults to us-east-1 which may
    # not match the deployment region.
    env = [
        client.V1EnvVar(name="AWS_REGION", value=AWS_REGION),
    ]

    return client.V1Container(
        name="runner",
        image=RUNNER_IMAGE,
        # Override the default ENTRYPOINT (bootstrap.sh) so that the Go
        # binary is executed directly.  If we omit 'command' then
        # Kubernetes appends our args to the existing entrypoint, causing
        # bootstrap.sh to receive unknown flags and exit 127.
        command=["/app/bin/ingest-sec-go"],
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
    """
    Construct a base set of Kubernetes labels for the given ticker.  In
    addition to standard app labels, a timestamp is included to aid in
    debugging and correlating Jobs to specific launch times.  However,
    Kubernetes imposes strict formatting rules on label values: they may
    only contain alphanumeric characters, dashes, underscores and dots,
    must start and end with an alphanumeric character, and cannot
    include colons.  Using the raw ISO8601 timestamp (e.g.
    ``2025-10-06T08:40:38Z``) violates this restriction because of the
    embedded colons.  To satisfy the constraints we generate a
    timestamp without colon characters using ``strftime``.  The output
    follows the pattern ``YYYY-MM-DDTHHMMSSZ`` which preserves human
    readability while conforming to the label regex.

    Args:
        ticker: Stock ticker symbol (case-insensitive).

    Returns:
        A dictionary of label key/value pairs safe for use on both Job
        metadata and pod templates.
    """
    # Generate a UTC timestamp that avoids characters disallowed in
    # Kubernetes label values.  A typical ISO8601 string like
    # ``2025-10-06T08:40:38Z`` contains colon characters, which are
    # rejected by Kubernetes.  Instead we remove the colons from the
    # time portion while retaining the date/time separator 'T' and
    # trailing 'Z' for readability.
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
    return {
        "app.kubernetes.io/name": "ingest-sec-go",
        "app.kubernetes.io/component": "runner",
        # Normalize the ticker to uppercase for consistency.  Label
        # values must start and end with an alphanumeric character,
        # which is satisfied by uppercase letters and digits.
        "sec.ticker": ticker.upper(),
        # Include the sanitized launch timestamp as a label.  This makes
        # it easy to filter Jobs by their launch time using label
        # selectors while avoiding invalid characters.
        "job.launchTs": timestamp,
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
    name = f"sec-{ticker.lower()}-{suffix}"

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
    name = f"sec-{ticker.lower()}-cron"

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
        # Avoid overlapping runs if a previous run is still executing
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
        existing = batch.read_namespaced_cron_job(name=name, namespace=NAMESPACE)
        # If we get here, replace it
        batch.replace_namespaced_cron_job(name=name, namespace=NAMESPACE, body=cron)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            batch.create_namespaced_cron_job(namespace=NAMESPACE, body=cron)
        else:
            raise

    return name
