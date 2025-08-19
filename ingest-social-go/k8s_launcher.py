"""
Helpers for launching Kubernetes Jobs and CronJobs for ingest workers.

This module encapsulates the logic for spawning Kubernetes Jobs and
CronJobs using the Python client. It reads configuration from
environment variables so that different clusters and images can be
targeted without code changes. The functions defined here are used by
the FastAPI orchestrator (see orchestrator.py).

Environment variables:
  RUNNER_IMAGE         – Docker image for the Go worker (required)
  RUNNER_SERVICEACCOUNT – ServiceAccount to attach to the Job pod
  IMAGE_PULL_SECRET    – Name of the imagePullSecret to use
  POD_NAMESPACE        – Namespace to create Jobs/CronJobs (defaults to "default")
  AWS_REGION           – Region for AWS SDK inside the pod (optional)
"""

import os
from kubernetes import client, config

# Determine namespace and default values from environment. These can be
# overridden when deploying the orchestrator via Helm values.
NAMESPACE = os.getenv("POD_NAMESPACE", "default")
RUNNER_IMAGE = os.getenv("RUNNER_IMAGE", "ghcr.io/cookie579/ingest-social-go:latest")
RUNNER_SA = os.getenv("RUNNER_SERVICEACCOUNT", "ingest-social-go")
IMAGE_PULL_SECRET = os.getenv("IMAGE_PULL_SECRET", "ghcr-creds")


def _container_args(source: str, ticker: str, extra: list[str] | None) -> list[str]:
    """Build the argument list for the Go worker.

    The worker binary is compiled into /app/bin/ingest-social-go at image
    build time. We always pass the subcommand (source) followed by
    --ticker=<TICKER>. Extra arguments from the request are appended
    verbatim. The ticker is upper‑cased here to keep a consistent
    partition key in Kinesis.
    """
    base = ["/app/bin/ingest-social-go", source, f"--ticker={ticker.upper()}"]
    return base + (extra or [])


def launch_job(source: str, ticker: str, extra: list[str] | None = None) -> None:
    """Create a Kubernetes Job for a one-off ingest run."""
    # Use in-cluster config. If this fails outside the cluster, the
    # orchestrator should catch and log the exception.
    config.load_incluster_config()
    batch = client.BatchV1Api()

    # Generate a name prefix; Kubernetes will append a unique suffix.
    name_prefix = f"{source}-{ticker.lower()}-"
    args = _container_args(source, ticker, extra)

    # Define the pod spec. The worker runs as a single container pod. We
    # propagate AWS_REGION (if provided) so the Go SDK picks up the
    # correct region. Additional envs can be added if needed.
    pod = client.V1PodSpec(
        service_account_name=RUNNER_SA,
        restart_policy="OnFailure",
        image_pull_secrets=[client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
        containers=[
            client.V1Container(
                name="runner",
                image=RUNNER_IMAGE,
                image_pull_policy="IfNotPresent",
                command=args[:1],  # binary path
                args=args[1:],     # subcommand and flags
                env=[client.V1EnvVar(name="AWS_REGION", value=os.getenv("AWS_REGION", "us-east-1"))],
            )
        ],
    )

    # Assemble the Job spec. We set backoff_limit to 3 and ttl to 1h after finish.
    tpl = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "ingest-social-go", "source": source, "ticker": ticker.lower()}),
        spec=pod,
    )
    job_spec = client.V1JobSpec(
        template=tpl,
        backoff_limit=3,
        ttl_seconds_after_finished=3600,
    )
    job = client.V1Job(
        metadata=client.V1ObjectMeta(generate_name=name_prefix, namespace=NAMESPACE),
        spec=job_spec,
    )

    batch.create_namespaced_job(namespace=NAMESPACE, body=job)


def launch_cronjob(source: str, ticker: str, schedule: str, extra: list[str] | None = None) -> None:
    """Create or update a Kubernetes CronJob for recurring ingest runs."""
    config.load_incluster_config()
    batch = client.BatchV1Api()

    args = _container_args(source, ticker, extra)
    pod = client.V1PodSpec(
        service_account_name=RUNNER_SA,
        restart_policy="OnFailure",
        image_pull_secrets=[client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
        containers=[
            client.V1Container(
                name="runner",
                image=RUNNER_IMAGE,
                image_pull_policy="IfNotPresent",
                command=args[:1],
                args=args[1:],
            )
        ],
    )
    tpl = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "ingest-social-go", "source": source, "ticker": ticker.lower()}),
        spec=pod,
    )
    job_spec = client.V1JobSpec(template=tpl, backoff_limit=3)
    cron = client.V1CronJob(
        metadata=client.V1ObjectMeta(
            name=f"{source}-{ticker.lower()}-cron",
            namespace=NAMESPACE,
        ),
        spec=client.V1CronJobSpec(
            schedule=schedule,
            successful_jobs_history_limit=2,
            failed_jobs_history_limit=2,
            job_template=client.V1JobTemplateSpec(spec=job_spec),
        ),
    )
    batch.create_namespaced_cron_job(namespace=NAMESPACE, body=cron)