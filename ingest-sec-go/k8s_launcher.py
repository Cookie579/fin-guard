"""
Helpers for launching Kubernetes Jobs and CronJobs for SEC ingest workers.

This module encapsulates the logic for spawning Kubernetes Jobs and
CronJobs using the Python client. It reads configuration from
environment variables so that different clusters and images can be
targeted without code changes. The functions defined here are used by
the FastAPI orchestrator (see orchestrator.py).

Environment variables:
  RUNNER_IMAGE          – Docker image for the ingest-sec-go worker (required)
  RUNNER_SERVICEACCOUNT – ServiceAccount to attach to the Job pod
  IMAGE_PULL_SECRET     – Name of the imagePullSecret to use
  POD_NAMESPACE         – Namespace to create Jobs/CronJobs (defaults to "default")
  AWS_REGION            – Region for AWS SDK inside the pod (optional)
  CONFIG_SECRET         – Name of the Secret containing config.json (optional)
"""

import os
from typing import List, Optional

from kubernetes import client, config

# Determine namespace and default values from environment. These can be
# overridden when deploying the orchestrator via Helm values.
NAMESPACE = os.getenv("POD_NAMESPACE", "default")
RUNNER_IMAGE = os.getenv(
    "RUNNER_IMAGE", "ghcr.io/cookie579/ingest-sec-go:latest"
)
RUNNER_SA = os.getenv("RUNNER_SERVICEACCOUNT", "ingest-sec-go-sa")
IMAGE_PULL_SECRET = os.getenv("IMAGE_PULL_SECRET", "ghcr-creds")
# Secret name containing config.json. Defaults to preprocess-config. The
# ingest-sec-go worker does not currently use this file but we mount it
# for consistency with other ingest workers.
CONFIG_SECRET = os.getenv("CONFIG_SECRET", "preprocess-config")


def _container_args(ticker: str, extra: Optional[List[str]] = None) -> List[str]:
    """Build the argument list for the ingest-sec-go worker.

    The worker binary is compiled into /app/bin/ingest-sec-go at image
    build time. We always pass the --ticker=<TICKER> flag to tell the
    program which company to process. Additional arguments from the
    request are appended verbatim. The ticker is upper‑cased here to
    keep a consistent partition key in DynamoDB and S3 keys.
    """
    base_cmd = ["/app/bin/ingest-sec-go"]
    args = ["--ticker", ticker.upper()]
    return base_cmd + args + (extra or [])


def launch_job(ticker: str, extra: Optional[List[str]] = None) -> None:
    """Create a Kubernetes Job for a one‑off SEC ingest run."""
    # Use in‑cluster config. If this fails outside the cluster, the
    # orchestrator should catch and log the exception.
    config.load_incluster_config()
    batch = client.BatchV1Api()

    # Generate a name prefix; Kubernetes will append a unique suffix.
    name_prefix = f"sec-{ticker.lower()}-"
    cmd = _container_args(ticker, extra)

    # Define the pod spec. The worker runs as a single container pod. We
    # propagate AWS_REGION (if provided) so the Go SDK picks up the
    # correct region. Create a secret volume for config.json. The Go
    # worker expects /app/config.json but currently does not use it; this
    # mount is harmless and allows future configuration.
    volume = client.V1Volume(
        name="config",
        secret=client.V1SecretVolumeSource(secret_name=CONFIG_SECRET),
    )
    volume_mount = client.V1VolumeMount(
        name="config",
        mount_path="/app/config.json",
        sub_path="config.json",
        read_only=True,
    )
    pod = client.V1PodSpec(
        service_account_name=RUNNER_SA,
        restart_policy="OnFailure",
        image_pull_secrets=[client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
        volumes=[volume],
        containers=[
            client.V1Container(
                name="runner",
                image=RUNNER_IMAGE,
                image_pull_policy="Always",
                command=cmd[:1],
                args=cmd[1:],
                env=[client.V1EnvVar(name="AWS_REGION", value=os.getenv("AWS_REGION", "us-east-1"))],
                volume_mounts=[volume_mount],
            )
        ],
    )

    # Assemble the Job spec. We set backoff_limit to 3 and ttl to 1h after finish.
    tpl = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "ingest-sec-go", "ticker": ticker.lower()}),
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


def launch_cronjob(ticker: str, schedule: str, extra: Optional[List[str]] = None) -> None:
    """Create or update a Kubernetes CronJob for recurring SEC ingest runs."""
    config.load_incluster_config()
    batch = client.BatchV1Api()
    cmd = _container_args(ticker, extra)
    volume = client.V1Volume(
        name="config",
        secret=client.V1SecretVolumeSource(secret_name=CONFIG_SECRET),
    )
    volume_mount = client.V1VolumeMount(
        name="config",
        mount_path="/app/config.json",
        sub_path="config.json",
        read_only=True,
    )
    pod = client.V1PodSpec(
        service_account_name=RUNNER_SA,
        restart_policy="OnFailure",
        image_pull_secrets=[client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
        volumes=[volume],
        containers=[
            client.V1Container(
                name="runner",
                image=RUNNER_IMAGE,
                image_pull_policy="Always",
                command=cmd[:1],
                args=cmd[1:],
                volume_mounts=[volume_mount],
            )
        ],
    )
    tpl = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "ingest-sec-go", "ticker": ticker.lower()}),
        spec=pod,
    )
    job_spec = client.V1JobSpec(template=tpl, backoff_limit=3)
    cron = client.V1CronJob(
        metadata=client.V1ObjectMeta(
            name=f"sec-{ticker.lower()}-cron",
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