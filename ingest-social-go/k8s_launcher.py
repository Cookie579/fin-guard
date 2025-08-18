import os
from kubernetes import client, config

NAMESPACE = os.getenv("POD_NAMESPACE", "default")
RUNNER_IMAGE = os.getenv("RUNNER_IMAGE", "ghcr.io/cookie579/ingest-social-go:go-2025-08-18")
RUNNER_SA     = os.getenv("RUNNER_SERVICEACCOUNT", "ingest-social-go")  # SA for the Job pod (IRSA)
IMAGE_PULL_SECRET = os.getenv("IMAGE_PULL_SECRET", "ghcr-creds")        # GHCR secret

def _container_args(source: str, ticker: str, extra: list[str]) -> list[str]:
    # Your Go CLI looks like: ingest-social-go <command> [flags]
    base = ["/app/ingest-social-go", source, f"-ticker={ticker}"]
    return base + (extra or [])

def launch_job(source: str, ticker: str, extra: list[str] | None = None):
    config.load_incluster_config()
    batch = client.BatchV1Api()

    name_prefix = f"{source}-{ticker.lower()}-"
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
                command=args[:1],         # ["/app/ingest-social-go"]
                args=args[1:],            # ["stocktwits","-ticker=AAPL",...]
                env=[
                    client.V1EnvVar(name="AWS_REGION", value=os.getenv("AWS_REGION","us-east-1")),
                    # add other env needed by the runner
                ],
                # optional resources:
                # resources=client.V1ResourceRequirements(
                #   requests={"cpu":"200m","memory":"256Mi"},
                #   limits={"cpu":"1","memory":"1Gi"},
                # ),
            )
        ]
    )

    tpl = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app":"ingest-social-go","source":source,"ticker":ticker.lower()}),
        spec=pod
    )
    job_spec = client.V1JobSpec(template=tpl, backoff_limit=3, ttl_seconds_after_finished=3600)
    job = client.V1Job(metadata=client.V1ObjectMeta(generate_name=name_prefix, namespace=NAMESPACE), spec=job_spec)

    batch.create_namespaced_job(namespace=NAMESPACE, body=job)

def launch_cronjob(source: str, ticker: str, schedule: str, extra: list[str] | None = None):
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
        ]
    )

    tpl = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app":"ingest-social-go","source":source,"ticker":ticker.lower()}),
        spec=pod
    )
    job_spec = client.V1JobSpec(template=tpl, backoff_limit=3)

    cron = client.V1CronJob(
        metadata=client.V1ObjectMeta(
            name=f"{source}-{ticker.lower()}-cron",
            namespace=NAMESPACE
        ),
        spec=client.V1CronJobSpec(
            schedule=schedule,
            successful_jobs_history_limit=2,
            failed_jobs_history_limit=2,
            job_template=client.V1JobTemplateSpec(spec=job_spec)
        )
    )
    batch.create_namespaced_cron_job(namespace=NAMESPACE, body=cron)
