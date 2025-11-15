"""
FinGuard Job Worker
===================

This module provides a simple framework for consuming analysis jobs from
Amazon SQS, discovering the relevant raw data via DynamoDB and S3, and
dispatching the work to the existing analyzers.  It is designed to
integrate the existing SEC, ESG and social sentiment analyzers with the
new decoupled pipeline described in the architecture overview.  Each
analyzer type has a dedicated FIFO queue (e.g. ``sec-analyzer.fifo``) and
an EventBridge rule that routes jobs of the appropriate type to that
queue.  Jobs themselves are small JSON envelopes containing the ticker,
type and time window to analyze.  The raw data resides in a data lake
under ``s3://fin-datalake/datalake/events/`` and is indexed in the
``fin_guard_partitions`` DynamoDB table by ticker, logical type and
timestamp.

The worker classes defined here abstract away the common logic of
polling SQS, parsing the EventBridge envelope, querying DynamoDB for
matching partitions, downloading the corresponding Parquet files from S3
and invoking the appropriate analyzer.  Concrete subclasses override
``_process_partition`` to perform type‑specific processing.  This
approach allows the existing SEC/ESG/social analyzers to remain
agnostic of AWS while still participating in the job‑driven pipeline.

Usage
-----

Each worker can be run as a standalone script or imported into a
larger service.  The following example starts a SEC worker that
consumes jobs from ``sec-analyzer.fifo`` and writes its outputs to a
``processed/`` prefix in the same S3 bucket:

.. code-block:: python

    from fin_guard_job_worker import SecJobWorker

    worker = SecJobWorker(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/sec-analyzer.fifo",
        table_name="fin_guard_partitions",
        output_bucket="fin-datalake",
        output_prefix="processed"
    )
    worker.run()

You may supply optional keyword arguments such as AWS region or
credentials via environment variables or directly when constructing
``boto3`` clients.

Note
----

This worker is designed for production use with AWS services.  When
running in a local development environment without access to AWS, the
``boto3`` calls will fail unless stubbed out or pointed at local
emulators such as ``localstack``.  The code is structured in a way that
allows the AWS clients to be replaced with mocks for testing.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import boto3  # type: ignore

from .analyze_sec import main as run_sec_analysis  # type: ignore
from .analyze_esg import main as run_esg_analysis  # type: ignore
from .analyze_social import SocialAnalyzer  # type: ignore


@dataclass
class JobEnvelope:
    """Simple container for a FinGuard job.

    The analyzers receive messages on their SQS queues whose body is an
    EventBridge event.  The actual job parameters live under
    ``event["detail"]``.  This dataclass extracts and exposes only the
    relevant fields required to discover and process the data.

    Attributes
    ----------
    job_id : str
        Unique identifier for the job.  Typically of the form
        ``<TICKER>-<TYPE>-<DATE>``.
    type : str
        Logical type of analysis to perform (``SEC``, ``ESG`` or ``SOCIAL``).
    ticker : str
        Ticker symbol identifying the company to analyze.
    window_from : Optional[str]
        ISO 8601 timestamp marking the beginning of the time window.
    window_to : Optional[str]
        ISO 8601 timestamp marking the end of the time window.
    priority : Optional[str]
        Priority hint used by upstream scheduling.  The worker currently
        ignores this field but could use it to prioritise urgent jobs.
    raw : Dict[str, object]
        The original ``detail`` dict from the EventBridge event.  This
        allows downstream processors to access additional fields without
        requiring changes to this dataclass.
    """

    job_id: str
    type: str
    ticker: str
    window_from: Optional[str]
    window_to: Optional[str]
    priority: Optional[str]
    raw: Dict[str, object]


class BaseJobWorker:
    """Abstract base class for FinGuard job workers.

    A job worker encapsulates the logic of polling its SQS queue,
    transforming messages into :class:`JobEnvelope` instances, querying
    DynamoDB for matching partitions and invoking a downstream analysis
    routine.  Concrete subclasses implement the `_process_partition`
    method to perform type‑specific work on a single partition.
    """

    # SQS long polling parameters.  Adjust these to tune throughput and
    # latency.  The default values wait up to 10 seconds for a message
    # and retrieve at most one message at a time.  In practice you can
    # increase ``MaxNumberOfMessages`` up to 10 to batch work.
    _SQS_WAIT_TIME = 10
    _SQS_MAX_MESSAGES = 1

    def __init__(
        self,
        *,
        queue_url: str,
        table_name: str,
        output_bucket: str,
        output_prefix: str = "processed",
        aws_region: Optional[str] = None,
        sqs_client: Optional[object] = None,
        dynamodb_client: Optional[object] = None,
        s3_client: Optional[object] = None,
    ) -> None:
        """Initialise the job worker.

        Parameters
        ----------
        queue_url : str
            Full URL of the SQS FIFO queue to poll for jobs (e.g.
            ``https://sqs.us-east-1.amazonaws.com/123456789012/sec-analyzer.fifo``).
        table_name : str
            Name of the DynamoDB table containing partitions metadata
            (typically ``fin_guard_partitions``).
        output_bucket : str
            S3 bucket name where processed outputs will be written.
        output_prefix : str, optional
            Prefix within the output bucket under which results will be
            stored.  Default is ``processed``.
        aws_region : str, optional
            AWS region in which to create clients.  If omitted, boto3
            will use the default region configured in the environment.
        sqs_client : boto3.client, optional
            Preconfigured SQS client.  If ``None``, a new client is
            created for the given region.
        dynamodb_client : boto3.client, optional
            Preconfigured DynamoDB client.  If ``None``, a new client
            is created.
        s3_client : boto3.client, optional
            Preconfigured S3 client.  If ``None``, a new client is
            created.
        """
        self.queue_url = queue_url
        self.table_name = table_name
        self.output_bucket = output_bucket
        self.output_prefix = output_prefix.rstrip("/")
        self._region = aws_region
        # Lazily initialise AWS clients so they can be mocked or
        # substituted during testing.  Use the provided clients if
        # supplied, otherwise create new ones.
        self._sqs = sqs_client or boto3.client("sqs", region_name=aws_region)
        self._dynamo = dynamodb_client or boto3.client(
            "dynamodb", region_name=aws_region
        )
        self._s3 = s3_client or boto3.client("s3", region_name=aws_region)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Begin polling the SQS queue indefinitely.

        This method enters an infinite loop and blocks until an
        interruption occurs (e.g. SIGINT).  In each iteration it waits
        for messages on the queue, processes them sequentially, and
        deletes them upon success.  If processing fails an exception
        will be raised, causing the message to become visible again
        after the visibility timeout.
        """
        while True:
            msgs = self._sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self._SQS_MAX_MESSAGES,
                WaitTimeSeconds=self._SQS_WAIT_TIME,
            ).get("Messages", [])
            if not msgs:
                continue
            for msg in msgs:
                try:
                    envelope = self._parse_message(msg)
                    # Skip messages not intended for this worker type.  This
                    # should not happen if EventBridge routing is configured
                    # correctly, but is defensive against misconfigurations.
                    if envelope.type.upper() != self.analyzer_type:
                        self._sqs.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                        continue
                    self._process_job(envelope)
                    # Delete message only after successful processing to
                    # honour at‑least‑once semantics.
                    self._sqs.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                except Exception as exc:
                    # Log the exception and allow the message to be retried
                    # after the visibility timeout expires.  In a real
                    # deployment you would integrate with a monitoring system.
                    print(f"⚠️  Error processing message: {exc}")

    # ------------------------------------------------------------------
    # Abstract methods
    # ------------------------------------------------------------------
    @property
    def analyzer_type(self) -> str:
        """Return the logical type handled by this worker.

        Must be implemented by subclasses to return one of ``SEC``,
        ``ESG`` or ``SOCIAL``.  This is used to validate incoming
        messages and filter DynamoDB partitions.
        """
        raise NotImplementedError

    def _process_partition(self, partition: Dict[str, object], job: JobEnvelope) -> None:
        """Handle a single partition.

        Subclasses override this to implement the actual analysis logic for
        one partition of raw data.  The ``partition`` dict contains the
        attributes returned by DynamoDB, including at least ``bucket`` and
        ``key``.  The ``job`` parameter provides the full job context.

        Implementations should download the raw file from S3, process it
        using the appropriate analyzer, write outputs back to S3 and
        optionally mark the partition as processed in DynamoDB.  Any
        exceptions raised here will cause the message to be retried.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _parse_message(self, msg: Dict[str, object]) -> JobEnvelope:
        """Parse a raw SQS message into a :class:`JobEnvelope`.

        The analyzers receive messages where the body is an EventBridge
        event.  The actual job details live under ``event["detail"]``.
        This helper extracts the relevant fields and returns a
        :class:`JobEnvelope` instance.  If required fields are missing
        a ``KeyError`` will be raised.
        """
        body = msg.get("Body")
        if not body:
            raise ValueError("Message body is empty")
        event = json.loads(body)
        detail = event.get("detail", {})
        job_id = detail["job_id"]  # raises KeyError if missing
        job_type = detail["type"].upper()
        ticker = detail["ticker"].upper()
        window = detail.get("window", {}) or {}
        window_from = window.get("from")
        window_to = window.get("to")
        priority = detail.get("priority")
        return JobEnvelope(
            job_id=job_id,
            type=job_type,
            ticker=ticker,
            window_from=window_from,
            window_to=window_to,
            priority=priority,
            raw=detail,
        )

    def _process_job(self, job: JobEnvelope) -> None:
        """Process a single job by querying DynamoDB and iterating partitions.

        This method performs the core steps common to all analyzers:

        * Build the DynamoDB partition key (``PK``) for the given ticker.
        * Query the table for all partitions associated with that ticker.
        * Filter partitions by logical type and optional time window.
        * Invoke the type‑specific ``_process_partition`` on each match.
        * Optionally update the partition's processed status in DynamoDB.
        """
        pk = f"TICKER#{job.ticker}"
        # Query DynamoDB for all items with the same PK.  We use
        # projection expression to limit the returned attributes.  The
        # sort key (SK) encodes the logical type and timestamp.
        response = self._dynamo.query(
            TableName=self.table_name,
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
        )
        items = response.get("Items", [])
        for item in items:
            logical_type = item.get("logical_type", {}).get("S")
            if not logical_type or logical_type.upper() != self.analyzer_type:
                continue
            # Extract the timestamp from the sort key or from first_seen_at if present.
            timestamp_iso: Optional[str] = None
            sk = item.get("SK", {}).get("S")
            if sk and "#" in sk:
                *_, ts = sk.split("#", 2)
                timestamp_iso = ts
            if not timestamp_iso:
                ts_attr = item.get("first_seen_at") or item.get("last_seen_at")
                if ts_attr and "S" in ts_attr:
                    timestamp_iso = ts_attr["S"]
            # Filter by window if provided
            if job.window_from or job.window_to:
                if not timestamp_iso:
                    continue
                ts_dt = datetime.fromisoformat(timestamp_iso)
                if job.window_from and ts_dt < datetime.fromisoformat(job.window_from):
                    continue
                if job.window_to and ts_dt > datetime.fromisoformat(job.window_to):
                    continue
            # Convert DynamoDB attributes to a plain dict for convenience
            partition: Dict[str, object] = {}
            for key, val in item.items():
                if isinstance(val, dict) and "S" in val:
                    partition[key] = val["S"]
                elif isinstance(val, dict) and "BOOL" in val:
                    partition[key] = val["BOOL"]
                else:
                    # Leave other attribute types as is
                    partition[key] = val
            self._process_partition(partition, job)
            # Optionally mark partition processed.  We set a Boolean flag
            # ``processed`` to True.  This is safe even if the flag does not
            # exist yet because DynamoDB will add it on update.  If a
            # timestamp attribute is preferred, extend this accordingly.
            try:
                self._dynamo.update_item(
                    TableName=self.table_name,
                    Key={"PK": {"S": pk}, "SK": {"S": item["SK"]["S"]}},
                    UpdateExpression="SET processed = :p",
                    ExpressionAttributeValues={":p": {"BOOL": True}},
                )
            except Exception:
                # Best effort; failures here should not halt processing
                pass


class SecJobWorker(BaseJobWorker):
    """Job worker for SEC analysis jobs."""

    @property
    def analyzer_type(self) -> str:
        return "SEC"

    def _process_partition(self, partition: Dict[str, object], job: JobEnvelope) -> None:
        """Run SEC analysis on a single partition.

        The SEC analyzer operates on plain text files.  Raw SEC data in the
        data lake is stored as Parquet files containing one or more
        columns.  This implementation downloads the Parquet file from S3,
        extracts the ``content`` column (assumed to contain the filing
        text), writes it to a temporary local file and then invokes the
        existing ``run_sec_analysis`` function.  The results are uploaded
        back to S3 under ``<output_bucket>/<output_prefix>/<ticker>/SEC/``.

        If the Parquet schema differs from this assumption you can
        override this method to perform the appropriate extraction.
        """
        bucket = partition.get("bucket")
        key = partition.get("key")
        if not isinstance(bucket, str) or not isinstance(key, str):
            raise ValueError("Partition is missing S3 location")
        # Download the Parquet file to memory
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        # Load into pandas to extract text.  We read the entire file
        # because SEC filings are generally small enough to fit in memory.
        import pandas as pd  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
        table = pq.read_table(source=body)
        df = table.to_pandas()
        if "content" not in df.columns:
            raise KeyError(f"Parquet file {key} lacks a 'content' column")
        # Concatenate all rows into a single text string.  Some filings
        # may include multiple parts; join them with double newlines.
        text_data = "\n\n".join(df["content"].astype(str).tolist())
        # Write to a temporary file on the local filesystem.  Using the
        # job_id ensures uniqueness and aids debugging.
        tmp_dir = "/tmp/fin_guard_sec"
        os.makedirs(tmp_dir, exist_ok=True)
        tmp_path = os.path.join(tmp_dir, f"{job.job_id}.txt")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(text_data)
        # Construct output filename based on ticker and job window.  We
        # reuse the existing convention: ``<ticker>__<YYYY-MM-DD>__10-K.json``.
        window_date = job.window_from.split("T")[0] if job.window_from else job.job_id
        out_fname = f"{job.ticker}__{window_date}__SEC.json"
        out_local = os.path.join(tmp_dir, out_fname)
        # Run the existing SEC analysis.  This writes JSON to the
        # specified output file.
        run_sec_analysis(input_file=tmp_path, output_file=out_local)
        # Upload the results back to S3.  Store under
        # ``<output_prefix>/<ticker>/SEC/<filename>``.
        dest_key = f"{self.output_prefix}/{job.ticker}/SEC/{out_fname}"
        with open(out_local, "rb") as fh:
            self._s3.upload_fileobj(fh, self.output_bucket, dest_key)


class EsgJobWorker(BaseJobWorker):
    """Job worker for ESG analysis jobs."""

    @property
    def analyzer_type(self) -> str:
        return "ESG"

    def _process_partition(self, partition: Dict[str, object], job: JobEnvelope) -> None:
        """Run ESG analysis on a single partition.

        Similar to SEC, ESG raw data is expected to be a Parquet file with
        a ``content`` column containing report text.  This method
        downloads the Parquet, extracts the text, writes it to a
        temporary file and invokes ``run_esg_analysis``.  The output is
        uploaded back to S3 under ``<ticker>/ESG/``.
        """
        bucket = partition.get("bucket")
        key = partition.get("key")
        if not isinstance(bucket, str) or not isinstance(key, str):
            raise ValueError("Partition is missing S3 location")
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        import pandas as pd  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
        table = pq.read_table(source=body)
        df = table.to_pandas()
        if "content" not in df.columns:
            raise KeyError(f"Parquet file {key} lacks a 'content' column")
        text_data = "\n\n".join(df["content"].astype(str).tolist())
        tmp_dir = "/tmp/fin_guard_esg"
        os.makedirs(tmp_dir, exist_ok=True)
        tmp_path = os.path.join(tmp_dir, f"{job.job_id}.txt")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(text_data)
        window_date = job.window_from.split("T")[0] if job.window_from else job.job_id
        out_fname = f"{job.ticker}__{window_date}__ESG.json"
        out_local = os.path.join(tmp_dir, out_fname)
        # run_esg_analysis requires both input and output paths
        run_esg_analysis(input_file=tmp_path, output_file=out_local)
        dest_key = f"{self.output_prefix}/{job.ticker}/ESG/{out_fname}"
        with open(out_local, "rb") as fh:
            self._s3.upload_fileobj(fh, self.output_bucket, dest_key)


class SocialJobWorker(BaseJobWorker):
    """Job worker for social sentiment analysis jobs."""

    @property
    def analyzer_type(self) -> str:
        return "SOCIAL"

    def _process_partition(self, partition: Dict[str, object], job: JobEnvelope) -> None:
        """Run social sentiment analysis on a single partition.

        Social partitions may come from multiple sources (e.g. StockTwits,
        YouTube) but share the same logical type ``SOCIAL``.  Each
        partition is expected to be a Parquet file with at least two
        columns: ``timestamp`` (ISO format) and ``content`` (post text).
        This method reads the Parquet into a pandas DataFrame and then
        computes aggregate sentiment metrics using :class:`SocialAnalyzer`.
        The results are saved as JSON and uploaded to S3 under
        ``<ticker>/SOCIAL/``.
        """
        bucket = partition.get("bucket")
        key = partition.get("key")
        if not isinstance(bucket, str) or not isinstance(key, str):
            raise ValueError("Partition is missing S3 location")
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        import pandas as pd  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
        table = pq.read_table(source=body)
        df = table.to_pandas()
        # Validate required columns
        if not {"timestamp", "content"}.issubset(df.columns):
            raise KeyError(
                f"Parquet file {key} must have 'timestamp' and 'content' columns"
            )
        # Convert DataFrame rows to list of dicts for SocialAnalyzer
        posts = df.to_dict(orient="records")
        analyser = SocialAnalyzer()
        metrics = analyser.compute_features(posts)
        # Use the job_id as part of the filename.  If the window is set
        # to a single day, incorporate it for clarity.
        window_date = job.window_from.split("T")[0] if job.window_from else job.job_id
        out_fname = f"{job.ticker}__{window_date}__SOCIAL.json"
        tmp_dir = "/tmp/fin_guard_social"
        os.makedirs(tmp_dir, exist_ok=True)
        out_local = os.path.join(tmp_dir, out_fname)
        with open(out_local, "w", encoding="utf-8") as fh:
            json.dump(metrics, fh, indent=2)
        dest_key = f"{self.output_prefix}/{job.ticker}/SOCIAL/{out_fname}"
        with open(out_local, "rb") as fh:
            self._s3.upload_fileobj(fh, self.output_bucket, dest_key)
