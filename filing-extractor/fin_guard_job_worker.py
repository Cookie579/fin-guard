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

from analyze_sec import main as run_sec_analysis  # type: ignore
from analyze_esg import main as run_esg_analysis  # type: ignore
from analyze_social import SocialAnalyzer  # type: ignore


@dataclass
class JobEnvelope:
    """Simple container for a FinGuard job.

    The analyzers receive messages on their SQS queues whose body is an
    EventBridge event.  The actual job parameters live under
    ``event["detail"]``.  This dataclass extracts and exposes only the
    relevant fields required to discover and process the data.  In
    addition to the original parameters (job id, type, ticker, time
    window, priority and raw payload), an optional ``reprocess`` flag
    indicates whether the analyzer should ignore the processed flag on
    partitions.  When ``reprocess`` is truthy the worker will
    re‑analyze all partitions regardless of their current processed
    status.  Otherwise, only those partitions with ``processed`` set
    to ``False`` or missing are considered.  This flag supports the
    "redo everything from scratch" use case described in the system
    design.

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
    reprocess : bool
        If ``True``, force re‑processing of all partitions for this job.
        Defaults to ``False`` when unspecified in the incoming event.
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
    reprocess: bool
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
        # Determine whether this job should reprocess all partitions.  Upstream
        # events may include a boolean "reprocess" field in the detail
        # payload.  If absent, default to False.  We coerce any truthy
        # value to ``True`` for convenience.
        reprocess_val = detail.get("reprocess")
        reprocess: bool = False
        if isinstance(reprocess_val, bool):
            reprocess = reprocess_val
        elif isinstance(reprocess_val, str):
            # Accept common truthy strings such as "true", "1", "yes".
            reprocess = reprocess_val.lower() in {"true", "1", "yes", "y"}
        elif reprocess_val is not None:
            # Fallback: any non‑None value is considered truthy
            reprocess = True
        return JobEnvelope(
            job_id=job_id,
            type=job_type,
            ticker=ticker,
            window_from=window_from,
            window_to=window_to,
            priority=priority,
            reprocess=reprocess,
            raw=detail,
        )

    def _process_job(self, job: JobEnvelope) -> None:
        """Process a single job by querying DynamoDB and iterating partitions.

        This method performs the core steps common to all analyzers:

        * Build the DynamoDB partition key (``PK``) for the given ticker.
        * Query the table for all partitions associated with that ticker.
        * Filter partitions by logical type, optional time window and processed
          status.  Unless ``job.reprocess`` is truthy, only partitions
          where ``processed`` is either ``False`` or missing are considered.
        * Convert DynamoDB attributes to plain Python types for the
          downstream analyser.
        * Invoke the type‑specific ``_process_partition`` on each match.
        * Mark each processed partition in DynamoDB using a conditional
          update that sets ``processed = True`` and records a
          ``processed_at`` timestamp.  The conditional update protects
          against races by requiring that ``processed`` is either absent
          or currently ``False``.  When ``job.reprocess`` is ``True`` the
          partitions are left untouched so that their state remains
          unchanged.
        """
        pk = f"TICKER#{job.ticker}"
        # Query DynamoDB for all items with the same PK.  We use
        # projection expression to limit the returned attributes.  The
        # sort key (SK) encodes the logical type and timestamp.
        # Determine the sort key prefix for this analyzer type.  The SK is
        # structured as ``SRC#<TYPE>#<timestamp>``.  Using ``begins_with``
        # narrows the query to only those partitions relevant to this
        # analyzer.  Without this filter DynamoDB would return all items
        # for the ticker, including other types, which we would then
        # discard in Python.
        sk_prefix = f"SRC#{self.analyzer_type}#"
        # Build the base query arguments.  We always specify the PK and
        # prefix condition.  When reprocessing is disabled we include a
        # filter expression to exclude already processed partitions.  This
        # reduces the amount of data returned and offloads filtering to
        # DynamoDB rather than Python.
        query_kwargs: Dict[str, object] = {
            "TableName": self.table_name,
            "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_pref)",
            "ExpressionAttributeValues": {
                ":pk": {"S": pk},
                ":sk_pref": {"S": sk_prefix},
            },
        }
        # Only include the filter to exclude processed items when not
        # explicitly reprocessing.  ``attribute_not_exists(processed)``
        # covers the case where the flag has never been set.  However,
        # ``processed`` is a reserved keyword in DynamoDB, so we must
        # reference it via an expression attribute name.  Using
        # ``#p`` as a placeholder allows the query to succeed.  See
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
        # for a list of reserved words.
        expr_attr_names: Optional[Dict[str, str]] = None
        filter_expr: Optional[str] = None
        if not job.reprocess:
            # Define a placeholder for the reserved attribute name
            expr_attr_names = {"#p": "processed"}
            # Only return items that have not been processed yet
            filter_expr = "(#p = :false) OR attribute_not_exists(#p)"
            # Append the boolean value for :false to the existing
            # ExpressionAttributeValues dict
            query_kwargs["ExpressionAttributeValues"][":false"] = {"BOOL": False}
        # Attach the expression attribute names and filter expression if defined
        if expr_attr_names:
            query_kwargs["ExpressionAttributeNames"] = expr_attr_names
        if filter_expr:
            query_kwargs["FilterExpression"] = filter_expr
        # Execute the query.  In a production deployment you may need to
        # handle pagination via LastEvaluatedKey; for simplicity we assume
        # that a single query will return all relevant items.
        response = self._dynamo.query(**query_kwargs)
        items = response.get("Items", [])
        for item in items:
            # Convert DynamoDB attributes to a plain dict for convenience.
            # Preserve booleans as Python bools and strings as Python str.
            partition: Dict[str, object] = {}
            for key, val in item.items():
                if isinstance(val, dict) and "S" in val:
                    partition[key] = val["S"]
                elif isinstance(val, dict) and "BOOL" in val:
                    partition[key] = val["BOOL"]
                else:
                    partition[key] = val
            # Extract the timestamp from the sort key or from first_seen_at if present.
            timestamp_iso: Optional[str] = None
            sk = item.get("SK", {}).get("S")  # type: ignore[assignment]
            if sk and "#" in sk:
                # SK structure: SRC#<TYPE>#<timestamp>
                parts = sk.split("#", 2)
                if len(parts) == 3:
                    timestamp_iso = parts[2]
            if not timestamp_iso:
                ts_attr = item.get("first_seen_at") or item.get("last_seen_at")
                if ts_attr and isinstance(ts_attr, dict) and "S" in ts_attr:
                    timestamp_iso = ts_attr["S"]  # type: ignore[assignment]
            # Filter by the job's time window if specified.  The timestamps
            # are ISO 8601 strings; convert to datetime for comparisons.
            if job.window_from or job.window_to:
                if not timestamp_iso:
                    continue
                try:
                    ts_dt = datetime.fromisoformat(timestamp_iso.rstrip("Z"))
                except Exception:
                    continue
                if job.window_from:
                    try:
                        if ts_dt < datetime.fromisoformat(job.window_from.rstrip("Z")):
                            continue
                    except Exception:
                        pass
                if job.window_to:
                    try:
                        if ts_dt > datetime.fromisoformat(job.window_to.rstrip("Z")):
                            continue
                    except Exception:
                        pass
            # Process the partition.  Any exceptions bubble up to the caller and
            # will result in the SQS message being retried.
            self._process_partition(partition, job)
            # If not reprocessing, mark this partition as processed with a timestamp.
            if not job.reprocess:
                now_iso = datetime.utcnow().isoformat() + "Z"
                try:
                    self._dynamo.update_item(
                        TableName=self.table_name,
                        Key={"PK": {"S": pk}, "SK": {"S": item["SK"]["S"]}},
                        UpdateExpression="SET processed = :true, processed_at = :ts",
                        ConditionExpression="attribute_not_exists(processed) OR processed = :false",
                        ExpressionAttributeValues={
                            ":true": {"BOOL": True},
                            ":false": {"BOOL": False},
                            ":ts": {"S": now_iso},
                        },
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

        The SEC Parquet may either:

        * already contain a ``content`` column with plain text, or
        * contain a ``txt_bytes`` column with raw filing text bytes.

        This method reads the Parquet from S3, normalises it to a
        ``content`` column, concatenates all rows into a single text
        blob and runs the existing SEC analysis.  Results are written
        back to S3 under ``<output_prefix>/<ticker>/SEC/``.
        """
        bucket = partition.get("bucket")
        key = partition.get("key")
        if not isinstance(bucket, str) or not isinstance(key, str):
            raise ValueError("Partition is missing S3 location")

        # Download Parquet bytes from S3
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()

        import io
        import pandas as pd  # type: ignore

        # Read Parquet from in-memory buffer
        df = pd.read_parquet(io.BytesIO(body))

        # Normalise to a 'content' column
        if "content" not in df.columns:
            if "txt_bytes" in df.columns:
                def _decode(b: object) -> str:
                    if isinstance(b, (bytes, bytearray)):
                        try:
                            return b.decode("utf-8", errors="replace")
                        except Exception:
                            return ""
                    return "" if b is None else str(b)
                df["content"] = df["txt_bytes"].apply(_decode)
            else:
                raise KeyError(
                    f"Parquet file {key} lacks a 'content' or 'txt_bytes' column"
                )

        # Concatenate all rows into a single text string
        text_data = "\n\n".join(df["content"].astype(str).tolist())

        # Write to a temporary file on the local filesystem.  Use a
        # cross-platform temp directory and sanitise the job id to avoid
        # illegal filename characters (e.g. ':' on Windows).
        import tempfile
        import re

        tmp_root = tempfile.gettempdir()
        tmp_dir = os.path.join(tmp_root, "fin_guard_sec")
        os.makedirs(tmp_dir, exist_ok=True)
        # Determine the filing date.  If a window is provided, use its date
        # component; otherwise derive it from the job id.  Sanitise any
        # illegal characters (e.g. ':' on Windows).
        window_date_raw: str
        if job.window_from:
            window_date_raw = job.window_from.split("T")[0]
        else:
            # job.job_id is of the form <ticker>-<type>-<timestamp>.  Extract
            # the timestamp portion and drop the time component for the date.
            parts = job.job_id.split("-", 2)
            if len(parts) == 3 and "T" in parts[2]:
                window_date_raw = parts[2].split("T")[0]
            else:
                window_date_raw = job.job_id  # fallback
        safe_window_date = re.sub(r'[:\\/]', '-', window_date_raw)
        # Construct the temporary filename and output filename using the
        # expected SEC naming convention: <Company>__<YYYY-MM-DD>__SEC.txt/json
        tmp_filename = f"{job.ticker}__{safe_window_date}__SEC.txt"
        tmp_path = os.path.join(tmp_dir, tmp_filename)
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(text_data)

        out_fname = f"{job.ticker}__{safe_window_date}__SEC.json"
        out_local = os.path.join(tmp_dir, out_fname)

        # Run existing SEC analysis
        run_sec_analysis(input_file=tmp_path, output_file=out_local)

        # Upload results to S3
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

        import io
        import pandas as pd  # type: ignore

        df = pd.read_parquet(io.BytesIO(body))
        if "content" not in df.columns:
            raise KeyError(f"Parquet file {key} lacks a 'content' column")
        text_data = "\n\n".join(df["content"].astype(str).tolist())
        # Write to a temporary file using a cross-platform temp directory
        # and a sanitised job id to avoid invalid filename characters.
        import tempfile
        import re

        tmp_root = tempfile.gettempdir()
        tmp_dir = os.path.join(tmp_root, "fin_guard_esg")
        os.makedirs(tmp_dir, exist_ok=True)
        safe_job_id = re.sub(r'[:\\/]', '-', job.job_id)
        tmp_path = os.path.join(tmp_dir, f"{safe_job_id}.txt")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(text_data)
        # Construct output filename; sanitise the date portion if using the job id
        window_date_raw = job.window_from.split("T")[0] if job.window_from else job.job_id
        safe_window_date = re.sub(r'[:\\/]', '-', window_date_raw)
        out_fname = f"{job.ticker}__{safe_window_date}__ESG.json"
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

        import io
        import pandas as pd  # type: ignore

        df = pd.read_parquet(io.BytesIO(body))
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
        # to a single day, incorporate it for clarity.  Sanitise the
        # date portion to avoid invalid filename characters (e.g. ':' on Windows).
        import tempfile
        import re
        window_date_raw = job.window_from.split("T")[0] if job.window_from else job.job_id
        safe_window_date = re.sub(r'[:\\/]', '-', window_date_raw)
        out_fname = f"{job.ticker}__{safe_window_date}__SOCIAL.json"
        tmp_root = tempfile.gettempdir()
        tmp_dir = os.path.join(tmp_root, "fin_guard_social")
        os.makedirs(tmp_dir, exist_ok=True)
        out_local = os.path.join(tmp_dir, out_fname)
        with open(out_local, "w", encoding="utf-8") as fh:
            json.dump(metrics, fh, indent=2)
        dest_key = f"{self.output_prefix}/{job.ticker}/SOCIAL/{out_fname}"
        with open(out_local, "rb") as fh:
            self._s3.upload_fileobj(fh, self.output_bucket, dest_key)
