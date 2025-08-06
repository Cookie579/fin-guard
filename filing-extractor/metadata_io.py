# metadata_io.py
"""Utility helpers to push metadata records to Kinesis and (optionally)
   persist a copy in fin‑processed‑filings.

Usage example
-------------
>>> from metadata_io import send_sec_metadata, send_esg_metadata
>>> rec = {
...     "company": "Apple",
...     "ticker": "AAPL",
...     "form": "10-K",
...     "filingDate": "2023-11-03",
...     "accession": "0000320193-23-000106",
...     "txt_s3_key": "Apple/texts/10-K/Apple-10-K-2023-11-03.txt",
...     "txt_bytes": 345678,
...     "ingest_ts": "2025-08-04T16:22:31Z",
... }
>>> send_sec_metadata(rec, stream_name="fin-guard-stream")
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict

import boto3

# AWS clients are created at import‑time so callers don’t need to pass them
s3 = boto3.client("s3")
kinesis = boto3.client("kinesis")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize(name: str) -> str:
    """Return a filesystem‑ and S3‑key‑safe version of *name*."""
    return re.sub(r"[\\/]+", "-", name).replace(" ", "_")


def _json_bytes(obj: Dict[str, Any]) -> bytes:
    """Serialize *obj* to UTF‑8 JSON bytes, no whitespace padding."""
    return json.dumps(obj, separators=(",", ":")).encode()

# ---------------------------------------------------------------------------
# SEC metadata
# ---------------------------------------------------------------------------

def send_sec_metadata(record: Dict[str, Any], *, stream_name: str,
                       bucket: str = "fin-processed-filings") -> None:
    """Push *record* to Kinesis and store a copy in **fin‑processed‑filings**.

    Parameters
    ----------
    record : dict
        The metadata dictionary that **must** contain at least:
            company, ticker, form, filingDate
    stream_name : str
        Kinesis Data Stream (or Firehose delivery stream) name.
    bucket : str, optional
        "fin-processed-filings" by default.
    """
    required = {"company", "ticker", "form", "filingDate"}
    missing = required - record.keys()
    if missing:
        raise KeyError(f"SEC record missing: {', '.join(sorted(missing))}")

    # 1️⃣  Send to Kinesis
    kinesis.put_record(
        StreamName=stream_name,
        Data=_json_bytes(record),
        PartitionKey=str(record["ticker"]),
    )

    # 2️⃣  Persist a clean copy in fin‑processed‑filings
    company = _sanitize(record["company"])
    form    = _sanitize(record["form"])
    date    = record["filingDate"]  # must be YYYY‑MM‑DD

    s3_key = f"{company}/filing-jsons/{form}/{company}-{form}-{date}-Cleaned.json"
    s3.put_object(Bucket=bucket, Key=s3_key, Body=_json_bytes(record))

    print(
        f"✅ SEC metadata: put to kinesis://{stream_name} and s3://{bucket}/{s3_key}")

# ---------------------------------------------------------------------------
# ESG metadata (stub)
# ---------------------------------------------------------------------------

def send_esg_metadata(record: Dict[str, Any], *, stream_name: str):
    """Placeholder for ESG JSON publisher (not yet implemented)."""
    # TODO: implement once ESG PDF/text pipeline is live.
    raise NotImplementedError("ESG metadata publishing not implemented yet.")
