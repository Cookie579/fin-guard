#!/usr/bin/env python3
"""
Kinesis Publisher for Filing Extractor
=======================================

This module provides a simple interface to publish processed filing records
to AWS Kinesis. After analysis completes (SEC/ESG/Social), the analyzer
scripts use this module to emit a STAGE=PROCESSED record that flows through
Kinesis to S3 for Athena queries.

The record format matches the Go `utils.Filing` struct used by the ingest
services, ensuring consistency across the data pipeline.

Example usage::

    from kinesis_client import publish_processed_filing

    filing_record = {
        "company": "Apple Inc.",
        "ticker": "AAPL",
        "source": "SEC",
        "form": "10-K",
        "filingDate": "2024-01-01",
        "accession": "0000320193-24-000123",
        "url": "",
        "stage": "PROCESSED"
    }
    publish_processed_filing(filing_record)
"""

import json
import logging
import os
from typing import Dict, Any

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    _HAVE_BOTO3 = True
except ImportError:
    _HAVE_BOTO3 = False

logger = logging.getLogger(__name__)

# Stream name and region from environment or defaults
KINESIS_STREAM = os.getenv("KINESIS_STREAM", "fin-guard-stream")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def publish_processed_filing(filing_record: Dict[str, Any]) -> bool:
    """
    Publish a processed filing record to Kinesis.

    Args:
        filing_record: Dict with keys: company, ticker, source, form,
                      filingDate, accession, url, stage

    Returns:
        True if published successfully, False otherwise
    """
    if not _HAVE_BOTO3:
        logger.warning("boto3 not installed; skipping Kinesis publish")
        return False

    # Ensure stage is set to PROCESSED
    filing_record["stage"] = "PROCESSED"

    try:
        # Create Kinesis client
        kinesis = boto3.client("kinesis", region_name=AWS_REGION)

        # Serialize record to JSON
        data = json.dumps(filing_record).encode("utf-8")

        # Use ticker as partition key for consistent sharding
        partition_key = filing_record.get("ticker", "UNKNOWN")

        # Put record to stream
        response = kinesis.put_record(
            StreamName=KINESIS_STREAM,
            Data=data,
            PartitionKey=partition_key
        )

        logger.info(
            "✅ Published PROCESSED record to Kinesis: %s (%s)",
            filing_record.get("ticker"),
            filing_record.get("source")
        )
        logger.debug(
            "  ➤ SequenceNumber: %s", response.get("SequenceNumber")
        )
        logger.debug(
            "  ➤ ShardId: %s", response.get("ShardId")
        )
        return True

    except (ClientError, BotoCoreError) as e:
        logger.error("❌ Failed to publish to Kinesis: %s", e)
        return False
    except Exception as e:
        logger.error("❌ Unexpected error publishing to Kinesis: %s", e)
        return False


def extract_metadata_from_path(file_path: str) -> Dict[str, str]:
    """
    Extract metadata from standardized file paths.

    Expected formats:
        - texts/NVDA/10-K/NVDA__2024-01-01__10-K.txt
        - texts/NVDA/ESG/NVDA__2023-12-31__ESG.txt
        - social/NVDA.json

    Args:
        file_path: Path to the input file

    Returns:
        Dict with keys: ticker, filing_date, form_type
    """
    from pathlib import Path

    path = Path(file_path)
    parts = path.stem.split("__")

    # For social files like "NVDA.json"
    if len(parts) == 1:
        return {
            "ticker": parts[0],
            "filing_date": "",
            "form_type": "SOCIAL"
        }

    # For SEC/ESG files like "NVDA__2024-01-01__10-K"
    return {
        "ticker": parts[0] if len(parts) > 0 else "",
        "filing_date": parts[1] if len(parts) > 1 else "",
        "form_type": parts[2] if len(parts) > 2 else path.parent.name
    }
