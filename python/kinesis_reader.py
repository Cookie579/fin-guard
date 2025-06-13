import boto3
import json
import time
from langdetect import detect
from botocore.exceptions import ClientError
from s3_uploader import upload_to_s3

STREAM_NAME = "fin-guard-stream"
REGION = "us-east-1"

def clean_text(txt: str) -> str:
    return txt.strip().replace("\n", " ").lower()

def process_kinesis_stream():
    client = boto3.client("kinesis", region_name=REGION)

    # ---- 1. Get first shard iterator ----
    try:
        desc = client.describe_stream(StreamName=STREAM_NAME)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
        itr_rsp = client.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType="LATEST"
        )
        shard_itr = itr_rsp.get("ShardIterator")
        if not shard_itr:
            print("❌ Could not obtain initial shard iterator")
            return
    except ClientError as e:
        print(f"❌ Kinesis init error: {e}")
        return

    print("🔁 Started processing Kinesis stream...")

    # ---- 2. Poll loop ----
    while True:
        try:
            rsp = client.get_records(ShardIterator=shard_itr, Limit=10)
            records = rsp.get("Records", [])

            if not records:
                print("ℹ️ No new records. Waiting...")
                time.sleep(1)

            for rec in records:
                try:
                    payload_raw = json.loads(rec["Data"])
                except json.JSONDecodeError as e:
                    print(f"⚠️ Failed to parse record JSON: {e}")
                    continue

                text_raw = payload_raw.get("text", "")
                if not text_raw:
                    print(f"⚠️ Skipped record without 'text': {payload_raw}")
                    continue

                try:
                    cleaned = clean_text(text_raw)
                    lang = detect(cleaned)
                except Exception as e:
                    print(f"⚠️ Failed to clean or detect language: {e}")
                    continue

                payload = {
                    "company": payload_raw.get("company") or payload_raw.get("companyName", "unknown"),
                    "filing_id": payload_raw.get("filing_id", "unknown"),
                    "language": lang,
                    "cleaned_text": cleaned,
                }

                upload_to_s3(payload)

            shard_itr = rsp.get("NextShardIterator")
            if not shard_itr:
                print("⚠️ No NextShardIterator returned – exiting reader")
                break

            time.sleep(1)  # avoid throttling

        except client.exceptions.ProvisionedThroughputExceededException:
            print("⚠️ Throttled – retrying in 2 seconds")
            time.sleep(2)

        except Exception as e:
            print(f"❌ Unexpected stream processing error: {e}")
            time.sleep(2)
