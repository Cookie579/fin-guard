# kinesis_reader.py
import boto3, json, time
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
        desc  = client.describe_stream(StreamName=STREAM_NAME)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
        itr_rsp  = client.get_shard_iterator(
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

    # ---- 2. Poll loop ----
    while True:
        try:
            rsp = client.get_records(ShardIterator=shard_itr, Limit=10)
            for rec in rsp.get("Records", []):
                payload_raw = json.loads(rec["Data"])
                cleaned = clean_text(payload_raw.get("text", ""))
                lang    = detect(cleaned)

                upload_to_s3({
                    "company":    payload_raw.get("company"),
                    "filing_id":  payload_raw.get("filing_id"),
                    "language":   lang,
                    "cleaned_text": cleaned,
                })

            shard_itr = rsp.get("NextShardIterator")
            if not shard_itr:
                print("⚠️   No NextShardIterator returned - exiting reader")
                break

            time.sleep(1)     # stay under 5 gets/sec shard-limit

        except client.exceptions.ProvisionedThroughputExceededException:
            print("⚠️   Throttled - backing off 2 s")
            time.sleep(2)

        except Exception as e:
            print(f"❌ Stream processing error: {e}")
            time.sleep(2)
