import boto3
import json
import time
from langdetect import detect
from botocore.exceptions import ClientError
from s3_uploader import upload_to_s3

STREAM_NAME = "fin-guard-stream"
REGION = "us-east-1"

def clean_text(text):
    return text.strip().replace("\n", " ").lower()

def process_kinesis_stream():
    client = boto3.client("kinesis", region_name=REGION)

    try:
        stream_desc = client.describe_stream(StreamName=STREAM_NAME)
        shard_id = stream_desc["StreamDescription"]["Shards"][0]["ShardId"]

        shard_iterator_response = client.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType="LATEST"
        )
        shard_iterator = shard_iterator_response.get("ShardIterator")

        if not shard_iterator:
            print("❌ Failed to get initial shard iterator.")
            return

    except ClientError as e:
        print(f"❌ Error initializing Kinesis stream: {e}")
        return

    while True:
        try:
            response = client.get_records(ShardIterator=shard_iterator, Limit=10)
            records = response.get("Records", [])

            for record in records:
                data = json.loads(record["Data"])
                cleaned = clean_text(data.get("text", ""))
                lang = detect(cleaned)
                payload = {
                    "company": data.get("company"),
                    "filing_id": data.get("filing_id"),
                    "language": lang,
                    "cleaned_text": cleaned
                }
                upload_to_s3(payload)

            shard_iterator = response.get("NextShardIterator")
            if not shard_iterator:
                print("⚠️ No NextShardIterator returned. Shard may be closed or expired.")
                break

            time.sleep(1)

        except client.exceptions.ProvisionedThroughputExceededException:
            print("⚠️ Throughput exceeded. Retrying after delay...")
            time.sleep(2)

        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            time.sleep(2)
