import boto3
import json
from langdetect import detect
from s3_uploader import upload_to_s3

STREAM_NAME = "your-kinesis-stream-name"
REGION = "us-east-1"

def clean_text(text):
    return text.strip().replace("\n", " ").lower()

def process_kinesis_stream():
    client = boto3.client("kinesis", region_name=REGION)
    response = client.describe_stream(StreamName=STREAM_NAME)
    shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

    shard_iterator = client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="LATEST"
    )["ShardIterator"]

    while True:
        records = client.get_records(ShardIterator=shard_iterator, Limit=10)
        for record in records["Records"]:
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
        shard_iterator = records["NextShardIterator"]
