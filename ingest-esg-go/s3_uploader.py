import json
import boto3

BUCKET_NAME = "fin-processed-filings" 

s3 = boto3.client("s3")

def upload_to_s3(data):
    key = f"{data['company']}/{data['filing_id']}.json"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(data))
