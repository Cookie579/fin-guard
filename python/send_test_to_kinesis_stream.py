import boto3
import json

kinesis = boto3.client("kinesis", region_name="us-east-1")

data = {
    "company": "OpenAI",
    "filing_id": "test-001",
    "text": "OpenAI has released a new language model that is making waves in the financial sector."
}

response = kinesis.put_record(
    StreamName="fin-guard-stream",  # 🔁 Replace this
    Data=json.dumps(data),
    PartitionKey="test-partition"
)

print("✅ Sent test record:", response)
