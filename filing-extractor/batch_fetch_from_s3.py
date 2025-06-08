import boto3
import json
import subprocess
from pathlib import Path
import time

# AWS S3 Configuration
BUCKET = "fin-processed-filings"
PREFIX = "Apple Inc./"
CLEANED_SUBDIR = "10-K-TXT"

# Local temp paths
LOCAL_JSON_DIR = "filing-jsons"
TEMP_TXT_PATH = Path("temp.txt")

s3 = boto3.client("s3")

def list_10k_json_keys():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

def download_json_from_s3(key):
    Path(LOCAL_JSON_DIR).mkdir(exist_ok=True)
    local_path = Path(LOCAL_JSON_DIR) / Path(key).name
    s3.download_file(BUCKET, key, str(local_path))
    return local_path

def fetch_and_save_text(index_url):
    result = subprocess.run(["python", "fetch_filing_text.py", index_url], capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ Error fetching:", index_url)
        print(result.stderr)
        return False
    return TEMP_TXT_PATH.exists()

def upload_cleaned_to_s3(filing_date):
    s3_key = f"Apple Inc./{CLEANED_SUBDIR}/{filing_date}.txt"
    s3.upload_file(str(TEMP_TXT_PATH), BUCKET, s3_key)
    print(f"✅ Uploaded to s3://{BUCKET}/{s3_key}")

def already_uploaded(filing_date):
    key = f"Apple Inc./{CLEANED_SUBDIR}/{filing_date}.txt"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

def process_all():
    keys = list_10k_json_keys()
    for key in keys:
        print(f"\n📄 Processing {key}...")
        json_path = download_json_from_s3(key)
        data = json.loads(json_path.read_text())

        url = data.get("linkToFilingDetails")
        date = data.get("filingDate")
        if not url or not date:
            print("⚠️ Missing link or date in JSON")
            continue

        if already_uploaded(date):
            print("✔️ Already uploaded, skipping.")
            continue

        if fetch_and_save_text(url):
            upload_cleaned_to_s3(date)
            TEMP_TXT_PATH.unlink(missing_ok=True)
            time.sleep(0.5)
        else:
            print("⚠️ Failed to fetch/parse.")

if __name__ == "__main__":
    process_all()