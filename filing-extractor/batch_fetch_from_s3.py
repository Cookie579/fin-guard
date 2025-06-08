import boto3
import json
import subprocess
from pathlib import Path
import time

# AWS S3 Configuration
BUCKET = "fin-processed-filings"

# Local paths
BASE_JSON_DIR = Path("filing-jsons")
LOCAL_TXT_PATH = Path("temp.txt")

s3 = boto3.client("s3")

def list_all_10k_json_keys():
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") and "/" in key:
                keys.append(key)
    return keys

def sanitize_name(name):
    return name.strip().replace("/", "-").replace(" ", "_")

def download_json_from_s3(key):
    company_name = sanitize_name(key.split("/")[0])
    local_dir = BASE_JSON_DIR / company_name
    local_dir.mkdir(parents=True, exist_ok=True)

    local_path = local_dir / Path(key).name
    s3.download_file(BUCKET, key, str(local_path))
    return local_path

def fetch_and_save_text(index_url, company_name, filing_date):
    result = subprocess.run(
        ["python", "fetch_filing_text.py", index_url, company_name, filing_date],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("❌ Script error:\n", result.stderr)
        return False

    safe_name = f"{company_name}_{filing_date}_-_10-K.txt".replace(" ", "_")
    filing_path = Path("texts") / "10-k" / safe_name
    if filing_path.exists():
        LOCAL_TXT_PATH.write_text(filing_path.read_text(), encoding="utf-8")
        filing_path.unlink()
        return True
    return False

def upload_cleaned_to_s3(company_name, filing_date):
    s3_key = f"{company_name}/texts/10-k/{filing_date}.txt"
    s3.upload_file(str(LOCAL_TXT_PATH), BUCKET, s3_key)
    print(f"✅ Uploaded to s3://{BUCKET}/{s3_key}")

def already_uploaded(company_name, filing_date):
    key = f"{company_name}/texts/10-k/{filing_date}.txt"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

def process_all():
    keys = list_all_10k_json_keys()
    for key in keys:
        print(f"\n📄 Processing {key}...")
        json_path = download_json_from_s3(key)
        data = json.loads(json_path.read_text())

        url     = data.get("linkToFilingDetails")
        date    = data.get("filingDate")
        company = sanitize_name(data.get("companyName", "Unknown"))

        if not url or not date:
            print("⚠️ Missing link or date in JSON")
            continue

        if already_uploaded(company, date):
            print("✔️ Already uploaded, skipping.")
            continue

        if fetch_and_save_text(url, company, date):
            upload_cleaned_to_s3(company, date)
            LOCAL_TXT_PATH.unlink(missing_ok=True)
            time.sleep(0.5)
        else:
            print("⚠️ Failed to fetch/parse.")

if __name__ == "__main__":
    process_all()
