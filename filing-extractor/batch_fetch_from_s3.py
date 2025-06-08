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

def list_all_json_keys():
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") and "/filing-jsons/" in key:
                keys.append(key)
    return keys

def sanitize_name(name):
    return name.strip().replace("/", "-").replace(" ", "_")

def download_json_from_s3(key):
    parts = key.split("/")
    if len(parts) < 4:
        print(f"⚠️ Unexpected key structure: {key}")
        return None, None, None

    company_name = sanitize_name(parts[0])
    form_type = sanitize_name(parts[2])
    local_dir = BASE_JSON_DIR / company_name / form_type
    local_dir.mkdir(parents=True, exist_ok=True)

    local_path = local_dir / Path(key).name
    s3.download_file(BUCKET, key, str(local_path))
    return local_path, company_name, form_type

def fetch_and_save_text(index_url, company_name, filing_date, form_type):
    result = subprocess.run(
        ["python", "fetch_filing_text.py", index_url, company_name, filing_date, form_type],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("❌ Script error:\n", result.stderr)
        return False

    safe_name = f"{company_name}__{filing_date}__{form_type}.txt"
    filing_path = Path("texts") / company_name / form_type / safe_name
    if filing_path.exists():
        content = filing_path.read_text(encoding="utf-8", errors="replace")
        LOCAL_TXT_PATH.write_text(content, encoding="utf-8")
        filing_path.unlink()
        return True
    return False

def upload_cleaned_to_s3(company_name, filing_date, form_type):
    s3_key = f"{company_name}/texts/{form_type}/{filing_date}.txt"
    s3.upload_file(str(LOCAL_TXT_PATH), BUCKET, s3_key)
    print(f"✅ Uploaded to s3://{BUCKET}/{s3_key}")

def already_uploaded(company_name, filing_date, form_type):
    key = f"{company_name}/texts/{form_type}/{filing_date}.txt"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

def process_all():
    keys = list_all_json_keys()
    for key in keys:
        print(f"\n📄 Processing {key}...")
        json_path, company, form_type = download_json_from_s3(key)
        if not json_path:
            continue

        data = json.loads(json_path.read_text())
        url       = data.get("linkToFilingDetails")
        date      = data.get("filingDate")

        if not url or not date or not company or not form_type:
            print("⚠️ Missing required fields in JSON.")
            continue

        if already_uploaded(company, date, form_type):
            print("✔️ Already uploaded, skipping.")
            continue

        if fetch_and_save_text(url, company, date, form_type):
            upload_cleaned_to_s3(company, date, form_type)
            LOCAL_TXT_PATH.unlink(missing_ok=True)
            time.sleep(0.5)
        else:
            print("⚠️ Failed to fetch/parse.")

if __name__ == "__main__":
    process_all()
