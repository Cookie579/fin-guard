#!/usr/bin/env python3
"""
Unified SEC + ESG downloader/processor — **zero hard‑coding edition**
===================================================================

This script pulls SEC filing JSONs **and** pre‑extracted ESG text files from S3,
keeps them locally in a consistent folder structure, and re‑uploads cleaned
SEC texts when needed.

Key features
------------
1. **Form‑type consistency** – one sanitised `form_type_safe` everywhere
2. **Automatic ticker → company mapping** (learned during the SEC pass)
3. **Robust fallbacks** for ESG (prefix match + filename token match)
4. **No external lookup files** required; idempotent & rerunnable
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import List, Optional, Tuple

import boto3

# -----------------------------------------------------------------------------
# AWS S3 settings
# -----------------------------------------------------------------------------
SEC_BUCKET = "fin-processed-filings"
ESG_BUCKET = "fin-processed-esg-reports"
ESG_PREFIX = "texts/"  # within ESG_BUCKET

# -----------------------------------------------------------------------------
# Local paths
# -----------------------------------------------------------------------------
BASE_JSON_DIR = Path("filing-jsons")     # SEC JSON cache
LOCAL_TXT_PATH = Path("temp.txt")        # temp scratch file
LOCAL_TEXTS_DIR = Path("texts")          # final plain‑text store

# -----------------------------------------------------------------------------
# Helper utils
# -----------------------------------------------------------------------------

def sanitize(name: str) -> str:
    """Return a filesystem‑safe version of *name*."""
    return name.strip().replace("/", "-").replace(" ", "_")

s3 = boto3.client("s3")

# -----------------------------------------------------------------------------
# In‑memory ticker → company map (populated on the fly)
# -----------------------------------------------------------------------------
TICKER_MAP: dict[str, str] = {}


def learn_mapping(ticker: str, company: str) -> None:
    ticker = ticker.upper().strip()
    company = sanitize(company)
    if ticker and ticker not in TICKER_MAP:
        TICKER_MAP[ticker] = company
        print(f"🧠  Learned mapping: {ticker} → {company}")


def ticker_to_company(ticker: str) -> Optional[str]:
    return TICKER_MAP.get(ticker.upper().strip())

# -----------------------------------------------------------------------------
# 1️⃣ SEC pipeline
# -----------------------------------------------------------------------------

def list_sec_json_keys() -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=SEC_BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") and "/filing-jsons/" in key:
                keys.append(key)
    return keys


def download_sec_json(key: str) -> Tuple[Optional[Path], Optional[str], Optional[str]]:
    parts = key.split("/")  # {CompanyFolder}/filing-jsons/{Form}/file.json
    if len(parts) < 4:
        print(f"⚠️  Bad SEC key: {key}")
        return None, None, None

    folder_company, form_raw = parts[0], parts[2]
    form_safe = sanitize(form_raw)  # e.g. DEF_14A

    dest_dir = BASE_JSON_DIR / sanitize(folder_company) / form_safe
    dest_dir.mkdir(parents=True, exist_ok=True)

    local_path = dest_dir / Path(key).name
    s3.download_file(SEC_BUCKET, key, str(local_path))
    return local_path, folder_company, form_safe


def fetch_and_clean_filing(url: str, company: str, date: str, form_safe: str) -> bool:
    """Call fetch_filing_text.py; if it succeeds, stage the cleaned text."""
    proc = subprocess.run(
        ["python", "fetch_filing_text.py", url, company, date, form_safe],
        capture_output=True, text=True, encoding="utf-8", errors="ignore"
    )
    if proc.returncode != 0:
        print("❌  scraper error:", proc.stderr.splitlines()[:2])
        return False

    fname = f"{company}__{date}__{form_safe}.txt"
    scraped = LOCAL_TEXTS_DIR / company / form_safe / fname
    if scraped.exists():
        LOCAL_TXT_PATH.write_text(scraped.read_text("utf-8", "replace"), "utf-8")
        scraped.unlink()
        return True
    return False


def sec_uploaded(company: str, date: str, form_safe: str) -> bool:
    key = f"{company}/texts/{form_safe}/{date}.txt"
    try:
        s3.head_object(Bucket=SEC_BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def upload_clean_filing(company: str, date: str, form_safe: str) -> None:
    key = f"{company}/texts/{form_safe}/{date}.txt"
    s3.upload_file(str(LOCAL_TXT_PATH), SEC_BUCKET, key)
    print(f"✅  Uploaded: s3://{SEC_BUCKET}/{key}")


def process_sec_filings() -> None:
    for key in list_sec_json_keys():
        print(f"\n📄  {key}")
        local_json, folder_company, form_safe = download_sec_json(key)
        if not local_json:
            continue

        data = json.loads(local_json.read_text("utf-8", "ignore"))
        url, date = data.get("linkToFilingDetails"), data.get("filingDate")
        if not url or not date:
            print("⚠️  Missing link/date → skip")
            continue

        comp = sanitize(data.get("companyName") or data.get("issuerName") or folder_company)

        # learn ticker(s)
        for fld in ("ticker", "tickers"):
            val = data.get(fld)
            if val:
                items = val if isinstance(val, list) else [val]
                for t in items:
                    learn_mapping(t, comp)
        if not any(data.get(fld) for fld in ("ticker", "tickers")):
            learn_mapping(folder_company, comp)

        if sec_uploaded(comp, date, form_safe):
            print("✔️  Already uploaded; skip")
            continue

        if fetch_and_clean_filing(url, comp, date, form_safe):
            upload_clean_filing(comp, date, form_safe)
            LOCAL_TXT_PATH.unlink(missing_ok=True)
            time.sleep(0.3)
        else:
            print("⚠️  SEC scrape failed")

# -----------------------------------------------------------------------------
# 2️⃣ ESG pipeline
# -----------------------------------------------------------------------------

def list_esg_keys() -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for p in paginator.paginate(Bucket=ESG_BUCKET, Prefix=ESG_PREFIX):
        for obj in p.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".txt"):
                keys.append(k)
    return keys


def fallback_from_filename(fname: str) -> Optional[str]:
    tokens = [tok for tok in re.split(r"[_\-]", fname) if len(tok) > 3]
    for tok in tokens:
        for d in BASE_JSON_DIR.iterdir():
            if tok.lower() in d.name.lower():
                return d.name
    return None


def download_esg_text(key: str) -> None:
    parts = key.split("/")  # texts/{TICKER}/…
    if len(parts) < 3:
        return

    ticker = parts[1]
    company = ticker_to_company(ticker)

    # 1️⃣ prefix‑match SEC folders
    if not company:
        for d in BASE_JSON_DIR.iterdir():
            if d.name.upper().startswith(ticker.upper()):
                company = d.name
                print(f"⚡ prefix {ticker} → {company}")
                break

    # 2️⃣ token match in filename
    if not company:
        company = fallback_from_filename(Path(key).name)
        if company:
            print(f"🔍 token match → {company}")

    if not company:
        company = sanitize(ticker)
        print(f"❓ no map for {ticker}; using ticker folder")
    else:
        learn_mapping(ticker, company)  # remember for future files

    dest_dir = LOCAL_TEXTS_DIR / sanitize(company) / "ESG"
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / Path(parts[-1]).name
    if dest.exists():
        return

    s3.download_file(ESG_BUCKET, key, str(dest))
    print(f"✅ ESG → {dest}")


def process_esg_texts() -> None:
    for key in list_esg_keys():
        download_esg_text(key)

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

def main() -> None:
    print("🚀  SEC pass …")
    process_sec_filings()

    print("\n🚀  ESG pass …")
    process_esg_texts()

    print("\n🎉  Done.")


if __name__ == "__main__":
    main()
